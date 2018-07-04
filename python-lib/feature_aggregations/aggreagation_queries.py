# coding: utf-8
import logging
from dataiku.sql import Expression, Column, Constant, Interval, SelectQuery, Window, TimeUnit, ColumnType

logger = logging.getLogger('afe')


def frequency_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]

    for population_idx, population in enumerate(aggregation_params.populations):
        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        layer_1.select_from(dataset)
        layer_1.select(keys_expr)
        window = Window(
            partition_by=keys_expr_no_timestamp,
            order_by=[Column(aggregation_params.timestamp_column)],
            end=transform_params.buffer_width
        )
        frequency_expr = Column('*').count().over(window)
        frequency_alias = 'frequency{}'.format(population_suffix)
        layer_1.select(frequency_expr, frequency_alias)
        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population
        )
        layer_1.alias('subquery__frequency_query_with_window'+str(population_idx))
        queries.append(layer_1)
    return queries


def frequency_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]

    for population_idx, population in enumerate(aggregation_params.populations):
        for (window_width, window_unit) in aggregation_params.windows:
            population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

            layer_1 = SelectQuery()
            layer_1.select_from(dataset)
            layer_1.select(keys_expr)
            prefilter = _prefilter(
                query=layer_1,
                aggregation_params=aggregation_params,
                transform_params=transform_params,
                population=population,
                window_width=window_width,
                window_unit=window_unit
            )

            layer_2 = SelectQuery()
            layer_2.select_from(layer_1, alias='layer_1')
            for k in keys_expr:
                layer_2.select(k)
                layer_2.group_by(k)
            frequency_expr = Column('*').count()
            frequency_alias = 'frequency_{}_{}{}'.format(window_width, window_unit, population_suffix)
            layer_2.select(frequency_expr, frequency_alias)

            layer_3 = SelectQuery()
            layer_3.select_from(layer_2, alias='layer_2')
            layer_3.select(keys_expr)
            layer_3.select(Column(frequency_alias))
            mean_frequency_expr = Column(frequency_alias).cast(ColumnType.FLOAT).div(int(n))
            mean_frequency_alias = 'mean_frequency_{}_{}{}'.format(window_width, window_unit, population_suffix)
            layer_3.select(mean_frequency_expr, mean_frequency_alias)

            queries.append(layer_3)
    return queries


def recency_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        layer_1.select_from(dataset)
        layer_1.select(timestamp_expr)
        layer_1.select(keys_expr_no_timestamp)
        window = Window(
            partition_by=keys_expr_no_timestamp,
            order_by=[timestamp_expr],
            end=transform_params.buffer_width
        )
        time_interval_expr = timestamp_expr.lag_diff(1, window).extract(TimeUnit.DAY)
        time_interval_alias = 'time_interval{}'.format(population_suffix)
        layer_1.select(time_interval_expr, time_interval_alias)
        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population
        )

        layer_2 = SelectQuery()
        layer_2.select_from(layer_1, alias='layer_1')
        layer_2.select(keys_expr)
        layer_2.select(Column(time_interval_alias))

        count_expr = Column('*').count().over(window)
        count_alias = 'count{}'.format(population_suffix)
        layer_2.select(count_expr, count_alias)

        delta_interval_expr = Column(time_interval_alias).lag_diff(1, window)
        delta_interval_alias = 'delta_interval{}'.format(population_suffix)
        layer_2.select(delta_interval_expr, delta_interval_alias)

        current_day_expr = timestamp_expr.cast(ColumnType.DATE)
        latest_timestamp = timestamp_expr.max().over(window)
        oldest_timestamp = timestamp_expr.min().over(window)
        recency_expr = current_day_expr.minus(latest_timestamp).extract(TimeUnit.DAY)
        recency_alias = 'recency{}'.format(population_suffix)

        layer_2.select(recency_expr, recency_alias)
        since_first_expr = current_day_expr.minus(oldest_timestamp).extract(TimeUnit.DAY)
        since_first_alias = 'num_day_since_first{}'.format(population_suffix)
        layer_2.select(since_first_expr, since_first_alias)

        # mean_interval_expr = time_interval_expr.avg().over(window)
        # mean_interval_alias = 'mean_interval_in_days{}'.format(population_suffix)
        # layer_2.select(mean_interval_expr, mean_interval_alias)

        # std_interval_expr = time_interval_expr.std_dev_samp().over(window)
        # std_interval_alias = 'std_interval_in_days{}'.format(population_suffix)
        # layer_2.select(std_interval_expr, std_interval_alias)

        layer_2_columns = [Column(count_alias), Column(since_first_alias), Column(recency_alias), Column(delta_interval_alias), Column(time_interval_alias)] #std_interval_alias, mean_interval_alias,

        layer_3 = SelectQuery()
        layer_3.select_from(layer_2, alias='layer_2')
        layer_3.select(keys_expr)
        layer_3.select(layer_2_columns)
        # TODO I don't understand the following loop
        for (window_width, window_unit) in aggregation_params.get_row_based_windows(): # aggregation_params.windows.get('row', ['unbounded']): TODO if empty rows windows
            mean_delta_interval_alias = 'mean_delta_interval_last_{}_rows{}'.format(window_width, population_suffix)
            std_delta_interval_alias = 'std_delta_interval_last_{}_rows{}'.format(window_width, population_suffix)
            window = Window(
                partition_by=keys_expr_no_timestamp,
                order_by=[timestamp_expr],
                start=window_width if isinstance(window_width, str) else window_width+transform_params.buffer_width,
                end=transform_params.buffer_width
            )
            mean_delta_interval = Column(delta_interval_alias).avg().over(window)
            std_delta_interval = Column(delta_interval_alias).std_dev_samp().over(window)
            layer_3.select(mean_delta_interval, mean_delta_interval_alias)
            layer_3.select(std_delta_interval, std_delta_interval_alias)

        queries.append(layer_3)
    return queries


def recency_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        layer_1.select_from(dataset)
        layer_1.select(keys_expr)
        window = Window(
            partition_by=keys_expr,
            order_by=[timestamp_expr]
        )
        time_interval_expr = timestamp_expr.lag_diff(1, window).extract(TimeUnit.DAY)
        time_interval_alias = 'time_interval{}'.format(population_suffix)
        layer_1.select(time_interval_expr, time_interval_alias)
        layer_1.select(timestamp_expr)
        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population
        )
        layer_1_columns = [timestamp_expr, Column(time_interval_alias)]

        layer_2 = SelectQuery()
        layer_2.select_from(layer_1, alias='layer_1')
        layer_2.select(keys_expr)
        layer_2.select(layer_1_columns)
        delta_interval_expr = Column(time_interval_alias).lag_diff(1, window)
        delta_interval_alias = 'delta_interval{}'.format(population_suffix)
        layer_2.select(delta_interval_expr, delta_interval_alias)

        layer_3 = SelectQuery()
        layer_3.select_from(layer_2, alias='layer_2')
        for k in keys_expr:
            layer_3.select(k)
            layer_3.group_by(k)
        count_expr = Column('*').count()
        count_alias = 'count{}'.format(population_suffix)
        layer_3.select(count_expr, count_alias)

        current_day_expr = Constant(transform_params.ref_date).cast(ColumnType.DATE)
        if transform_params.buffer_unit is not None:
            current_day_expr.minus(Interval(transform_params.buffer_width, transform_params.buffer_unit))
        recency_expr = current_day_expr.minus(timestamp_expr.max()).extract(TimeUnit.DAY)
        recency_alias = 'recency{}'.format(population_suffix)
        layer_3.select(recency_expr, recency_alias)
        since_first_expr = current_day_expr.minus(timestamp_expr.min()).extract(TimeUnit.DAY)
        since_first_alias = 'num_day_since_first{}'.format(population_suffix)
        layer_3.select(since_first_expr, since_first_alias)

        # mean_interval_expr = Column(time_interval_alias).avg()
        # mean_interval_alias = 'mean_interval_in_days{}'.format(population_suffix)
        # layer_3.select(mean_interval_expr, mean_interval_alias)

        # std_interval_expr = Column(time_interval_alias).std_dev_samp()
        # std_interval_alias = 'std_interval_in_days{}'.format(population_suffix)
        # layer_3.select(std_interval_expr, std_interval_alias)

        # mean_delta_interval_expr = Column(delta_interval_alias).avg()
        # mean_delta_interval_alias = 'mean_delta_interval{}'.format(population_suffix)
        # layer_3.select(mean_delta_interval_expr, mean_delta_interval_alias)

        # std_delta_interval_expr = Column(delta_interval_alias).std_dev_samp()
        # std_delta_interval_alias = 'std_delta_interval{}'.format(population_suffix)
        # layer_3.select(std_delta_interval_expr, std_delta_interval_alias)

        queries.append(layer_3)
    return queries


def information_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        for window_index in xrange(aggregation_params.num_windows_per_type):
            population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)
            window_suffix = _get_window_suffix(window_index, aggregation_params.num_windows_per_type)

            layer_1 = SelectQuery()
            layer_1.select_from(dataset)
            layer_1.select(keys_expr)
            layer_1.select([Column(c) for c in aggregation_params.numerical_columns])
            layer_1.select([Column(c) for c in aggregation_params.categorical_columns])
            for column in aggregation_params.categorical_columns:
                window1 = Window(
                    partition_by=keys_expr_no_timestamp + [Column(column)],
                    order_by=[timestamp_expr],
                    end=transform_params.buffer_width
                )
                rank_expr = Expression().rank().over(window1)
                rank_alias = 'rank_{}'.format(column[1:-1])
                layer_1.select(rank_expr, rank_alias)

                window2 = Window(
                    partition_by=keys_expr + [Column(column)],
                    end=transform_params.buffer_width
                )
                first_seen_expr = timestamp_expr.eq(timestamp_expr.min().over(window2))
                first_seen_alias = 'first_seen_{}'.format(column[1:-1])
                layer_1.select(first_seen_expr, first_seen_alias)
            prefilter = _prefilter(
                query=layer_1,
                aggregation_params=aggregation_params,
                transform_params=transform_params,
                population=population,
                window_index=window_index
            )

            layer_2_columns = []
            layer_2 = SelectQuery()
            layer_2.select_from(layer_1, alias='layer_1')
            layer_2.select(keys_expr)
            for column in aggregation_params.categorical_columns:
                rank_col = 'rank_{}'.format(column[1:-1])
                window = Window(
                    partition_by=keys_expr_no_timestamp + [Column(column)],
                    order_by=[timestamp_expr],
                    end=transform_params.buffer_width
                )
                first_seen_condtion_1_expr = Column(rank_col).eq(Column(rank_col).lag(1).over(window)).coalesce('false').eq(Column('false')) #TODO what?
                first_seen_condition_2_expr = Column(first_seen_alias).eq(Constant('true'))
                first_seen_expr = first_seen_condition_1_expr.and_(first_seen_condtion_2_expr)
                first_seen_alias = 'first_seen_{}'.format(column[1:-1])
                layer_2.select(first_seen_expr, first_seen_alias)
            for (window_width, window_unit) in aggregation_params.get_row_based_windows(): # aggregation_params.windows.get('row', ['unbounded']): TODO if empty rows windows
                for column in aggregation_params.numerical_columns:
                    if window_width is None:
                        sum_alias = 'sum_{}{}{}'.format(column[1:-1], window_suffix, population_suffix)
                        avg_alias = 'avg_{}{}{}'.format(column[1:-1], window_suffix, population_suffix)
                        std_dev_alias = 'std_{}{}{}'.format(column[1:-1], window_suffix, population_suffix)
                    else:
                        sum_alias = 'sum_{}_last_{}_times{}{}'.format(column[1:-1], window_width, window_suffix, population_suffix)
                        avg_alias = 'avg_{}_last_{}_times{}{}'.format(column[1:-1], window_width, window_suffix, population_suffix)
                        std_dev_alias = 'std_{}_last_{}_times{}{}'.format(column[1:-1], window_width, window_suffix, population_suffix)
                    layer_2_columns.extend([Column(sum_alias), Column(avg_alias), Column(std_dev_alias)])
                    window = Window(
                        partition_by=keys_expr_no_timestamp,
                        order_by=[timestamp_expr],
                        start=window_width if isinstance(window_width, str) else window_width+transform_params.buffer_width,
                        end=transform_params.buffer_width
                    )
                    sum_expr = Column(column).sum().over(window)
                    avg_expr = Column(column).avg().over(window)
                    std_operation = Column(column).std_dev_samp().over(window)
                    layer_2.select(sum_expr, sum_alias)
                    layer_2.select(avg_expr, avg_alias)
                    layer_2.select(std_operation, std_dev_alias)

            layer_3 = SelectQuery()
            layer_3.select_from(layer_2, alias='layer_2')
            layer_3.select(keys_expr)
            layer_3.select(layer_2_columns)
            window = Window(
                partition_by=keys_expr_no_timestamp,
                order_by=[timestamp_expr],
                end=transform_params.buffer_width
            )
            for column in aggregation_params.categorical_columns:
                first_seen_alias = 'first_seen_{}'.format(column[1:-1])
                num_distinct_expr = Column(first_seen_alias).condition(Constant(1), Constant(0)).sum().over(window) # TODO what?
                num_distinct_alias = 'num_distinct_{}{}{}'.format(column[1:-1], window_suffix, population_suffix)
                layer_3.select(num_distinct_expr, num_distinct_alias)

            queries.append(layer_3)
    return queries


def information_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]

    for population_idx, population in enumerate(aggregation_params.populations):
        for window_index in xrange(aggregation_params.num_windows_per_type):
            for (window_width, window_unit) in aggregation_params.windows:
                population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)
                window_suffix = _get_window_suffix(window_index, aggregation_params.num_windows_per_type)

                layer_1 = SelectQuery()
                layer_1.select_from(dataset)
                layer_1.select(keys_expr)
                layer_1.select([Column(c) for c in aggregation_params.numerical_columns])
                layer_1.select([Column(c) for c in aggregation_params.categorical_columns])
                prefilter = _prefilter(
                    query=layer_1,
                    aggregation_params=aggregation_params,
                    transform_params=transform_params,
                    population=population,
                    window_width=window_width,
                    window_unit=window_unit,
                    window_index=window_index
                )

                layer_2 = SelectQuery()
                layer_2.select_from(layer_1, alias='layer_1')
                for k in keys_expr:
                    layer_2.select(k)
                    layer_2.group_by(k)
                for column in aggregation_params.categorical_columns:
                    num_distinct_expr = Column(column).count_distinct()
                    num_distinct_alias = 'num_distinct_{}_last_{}_{}{}{}'.format(column[1:-1], window_width, window_unit, window_suffix, population_suffix)
                    layer_2.select(num_distinct_expr, num_distinct_alias)
                for column in aggregation_params.numerical_columns:
                    sum_expr = Column(column).sum()
                    sum_alias = 'sum_{}_last_{}_{}{}{}'.format(column[1:-1], window_width, window_unit, window_suffix, population_suffix)
                    layer_2.select(sum_expr, sum_alias)

                    avg_expr = Column(column).avg()
                    avg_alias = 'avg_{}_last_{}_{}{}{}'.format(column[1:-1], window_width, window_unit, window_suffix, population_suffix)
                    layer_2.select(avg_expr, avg_alias)

                    std_dev_expr = Column(column).std_dev_samp()
                    std_dev_alias = 'std_{}_last_{}_{}{}{}'.format(column[1:-1], window_width, window_unit, window_suffix, population_suffix)
                    layer_2.select(std_dev_expr, std_dev_alias)
                queries.append(layer_2)
    return queries


def distribution_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]

    for population_idx, population in enumerate(aggregation_params.populations):
        for column_name in aggregation_params.categorical_columns:
            population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

            category_list = list(categorical_columns_stats[column_name]) # list of list
            all_categories = [x for y in category_list for x in y] # combine all the previous values in the list to create the NOT IN list
            category_list.append(all_categories)
            column_expr = Column(column_name)

            for (window_width, window_unit) in aggregation_params.windows:
                layer_1 = SelectQuery()
                layer_1.select_from(dataset)
                for k in keys_expr:
                    layer_1.select(k)
                    layer_1.group_by(k)
                layer_1.select(column_expr)
                count_expr = Column('*').count()
                count_alias = 'count_instance'
                layer_1.select(count_expr, count_alias)
                layer_1.group_by(column_expr)
                prefilter = _prefilter(
                    query=layer_1,
                    aggregation_params=aggregation_params,
                    transform_params=transform_params,
                    population=population,
                    window_width=window_width,
                    window_unit=window_unit
                )

                layer_2 = SelectQuery()
                layer_2.select_from(layer_1, alias='layer_1')
                layer_2.select(keys_expr)
                layer_2.select(column_expr)
                sum_of_counts = Column(count_alias).sum().over(Window(partition_by=keys_expr))
                percentage_expr = Column(count_alias).div(sum_of_counts).times(100)
                percentage_alias = 'percentage'
                layer_2.select(percentage_expr, percentage_alias)

                layer_3 = SelectQuery()
                layer_3.select_from(layer_2, alias='layer_2')
                for k in keys_expr:
                    layer_3.select(k)
                    layer_3.group_by(k)
                for value_index, distinct_value in enumerate(category_list):
                    in_or_not_in = 'IN'
                    value_name = 'val_{}'.format(distinct_value[0])
                    if value_index == len(category_list) - 1: #last list
                        in_or_not_in = 'NOT IN'
                        value_name = 'other_values'
                    s = ''.join(value_name.split()) #TODO what?
                    perc_value_alias = 'perc_{}_col_{}_last_{}_{}{}'.format(s, column_name[1:-1], window_width, window_unit, population_suffix)
                    if in_or_not_in == 'IN':
                        perc_value = column_expr.in_(distinct_value).cast(ColumnType.INT).times(Column(percentage_alias)).sum()
                    else:
                        perc_value = column_expr.in_(distinct_value).not_().cast(ColumnType.INT).times(Column(percentage_alias)).sum()
                    layer_3.select(perc_value, perc_value_alias)
                queries.append(layer_3)
    return queries


# THIS DOESNT WORK FOR NOW, PERFORMANCE ISSUE
# def distribution_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
# then don't include it in the code to be reviewed :)
# TODO


def monetary_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    monetary_column_expr = Column(aggregation_params.monetary_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        layer_1.select_from(dataset)
        layer_1.select(keys_expr)
        layer_1.select(monetary_column_expr)

        amount_with_cent_expr = monetary_column_expr.floor().ne(monetary_column_expr.ceil())
        amount_with_cent_alias = 'amount_with_cent{}'.format(population_suffix)
        layer_1.select(amount_with_cent_expr, amount_with_cent_alias)

        is_round_number_expr = monetary_column_expr.cast('numeric').mod(10).eq(0)
        is_round_number_alias = 'is_round_number{}'.format(population_suffix)
        layer_1.select(is_round_number_expr, is_round_number_alias)

        order_of_magnitude_expr = monetary_column_expr.log().plus(1).floor()
        order_of_magnitude_alias = 'order_of_magnitude{}'.format(population_suffix)
        layer_1.select(order_of_magnitude_expr, order_of_magnitude_alias)

        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population
        )
        layer_1_columns = [Column(amount_with_cent_alias), Column(is_round_number_alias), Column(order_of_magnitude_alias)]

        layer_2 = SelectQuery()
        layer_2.select_from(layer_1, alias='layer_1')
        layer_2.select(keys_expr)
        layer_2.select(layer_1_columns)
        layer_2.select(monetary_column_expr)
        window = Window(
            partition_by=keys_expr_no_timestamp,
            order_by=[Column(aggregation_params.timestamp_column)],
            end=transform_params.buffer_width
        )
        max_price_expr = monetary_column_expr.min().over(window)
        max_price_alias = 'max_price{}'.format(population_suffix)
        layer_2.select(max_price_expr, max_price_alias)
        min_price_expr = monetary_column_expr.max().over(window)
        min_price_alias = 'min_price{}'.format(population_suffix)
        layer_2.select(min_price_expr, min_price_alias)

        layer_3 = SelectQuery()
        layer_3.select_from(layer_2, alias='layer_2')
        layer_3.select(keys_expr)
        layer_3.select(monetary_column_expr)
        layer_3.select(layer_1_columns) # copy feature from layer 1, this is not a typo

        is_max_price_expr = monetary_column_expr.le(Column(min_price_alias))
        is_max_price_alias = 'is_max_price{}'.format(population_suffix)
        layer_3.select(is_max_price_expr, is_max_price_alias)
        is_min_price_expr = monetary_column_expr.ge(Column(max_price_alias))
        is_min_price_alias = 'is_min_price{}'.format(population_suffix)
        layer_3.select(is_min_price_expr, is_min_price_alias)

        queries.append(layer_3)
    return queries


def cross_reference_count_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_without_timestamp_col = [k for k in aggregation_params.keys]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        layer_1.select(keys_expr)
        layer_1.select_from(dataset)
        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population,
        )
        for column in aggregation_params.cross_reference_columns:
            concat_key_column = " || ', ' || ".join(keys_without_timestamp_col) # TODO no SQL! and I don't understand what it does anyway
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            layer_1.select(Column(concat_key_column), concat_key_by_col_alias)
            layer_1.select(Column(column))

        layer_2 = SelectQuery()
        layer_2.select_from(layer_1, alias='layer_1')
        layer_2.select(keys_expr)
        for column in aggregation_params.cross_reference_columns:
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            layer_2.select(Column(column))
            layer_2.select(Column(concat_key_by_col_alias))
            window = Window(
                partition_by=[Column(column), Column(concat_key_by_col_alias)],
                order_by=[timestamp_expr],
                end=transform_params.buffer_width
            )
            rank_expr = Expression().rank().over(window)
            rank_alias = 'rank_{}'.format(concat_key_by_col_alias)
            layer_2.select(rank_expr, rank_alias)
            first_seen_alias = 'first_seen_{}'.format(concat_key_by_col_alias)
            first_seen_expr = timestamp_expr.eq(timestamp_expr.min().over(window))
            layer_2.select(first_seen_expr, first_seen_alias)

        layer_3 = SelectQuery()
        layer_3.select_from(layer_2, alias='layer_2')
        layer_3.select(keys_expr)
        for column in aggregation_params.cross_reference_columns:
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            rank_alias = 'rank_{}'.format(concat_key_by_col_alias)
            first_seen_alias = 'first_seen_{}'.format(concat_key_by_col_alias)
            layer_3.select(Column(column))
            layer_3.select(Column(rank_alias))
            layer_3.select(Column(first_seen_alias))
            first_seen_condition_1_expr = Column(first_seen_alias).eq(Constant('true'))
            window = Window(
                partition_by=keys_expr_no_timestamp+[Column(column)],
                order_by=[timestamp_expr],
                end=transform_params.buffer_width
            )
            first_seen_condtion_2_expr = Column(rank_alias).eq(Column(rank_alias).lag(1).over(window)).coalesce('false').eq(Column('false')) # TODO what?

            first_seen_expr = first_seen_condition_1_expr.and_(first_seen_condtion_2_expr)
            layer_3.select(first_seen_expr, first_seen_alias)

        layer_4 = SelectQuery()
        layer_4.select_from(layer_3, alias='layer_3')
        layer_4.select(keys_expr)
        for column in aggregation_params.cross_reference_columns:
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            first_seen_alias = 'first_seen_{}'.format(concat_key_by_col_alias)
            layer_4.select(Column(column))
            layer_4.select(Column(first_seen_alias))
            window = Window(
                partition_by=[Column(column)],
                order_by=[timestamp_expr],
                end=transform_params.buffer_width
            )
            num_distinct_expr = Column(first_seen_alias).condition(Constant(1), Constant(0)).sum().over(window) #TODO what?
            num_distinct_alias = 'num_rows_with_same_value_in_{}{}'.format(column, population_suffix)
            layer_4.select(num_distinct_expr, num_distinct_alias)

        queries.append(layer_4)
    return queries


def _get_population_suffix(idx, populations):
    return '_pop_{}'.format(idx) if len(populations) > 1 else ''


def _get_window_suffix(idx, num_windows_per_type):
    return '_window_{}'.format(idx) if num_windows_per_type > 1 else ''


def _prefilter(query, aggregation_params, transform_params, population=None, window_index=0, window_width=0, window_unit=None):
    typestamp_expr = Column(aggregation_params.timestamp_column)
    ref_date_expr = Constant(transform_params.ref_date).cast(ColumnType.DATE)
    time_filter = None
    if aggregation_params.is_rolling_window():
        time_filter = typestamp_expr.le(ref_date_expr) # no buffer days for rolling window (TODO unclear comment)
    elif transform_params.buffer_unit is not None:
        buffer_interval = Interval(transform_params.buffer_width, transform_params.buffer_unit)
        if window_width > 0 and window_unit is not None:
            base_interval = Interval(window_width, window_unit)
            shift = Constant(window_index).times(base_interval)
            upper = ref_date_expr.minus(shift).minus(buffer_interval)
            lower = upper.minus(base_interval)
            time_filter = typestamp_expr.ge(lower).and_(typestamp_expr.le(upper))
        else:
            ref = ref_date_expr.minus(buffer_interval)
            time_filter = typestamp_expr.le(ref)

    if population is not None:
        query.where(population)
    if time_filter is not None:
        query.where(time_filter)
