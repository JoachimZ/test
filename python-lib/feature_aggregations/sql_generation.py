# coding: utf-8
from dataiku.sql import Column, Constant, Expression, Interval, SelectQuery, Window, TimeUnit, JoinTypes, ColumnType, toSQL


def make_max_time_interval_query(timestamp_column, resolved_ref_date, dataset):
    max_time_interval = Constant(resolved_ref_date).minus(Column(timestamp_column)).extract(TimeUnit.DAY).max()
    query = SelectQuery()
    query.select(max_time_interval, alias="max_time_interval") #TODO naming
    query.select_from(dataset)

    return toSQL(query, dataset=dataset)


def make_most_recent_timestamp_query(timestamp_column, dataset):
    query = SelectQuery()
    query.select(Column(timestamp_column).max(), alias='most_recent_timestamp')
    query.select_from(dataset)

    return toSQL(query, dataset=dataset)


def make_distinct_values_query(column, dataset):
    column = Column(column)

    layer_1 = SelectQuery()
    layer_1.select(column)
    layer_1.select(Column('*').count(), 'count')
    layer_1.select_from(dataset)
    layer_1.group_by(column)

    count = Column('count')
    layer_2 = SelectQuery()
    layer_2.select(column)
    layer_2.select(count.div(count.sum().over(Window())).times(100), 'distribution')
    layer_2.select_from(layer_1, alias='layer_1')

    return toSQL(layer_2, dataset=dataset)


def make_full_transform_query(aggregation_queries, dataset, aggregation_params, transform_params):
    inner = SelectQuery()
    inner.select_from(dataset)
    if aggregation_params.is_rolling_window():
        inner.select(Column('*'))
    else:
        # inner.distinct() #TODO why?!
        for key in aggregation_params.get_effective_keys():
            inner.select(Column(key))
    prefilter = _make_prefilter(aggregation_params, transform_params.ref_date)
    inner.where(prefilter)

    outer = SelectQuery()
    outer.select_from(inner, alias='inner')
    if aggregation_params.is_rolling_window():
        outer.select(Column('*', 'inner'))
    else:
        for col in aggregation_params.get_effective_keys(): #+ feature_names:
            outer.select(Column(col, 'inner'))
    for idx, agg_query in enumerate(aggregation_queries):
        agg_query.alias(agg_query.get_alias() or 'cte_'+str(idx)) #TODO remove, make sure they have ids
        outer.with_cte(agg_query)
        join_cond = Expression()
        for key in aggregation_params.get_effective_keys():
            join_cond = join_cond.and_(Column(key, 'inner').eq_null_unsafe(Column(key, agg_query.get_alias())))
        outer.join(agg_query.get_alias(), JoinTypes.LEFT, join_cond)
        for col in agg_query.get_columns_alias():
            outer.select(Column(col, agg_query.get_alias()))
    return toSQL(outer, dataset=dataset)


def _make_prefilter(aggregation_params, ref_date):
    timestamp_column_expr = Column(aggregation_params.timestamp_column)
    ref_date_expr = Constant(ref_date).cast(ColumnType.DATE)
    if aggregation_params.is_rolling_window():
        return timestamp_column_expr.le(ref_date_expr)
    else:
        if aggregation_params.windows and not aggregation_params.is_rolling_window():
            widest_window = _get_widest_window(aggregation_params)
            if widest_window is not None:
                return timestamp_column_expr.le(ref_date_expr.minus(Interval(widest_window[0], widest_window[1])))
        return timestamp_column_expr.le(ref_date_expr)


def _get_widest_window(aggregation_params): # or null if there is an unbounded window
    if aggregation_params.params.whole_history_window_enabled:
        return None
    units = ['day', 'week', 'month', 'row']
    widest = None
    for window in aggregation_params.windows:
        if window[0] >= widest[0] and units.index(window[1]) >= units.index(widest[1]): #NB: 60 days < 1 month here
            widest = window
    return widest
