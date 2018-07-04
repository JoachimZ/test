# coding: utf-8
import logging

import dataiku
from dataiku.customrecipe import *

from feature_aggregations import FeatureAggregator, FileManager
from feature_aggregations.recipe_config_to_params import get_transform_params

logging.basicConfig(level=logging.INFO, format='afe plugin %(levelname)s - %(message)s')

# --- Get IOs

input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

input_folder_name = get_input_names_for_role('input_folder')[0]
input_folder = dataiku.Folder(input_folder_name)

output_dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(output_dataset_name)

# --- Get configuration

recipe_config = get_recipe_config()
transform_params = get_transform_params(recipe_config)
file_manager = FileManager(input_folder)
aggregation_params, cardinality_limiter_params, categorical_columns_stats = file_manager.read_feature_aggregator_config(input_folder)

# --- Run

feature_aggregator = FeatureAggregator(
        aggregation_params=aggregation_params,
        cardinality_limiter_params=cardinality_limiter_params,
        categorical_columns_stats=categorical_columns_stats
    )
feature_aggregator.transform(input_dataset, output_dataset, transform_params)