# coding: utf-8
import logging

import dataiku
from dataiku.customrecipe import *

from feature_aggregations import FeatureAggregator, FileManager
from feature_aggregations.recipe_config_to_params import get_aggregation_params, get_cardinality_limiter_params

logging.basicConfig(level=logging.INFO, format='afe plugin %(levelname)s - %(message)s')

# --- Get IOs

input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# --- Get configuration

recipe_config = get_recipe_config()
aggregation_params = get_aggregation_params(recipe_config)
cardinality_limiter_params = get_cardinality_limiter_params(recipe_config)
file_manager = FileManager(output_folder)

# --- Run

feature_aggregator = FeatureAggregator(
        aggregation_params=aggregation_params,
        cardinality_limiter_params=cardinality_limiter_params
    )
feature_aggregator.fit(input_dataset)

# --- Write output

file_manager.write_feature_aggregator_config(feature_aggregator)