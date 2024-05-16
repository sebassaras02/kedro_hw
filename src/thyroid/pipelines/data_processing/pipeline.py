"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.5
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import process_nulls, anova_test_filtering, encode_categoricals, scale_columns


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
                    node(
                        func=process_nulls,
                        inputs="cancer",
                        outputs="data_with_no_nulls",
                        name="process_nulls_node"
                    ),
                    node(
                        func=anova_test_filtering,
                        inputs=["data_with_no_nulls", "params:target_column"],
                        outputs="data_with_selected_features",
                        name="anova_test_filtering_node"
                    ),
                    node(
                        func=encode_categoricals,
                        inputs=["data_with_selected_features", "params:target_column"],
                        outputs="data_with_encoded_categoricals",
                        name="encode_categoricals_node"
                    ),
                    node(
                        func=scale_columns,
                        inputs=["data_with_encoded_categoricals", "params:target_column"],
                        outputs="model_input_table",
                        name="scale_columns_node"
                    )                
    ])
