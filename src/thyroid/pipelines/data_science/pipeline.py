"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.19.5
"""

from kedro.pipeline import Pipeline, pipeline, node
from .nodes import train_test_data, train_model, test_model


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=train_test_data,
            inputs=["model_input_table", "params:target_column"],
            outputs=["X_train", "X_test", "Y_train", "Y_test"]
        ),
        node(
            func=train_model,
            inputs=["X_train", "Y_train"],
            outputs="classifier"
        ),
        node(
            func=test_model,
            inputs=["X_test", "Y_test", "classifier"],
            outputs=None
        )
    ])
