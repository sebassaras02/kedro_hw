"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.19.5
"""
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from typing import Tuple
import pandas as pd
import numpy as np

def train_test_data(df: pd.DataFrame, target_colum: str)-> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function splits the data into training and test sets.

    Args:
        df: pd.DataFrame - the input DataFrame
        target_column: str - the target column

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] - a tuple of 4 DataFrames
    """
    X = df.drop(columns=[target_colum], axis=1)
    y = df[target_colum]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    return X_train, X_test, y_train, y_test

def train_model(X_train: pd.DataFrame, Y_train: pd.DataFrame)-> RandomForestClassifier:
    """
    This function trains the model.

    Args:
        X_train: pd.DataFrame - the input DataFrame
        Y_train: pd.DataFrame - the target column
    
    Returns:
        RandomForestClassifier - a trained model
    """
    clf = RandomForestClassifier()
    clf.fit(X_train, Y_train)
    return clf

def test_model(X_test: pd.DataFrame, y_test: pd.DataFrame, model: RandomForestClassifier)-> float:
    """
    This function tests the model.

    Args:
        X_test: pd.DataFrame - the input DataFrame
        y_test: pd.DataFrame - the target column
        model: RandomForestClassifier - the trained model
    
    Returns:
        float - the accuracy of the model
    """
    Ypred = model.predict(X_test)
    print(f"Accuracy: {accuracy_score(y_test, Ypred)}")

