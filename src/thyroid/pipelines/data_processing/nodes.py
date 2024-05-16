"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.5
"""
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder


def time_feature_eng(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function extracts time features from the Timestamp column.

    Args:
        df: pd.DataFrame - the input DataFrame
    
    Returns:
        pd.DataFrame - the DataFrame with additional time features
    """
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Hour'] = df['Timestamp'].dt.hour
    df['Minute'] = df['Timestamp'].dt.minute
    df['Seconds'] = df['Timestamp'].dt.second
    df['Month'] = df['Timestamp'].dt.month
    df['Day'] = df['Timestamp'].dt.day
    df['Weekday'] = df['Timestamp'].dt.weekday
    return df

def process_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function fills null values in the DataFrame.

    Args:
        df: pd.DataFrame - the input DataFrame
    
    Returns:
        pd.DataFrame - the DataFrame with filled null values
    """
    if df.isnull().sum().sum() > 0:
        df = df.fillna(0)
        return df
    else:
        return df
    
def anova_test_filtering(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """
    This function performs ANOVA test to select the most important features.

    Args:
        df: pd.DataFrame - the input DataFrame
        target_col: str - the target column

    Returns:
        pd.DataFrame - the DataFrame with selected features
    """
    #useful columns
    useful_columns = []

    #obtener las columnas numericas
    numeric_columns = list(df.select_dtypes(include=['float', 'int']).columns)

    for col in numeric_columns:

        groups = df.groupby(target_col)[col].apply(list)
        f_statistic, p_value = stats.f_oneway(*groups)
        if p_value < 0.05:
            useful_columns.append(col)
    return df[useful_columns]

def encode_categoricals(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """
    This function encodes categorical columns in the DataFrame.

    Args:
        df: pd.DataFrame - the input DataFrame
        target_col: str - the name of the target column
    
    Returns:
        pd.DataFrame - the DataFrame with encoded categorical columns
    """
    categorical_columns = list(df.select_dtypes(include=['object']).columns)

    if target_col in categorical_columns:
        categorical_columns.remove(target_col)
        
    if len(categorical_columns) > 0:
        for col in categorical_columns:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[[col]])
        return df
    else:
        return df
    
def scale_columns(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """
    This function scales all columns in the DataFrame.

    Args:
        df: pd.DataFrame - the input DataFrame
        target_col: str - the target column to avoid scaling
    
    Returns:
        pd.DataFrame - the DataFrame with scaled columns
    """
    numerical_columns = list(df.select_dtypes(include=['float', 'int']).columns)
    
    if target_col in numerical_columns:
        numerical_columns.remove(target_col)

    if len(numerical_columns) > 0:
        for col in numerical_columns:
            scaler = StandardScaler()
            df[col] = scaler.fit_transform(df[[col]])
        return df
    else:
        return df