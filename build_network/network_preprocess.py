import pandas as pd
import numpy as np

def convert_columns(df):
    for column in df.columns:
        if df[column].dtype == 'object':  # Assuming 'object' dtype implies strings or mixed types
            if df[column].str.isnumeric().all():
                df[column] = pd.to_numeric(df[column])
            else:
                df[column] = pd.factorize(df[column])[0]  # Convert non-numeric categorical data to integer labels
        elif df[column].dtype in ['int64', 'float64']:
            df[column] = df[column].astype(np.float32)  # Ensure all numeric data is float32 for PyTorch
    return df

def preprocess_features(df):
    # Handle missing values
    df = df.ffill().bfill().fillna(0)

    # Convert categorical data using one-hot encoding or factorization
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column], _ = pd.factorize(df[column])
        elif df[column].dtype in ['int64', 'float64']:
            df[column] = df[column].astype(float)

    return df

def preprocess_data(nodes):
    processed_nodes = {}
    for node_type, df in nodes.items():
        processed_df = preprocess_features(df)
        processed_nodes[node_type] = processed_df
    return processed_nodes

