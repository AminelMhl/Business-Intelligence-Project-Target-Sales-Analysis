import pandas as pd

def handle_missing_values(df, fill_value=0):
    """Fills missing values in a DataFrame."""
    return df.fillna(fill_value)

def drop_duplicates(df):
    """Drops duplicate rows from a DataFrame."""
    return df.drop_duplicates()

def standardize_column_names(df):
    """Standardizes column names to lowercase with underscores."""
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

def drop_columns(df, columns_to_drop):
    """Drops specified columns from a DataFrame."""
    return df.drop(columns=columns_to_drop, axis=1)

def handle_outliers(df, columns):
    """Cleans outliers in specified columns."""
    for col in columns:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
    return df

def convert_data_types(df, conversions):
    """Converts specified columns to appropriate data types."""
    for col, dtype in conversions.items():
        df[col] = df[col].astype(dtype)
    return df

def encode_categorical(df, columns):
    """Encodes categorical columns."""
    return pd.get_dummies(df, columns=columns, drop_first=True)

def remove_invalid_data(df, column, valid_values):
    """Removes rows where the column value is not in valid_values."""
    return df[df[column].isin(valid_values)]

def reset_and_sort(df, sort_column):
    """Resets the index and sorts the DataFrame by a specific column."""
    return df.sort_values(by=sort_column).reset_index(drop=True)





def clean_data(df, fill_value=0, columns_to_drop=None, outlier_columns=None, dtype_conversions=None, categorical_columns=None, sort_column=None):
    """
    Performs comprehensive data cleaning on a DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame to clean.
        fill_value (int/float/str): The value to fill missing values with.
        columns_to_drop (list): List of columns to drop.
        outlier_columns (list): List of columns to handle outliers for.
        dtype_conversions (dict): Dictionary of column names and their target data types.
        categorical_columns (list): List of categorical columns to encode.
        sort_column (str): Column to sort the DataFrame by.
    
    Returns:
        DataFrame: The cleaned DataFrame.
    """
    # Fill missing values
    df = handle_missing_values(df, fill_value)
    
    # Drop duplicates
    df = drop_duplicates(df)
    
    # Standardize column names
    df = standardize_column_names(df)
    
    # Drop specified columns
    if columns_to_drop:
        df = drop_columns(df, columns_to_drop)
    
    # Handle outliers
    if outlier_columns:
        df = handle_outliers(df, outlier_columns)
    
    # Convert data types
    if dtype_conversions:
        df = convert_data_types(df, dtype_conversions)
    
    # Encode categorical columns
    if categorical_columns:
        df = encode_categorical(df, categorical_columns)
    
    # Reset and sort by column if specified
    if sort_column:
        df = reset_and_sort(df, sort_column)
    
    return df