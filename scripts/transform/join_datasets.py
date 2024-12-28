# transform/join_datasets.py

import pandas as pd

def join_datasets(left_df, right_df, on_column, how="inner"):
    """
    Joins two DataFrames on a specified column.

    :param left_df: Left DataFrame
    :param right_df: Right DataFrame
    :param on_column: Column name to join on
    :param how: Type of join - 'inner', 'left', 'right', 'outer'
    :return: Merged DataFrame
    """
    return pd.merge(left_df, right_df, on=on_column, how=how)
