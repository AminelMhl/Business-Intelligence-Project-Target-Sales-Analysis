import pandas as pd
from sqlalchemy import create_engine

def load_to_dwh(df, dwh_connection_string, table_name):
    """
    Loads a DataFrame into a Data Warehouse (PostgreSQL example).

    Args:
        df (DataFrame): The DataFrame to load.
        dwh_connection_string (str): SQLAlchemy connection string for the DWH.
        table_name (str): The target table name in the DWH.
    """
    # Create connection to DWH
    engine = create_engine(dwh_connection_string)

    # Load data into the DWH
    with engine.connect() as connection:
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)
        print(f"Data loaded successfully into table: {table_name}")
