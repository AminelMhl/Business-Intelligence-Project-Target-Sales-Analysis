import os
import pandas as pd

def save_to_excel(df, output_path, engine="openpyxl"):
    """
    Saves a DataFrame to an Excel file.
    
    :param df: pandas DataFrame to save
    :param output_path: Path where the Excel file will be saved
    :param engine: Engine to use for saving the Excel file (default: openpyxl)
    """
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save DataFrame to Excel
    df.to_excel(output_path, index=False, engine=engine)
    print(f"Data saved successfully to '{output_path}'")