import os

def save_to_csv(df, output_path):
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save DataFrame to Csv
    df.to_csv(output_path, index=False)
    print(f"Data saved successfully to '{output_path}'")
