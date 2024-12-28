import pandas as pd
from transform import clean_data
from transform.clean_data import clean_data
from extract import reads_local_files
from extract.reads_local_files import sales, marketing, inv_city  
from transform.join_datasets import join_datasets

def main():
    """Main workflow for cleaning and displaying data."""
    # Step 1: Clean sales data
    print("\n--- Cleaning Sales Data ---")
    cleaned_sales = clean_data(
        sales,
        fill_value=0,
        columns_to_drop=None,  # Example: Replace with columns to drop if needed
        outlier_columns=["quantity", "price"] if "quantity" in sales.columns and "price" in sales.columns else None,
        dtype_conversions={"quantity": "int", "price": "float"} if "quantity" in sales.columns else None,
        categorical_columns=["category"] if "category" in sales.columns else None
    )
    print(cleaned_sales.head())

    # Step 2: Clean marketing data
    print("\n--- Cleaning Marketing Data ---")
    cleaned_marketing = clean_data(
        marketing,
        fill_value={"budget": marketing["budget"].median()} if "budget" in marketing.columns else 0,
        columns_to_drop=["ad_id"] if "ad_id" in marketing.columns else None,
        outlier_columns=["budget"] if "budget" in marketing.columns else None,
        dtype_conversions={"budget": "float"} if "budget" in marketing.columns else None,
        categorical_columns=["region"] if "region" in marketing.columns else None
    )
    print(cleaned_marketing.head())

    # Step 3: Clean inventory by city data
    print("\n--- Cleaning Inventory by City Data ---")
    cleaned_inv_city = clean_data(
        inv_city,
        fill_value=0,
        columns_to_drop=["warehouse_id"] if "warehouse_id" in inv_city.columns else None,
        outlier_columns=["stock"] if "stock" in inv_city.columns else None,
        dtype_conversions={"stock": "int"} if "stock" in inv_city.columns else None,
        categorical_columns=["city"] if "city" in inv_city.columns else None
    )
    print(cleaned_inv_city.head())

    
    # Join datasets: First join sales and inventory by city
    print("\n--- Joining Sales and Inventory by City ---")
    merged_sales_inv = join_datasets(cleaned_sales, cleaned_inv_city, on_column="product_id", how="inner")
    print(merged_sales_inv.head())

    # Join the resulting DataFrame with marketing data
    print("\n--- Joining with Marketing Data ---")
    final_merged_data = join_datasets(merged_sales_inv, cleaned_marketing, on_column="product_id", how="inner")
    print(final_merged_data.head())

# Run the workflow
if __name__ == "__main__":
    main()
