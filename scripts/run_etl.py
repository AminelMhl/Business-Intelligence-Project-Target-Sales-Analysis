import pandas as pd
from transform.clean_data import clean_data
from extract.reads_local_files import customers, order_items, payments, orders, sellers, products
from transform.join_datasets import join_datasets
from load.save_to_csv import save_to_csv
from load.load_to_dwh import load_to_dwh

def main():
    """Main workflow for cleaning and displaying data."""
    
    # Step 1: Clean customers data
    print("\n--- Cleaning Customers Data ---")
    cleaned_customers = clean_data(
        customers,
        fill_value=0,
        columns_to_drop=["customer_zip_code_prefix", "customer_state"],
        outlier_columns=None,
        dtype_conversions=None
    )
    print(cleaned_customers.head())
    
    # Step 2: Clean order items data
    print("\n--- Cleaning Order Items Data ---")
    cleaned_order_items = clean_data(
        order_items,
        fill_value=0,
        columns_to_drop=["shipping_limit_date", "freight_value"],
        outlier_columns=["price"] if "price" in order_items.columns else None,
        dtype_conversions={"order_item_id": "int", "price": "float"}
    )
    print(cleaned_order_items.head())
    
    # Step 3: Clean payments data
    print("\n--- Cleaning Payments Data ---")
    cleaned_payments = clean_data(
        payments,
        fill_value=0,
        columns_to_drop=["payment_sequential", "payment_installments"],
        outlier_columns=["payment_value"] if "payment_value" in payments.columns else None,
        dtype_conversions={"payment_value": "float"}
    )
    print(cleaned_payments.head())
    
    # Step 4: Clean orders data
    print("\n--- Cleaning Orders Data ---")
    cleaned_orders = clean_data(
        orders,
        fill_value=0,
        columns_to_drop=[
            "order_status", "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"
        ],
        outlier_columns=None,
        dtype_conversions=None
    )
    print(cleaned_orders.head())
    
    # Step 5: Clean sellers data
    print("\n--- Cleaning Sellers Data ---")
    cleaned_sellers = clean_data(
        sellers,
        fill_value=0,
        columns_to_drop=["seller_zip_code_prefix", "seller_state"],
        outlier_columns=None,
        dtype_conversions=None
    )
    print(cleaned_sellers.head())
    
    # Step 6: Clean products data
    print("\n--- Cleaning Products Data ---")
    cleaned_products = clean_data(
        products,
        fill_value=0,
        columns_to_drop=[
            "product_name_length", "product_description_length", "product_photos_qty",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
        ],
        outlier_columns=None,
        dtype_conversions=None
    )
    print(cleaned_products.head())

    # Step 7: Join datasets
    print("\n--- Joining Datasets ---")
    # Join orders with customers
    orders_with_customers = join_datasets(cleaned_orders, cleaned_customers, on_column="customer_id", how="inner")
    print(orders_with_customers.head())

    # Join orders with order items
    orders_with_items = join_datasets(orders_with_customers, cleaned_order_items, on_column="order_id", how="inner")
    print(orders_with_items.head())

    # Join payments with orders
    orders_with_payments = join_datasets(orders_with_items, cleaned_payments, on_column="order_id", how="inner")
    print(orders_with_payments.head())

    # Join products with order items
    products_with_orders = join_datasets(orders_with_payments, cleaned_products, on_column="product_id", how="inner")
    print(products_with_orders.head())

    # Join sellers with products/orders
    final_merged_data = join_datasets(products_with_orders, cleaned_sellers, on_column="seller_id", how="inner")
    print("\n--- Final Merged Data ---")
    print(final_merged_data.head())


    output_path = "Business_Intelligence_project/data/processed/final_data.csv"
    save_to_csv(final_merged_data, output_path)


    # final_merged_data.to_csv("Business_Intelligence_project/data/processed/final_data.csv", index=False)
    # print("Final cleaned data exported to 'data/processed/final_data.csv'")
    # output_path = "Business_Intelligence_project/data/processed/final_data.xlsx"
    # save_to_excel(final_merged_data, output_path)


# Run the workflow
if __name__ == "__main__":
    main()
