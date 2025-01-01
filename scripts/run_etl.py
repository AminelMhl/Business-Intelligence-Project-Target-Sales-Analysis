from transform.clean_data import clean_data
from extract.reads_local_files import customers, order_items, payments, orders, sellers, products
from transform.join_datasets import join_datasets
from load.save_to_csv import save_to_csv
from load.save_to_excel import save_to_excel
from load.load_to_dwh import load_to_dwh

def main():
    """Main workflow for cleaning and displaying data."""
    
    # Step 1: Clean customers data
    print("\n--- Cleaning Customers Data ---")
    cleaned_customers = clean_data(
        customers,
        fill_value=0,
        columns_to_drop=["customer_zip_code_prefix", "customer_unique_id"], 
        outlier_columns=None,
        dtype_conversions=None
    )
    print(cleaned_customers.head())
    output_path = "./Modeling and Storage/Modeling/customer_dimension.csv"
    save_to_csv(cleaned_customers, output_path)
    output_path = "./Modeling and Storage/Modeling/dimensions excel_format/customer_dimension.xlsx"
    save_to_excel(cleaned_customers, output_path)
    
    # Step 2: Clean order items data
    print("\n--- Cleaning Order Items Data ---")
    cleaned_order_items = clean_data(
        order_items,
        fill_value=0,
        columns_to_drop=["shipping_limit_date", "freight_value", "order_item_id"],
        outlier_columns=["price"] if "price" in order_items.columns else None,
        dtype_conversions={"price": "float"}
    )
    print(cleaned_order_items.head())
    print(cleaned_customers.head())
    
    # Step 3: Clean payments data
    print("\n--- Cleaning Payments Data ---")
    unique_payments = payments[['payment_type']].drop_duplicates().reset_index(drop=True)
    unique_payments['payment_type_id'] = range(1, len(unique_payments) + 1)
    unique_payments = unique_payments[['payment_type_id', 'payment_type']]
    print(unique_payments.head())
    output_path = "./Modeling and Storage/Modeling/payment_dimension.csv"
    save_to_csv(unique_payments, output_path)
    output_path = "./Modeling and Storage/Modeling/dimensions excel_format/payment_dimension.xlsx"
    save_to_excel(unique_payments, output_path)
    
    # Step 4: Update orders data to refer to payments primary key
    print("\n--- Updating Orders Data ---")
    orders2 = orders.merge(payments[['order_id', 'payment_type']], on='order_id', how='left')
    orders3 = orders2.merge(unique_payments, on='payment_type', how='left')
    print("\n--- Cleaning Orders Data ---")
    cleaned_orders = clean_data(
        orders3,
        fill_value=0,
        columns_to_drop=[
            "order_status", "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
            "payment_type"
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
        columns_to_drop=["seller_zip_code_prefix"],
        outlier_columns=None,
        dtype_conversions=None
    )
    print(cleaned_sellers.head())
    output_path = "./Modeling and Storage/Modeling/seller_dimension.csv"
    save_to_csv(cleaned_sellers, output_path)
    output_path = "./Modeling and Storage/Modeling/dimensions excel_format/seller_dimension.xlsx"
    save_to_excel(cleaned_sellers, output_path)
    
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
    output_path = "./Modeling and Storage/Modeling/product_dimension.csv"
    save_to_csv(cleaned_products, output_path)
    output_path = "./Modeling and Storage/Modeling/dimensions excel_format/product_dimension.xlsx"
    save_to_excel(cleaned_products, output_path)

    # Step 7: Join datasets
    print("\n--- Joining Datasets ---")
    # Join orders with order items
    orders_with_order_items = join_datasets(cleaned_orders, cleaned_order_items, on_column="order_id", how="inner")
    print(orders_with_order_items.head())
    output_path= "./Modeling and Storage/Modeling/fact_order.csv"
    save_to_csv(orders_with_order_items, output_path)
    output_path = "./Modeling and Storage/Modeling/dimensions excel_format/fact_order.xlsx"
    save_to_excel(orders_with_order_items, output_path)
    
    # Join orders with customers
    orders_with_customers = join_datasets(cleaned_orders, cleaned_customers, on_column="customer_id", how="inner")
    print(orders_with_customers.head())

    # Join orders with order items
    orders_with_items = join_datasets(orders_with_customers, cleaned_order_items, on_column="order_id", how="inner")
    print(orders_with_items.head())

    # Join payments with orders
    orders_with_payments = join_datasets(orders_with_items, unique_payments, on_column="payment_type_id", how="inner")
    print(orders_with_payments.head())

    # Join products with order items
    products_with_orders = join_datasets(orders_with_payments, cleaned_products, on_column="product_id", how="inner")
    print(products_with_orders.head())

    # Join sellers with products/orders
    final_merged_data = join_datasets(products_with_orders, cleaned_sellers, on_column="seller_id", how="inner")
    print("\n--- Final Merged Data ---")
    print(final_merged_data.head())

    output_path = "./data/processed/final_data.csv"
    save_to_csv(final_merged_data, output_path)

    output_path = "./data/processed/final_data.xlsx"
    save_to_excel(final_merged_data, output_path)
    
    dwh_connection_string = "sqlite:///./data/processed/final_data.db"
    
    print(f"DWH Connection String: {dwh_connection_string}")
    load_to_dwh(final_merged_data, dwh_connection_string, table_name="final_data")

if __name__ == "__main__":
    main()