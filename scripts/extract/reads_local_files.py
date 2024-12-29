import pandas as pd

order_items = pd.read_csv('Business_Intelligence_project/data/raw/order_items.csv')
payments = pd.read_csv('Business_Intelligence_project/data/raw/payments.csv')
orders = pd.read_csv('Business_Intelligence_project/data/raw/orders.csv')
sellers = pd.read_json('Business_Intelligence_project/data/raw/sellers.json')
products = pd.read_csv('Business_Intelligence_project/data/raw/products.csv')
customers = pd.read_json('Business_Intelligence_project/data/raw/customers.json')