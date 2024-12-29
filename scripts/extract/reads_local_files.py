import pandas as pd

order_items = pd.read_csv('./data/raw/order_items.csv')
payments = pd.read_csv('./data/raw/payments.csv')
orders = pd.read_csv('./data/raw/orders.csv')
sellers = pd.read_json('./data/raw/sellers.json')
products = pd.read_csv('./data/raw/products.csv')
customers = pd.read_json('./data/raw/customers.json')