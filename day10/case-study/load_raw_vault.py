
import hashlib
import pandas as pd
from datetime import datetime
import os

def hash_key(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


os.makedirs("raw_vault", exist_ok=True)


customers = pd.read_csv("staging_data/staging_customers.csv")
products = pd.read_csv("staging_data/staging_products.csv")
sales = pd.read_csv("staging_data/staging_sales.csv")

load_date = datetime.now().strftime("%Y-%m-%d")
record_source = "RetailSystem"
batch_id = 1


customers["Hub_Customer_HK"] = customers["customer_id"].astype(str).apply(hash_key)
products["Hub_Product_HK"] = products["product_sku"].astype(str).apply(hash_key)

customers_hub = customers[["Hub_Customer_HK", "customer_id"]].copy()
customers_hub["Load_Date"] = load_date
customers_hub["Record_Source"] = record_source
customers_hub["Batch_ID"] = batch_id

products_hub = products[["Hub_Product_HK", "product_sku"]].copy()
products_hub["Load_Date"] = load_date
products_hub["Record_Source"] = record_source
products_hub["Batch_ID"] = batch_id


sat_customer = customers[["Hub_Customer_HK", "name", "address", "contact"]].copy()
sat_customer["Load_Date"] = load_date
sat_customer["Record_Source"] = record_source
sat_customer["Batch_ID"] = batch_id

sat_product = products[["Hub_Product_HK", "name", "category", "price"]].copy()
sat_product["Load_Date"] = load_date
sat_product["Record_Source"] = record_source
sat_product["Batch_ID"] = batch_id


sales["Hub_Customer_HK"] = sales["customer_id"].astype(str).apply(hash_key)
sales["Hub_Product_HK"] = sales["product_sku"].astype(str).apply(hash_key)
sales["Link_Purchase_HK"] = sales.apply(lambda row: hash_key(row["Hub_Customer_HK"] + row["Hub_Product_HK"] + row["purchase_date"]), axis=1)

link_purchase = sales[["Link_Purchase_HK", "Hub_Customer_HK", "Hub_Product_HK"]].copy()
link_purchase["Load_Date"] = load_date
link_purchase["Record_Source"] = record_source
link_purchase["Batch_ID"] = batch_id

sat_purchase = sales[["Link_Purchase_HK", "purchase_date", "quantity", "sales_amount"]].copy()
sat_purchase["Load_Date"] = load_date
sat_purchase["Record_Source"] = record_source
sat_purchase["Batch_ID"] = batch_id


customers_hub.to_csv("raw_vault/Hub_Customer.csv", index=False)
products_hub.to_csv("raw_vault/Hub_Product.csv", index=False)
link_purchase.to_csv("raw_vault/Link_Purchase.csv", index=False)
sat_customer.to_csv("raw_vault/Sat_Customer_Details.csv", index=False)
sat_product.to_csv("raw_vault/Sat_Product_Details.csv", index=False)
sat_purchase.to_csv("raw_vault/Sat_Purchase_Details.csv", index=False)
