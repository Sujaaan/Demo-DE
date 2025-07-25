
import pandas as pd
import os

os.makedirs("star_schema_views", exist_ok=True)


pit_customer = pd.read_csv("pit_tables/PIT_Customer.csv")
pit_product = pd.read_csv("pit_tables/PIT_Product.csv")
pit_purchase = pd.read_csv("pit_tables/PIT_Purchase.csv")


dim_customer = pit_customer[["Hub_Customer_HK", "customer_id", "name", "address", "contact"]]
dim_product = pit_product[["Hub_Product_HK", "product_sku", "name", "category", "price"]]

dim_customer.to_csv("star_schema_views/Dim_Customer.csv", index=False)
dim_product.to_csv("star_schema_views/Dim_Product.csv", index=False)


fact_purchase = pit_purchase[[
    "Link_Purchase_HK", "Hub_Customer_HK", "Hub_Product_HK",
    "purchase_date", "quantity", "sales_amount"
]]
fact_purchase.to_csv("star_schema_views/Fact_Purchase.csv", index=False)
