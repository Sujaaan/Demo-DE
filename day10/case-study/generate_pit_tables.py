
import pandas as pd
import os

os.makedirs("pit_tables", exist_ok=True)


hub_customer = pd.read_csv("raw_vault/Hub_Customer.csv")
sat_customer = pd.read_csv("raw_vault/Sat_Customer_Details.csv")

hub_product = pd.read_csv("raw_vault/Hub_Product.csv")
sat_product = pd.read_csv("raw_vault/Sat_Product_Details.csv")

link_purchase = pd.read_csv("raw_vault/Link_Purchase.csv")
sat_purchase = pd.read_csv("raw_vault/Sat_Purchase_Details.csv")


pit_customer = sat_customer.sort_values('Load_Date').drop_duplicates('Hub_Customer_HK', keep='last')
pit_customer = hub_customer.merge(pit_customer, on='Hub_Customer_HK')
pit_customer.to_csv("pit_tables/PIT_Customer.csv", index=False)


pit_product = sat_product.sort_values('Load_Date').drop_duplicates('Hub_Product_HK', keep='last')
pit_product = hub_product.merge(pit_product, on='Hub_Product_HK')
pit_product.to_csv("pit_tables/PIT_Product.csv", index=False)


pit_purchase = link_purchase.merge(pit_customer, on='Hub_Customer_HK', how='left')
pit_purchase = pit_purchase.merge(pit_product, on='Hub_Product_HK', how='left')
pit_purchase = pit_purchase.merge(sat_purchase, on='Link_Purchase_HK', how='left')
pit_purchase.to_csv("pit_tables/PIT_Purchase.csv", index=False)
