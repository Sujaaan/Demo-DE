
import pandas as pd

def check_collisions(path, key_col, label):
    df = pd.read_csv(path)
    if df.duplicated(key_col).any():
        print(f"Collision detected in {label}")
    else:
        print(f"No collision in {label}")

check_collisions("raw_vault/Hub_Customer.csv", "Hub_Customer_HK", "Hub_Customer")
check_collisions("raw_vault/Hub_Product.csv", "Hub_Product_HK", "Hub_Product")
check_collisions("raw_vault/Link_Purchase.csv", "Link_Purchase_HK", "Link_Purchase")
