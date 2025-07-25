import pandas as pd

fact = pd.read_csv("C:/Users/sujan.sudhakar/Desktop/DE-Training/day10/case-study/star_schema_views/Fact_Purchase.csv")
dim_product = pd.read_csv("C:/Users/sujan.sudhakar/Desktop/DE-Training/day10/case-study/star_schema_views/dim_product.csv")
merged_fact = pd.merge(fact, dim_product, on="Hub_Product_HK")
revenue_by_category = merged_fact.groupby("category")["sales_amount"].sum()
print(revenue_by_category)
