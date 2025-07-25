import pandas as pd

fact = pd.read_csv("C:/Users/sujan.sudhakar/Desktop/DE-Training/day10/case-study/star_schema_views/Fact_Purchase.csv")
dim_customer = pd.read_csv("C:/Users/sujan.sudhakar/Desktop/DE-Training/day10/case-study/star_schema_views/dim_customer.csv")

merged = pd.merge(
    fact,
    dim_customer,
    left_on='Hub_Customer_HK',
    right_on='Hub_Customer_HK'
)


result = merged[merged['name'] == 'Alice']

print(result)
