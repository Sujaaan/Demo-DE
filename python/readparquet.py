import pandas as pd
df = pd.read_parquet("output.parquet")
print(df.head())