from pyspark.sql import SparkSession

# Create SparkSession with Delta configs
spark = SparkSession.builder.appName("lab1APP") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Define paths (make sure the folders exist or are writable)
parquet_path = "C:/Users/sujan.sudhakar/Desktop/customer_parquet"
delta_path = "C:/Users/sujan.sudhakar/Desktop/customer_delta"

# Sample data
data = [
    (1, 'Alice', 'UK'),
    (2, 'Bob', 'Germany'),
    (3, 'Carlos', 'Spain')
]

# Create DataFrame
df = spark.createDataFrame(data, ['user_id', 'name', 'country'])
df.write.mode("overwrite").parquet(parquet_path)
df.show()

# Read Parquet back
parquet_df = spark.read.parquet(parquet_path)

# Write as Delta
parquet_df.write.format("delta").mode("overwrite").save(delta_path)

# Register as a Delta table
spark.sql(f"CREATE TABLE IF NOT EXISTS customer_delta USING DELTA LOCATION '{delta_path}'")

# Query the Delta table
print("\n--- Initial Delta Table ---")
result1_df = spark.sql(f"SELECT * FROM delta.`{delta_path}`")
result1_df.show()

# Delete user_id = 2
spark.sql(f"DELETE FROM delta.`{delta_path}` WHERE user_id = 2")

# Show after delete
print("\n--- After Deletion ---")
result3_df = spark.sql(f"SELECT * FROM delta.`{delta_path}`")
result3_df.show()

# Describe history
print("\n--- Delta Table History ---")
result4_df = spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
result4_df.show()

# Read from version 0
print("\n--- Reading Version 0 (Time Travel) ---")
result5_df = spark.sql(f"SELECT * FROM delta.`{delta_path}` VERSION AS OF 0")
result5_df.show()

# Restore to version 0
print("\n--- Restoring to Version 0 ---")
spark.sql("RESTORE TABLE customer_delta TO VERSION AS OF 0")

# Show after restore
print("\n--- After Restore ---")
result6_df = spark.sql(f"SELECT * FROM delta.`{delta_path}`")
result6_df.show()
