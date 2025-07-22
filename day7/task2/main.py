from pyspark.sql import SparkSession
import time
import random
import os

# Create SparkSession with Delta configs
spark = SparkSession.builder \
    .appName("ZOrderDeltaExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.catalog.DeltaCatalog") \
    .getOrCreate()

# Delta table output path
delta_path = "output/zorder_demo"
os.makedirs("output", exist_ok=True)

# Generate dummy data with multiple regions
regions = ["US", "EU", "ASIA", "AU", "AFRICA"]
data = [(i, f"user_{i}", random.choice(regions)) for i in range(1, 5001)]
df = spark.createDataFrame(data, ["id", "name", "region"])

# Save as Delta table
df.write.format("delta").mode("overwrite").save(delta_path)
print("\n✅ Delta table created with 5000 records across multiple regions.\n")

# Query before ZORDER optimization
print("⏱ Running query BEFORE ZORDER on region = 'US'")
start_time = time.time()
us_count_before = spark.read.format("delta").load(delta_path) \
    .filter("region = 'US'") \
    .count()
time_before = round(time.time() - start_time, 3)
print(f"US Count: {us_count_before} | Time: {time_before} seconds\n")

# Optimize the table with ZORDER
print("⚙️ Running OPTIMIZE ZORDER BY (region)...")
spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (region)")
print("✅ Optimization done.\n")

# Query after ZORDER optimization
print("⏱ Running query AFTER ZORDER on region = 'US'")
start_time = time.time()
us_count_after = spark.read.format("delta").load(delta_path) \
    .filter("region = 'US'") \
    .count()
time_after = round(time.time() - start_time, 3)
print(f"US Count: {us_count_after} | Time: {time_after} seconds\n")

# Compare performance
improvement = round((time_before - time_after) / time_before * 100, 2)
print(f"📈 Query improved by approx. {improvement}% after Z-Ordering (if > 0).")

# Stop Spark session
spark.stop()
