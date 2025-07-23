import time
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Partitioning Benchmark") \
    .master("local[*]") \
    .getOrCreate()

countries = ["India", "USA", "UK", "Germany", "Japan"]
data = [(random.choice(countries), random.randint(18, 70), random.randint(30000, 150000)) for _ in range(1_000_000)]
df = spark.createDataFrame(data, ["country", "age", "income"])

df.write.mode("overwrite").parquet("unpartitioned_data")
df.write.mode("overwrite").partitionBy("country").parquet("partitioned_data")

unpartitioned_df = spark.read.parquet("unpartitioned_data")
start_time = time.time()
unpartitioned_df.filter(col("country") == "India").count()
unpartitioned_duration = time.time() - start_time

partitioned_df = spark.read.parquet("partitioned_data")
start_time = time.time()
partitioned_df.filter(col("country") == "India").count()
partitioned_duration = time.time() - start_time

print(f"\nNon-Partitioned Query Time: {unpartitioned_duration:.4f} seconds")
print(f"Partitioned   Query Time: {partitioned_duration:.4f} seconds")

spark.stop()
