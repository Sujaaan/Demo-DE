from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CachePipeline") \
    .getOrCreate()

# Read Parquet output from Lab 2
df = spark.read.parquet("C:/Users/sujan.sudhakar/Desktop/DE-Training/spark-day2/lab2-dataframes-api/output/user_activity_stats/region=North/part-00000-7f0e0fa8-2a77-4e62-bc0b-3e7281ab0196.c000.snappy.parquet")

# Apply transformations before caching
cached_df = df.filter(col("total_duration_min") > 50) \
    .withColumn("engagement_score", col("total_duration_hrs") * 1.5 + col("avg_duration_hrs")) \
    .cache()

# Trigger caching
cached_df.count()

# Continue pipeline: group and sort
result = cached_df.groupBy("name") \
    .count() \
    .orderBy(desc("count"))

# Show result
result.show()

# Keep session alive briefly to check Spark UI
input("Press Enter to stop Spark (check http://localhost:4040 before closing)...")

spark.stop()
