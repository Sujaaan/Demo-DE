from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CheckpointPipeline") \
    .getOrCreate()

# Set a checkpoint directory
spark.sparkContext.setCheckpointDir("checkpoint_dir")

# Read Parquet output from Lab 2
df = spark.read.parquet("output/user_activity_stats")

# Apply transformations before checkpointing
checkpointed_df = df.filter(col("total_duration_min") > 50) \
    .withColumn("engagement_score", col("total_duration_hrs") * 1.5 + col("avg_duration_hrs")) \
    .checkpoint()

# Trigger checkpointing
checkpointed_df.count()

# Continue pipeline: group and sort
result = checkpointed_df.groupBy("region") \
    .count() \
    .orderBy(desc("count"))

# Show result
result.show()

# Keep session alive briefly to check Spark UI
input("Press Enter to stop Spark (check http://localhost:4040 before closing)...")

spark.stop()
