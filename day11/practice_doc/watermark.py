# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, expr, window, count, when, sum
# import time
# import os

# python_path = r"C:\Users\sujan.sudhakar\AppData\Local\Programs\Python\Python310\python.exe"
# os.environ['PYSPARK_PYTHON'] = python_path
# os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

# # Configure Spark session
# spark = SparkSession.builder \
#     .appName("WatermarkDemoWorking") \
#     .master("local[2]") \
#     .getOrCreate()

# # Create static data to show input example
# static_data = [
#     # Format: (seconds_ago, terminal, zone, status)
#     (15, "A", "P1", "Occupied"),    # Late (will be dropped)
#     (8, "B", "P2", "Available"),    # On-time
#     (5, "C", "P3", "Occupied"),     # On-time
#     (20, "A", "P4", "Available")    # Late (will be dropped)
# ]

# # Create and show static input DataFrame
# current_time = int(time.time())
# input_df = spark.createDataFrame(
#     [(current_time - sec, term, zone, stat, 
#       "Late" if sec > 10 else "On-time") 
#      for sec, term, zone, stat in static_data],
#     ["event_time", "terminal", "zone", "status", "data_type"]
# ).withColumn("event_time", col("event_time").cast("timestamp"))

# print("=== INPUT DATA EXAMPLE ===")
# print("Records marked 'Late' should be dropped by watermark")
# input_df.show(truncate=False)

# # Now create the actual streaming source
# stream_df = spark.readStream \
#     .format("rate") \
#     .option("rowsPerSecond", 2) \
#     .load()

# # Simulate parking data with some late arrivals
# parking_df = stream_df.withColumn("terminal", 
#                         expr("CASE WHEN value % 3 = 0 THEN 'A' WHEN value % 3 = 1 THEN 'B' ELSE 'C' END")) \
#     .withColumn("zone", expr("concat('P', (value % 5) + 1)")) \
#     .withColumn("status", expr("CASE WHEN value % 4 = 0 THEN 'Occupied' ELSE 'Available' END")) \
#     .withColumn("event_time", 
#                expr("CASE WHEN value % 10 = 0 THEN timestamp - INTERVAL 15 SECONDS "  # Late
#                     "WHEN value % 5 = 0 THEN timestamp - INTERVAL 8 SECONDS "        # On-time
#                     "ELSE timestamp END")).withColumn("data_type",
#                expr("CASE WHEN value % 10 = 0 THEN 'Late (15s)' "
#                     "WHEN value % 5 = 0 THEN 'On-time (8s)' "
#                     "ELSE 'Current' END"))

# # Apply watermark (10 seconds)
# watermarked_df = parking_df.withWatermark("event_time", "10 seconds")

# # Process with windowing
# result = watermarked_df.groupBy(
#     window("event_time", "1 minute"),
#     "terminal",
#     "zone"
# ).agg(
#     count("*").alias("count"),
#     sum(when(col("status") == "Occupied", 1).otherwise(0)).alias("occupied"),
#     expr("collect_list(data_type)").alias("processed_data_types")
# )

# # Start the streaming query
# query = result.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# print("\n=== STREAM PROCESSING STARTED ===")
# print("Watermark set at 10 seconds - late data will be dropped")
# print("Running for 30 seconds to demonstrate...")

# time.sleep(30)

# query.stop()
# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window, count, when, sum, from_unixtime
import time

spark = SparkSession.builder \
    .appName("WorkingWatermarkDemo") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 1. First show static example of what will happen
print("=== EXPECTED BEHAVIOR ===")
print("Records older than 10 seconds will be dropped by watermark\n")

static_data = [
    (15, "Late (15s)"),  # Will be dropped
    (8, "On-time (8s)"), # Will be processed
    (3, "Current")       # Will be processed
]

current_time = int(time.time())
example_df = spark.createDataFrame(
    [(current_time - sec, f"Terminal-{i}", f"P{i}", stat) 
     for i, (sec, stat) in enumerate(static_data)],
    ["event_time", "terminal", "zone", "data_type"]
).withColumn("event_time", from_unixtime(col("event_time")))

example_df.show(truncate=False)

# 2. Now run actual streaming with guaranteed data
print("\n=== STARTING STREAM ===")

# Create stream with immediate data
stream_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5).option("numPartitions", 1).load()

# Transform with immediate data points
parking_df = stream_df.withColumn("terminal", expr("'Terminal-' || (value % 3)")) \
    .withColumn("zone", expr("'P' || (value % 5 + 1)")) \
    .withColumn("status", expr("IF(value % 4 = 0, 'Occupied', 'Available')")) \
    .withColumn("event_time", 
               expr("""
                   CASE 
                     WHEN value < 3 THEN timestamp - INTERVAL 15 SECONDS  # First 3 records are late
                     WHEN value < 6 THEN timestamp - INTERVAL 8 SECONDS   # Next 3 are on-time
                     ELSE timestamp                                      # Rest are current
                   END
               """)) \
    .withColumn("data_type",
               expr("""
                   CASE 
                     WHEN value < 3 THEN 'Late (15s)'
                     WHEN value < 6 THEN 'On-time (8s)'
                     ELSE 'Current'
                   END
               """))

# Apply watermark
watermarked_df = parking_df.withWatermark("event_time", "10 seconds")

# Process with windowing
result = watermarked_df.groupBy(
    window("event_time", "30 seconds"),  # Smaller window for demo
    "terminal",
    "zone"
).agg(
    count("*").alias("count"),
    sum(when(col("status") == "Occupied", 1).otherwise(0)).alias("occupied"),
    expr("collect_list(data_type)").alias("included_data_types")
)

# Start query with longer await
query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Let it run long enough to see multiple batches
print("\nStreaming for 15 seconds...")
time.sleep(15)

query.stop()
spark.stop()