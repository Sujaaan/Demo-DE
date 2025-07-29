from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_unixtime, count, window, avg, unix_timestamp, when , sum
import time
import os

# Set Python paths
python_path = r"C:\Users\sujan.sudhakar\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

# Configure Spark session with better settings for local execution
spark = SparkSession.builder \
    .appName("ParkingStreamAnalysis") \
    .master("local[2]").config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2") \
    .getOrCreate()

# Create a dummy streaming source with reduced rate
stream_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5).option("numPartitions", 2) \
    .load()

# Simulate schema with parking fields
parking_df = stream_df.withColumn("terminal", 
                        expr("CASE WHEN value % 3 = 0 THEN 'A' WHEN value % 3 = 1 THEN 'B' ELSE 'C' END")) \
    .withColumn("zone", expr("concat('P', (value % 5) + 1)")) \
    .withColumn("slot_id", (col("value") % 500) + 1) \
    .withColumn("status", expr("CASE WHEN value % 4 = 0 THEN 'Occupied' ELSE 'Available' END")) \
    .withColumn("event_time", from_unixtime(col("timestamp").cast("long")).cast("timestamp")) \
    .drop("value")  # Remove unused column

# Print schema
parking_df.printSchema()

# 1. Zone congestion detection (simplified window)
zoneCongestionQuery = parking_df \
    .groupBy(
        "terminal", 
        "zone", 
        window("event_time", "1 minute")
    ) \
    .agg(
        sum(when(col("status") == "Occupied", 1).otherwise(0)).alias("occupied_count"),
        count("*").alias("total_slots")
    ) \
    .withColumn("occupancy_rate", col("occupied_count") / col("total_slots")) \
    .filter(col("occupancy_rate") > 0.5).select("terminal", "zone", "window", "occupancy_rate")

# 2. Average occupancy with proper window handling
avaregeQuery = parking_df \
    .withColumn("occupied", when(col("status") == "Occupied", 1).otherwise(0)) \
    .groupBy(
        window(
            col("event_time"), 
            "15 seconds", 
            "5 seconds"
        ),
        "terminal"
    ) \
    .agg(avg("occupied").alias("avg_occupancy")) \
    .withColumn("high_occupancy_alert", col("avg_occupancy") > 0.85) \
    .select("terminal", "window", "avg_occupancy", "high_occupancy_alert")

# Start queries with proper error handling
try:
    print("Starting streaming queries...")
    
    query1 = zoneCongestionQuery.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10).queryName("ZoneCongestion") \
        .start()
    
    query2 = avaregeQuery.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .queryName("TerminalOccupancy") \
        .start()
    
    # Let queries run for 10 seconds
    print("Streaming active for 10 seconds...")
    time.sleep(10)
    
except Exception as e:
    print(f"Error during streaming: {str(e)}")
finally:
    print("Stopping streaming queries...")
    query1.stop()
    query2.stop()
    spark.stop()
    print("Spark session stopped.")