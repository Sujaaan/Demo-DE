from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import sys

# Set Python paths (use your actual Python executable path)
python_path = r"C:\Users\sujan.sudhakar\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

# Create Spark session with streaming support
spark = SparkSession.builder \
    .appName("ParkingStreamingAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# ==============================================
# MICRO-BATCH STREAMING VERSION
# ==============================================

# 1. Create a streaming DataFrame simulating sensor data
stream_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1).option("numPartitions", 1) .load()

# 2. Transform into parking data format
parking_stream = stream_df.withColumn("terminal", 
                          expr("CASE WHEN value % 3 = 0 THEN 'A' WHEN value % 3 = 1 THEN 'B' ELSE 'C' END")) \
    .withColumn("zone", expr("concat('P', (value % 5) + 1)")) \
    .withColumn("slot_id", (col("value") % 500) + 1) \
    .withColumn("status", expr("CASE WHEN value % 4 = 0 THEN 'Occupied' ELSE 'Available' END")) \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .drop("value")

# 3. Define the streaming processing logic
def process_stream(batch_df, batch_id):
    print(f"\nProcessing batch {batch_id}...")
    
    # Your original analysis logic (adapted for micro-batches)
    # 1. Zone congestion detection
    zone_stats = batch_df.groupBy("terminal", "zone").agg(
        count("slot_id").alias("total_slots"),
        sum(when(col("status") == "Occupied", 1).otherwise(0)).alias("occupied_slots"),
        (sum(when(col("status") == "Occupied", 1).otherwise(0))/count("slot_id")).alias("occupancy_rate")
    )
    
    congested = zone_stats.filter(col("occupancy_rate") > 0.8)
    print("\nCongested zones in this batch:")
    congested.show()
    
    # 2. Windowed averages (5-second windows)
    window_spec = Window.partitionBy("terminal", "zone").orderBy("timestamp").rangeBetween(-5, 0)
    windowed = batch_df.withColumn("rolling_occupancy",
        avg(when(col("status") == "Occupied", 1.0).otherwise(0.0)).over(window_spec))
    
    print("\nRolling window occupancy:")
    windowed.select("terminal", "zone", "timestamp", "rolling_occupancy").show()
    
    # 3. Terminal alerts
    terminal_stats = batch_df.groupBy("terminal").agg(
        (sum(when(col("status") == "Occupied", 1).otherwise(0))/count("slot_id")).alias("occupancy_rate")
    )
    
    alerts = terminal_stats.filter(col("occupancy_rate") > 0.85)
    print("\nTerminal alerts in this batch:")
    alerts.show()

# 4. Start the streaming query with micro-batches
query = parking_stream.writeStream \
    .foreachBatch(process_stream).outputMode("update") \
    .trigger(processingTime="5 seconds").start()

print("Streaming started...")
query.awaitTermination()  # Runs until manually stopped