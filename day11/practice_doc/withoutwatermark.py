from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col
import os
import time
from threading import Thread

python_path = r"C:\Users\sujan.sudhakar\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

def stream_to_delta():
    try:
        # Start Spark session with Delta Lake configuration
        spark = SparkSession.builder \
            .appName("StreamToDeltaLake") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        # Create paths (cross-platform compatible)
        base_dir = os.path.join("file:", os.path.abspath("delta_streaming"))
        delta_path = os.path.join(base_dir, "windowed_counts")
        checkpoint_path = os.path.join(base_dir, "checkpoints")

        # Simulated input stream (rate source)
        stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

        # Windowed aggregation without watermark
        windowed_counts = stream_df \
            .groupBy(window(col("timestamp"), "10 seconds")) \
            .count()

        # Write streaming output to Delta Lake
        query = windowed_counts.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", delta_path) \
            .start()

        print(f"Streaming to Delta Lake at: {delta_path}")
        print("Use Ctrl+C to stop the stream...")

        # Thread for clean shutdown handling
        def await_termination():
            query.awaitTermination()

        termination_thread = Thread(target=await_termination)
        termination_thread.start()

        # Keep main thread alive
        while termination_thread.is_alive():
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping the stream gracefully...")
        query.stop()
        termination_thread.join()
        print("Stream successfully stopped.")
        
        # Verify Delta table
        print("\nDelta table contents:")
        spark.read.format("delta").load(delta_path).show(truncate=False)
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        if 'query' in locals():
            query.stop()
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    stream_to_delta() 