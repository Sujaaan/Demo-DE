from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import time
from datetime import datetime
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\sujan.sudhakar\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.11"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\sujan.sudhakar\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.11"

def print_with_timestamp(message):
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')}] {message}")

def main():
    # 1. Configure Spark to reduce logging
    spark = SparkSession.builder \
        .appName("VisibleMicroBatch") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "1").config("spark.ui.showConsoleProgress", "false").getOrCreate()
    
    # 2. Lower log level (critical!)
    spark.sparkContext.setLogLevel("ERROR")

    # 3. Create stream with higher row rate
    df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 20).load() \
        .withColumn("processingTime", current_timestamp())

    # 4. Explicitly configure console output
    print_with_timestamp("=== STARTING (Wait 5 sec for first batch) ===")
    query = df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20).start()

    # 5. Force progress reports
    for i in range(1, 11):
        time.sleep(1)
        print_with_timestamp(f"Progress: {query.lastProgress}")  # Forces output
        
    query.stop()
    spark.stop()

if __name__ == "__main__":
    main()