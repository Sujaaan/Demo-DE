from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from delta.tables import DeltaTable

# Initialize Spark Session with Delta Lake configs
spark = SparkSession.builder \
    .appName("DeltaCSVWithSchema") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Define schema for the CSV
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

# Path to CSV file
csv_path = "C:/Users/sujan.sudhakar/Desktop/DE-Training/day7/users.csv"

# Read CSV with schema
df = spark.read.option("header", "true").schema(schema).csv(csv_path)

# Write to Delta Lake
delta_path = "output/path/delta_table"
df.write.format("delta").mode("overwrite").save(delta_path)

# Read from Delta and show as table
print("Delta Table Content:")
df_delta = spark.read.format("delta").load(delta_path)
df_delta.show(truncate=False)

# Stop the Spark session
spark.stop()
