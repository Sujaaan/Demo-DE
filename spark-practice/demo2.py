from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Execution Model") \
    .getOrCreate()

df = spark.range(1, 1000000).withColumn("squared", col("id") * col("id"))
df.show()

# Pause to keep Spark UI alive
input("✅ Spark job is running. Visit http://localhost:4040 to view Spark UI.\nPress Enter to stop...")
