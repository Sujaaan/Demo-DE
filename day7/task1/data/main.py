from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os

spark = SparkSession.builder \
    .appName("Join JSON with CV Text") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

json_path = "users.json"
cv_folder = "data"
output_path = "output_delta_cv"

df = spark.read.json(json_path)

def read_cv_file(name, id):
    filename = f"{name}{id}cv.txt"
    filepath = os.path.join(cv_folder, filename)
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return file.read()
    except:
        return None

from pyspark.sql.functions import udf
read_cv_udf = udf(lambda name, id: read_cv_file(name, id), StringType())

df_with_cv = df.withColumn("cv_text", read_cv_udf("name", "id"))

df_with_cv.coalesce(1).write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

print("✅ Written to:", output_path)
spark.stop()
