# from pyspark.sql import SparkSession
# import time

# spark = SparkSession.builder.appName("DummyStream").master("local[*]").getOrCreate()

# df = spark.readStream.format("CloudFiles").option("CloudFiles.format","csv").load("https://blobstoragetestvikash551.blob.core.windows.net/csvfiles/customers-100.csv?sp=r&st=2025-07-28T11:52:08Z&se=2025-07-28T20:07:08Z&spr=https&sv=2024-11-04&sr=b&sig=LGBVvdl%2B6V6erhsImi3nWGxJVKiHzalRYlul%2Fg8LKas%3D")

# df.writeStream.format("console").outputMode("append").option("truncate",False).start()

from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AzureBlobWithSAS") \
    .master("local[*]") \
    .getOrCreate()

# Your SAS URL components
sas_url = "https://blobstoragetestvikash551.blob.core.windows.net/csvfiles/customers-100.csv?sp=r&st=2025-07-28T12:06:27Z&se=2025-07-28T20:21:27Z&spr=https&sv=2024-11-04&sr=b&sig=cTS9wS%2FZQTaaF6nlR%2BOxgoKODZUvxh0XnuTnva%2F4a7c%3D"

# Extract components from SAS URL
storage_account = "blobstoragetestvikash551"
container_name = "csvfiles"
file_path = "customers-100.csv"
sas_token = "sp=r&st=2025-07-28T12:06:27Z&se=2025-07-28T20:21:27Z&spr=https&sv=2024-11-04&sr=b&sig=cTS9wS%2FZQTaaF6nlR%2BOxgoKODZUvxh0XnuTnva%2F4a7c%3D"

# Configure SAS token
spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net",
    sas_token
)

# Load the CSV file
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{file_path}")

# Show the data
print("=== CSV DATA ===")
df.show(5)

# For streaming version:
streaming_df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .load(f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{file_path}")

query = streaming_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

print("\nStreaming started... (will run for 15 seconds)")
time.sleep(15)
query.stop()
spark.stop()