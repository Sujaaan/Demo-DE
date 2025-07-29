from pyspark.sql.functions import col, to_date


storage_account = "sampledatastoragevikash"
input_container = "csvfiles"
output_container = "parquet-data"
input_file = "customers-100.csv"
output_folder = "filtered_customers"

sas_token = "sp=racwdyti&st=2025-07-29T11:37:37Z&se=2025-07-29T19:52:37Z&spr=https&sv=2024-11-04&sr=b&sig=ehZayDdUwm3RhjtuFe4W%2BFuhfGJlfk87XMwuBLiAWrQ%3D"
spark.conf.set(f"fs.azure.sas.{output_container}.{storage_account}.blob.core.windows.net", sas_token)


input_path = f"wasbs://{input_container}@{storage_account}.blob.core.windows.net/{input_file}"
output_path = f"wasbs://{output_container}@{storage_account}.blob.core.windows.net/{output_folder}"


try:
    display(dbutils.fs.ls(f"wasbs://{output_container}@{storage_account}.blob.core.windows.net/"))
except Exception as e:
    print(f"Error: {e}")
    print("Please manually create the container first as shown in instructions above")
    raise


df = spark.read.option("header", True).csv(input_path)
df = df.withColumn("Subscription Date", to_date(col("Subscription Date")))
filtered_df = df.filter(col("Subscription Date") < "2021-08-11")


filtered_df.write.mode("overwrite").parquet(output_path)


print(f"Successfully saved to: {output_path}")
display(dbutils.fs.ls(output_path))