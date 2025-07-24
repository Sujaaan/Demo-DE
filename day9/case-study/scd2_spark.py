from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
import os


spark = SparkSession.builder \
    .appName("SCD2_Customer_Dimension") \
    .getOrCreate()


dim_customer_path = "scd2_dim_customer.parquet"


source_df = spark.createDataFrame([
    (1, "Alice", "New York", "Gold"),
    (2, "Bob", "San Francisco", "Silver"),
    (3, "Charlie", "Seattle", "Platinum")  
], ["CustomerID", "Name", "Address", "LoyaltyTier"])


if os.path.exists(dim_customer_path):
    existing_df = spark.read.parquet(dim_customer_path)
else:
    schema = StructType([
        StructField("CustomerSK", IntegerType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("LoyaltyTier", StringType(), True),
        StructField("StartDate", DateType(), True),
        StructField("EndDate", DateType(), True),
        StructField("IsCurrent", BooleanType(), True)
    ])
    existing_df = spark.createDataFrame([], schema)


latest_df = existing_df.filter(col("IsCurrent") == True)

joined_df = source_df.alias("src").join(
    latest_df.alias("tgt"),
    on="CustomerID",
    how="left"
)


changed_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier")) |
    (col("tgt.CustomerID").isNull())  
).select("src.*")


expired_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |    
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("tgt.*") \
.withColumn("EndDate", current_date()) \
.withColumn("IsCurrent", lit(False))


new_version_df = changed_df \
    .withColumn("CustomerSK", monotonically_increasing_id()) \
    .withColumn("StartDate", current_date()) \
    .withColumn("EndDate", lit(None).cast("date")) \
    .withColumn("IsCurrent", lit(True)) \
    .select("CustomerSK", "CustomerID", "Name", "Address", "LoyaltyTier", "StartDate", "EndDate", "IsCurrent")


unchanged_df = existing_df.filter("IsCurrent = true") \
    .join(expired_df.select("CustomerSK"), "CustomerSK", "left_anti")

final_df = unchanged_df.unionByName(expired_df).unionByName(new_version_df)


final_df.write.mode("overwrite").parquet(dim_customer_path)


final_df.orderBy("CustomerID", "StartDate").show(truncate=False)
