from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg

spark = SparkSession.builder.appName("Dataframe API Essentials").getOrCreate()

users = spark.read.option("header",True).csv("users.csv")
activity = spark.read.option("header",True).csv("activity.csv")

users = users.withColumn("age", col("age").cast("int"))
activity = activity.withColumn("duration_min", col("duration_min").cast("double"))

filtered_users = users.filter(col("age") > 30)

joined = activity.join(filtered_users, activity["user_id"] == filtered_users["id"], "inner")

agg_df = joined.groupBy("id", "name", "region") \
    .agg(
        sum("duration_min").alias("total_duration_min"),
        avg("duration_min").alias("avg_duration_min")
    )

final_df = agg_df.selectExpr(
    "id", "name", "region",
    "total_duration_min",
    "avg_duration_min",
    "total_duration_min / 60 as total_duration_hrs",
    "avg_duration_min / 60 as avg_duration_hrs"
)

final_df.write.mode("overwrite").partitionBy("region").parquet("output/user_activity_stats")

final_df.show()