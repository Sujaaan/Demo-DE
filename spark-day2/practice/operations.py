from pyspark.sql import SparkSession

from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataFrame Lab").master("local[*]").getOrCreate()

orders = spark.read.csv("orders.csv",header = True, inferSchema = True)
products = spark.read.csv("products.csv",header = True, inferSchema = True)

orders = orders.withColumn("price", col("price").cast("double")) \
               .withColumn("quantity", col("quantity").cast("int"))


orders = orders.withColumn("revenue", products("price") * orders("quantity"))

# Join orders with products on product_id
joined = orders.join(products, "product_id")

# Group by category and sum revenue
revenue_by_category = joined.groupBy("category").agg(sum("revenue").alias("total_revenue"))

revenue_by_category.show()