from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame Lab").master("local[*]").getOrCreate()

df = spark.read.csv("data.csv",header = True, inferSchema = True)

#finalData = df.filter("salary > 100").groupBy("name").sum("salary")
finalData = df.withColumn("tax",df.salary*0.18)
finalData.show()