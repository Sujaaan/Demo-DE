from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame Lab").master("local[*]").getOrCreate()

countlines = spark.read.text("C:/Users/sujan.sudhakar/Desktop/DE-Training/spark-day2/sample.txt").count()
print(f"number of lines are {countlines}")