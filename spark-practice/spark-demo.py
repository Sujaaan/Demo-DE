from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame Lab").master("local[*]").getOrCreate();

print("spark Session created");
