spark-submit ^
  --driver-class-path "C:\spark\jars\delta-spark_2.13-4.0.0.jar;C:\spark\jars\delta-storage-4.0.0.jar" ^
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" ^
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" ^
  withoutwatermark.py