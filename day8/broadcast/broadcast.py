from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import random
import time


spark = SparkSession.builder.appName("BroadcastJoinExample").master("local[*]").getOrCreate()


departments = [(1, "HR"), (2, "Engineering"), (3, "Marketing"), (4, "Finance"), (5, "Sales")]
dept_df = spark.createDataFrame(departments, ["dept_id", "dept_name"])


employees = [(random.randint(1, 5), f"Employee_{i}") for i in range(1_000_000)]
emp_df = spark.createDataFrame(employees, ["dept_id", "employee_name"])


start = time.time()
emp_df.join(dept_df, "dept_id").count()
normal_duration = time.time() - start


start = time.time()
emp_df.join(broadcast(dept_df), "dept_id").count()
broadcast_duration = time.time() - start


print(f"Without Broadcast Join: {normal_duration:.4f} seconds")
print(f"With    Broadcast Join: {broadcast_duration:.4f} seconds")


spark.stop()
