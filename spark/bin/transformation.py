# Import libraries
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

## Report:
    # 1.ID
    # 2.Title
    # 3.month_revenue
    # 4.total_revenue
    # 5.rank_change

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("hive.metastore.uris", "thrift://localhost:9083")\
    .enableHiveSupport()\
    .getOrCreate()
# Tạo DataFrame từ dữ liệu
data = [("John", 28), ("Jane", 24), ("Alice", 32)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Lưu DataFrame vào Hive
    # Create a Hive table
tableName = "test1"
df.write.format("hive").mode("append").saveAsTable(tableName)