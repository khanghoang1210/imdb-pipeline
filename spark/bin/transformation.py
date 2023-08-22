# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
## Report:
    # 1.ID
    # 2.Title
    # 3.month_revenue
    # 4.total_revenue
    # 5.rank_change

# Create Spark Object
spark = SparkSession\
            .builder \
            .master("local")\
            .appName("Weekly Movie Revenue Report") \
            .config("hive.metastore.uris", "thrift://localhost:9083")\
            .config("hive.exec.dynamic.partition", "true")\
            .config("hive.exec.dynamic.partition.mode", "nonstrict")\
            .enableHiveSupport()\
            .getOrCreate()

# Load data from datalake to spark dataframe
df_movies = spark.read.csv("hdfs://localhost:9000/hive/user/datalake/movies", header=True).drop("year","month","day")
df_revenue = spark.read.csv("hdfs://localhost:9000/hive/user/datalake/movie_revenue", header=True).drop("year","month","day")

df = df_movies.select(col("movie_id").alias("Id"), col("title"))
preDF = df_revenue.withColumn("revenue", df_revenue.revenue.cast("double"))



#df.write.format("hive").mode("append").saveAsTable(tableName)