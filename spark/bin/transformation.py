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

# Transform column revenue
df_revenue = df_revenue.withColumn("revenue",regexp_replace(col('revenue'), ',', '').cast("int"))

# Get available columns
df = df_movies.select(col("movie_id").alias("id"), col("title"))
total_revenue = df_revenue.groupBy("id").agg(sum("revenue").alias("total_revenue"))

# Join main columns 
df_joined = df_revenue.join(df_movies, df_revenue.id == df_movies.movie_id).drop(df_movies.crawled_date)

# Get latest week
max_crawled_date = df_joined.select(max(col('crawled_date'))).collect()[0][0]
seven_days_ago = max_crawled_date - expr("INTERVAL 7 DAYS")
df_filtered = df_joined.filter(col('crawled_date') >= seven_days_ago)

# Get week revenue of each movie
df_week_revenue = df_filtered.groupBy('movie_id').agg(sum('revenue').alias('week_revenue'))

# Aggarate dataframe
df_res = df.join(total_revenue, df["id"]==total_revenue["id"],"right").join(df_week_revenue, df.id==df_week_revenue.movie_id).drop("movie_id")\
                                                                                                                            .drop(total_revenue["id"])





#df.write.format("hive").mode("append").saveAsTable(tableName)