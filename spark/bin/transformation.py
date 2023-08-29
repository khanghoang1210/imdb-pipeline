# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging
import logging.config
import ingestion
from utils import parse_args
## Report:
    # 1.ID
    # 2.Title
    # 3.month_revenue
    # 4.total_revenue
    # 5.rank_change
# Set logger 
logging.config.fileConfig(fname='./spark/config/logging.conf')
logger = logging.getLogger(__name__)

try:
    logger.info("Transformation is started...")
    # Create Spark Object
    spark = SparkSession\
                .builder \
                .master("local")\
                .appName("Daily Movie Revenue Report") \
                .config("hive.metastore.uris", "thrift://localhost:9083")\
                .config("hive.exec.dynamic.partition", "true")\
                .config("hive.exec.dynamic.partition.mode", "nonstrict")\
                .enableHiveSupport()\
                .getOrCreate()
    logger.info("Spark object is created.")

    # get latest date in hdfs 
    excecution_date = ingestion.last_crawled_date
    split_col = split(excecution_date, "-")
    day = split_col[2]
    month = 

    # Load data from datalake to spark dataframe
    df_movies = spark.read.csv("hdfs://localhost:9000/hive/user/datalake/movies", header=True).drop("year","month","day")
    logger.info("Table movies is loaded into spark dataframe")
    df_revenue = spark.read.csv("hdfs://localhost:9000/hive/user/datalake/movie_revenue", header=True).drop("year","month","day")
    logger.info("Table revenue is loaded into spark dataframe")
    
    # # Transform datatype of columns
    df_revenue = df_revenue.withColumn("rank", df_revenue.rank.cast("int"))
    df_revenue = df_revenue.withColumn("crawled_date", df_revenue.crawled_date.cast("date"))
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

    # Join dataframe
    df_res = df.join(total_revenue, df["id"]==total_revenue["id"],"right")\
            .join(df_week_revenue, df.id==df_week_revenue.movie_id).drop("movie_id")\
                                                                    .drop(total_revenue["id"])

    #df_res.show()
    #df_filtered.show(300)

    # Define window function
    window_spec = Window.orderBy(col("crawled_date")).partitionBy("id")

    # Get max crawled date for each movie
    df_max_date = df_revenue.groupBy("id").agg(max("crawled_date").alias("max_date"))
    df_max_date = df_max_date.withColumnRenamed("id","movie_id")

    # Get previous date from max date
    df_with_max_date = df_revenue.withColumn("max_crawled_date", max("crawled_date").over(window_spec))
    df_prev_date = df_with_max_date.withColumn("previous_date", lag("max_crawled_date").over(window_spec))\
                                   .withColumn("prev_rank", lag("rank").over(window_spec))

    # Join dataframe for calc rank change
    df_result = df_prev_date.join(df_max_date).where((df_prev_date.crawled_date==df_max_date.max_date) & (df_prev_date['id']==df_max_date['movie_id']))
    # Get rank change for each movie
    rank_change = df_result.withColumn("rank_change",df_result.prev_rank-df_result.rank)
    rank_change = rank_change.withColumn("rank_change", coalesce("rank_change", lit(0)))

    #rank_change.show()

    # Weekly movie revenue report
    analysis = df_res.join(rank_change, df_res.id==rank_change.id).select(df_res["*"], rank_change["rank_change"])
    analysis.show(50)

    logger.info("Genarated analysis report")

    analysis.write.format("hive").mode("append").saveAsTable("reports.weekly_movies_revenue")
    logger.info("Save dataframe into Hive completed.")
except Exception as exp:
    logger.error("Error occur in method transfromation. Please check the Stack Trace, ", str(exp))
    raise
else:
    logger.info("Transformation is completed.")