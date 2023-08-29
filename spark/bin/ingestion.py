# Import libraries
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
import logging
import logging.config
from py4j.java_gateway import java_import
from hdfs import InsecureClient
from dotenv import load_dotenv
import os
from utils import parse_args

# Load variables from dotenv file
load_dotenv()

# Set logger 
logging.config.fileConfig(fname='./spark/config/logging.conf')
logger = logging.getLogger(__name__)

# Create Spark object
try:
    logger.info("Spark Job is started...")

    spark = SparkSession \
            .builder\
            .master('local')\
            .appName("Ingestion - from PostgeSQL to Hive")\
            .config("spark.jars", "./spark/lib/postgresql-42.5.4.jar")\
            .getOrCreate()
    
    
    # receive argument
    args = parse_args()
    table_name = args.table_name

    # Get variables
    password = os.getenv("password_postgres")
    user = os.getenv("user_postgres")

    # Import necessary Hadoop classes
    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    # jvm = spark_session._jvm
    # jsc = spark_session._jsc
    # fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

    hdfs_client = InsecureClient("http://localhost:9870", user="hive")

    # Full datalake path
    file_path = f"hdfs://localhost:9000/hive/user/datalake/{table_name}"

    # Get latest record in hdfs
    table_location = f"/hive/user/datalake/{table_name}"
    table_query = ""
    
    # Check record exists in hdfs
    exists = hdfs_client.status(table_location, strict=False)

    if not exists:  
        table_query = f"(SELECT * FROM {table_name}) AS tmp"
        logger.info(f"Path '{table_location}' created successfully in HDFS with new data")
    else:
        df = spark.read.csv(file_path, header=True)
        
        # Get latest value of crawled_date field
        last_crawled_date = df.select("crawled_date").agg({"crawled_date": "max"}).collect()[0]["max(crawled_date)"]
        table_query = f"(SELECT * FROM {table_name} WHERE (crawled_date > DATE '{last_crawled_date}')) AS tmp"
        logger.info(f"Path '{table_location}' already exists in HDFS. Incremental load is started.")

    # Read latest record from Postgres to Spark DataFrame
    jdbcDF = spark.read \
              .format("jdbc") \
              .option("url", "jdbc:postgresql://localhost:5434/postgres") \
              .option("dbtable", table_query) \
              .option("user", user) \
              .option("password", password) \
              .option("driver", "org.postgresql.Driver") \
              .load()\
              
    logger.info("Read data completed.")
    
    # Validate dataframe
    logger.info(jdbcDF.show())
    logger.info(jdbcDF.printSchema())
    
    # Load spark dataframe into datalake
    logger.info("Load Spark DataFrame into HDFS is started...")

    # Make partition to save data
    split_col = split(jdbcDF["crawled_date"], "-")

    jdbcDF = jdbcDF.withColumn("year", split_col[0].cast("int"))
    jdbcDF = jdbcDF.withColumn("month", split_col[1].cast("int"))
    jdbcDF = jdbcDF.withColumn("day", split_col[2].cast("int"))

    jdbcDF.coalesce(1) \
        .write \
        .format('csv') \
        .partitionBy("year","month","day")\
        .save(file_path, header=True, compression='snappy', mode='append')\


    logger.info("Ingestion or incremental load is completed.")
 
except Exception as exp:
    logger.error("Error in method ingestion. Please check the Stack Trace: %s", str(exp), exc_info=True)
    raise
else:
    logger.info("Spark object is created.")
