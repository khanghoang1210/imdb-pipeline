# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id, to_date
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

    hdfs_client = InsecureClient("http://localhost:9870", user="hive")

    # Get lastest record


    # Full datalake path
    file_path = f"hdfs://localhost:9000/hive/user/datalake/{table_name}"

    # Path to datalake   
    table_location = f"/hive/user/datalake/"

    query = ""
    last_crawled_date = df.select("crawled_date").agg({"crawled_date": "max"}).collect()[0]["max(crawled_date)"]
    if not hdfs_client.status(table_location, strict=False):  
        hdfs_client.makedirs(table_location)
        # df = spark.read.csv(file_path)
        # record_id = df
        logger.info(f"Path '{table_location}' created successfully in HDFS.")
    else:
        logger.info(f"Path '{table_location}' already exists in HDFS.")



    # Read data from Postgres to Spark DataFrame
    jdbcDF = spark.read \
              .format("jdbc") \
              .option("url", "jdbc:postgresql://localhost:5434/postgres") \
              .option("dbtable", table_name) \
              .option("user", user) \
              .option("password", password) \
              .option("driver", "org.postgresql.Driver") \
              .load()\
              
    logger.info("Read data completed.")
    jdbcDF = jdbcDF.withColumn("idx", monotonically_increasing_id())
    
    # Validate dataframe
    
    jdbcDF.show()
    jdbcDF.printSchema()
    

    
    # Load spark dataframe into datalake
    logger.info("Load Spark DataFrame into HDFS is started...")

    jdbcDF.coalesce(1) \
        .write \
        .format('csv') \
        .save(file_path, header=True, compression='snappy')

    logger.info("Ingestion is completed.")
 
except Exception as exp:
    logger.error("Error in method ingestion. Please check the Stack Trace: %s", str(exp), exc_info=True)
    raise
else:
    logger.info("Spark object is created.")
