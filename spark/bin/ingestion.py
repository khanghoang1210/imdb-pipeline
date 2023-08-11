# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging
import logging.config
from dotenv import load_dotenv
import os

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
    
    password = os.getenv("password_postgres")
    user = os.getenv("user_postgres")
    table_name = "movies"
    df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5434/postgres") \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
    df.show()
    df.printSchema()
    logger.info("Read data completed.")
except Exception as exp:
    logger.error("Error in method ingestion. Please check the Stack Trace: %s", str(exp), exc_info=True)
    raise
else:
    logger.info("Spark object is created.")
