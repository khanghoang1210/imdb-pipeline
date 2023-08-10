# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging
import logging.config

# Set logger 
logging.config.fileConfig(fname='./spark/config/logging.conf')
logger = logging.getLogger(__name__)

# Creat Spark object
try:
    logger.info("Spark Job is started...")
    spark = SparkSession \
            .builder\
            .master('local')\
            .appName("Ingestion - from PostgeSQL to Hive")\
            .getOrCreate()
except Exception as exp:
    logger.error("Error in method ingestion. Please check the Stack Trace, ", str(exp), exc_info=True)
    raise
else:
    logger.info("Spark object is created.")
