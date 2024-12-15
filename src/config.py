from pyspark.sql import SparkSession
import logging

API_KEY = "Your API key"
BUS_LINE = "B52"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "HDFS USER NAME"
HDFS_OUTPUT_DIR = "/bus_tracking/B52"
PROCESSED_DATA_DIR = "/bus_tracking/processed"
UPDATE_INTERVAL = 20  # seconds

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bus_tracker.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('bus_tracker')

def create_spark_session():
    try:
        return SparkSession.builder \
            .appName("B52BusTracker") \
            .master("local[*]") \
            .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
            .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
            .getOrCreate()
    except Exception as e:
        logger = setup_logging()
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise
