import os
import sys
import logging
import traceback
import time

# Ensure pyspark is in the path
import site
site.addsitedir('/opt/spark/python')
site.addsitedir('/opt/spark/python/lib/py4j-0.10.9.5-src.zip')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/opt/spark-data/spark_app.log", mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def create_sample_data(spark):
    """Create sample data if input path is empty"""
    logger.info("Creating sample data")
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("value", IntegerType(), True)
    ])

    sample_data = [
        ("electronics", 100),
        ("clothing", 75),
        ("books", 50),
        ("electronics", 200),
        ("clothing", 125),
        ("books", 80)
    ]

    return spark.createDataFrame(sample_data, schema)

def create_spark_session(max_retries=3):
    """Create Spark session with retry mechanism"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to create Spark session (Attempt {attempt + 1})")

            # Try to connect to Spark cluster
            spark = SparkSession.builder \
                .appName("Sample Spark Processing") \
                .master("spark://spark-master:7077") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.driver.host", "spark-master") \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .config("spark.driver.port", "7077") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.executor.memory", "1g") \
                .config("spark.driver.memory", "1g") \
                .config("spark.submit.deployMode", "client") \
                .getOrCreate()

            # Verify Spark context
            spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session created successfully")
            return spark

        except Exception as e:
            logger.warning(f"Failed to create Spark session: {e}")

            # If it's the last attempt, try local mode
            if attempt == max_retries - 1:
                logger.warning("Falling back to local Spark mode")
                spark = SparkSession.builder \
                    .appName("Sample Spark Processing (Local)") \
                    .master("local[*]") \
                    .getOrCreate()
                return spark

            # Wait before retrying
            time.sleep(5)

    raise RuntimeError("Could not create Spark session")

def main(input_path, output_path):
    # Detailed logging for debugging
    logger.info("Starting Spark job")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")

    try:
        # Create Spark session
        spark = create_spark_session()

        # Log Spark configuration for debugging
        logger.info("Spark Configuration:")
        for key, value in spark.sparkContext.getConf().getAll():
            logger.info(f"{key}: {value}")

        # Ensure input path exists
        os.makedirs(input_path, exist_ok=True)

        # List contents of input path
        logger.info(f"Contents of input path: {os.listdir(input_path)}")

        # Read or create sample data
        if not os.listdir(input_path):
            logger.warning(f"No data found in {input_path}. Generating sample data.")
            df = create_sample_data(spark)
        else:
            # Read input data
            logger.info(f"Reading data from {input_path}")
            df = spark.read.csv(input_path, header=True, inferSchema=True)

        # Log DataFrame details
        logger.info("Input DataFrame details:")
        df.printSchema()
        df.show()

        # Simple data processing example
        processed_df = df.groupBy("category") \
            .agg(count("*").alias("count"),
                 count("value").alias("total_entries"),
                 col("category"))

        # Add a constant column to demonstrate more complex processing
        processed_df = processed_df.withColumn("data_source", lit("sample_processing"))

        # Ensure output directory exists
        logger.info(f"Creating output directory: {output_path}")
        os.makedirs(output_path, exist_ok=True)

        # Write output
        logger.info("Writing processed data")
        processed_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        logger.info(f"Processing complete. Results written to {output_path}")

        # Display results for debugging
        processed_df.show()

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error("Incorrect number of arguments")
        print("Usage: spark-submit sample_spark_app.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    main(input_path, output_path)
