"""
P16 Enterprise ML Pipeline - STDF Parser and Kafka Ingestion
Spark Structured Streaming job to parse STDF files from Kafka and write to Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Delta Lake schemas
RAW_STDF_SCHEMA = StructType([
    StructField("stdf_filename", StringType(), False),
    StructField("lot_id", StringType(), False),
    StructField("wafer_id", StringType(), False),
    StructField("device", StringType(), False),
    StructField("test_program", StringType(), True),
    StructField("test_timestamp", TimestampType(), False),
    StructField("die_x", IntegerType(), False),
    StructField("die_y", IntegerType(), False),
    StructField("bin", IntegerType(), False),
    StructField("hbin", IntegerType(), False),
    StructField("soft_bin", IntegerType(), True),
    StructField("site_num", IntegerType(), True),
    StructField("test_time_ms", FloatType(), True),
    StructField("parametric_tests", MapType(StringType(), FloatType()), True),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False)
])

def create_spark_session():
    """Create Spark session with Delta Lake configuration"""
    spark = SparkSession.builder \
        .appName("P16_STDF_Kafka_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark

@udf(returnType=StructType([
    StructField("lot_id", StringType()),
    StructField("wafer_id", StringType()),
    StructField("device", StringType()),
    StructField("test_program", StringType()),
    StructField("test_timestamp", TimestampType()),
    StructField("die_x", IntegerType()),
    StructField("die_y", IntegerType()),
    StructField("bin", IntegerType()),
    StructField("hbin", IntegerType()),
    StructField("soft_bin", IntegerType()),
    StructField("site_num", IntegerType()),
    StructField("test_time_ms", FloatType()),
    StructField("parametric_tests", MapType(StringType(), FloatType()))
]))
def parse_stdf_simple(stdf_bytes):
    """
    Simplified STDF parser for demo purposes
    In production, use pystdf library to parse binary STDF format
    
    For now, generates synthetic data to simulate STDF parsing
    """
    import random
    import datetime
    
    # Generate synthetic STDF data
    lot_id = f"TC41x_LOT{random.randint(100, 999)}"
    wafer_id = f"W{random.randint(1, 25):02d}"
    device = random.choice(["TC41x", "TC43x", "TC45x"])
    test_program = "FT_V2.3"
    test_timestamp = datetime.datetime.now()
    die_x = random.randint(0, 50)
    die_y = random.randint(0, 50)
    bin_num = random.choices([1, 2, 3, 4, 5], weights=[0.85, 0.05, 0.03, 0.04, 0.03])[0]
    hbin = bin_num
    soft_bin = bin_num * 10
    site_num = random.randint(1, 4)
    test_time_ms = random.uniform(50, 200)
    
    # Generate parametric test results (typical tests)
    parametric_tests = {
        "IDDQ": random.gauss(1.2, 0.15),
        "VTH": random.gauss(0.7, 0.05),
        "FREQ": random.gauss(2400, 50),
        "VCCMIN": random.gauss(1.0, 0.02),
        "LEAKAGE": random.gauss(0.5, 0.1),
        "TEMP": random.gauss(85, 2)
    }
    
    return {
        "lot_id": lot_id,
        "wafer_id": wafer_id,
        "device": device,
        "test_program": test_program,
        "test_timestamp": test_timestamp,
        "die_x": die_x,
        "die_y": die_y,
        "bin": bin_num,
        "hbin": hbin,
        "soft_bin": soft_bin,
        "site_num": site_num,
        "test_time_ms": test_time_ms,
        "parametric_tests": parametric_tests
    }

def process_kafka_stream(spark, kafka_bootstrap_servers, checkpoint_location, delta_table_path):
    """
    Process STDF files from Kafka topic and write to Delta Lake
    """
    logger.info(f"Starting Kafka stream processing from {kafka_bootstrap_servers}")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "stdf_ingestion") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()
    
    logger.info("Kafka stream configured successfully")
    
    # Parse STDF data
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("stdf_filename"),
        parse_stdf_simple(col("value")).alias("stdf_data"),
        current_timestamp().alias("ingestion_timestamp")
    )
    
    # Flatten struct and add partitioning columns
    final_df = parsed_df.select(
        col("stdf_filename"),
        col("stdf_data.lot_id").alias("lot_id"),
        col("stdf_data.wafer_id").alias("wafer_id"),
        col("stdf_data.device").alias("device"),
        col("stdf_data.test_program").alias("test_program"),
        col("stdf_data.test_timestamp").alias("test_timestamp"),
        col("stdf_data.die_x").alias("die_x"),
        col("stdf_data.die_y").alias("die_y"),
        col("stdf_data.bin").alias("bin"),
        col("stdf_data.hbin").alias("hbin"),
        col("stdf_data.soft_bin").alias("soft_bin"),
        col("stdf_data.site_num").alias("site_num"),
        col("stdf_data.test_time_ms").alias("test_time_ms"),
        col("stdf_data.parametric_tests").alias("parametric_tests"),
        col("ingestion_timestamp"),
        year(col("ingestion_timestamp")).alias("year"),
        month(col("ingestion_timestamp")).alias("month"),
        dayofmonth(col("ingestion_timestamp")).alias("day")
    )
    
    # Write to Delta Lake with partitioning
    query = final_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .partitionBy("year", "month", "day", "device") \
        .start(delta_table_path)
    
    logger.info(f"Streaming query started, writing to {delta_table_path}")
    
    return query

def batch_process_stdf_files(spark, input_path, delta_table_path):
    """
    Batch process existing STDF files (for initial load or backfill)
    """
    logger.info(f"Starting batch processing from {input_path}")
    
    # For demo, create synthetic data
    # In production, read actual STDF files using pystdf
    
    from pyspark.sql import Row
    import random
    import datetime
    
    # Generate 1000 synthetic STDF records
    data = []
    for i in range(1000):
        lot_id = f"TC41x_LOT{random.randint(100, 999)}"
        wafer_id = f"W{random.randint(1, 25):02d}"
        device = random.choice(["TC41x", "TC43x", "TC45x"])
        
        for die_idx in range(random.randint(500, 1000)):  # 500-1000 die per wafer
            die_x = random.randint(0, 50)
            die_y = random.randint(0, 50)
            bin_num = random.choices([1, 2, 3, 4, 5], weights=[0.85, 0.05, 0.03, 0.04, 0.03])[0]
            
            parametric_tests = {
                "IDDQ": random.gauss(1.2, 0.15),
                "VTH": random.gauss(0.7, 0.05),
                "FREQ": random.gauss(2400, 50),
                "VCCMIN": random.gauss(1.0, 0.02),
                "LEAKAGE": random.gauss(0.5, 0.1),
                "TEMP": random.gauss(85, 2)
            }
            
            timestamp = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
            
            data.append(Row(
                stdf_filename=f"stdf_{i:04d}.std",
                lot_id=lot_id,
                wafer_id=wafer_id,
                device=device,
                test_program="FT_V2.3",
                test_timestamp=timestamp,
                die_x=die_x,
                die_y=die_y,
                bin=bin_num,
                hbin=bin_num,
                soft_bin=bin_num * 10,
                site_num=random.randint(1, 4),
                test_time_ms=random.uniform(50, 200),
                parametric_tests=parametric_tests,
                ingestion_timestamp=datetime.datetime.now(),
                year=timestamp.year,
                month=timestamp.month,
                day=timestamp.day
            ))
    
    # Create DataFrame
    df = spark.createDataFrame(data)
    
    # Write to Delta Lake
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month", "day", "device") \
        .save(delta_table_path)
    
    logger.info(f"Batch processing completed: {len(data)} records written")
    
    return len(data)

def main():
    """Main entry point"""
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "kafka-broker:29092"
    CHECKPOINT_LOCATION = "s3a://delta-lake/checkpoints/raw_stdf"
    DELTA_TABLE_PATH = "s3a://delta-lake/tables/raw_stdf"
    
    # Create Spark session
    spark = create_spark_session()
    
    # Check if running in streaming or batch mode
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    
    try:
        if mode == "streaming":
            # Start streaming ingestion from Kafka
            logger.info("Starting streaming mode")
            query = process_kafka_stream(
                spark, 
                KAFKA_BOOTSTRAP_SERVERS, 
                CHECKPOINT_LOCATION, 
                DELTA_TABLE_PATH
            )
            
            # Wait for termination
            query.awaitTermination()
            
        else:
            # Batch processing for initial load
            logger.info("Starting batch mode")
            input_path = sys.argv[2] if len(sys.argv) > 2 else "/app/data/stdf"
            records_written = batch_process_stdf_files(
                spark, 
                input_path, 
                DELTA_TABLE_PATH
            )
            logger.info(f"Batch processing completed successfully: {records_written} records")
    
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
