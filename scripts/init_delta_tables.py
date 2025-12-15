"""
Initialize Delta Lake tables with proper schemas
Run this script once to create all Delta Lake tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake"""
    spark = SparkSession.builder \
        .appName("P16_Init_Delta_Tables") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
    return spark

def create_raw_stdf_table(spark):
    """Create raw STDF table"""
    schema = StructType([
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
    
    # Create empty DataFrame
    df = spark.createDataFrame([], schema)
    
    # Write to Delta Lake
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "device") \
        .save("s3a://delta-lake/tables/raw_stdf")
    
    logger.info("Created raw_stdf table")

def create_wafer_features_table(spark):
    """Create wafer-level features table"""
    schema = StructType([
        StructField("lot_id", StringType(), False),
        StructField("wafer_id", StringType(), False),
        StructField("device", StringType(), False),
        StructField("total_die", LongType(), True),
        StructField("wafer_width", LongType(), True),
        StructField("wafer_height", LongType(), True),
        StructField("passed_die", LongType(), True),
        StructField("failed_die", LongType(), True),
        StructField("wafer_yield", DoubleType(), True),
        StructField("edge_die_yield", DoubleType(), True),
        StructField("center_die_yield", DoubleType(), True),
        StructField("avg_test_time_ms", DoubleType(), True),
        StructField("parametric_mean_IDDQ", DoubleType(), True),
        StructField("parametric_std_IDDQ", DoubleType(), True),
        StructField("parametric_mean_VTH", DoubleType(), True),
        StructField("parametric_std_VTH", DoubleType(), True),
        StructField("parametric_mean_FREQ", DoubleType(), True),
        StructField("parametric_mean_VCCMIN", DoubleType(), True),
        StructField("parametric_mean_LEAKAGE", DoubleType(), True),
        StructField("parametric_mean_TEMP", DoubleType(), True),
        StructField("quadrant_q1_yield", DoubleType(), True),
        StructField("quadrant_q2_yield", DoubleType(), True),
        StructField("quadrant_q3_yield", DoubleType(), True),
        StructField("quadrant_q4_yield", DoubleType(), True),
        StructField("last_test_timestamp", TimestampType(), True),
        StructField("feature_timestamp", TimestampType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False)
    ])
    
    df = spark.createDataFrame([], schema)
    
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "device") \
        .save("s3a://delta-lake/tables/wafer_features")
    
    logger.info("Created wafer_features table")

def create_lot_features_table(spark):
    """Create lot-level features table"""
    schema = StructType([
        StructField("lot_id", StringType(), False),
        StructField("device", StringType(), False),
        StructField("total_wafers", LongType(), True),
        StructField("lot_avg_yield", DoubleType(), True),
        StructField("lot_yield_std", DoubleType(), True),
        StructField("lot_min_yield", DoubleType(), True),
        StructField("lot_max_yield", DoubleType(), True),
        StructField("lot_avg_edge_yield", DoubleType(), True),
        StructField("lot_avg_center_yield", DoubleType(), True),
        StructField("lot_total_die", LongType(), True),
        StructField("lot_passed_die", LongType(), True),
        StructField("lot_completion_time", TimestampType(), True),
        StructField("feature_timestamp", TimestampType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False)
    ])
    
    df = spark.createDataFrame([], schema)
    
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "device") \
        .save("s3a://delta-lake/tables/lot_features")
    
    logger.info("Created lot_features table")

def create_model_predictions_table(spark):
    """Create model predictions table for tracking all predictions"""
    schema = StructType([
        StructField("prediction_id", StringType(), False),
        StructField("model_name", StringType(), False),
        StructField("model_version", StringType(), False),
        StructField("lot_id", StringType(), True),
        StructField("wafer_id", StringType(), True),
        StructField("input_features", MapType(StringType(), DoubleType()), True),
        StructField("prediction", DoubleType(), False),
        StructField("confidence", DoubleType(), True),
        StructField("prediction_timestamp", TimestampType(), False),
        StructField("latency_ms", DoubleType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False)
    ])
    
    df = spark.createDataFrame([], schema)
    
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "model_name") \
        .save("s3a://delta-lake/tables/model_predictions")
    
    logger.info("Created model_predictions table")

def main():
    """Initialize all Delta Lake tables"""
    spark = create_spark_session()
    
    try:
        logger.info("Initializing Delta Lake tables...")
        
        create_raw_stdf_table(spark)
        create_wafer_features_table(spark)
        create_lot_features_table(spark)
        create_model_predictions_table(spark)
        
        logger.info("All Delta Lake tables initialized successfully")
        
        # Verify tables
        logger.info("\nVerifying tables:")
        tables = [
            "s3a://delta-lake/tables/raw_stdf",
            "s3a://delta-lake/tables/wafer_features",
            "s3a://delta-lake/tables/lot_features",
            "s3a://delta-lake/tables/model_predictions"
        ]
        
        for table_path in tables:
            count = spark.read.format("delta").load(table_path).count()
            logger.info(f"  {table_path}: {count} records")
        
    except Exception as e:
        logger.error(f"Failed to initialize tables: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
