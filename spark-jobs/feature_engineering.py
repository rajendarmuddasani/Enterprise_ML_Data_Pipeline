"""
P16 Enterprise ML Pipeline - Feature Engineering
Spark job to compute wafer-level features from raw STDF data and write to Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake configuration"""
    spark = SparkSession.builder \
        .appName("P16_Feature_Engineering") \
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

def compute_wafer_features(spark, raw_stdf_path, output_path):
    """
    Compute wafer-level aggregate features from die-level STDF data
    
    Features computed:
    - Wafer yield (overall and by bin)
    - Edge die yield
    - Parametric test statistics (mean, std, min, max, percentiles)
    - Spatial pattern features
    - Test correlation metrics
    """
    logger.info(f"Computing wafer features from {raw_stdf_path}")
    
    # Read raw STDF data from Delta Lake
    raw_df = spark.read.format("delta").load(raw_stdf_path)
    
    logger.info(f"Loaded {raw_df.count()} raw STDF records")
    
    # ===== 1. Basic Wafer-Level Aggregations =====
    wafer_basic = raw_df.groupBy("lot_id", "wafer_id", "device").agg(
        count("*").alias("total_die"),
        countDistinct("die_x").alias("wafer_width"),
        countDistinct("die_y").alias("wafer_height"),
        count(when(col("bin") == 1, 1)).alias("passed_die"),
        count(when(col("bin") != 1, 1)).alias("failed_die"),
        (count(when(col("bin") == 1, 1)) / count("*")).alias("wafer_yield"),
        avg("test_time_ms").alias("avg_test_time_ms"),
        max("test_timestamp").alias("last_test_timestamp")
    )
    
    # ===== 2. Bin Distribution =====
    bin_dist = raw_df.groupBy("lot_id", "wafer_id", "device", "bin").agg(
        count("*").alias("bin_count")
    ).groupBy("lot_id", "wafer_id", "device").agg(
        collect_list(struct("bin", "bin_count")).alias("bin_distribution")
    )
    
    # ===== 3. Edge Die Yield =====
    # Define edge as outer 2 units in x and y
    edge_window = Window.partitionBy("lot_id", "wafer_id")
    
    edge_df = raw_df.withColumn("max_x", max("die_x").over(edge_window)) \
        .withColumn("max_y", max("die_y").over(edge_window)) \
        .withColumn("is_edge",
            when(
                (col("die_x") < 2) | (col("die_x") > col("max_x") - 2) |
                (col("die_y") < 2) | (col("die_y") > col("max_y") - 2),
                1
            ).otherwise(0)
        )
    
    edge_features = edge_df.groupBy("lot_id", "wafer_id", "device").agg(
        count(when((col("is_edge") == 1) & (col("bin") == 1), 1)).alias("edge_passed"),
        count(when(col("is_edge") == 1, 1)).alias("edge_total"),
        (count(when((col("is_edge") == 1) & (col("bin") == 1), 1)) / 
         count(when(col("is_edge") == 1, 1))).alias("edge_die_yield"),
        (count(when((col("is_edge") == 0) & (col("bin") == 1), 1)) / 
         count(when(col("is_edge") == 0, 1))).alias("center_die_yield")
    )
    
    # ===== 4. Parametric Statistics =====
    # Extract parametric tests from map column
    parametric_cols = ["IDDQ", "VTH", "FREQ", "VCCMIN", "LEAKAGE", "TEMP"]
    
    parametric_df = raw_df
    for param in parametric_cols:
        parametric_df = parametric_df.withColumn(
            f"test_{param}",
            col("parametric_tests").getItem(param)
        )
    
    parametric_stats = parametric_df.groupBy("lot_id", "wafer_id", "device").agg(
        *[mean(f"test_{param}").alias(f"parametric_mean_{param}") for param in parametric_cols],
        *[stddev(f"test_{param}").alias(f"parametric_std_{param}") for param in parametric_cols],
        *[min(f"test_{param}").alias(f"parametric_min_{param}") for param in parametric_cols],
        *[max(f"test_{param}").alias(f"parametric_max_{param}") for param in parametric_cols],
        *[expr(f"percentile_approx(test_{param}, 0.05)").alias(f"parametric_p5_{param}") for param in parametric_cols],
        *[expr(f"percentile_approx(test_{param}, 0.95)").alias(f"parametric_p95_{param}") for param in parametric_cols]
    )
    
    # ===== 5. Spatial Pattern Features =====
    # Compute quadrant yields (divide wafer into 4 quadrants)
    spatial_df = raw_df.withColumn("max_x", max("die_x").over(edge_window)) \
        .withColumn("max_y", max("die_y").over(edge_window)) \
        .withColumn("quadrant",
            when((col("die_x") <= col("max_x") / 2) & (col("die_y") <= col("max_y") / 2), "Q1")
            .when((col("die_x") > col("max_x") / 2) & (col("die_y") <= col("max_y") / 2), "Q2")
            .when((col("die_x") <= col("max_x") / 2) & (col("die_y") > col("max_y") / 2), "Q3")
            .otherwise("Q4")
        )
    
    quadrant_yields = spatial_df.groupBy("lot_id", "wafer_id", "device", "quadrant").agg(
        (count(when(col("bin") == 1, 1)) / count("*")).alias("yield")
    ).groupBy("lot_id", "wafer_id", "device").pivot("quadrant").agg(
        first("yield")
    ).withColumnRenamed("Q1", "quadrant_q1_yield") \
     .withColumnRenamed("Q2", "quadrant_q2_yield") \
     .withColumnRenamed("Q3", "quadrant_q3_yield") \
     .withColumnRenamed("Q4", "quadrant_q4_yield")
    
    # ===== 6. Combine All Features =====
    wafer_features = wafer_basic \
        .join(bin_dist, ["lot_id", "wafer_id", "device"], "left") \
        .join(edge_features, ["lot_id", "wafer_id", "device"], "left") \
        .join(parametric_stats, ["lot_id", "wafer_id", "device"], "left") \
        .join(quadrant_yields, ["lot_id", "wafer_id", "device"], "left")
    
    # Add metadata
    wafer_features = wafer_features \
        .withColumn("feature_timestamp", current_timestamp()) \
        .withColumn("year", year(col("last_test_timestamp"))) \
        .withColumn("month", month(col("last_test_timestamp"))) \
        .withColumn("day", dayofmonth(col("last_test_timestamp")))
    
    # Write to Delta Lake
    wafer_features.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "device") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    feature_count = wafer_features.count()
    logger.info(f"Feature engineering completed: {feature_count} wafer-level features written to {output_path}")
    
    return feature_count

def compute_lot_features(spark, wafer_features_path, output_path):
    """
    Compute lot-level aggregate features from wafer-level features
    """
    logger.info(f"Computing lot features from {wafer_features_path}")
    
    # Read wafer features
    wafer_df = spark.read.format("delta").load(wafer_features_path)
    
    # Compute lot-level aggregations
    lot_features = wafer_df.groupBy("lot_id", "device").agg(
        count("*").alias("total_wafers"),
        avg("wafer_yield").alias("lot_avg_yield"),
        stddev("wafer_yield").alias("lot_yield_std"),
        min("wafer_yield").alias("lot_min_yield"),
        max("wafer_yield").alias("lot_max_yield"),
        avg("edge_die_yield").alias("lot_avg_edge_yield"),
        avg("center_die_yield").alias("lot_avg_center_yield"),
        sum("total_die").alias("lot_total_die"),
        sum("passed_die").alias("lot_passed_die"),
        max("last_test_timestamp").alias("lot_completion_time")
    )
    
    # Add metadata
    lot_features = lot_features \
        .withColumn("feature_timestamp", current_timestamp()) \
        .withColumn("year", year(col("lot_completion_time"))) \
        .withColumn("month", month(col("lot_completion_time"))) \
        .withColumn("day", dayofmonth(col("lot_completion_time")))
    
    # Write to Delta Lake
    lot_features.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "device") \
        .save(output_path)
    
    lot_count = lot_features.count()
    logger.info(f"Lot features computed: {lot_count} lots")
    
    return lot_count

def main():
    """Main entry point"""
    # Configuration
    RAW_STDF_PATH = "s3a://delta-lake/tables/raw_stdf"
    WAFER_FEATURES_PATH = "s3a://delta-lake/tables/wafer_features"
    LOT_FEATURES_PATH = "s3a://delta-lake/tables/lot_features"
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Compute wafer-level features
        logger.info("Starting wafer feature engineering...")
        wafer_count = compute_wafer_features(spark, RAW_STDF_PATH, WAFER_FEATURES_PATH)
        
        # Compute lot-level features
        logger.info("Starting lot feature engineering...")
        lot_count = compute_lot_features(spark, WAFER_FEATURES_PATH, LOT_FEATURES_PATH)
        
        logger.info(f"Feature engineering completed successfully:")
        logger.info(f"  - Wafer features: {wafer_count}")
        logger.info(f"  - Lot features: {lot_count}")
        
    except Exception as e:
        logger.error(f"Feature engineering failed: {str(e)}")
        raise
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
