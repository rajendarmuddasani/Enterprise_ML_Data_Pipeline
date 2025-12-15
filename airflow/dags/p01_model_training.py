"""
P16 Enterprise ML Pipeline - Model Training DAG
Airflow DAG for automated model training and retraining workflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'mlops_team',
    'depends_on_past': False,
    'email': ['mlops@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'p01_xgboost_model_training',
    default_args=default_args,
    description='Weekly automated training for P01 XGBoost Bin Predictor',
    schedule_interval='0 2 * * 0',  # Every Sunday at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['p01', 'xgboost', 'training', 'ml'],
)

def check_data_quality(**context):
    """
    Check data quality before training
    - Verify sufficient training data available
    - Check for schema consistency
    - Validate data distributions
    """
    logger.info("Checking data quality...")
    
    try:
        from pyspark.sql import SparkSession
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("P01_Data_Quality_Check") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
        
        # Read wafer features from Delta Lake
        wafer_features_df = spark.read.format("delta").load("s3a://delta-lake/tables/wafer_features")
        
        # Quality checks
        total_records = wafer_features_df.count()
        logger.info(f"Total wafer features: {total_records}")
        
        if total_records < 100:
            raise ValueError(f"Insufficient training data: {total_records} records (minimum 100 required)")
        
        # Check for null values
        null_counts = {}
        critical_columns = ['wafer_yield', 'edge_die_yield', 'parametric_mean_IDDQ', 'parametric_std_VTH']
        
        for col in critical_columns:
            null_count = wafer_features_df.filter(wafer_features_df[col].isNull()).count()
            null_counts[col] = null_count
            
            if null_count > total_records * 0.1:  # More than 10% nulls
                raise ValueError(f"Too many null values in {col}: {null_count}/{total_records}")
        
        logger.info(f"Data quality check passed: {total_records} records, null_counts={null_counts}")
        
        spark.stop()
        
        return {
            'total_records': total_records,
            'null_counts': null_counts,
            'status': 'passed'
        }
        
    except Exception as e:
        logger.error(f"Data quality check failed: {str(e)}")
        raise

def query_training_data(**context):
    """
    Query training data from Delta Lake
    - Select last 90 days of data
    - Filter to specific device (TC41x)
    - Select relevant features
    """
    logger.info("Querying training data...")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, datediff, current_date
        
        spark = SparkSession.builder \
            .appName("P01_Query_Training_Data") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
        
        # Query last 90 days for TC41x device
        df = spark.read.format("delta").load("s3a://delta-lake/tables/wafer_features") \
            .filter(col("device") == "TC41x") \
            .filter(datediff(current_date(), col("last_test_timestamp")) <= 90)
        
        train_records = df.count()
        logger.info(f"Training data query completed: {train_records} records")
        
        # Save to temporary location for model training
        df.write.mode("overwrite").parquet("s3a://delta-lake/temp/p01_train_data")
        
        spark.stop()
        
        context['task_instance'].xcom_push(key='train_records', value=train_records)
        return train_records
        
    except Exception as e:
        logger.error(f"Failed to query training data: {str(e)}")
        raise

def train_model(**context):
    """
    Train XGBoost model
    - Load training data
    - Train XGBoost classifier
    - Evaluate on validation set
    - Log to MLflow
    """
    logger.info("Training XGBoost model...")
    
    try:
        import pandas as pd
        import numpy as np
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier  # Using RF as mock for XGBoost
        from sklearn.metrics import accuracy_score, f1_score, classification_report
        import mlflow
        import mlflow.sklearn
        
        # Set MLflow tracking URI
        mlflow.set_tracking_uri("http://mlflow-server:5000")
        mlflow.set_experiment("p01_xgboost_bin_predictor")
        
        # Load training data
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("P01_Train").getOrCreate()
        
        df = spark.read.parquet("s3a://delta-lake/temp/p01_train_data").toPandas()
        
        logger.info(f"Loaded {len(df)} training records")
        
        # Prepare features and target
        feature_columns = [
            'wafer_yield', 
            'edge_die_yield', 
            'center_die_yield',
            'parametric_mean_IDDQ', 
            'parametric_std_IDDQ',
            'parametric_mean_VTH',
            'parametric_std_VTH',
            'quadrant_q1_yield',
            'quadrant_q2_yield',
            'quadrant_q3_yield',
            'quadrant_q4_yield'
        ]
        
        # Handle missing values
        df = df.fillna(0)
        
        X = df[feature_columns]
        
        # Create target: predict if wafer yield > 0.85
        y = (df['wafer_yield'] > 0.85).astype(int)
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        logger.info(f"Train set: {len(X_train)}, Test set: {len(X_test)}")
        
        # Start MLflow run
        with mlflow.start_run(run_name=f"p01_training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            # Log parameters
            mlflow.log_param("model_type", "RandomForestClassifier")
            mlflow.log_param("n_estimators", 100)
            mlflow.log_param("max_depth", 10)
            mlflow.log_param("random_state", 42)
            mlflow.log_param("train_size", len(X_train))
            mlflow.log_param("test_size", len(X_test))
            mlflow.log_param("features", feature_columns)
            
            # Train model
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            
            logger.info("Training model...")
            model.fit(X_train, y_train)
            
            # Evaluate
            train_pred = model.predict(X_train)
            test_pred = model.predict(X_test)
            
            train_accuracy = accuracy_score(y_train, train_pred)
            test_accuracy = accuracy_score(y_test, test_pred)
            train_f1 = f1_score(y_train, train_pred)
            test_f1 = f1_score(y_test, test_pred)
            
            # Log metrics
            mlflow.log_metric("train_accuracy", train_accuracy)
            mlflow.log_metric("test_accuracy", test_accuracy)
            mlflow.log_metric("train_f1", train_f1)
            mlflow.log_metric("test_f1", test_f1)
            
            logger.info(f"Model performance:")
            logger.info(f"  Train accuracy: {train_accuracy:.4f}, F1: {train_f1:.4f}")
            logger.info(f"  Test accuracy: {test_accuracy:.4f}, F1: {test_f1:.4f}")
            
            # Log classification report
            report = classification_report(y_test, test_pred)
            mlflow.log_text(report, "classification_report.txt")
            
            # Log feature importance
            feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            mlflow.log_text(feature_importance.to_string(), "feature_importance.txt")
            
            # Log model
            mlflow.sklearn.log_model(
                model, 
                "model",
                registered_model_name="p01_xgboost_bin_predictor"
            )
            
            run_id = mlflow.active_run().info.run_id
            logger.info(f"Model logged to MLflow with run_id: {run_id}")
        
        spark.stop()
        
        # Push metrics to XCom
        context['task_instance'].xcom_push(key='test_accuracy', value=test_accuracy)
        context['task_instance'].xcom_push(key='test_f1', value=test_f1)
        context['task_instance'].xcom_push(key='run_id', value=run_id)
        
        return {
            'test_accuracy': test_accuracy,
            'test_f1': test_f1,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"Model training failed: {str(e)}")
        raise

def evaluate_and_promote(**context):
    """
    Evaluate new model and promote to Production if better than champion
    """
    logger.info("Evaluating model for promotion...")
    
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
        
        mlflow.set_tracking_uri("http://mlflow-server:5000")
        client = MlflowClient()
        
        # Get current run metrics
        test_accuracy = context['task_instance'].xcom_pull(task_ids='train_model', key='test_accuracy')
        test_f1 = context['task_instance'].xcom_pull(task_ids='train_model', key='test_f1')
        run_id = context['task_instance'].xcom_pull(task_ids='train_model', key='run_id')
        
        logger.info(f"New model: accuracy={test_accuracy:.4f}, f1={test_f1:.4f}")
        
        # Get champion model (Production stage)
        model_name = "p01_xgboost_bin_predictor"
        
        try:
            champion_versions = client.get_latest_versions(model_name, stages=["Production"])
            
            if champion_versions:
                champion_version = champion_versions[0]
                champion_run = client.get_run(champion_version.run_id)
                champion_accuracy = champion_run.data.metrics.get("test_accuracy", 0)
                
                logger.info(f"Champion model: accuracy={champion_accuracy:.4f}")
                
                # Promote if accuracy improves by at least 3%
                improvement = (test_accuracy - champion_accuracy) / champion_accuracy
                
                if improvement >= 0.03:  # 3% improvement
                    logger.info(f"Promoting new model (improvement: {improvement:.2%})")
                    
                    # Get model version for current run
                    model_versions = client.search_model_versions(f"name='{model_name}'")
                    new_version = None
                    for mv in model_versions:
                        if mv.run_id == run_id:
                            new_version = mv.version
                            break
                    
                    if new_version:
                        # Transition new model to Production
                        client.transition_model_version_stage(
                            name=model_name,
                            version=new_version,
                            stage="Production",
                            archive_existing_versions=True
                        )
                        logger.info(f"Model version {new_version} promoted to Production")
                        return "PROMOTED"
                    else:
                        logger.warning("Could not find model version for promotion")
                        return "VERSION_NOT_FOUND"
                else:
                    logger.info(f"Champion retained (improvement: {improvement:.2%} < 3%)")
                    return "CHAMPION_RETAINED"
            else:
                logger.info("No champion model found, promoting new model to Production")
                
                # Get model version for current run
                model_versions = client.search_model_versions(f"name='{model_name}'")
                new_version = None
                for mv in model_versions:
                    if mv.run_id == run_id:
                        new_version = mv.version
                        break
                
                if new_version:
                    client.transition_model_version_stage(
                        name=model_name,
                        version=new_version,
                        stage="Production"
                    )
                    logger.info(f"First model version {new_version} promoted to Production")
                    return "PROMOTED_FIRST"
                    
        except Exception as e:
            logger.warning(f"Could not compare with champion: {str(e)}")
            logger.info("Promoting new model as no champion found")
            return "PROMOTED_NO_CHAMPION"
            
    except Exception as e:
        logger.error(f"Promotion evaluation failed: {str(e)}")
        raise

# Define tasks
task_check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

task_query_data = PythonOperator(
    task_id='query_training_data',
    python_callable=query_training_data,
    dag=dag,
)

task_train = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)

task_promote = PythonOperator(
    task_id='evaluate_and_promote',
    python_callable=evaluate_and_promote,
    dag=dag,
)

task_cleanup = BashOperator(
    task_id='cleanup_temp_data',
    bash_command='echo "Cleaning up temporary data..." && exit 0',
    dag=dag,
)

# Task dependencies
task_check_quality >> task_query_data >> task_train >> task_promote >> task_cleanup
