# P16 Quick Start Guide

Get the P16 Enterprise ML Platform up and running in 10 minutes!

## Prerequisites Check

Before starting, ensure you have:

- [ ] **Docker Desktop 27.0+** installed and running
- [ ] **16GB RAM** allocated to Docker
- [ ] **50GB free disk space**
- [ ] **Internet connection** for downloading Docker images

## 1-Minute Quick Start

```bash
# Clone repository
git clone https://github.com/your-org/P16_Enterprise_ML_Data_Pipeline.git
cd P16_Enterprise_ML_Data_Pipeline

# Run automated setup
./startup.sh
```

The startup script will:
1. ✅ Check prerequisites
2. ✅ Start all Docker services
3. ✅ Initialize databases
4. ✅ Create Delta Lake tables
5. ✅ Generate synthetic test data
6. ✅ Compute features
7. ✅ Display access URLs

## Manual Setup (Alternative)

If you prefer step-by-step control:

### Step 1: Start Services (2 minutes)

```bash
# Create environment file
cp .env.template .env

# Start all services
docker-compose up -d

# Wait for services to initialize
sleep 30

# Check status
docker-compose ps
```

Expected: All services showing "Up" status

### Step 2: Initialize Airflow (1 minute)

```bash
# Initialize database
docker-compose exec airflow-webserver airflow db init

# Create admin user (password: admin)
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### Step 3: Create Delta Lake Tables (2 minutes)

```bash
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/scripts/init_delta_tables.py
```

Expected output: "All Delta Lake tables initialized successfully"

### Step 4: Generate Test Data (3 minutes)

```bash
# Generate 1000 synthetic STDF records
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/jobs/stdf_kafka_ingestion.py batch

# Compute features
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/jobs/feature_engineering.py
```

Expected: "Feature engineering completed successfully"

### Step 5: Verify Setup (1 minute)

```bash
# Run verification script
./verify.sh

# Or manually test API
curl -X POST "http://localhost:8000/predict/p01_xgboost" \
  -H "Content-Type: application/json" \
  -d '{"features": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88]}'
```

Expected: JSON response with prediction, confidence, and latency

## Access the Platform

Once running, access these URLs:

| Service | URL | Credentials |
|---------|-----|-------------|
| **FastAPI (Model Serving)** | http://localhost:8000/docs | - |
| **Kafka UI** | http://localhost:8080 | - |
| **Spark Master** | http://localhost:8081 | - |
| **Airflow** | http://localhost:8084 | admin/admin |
| **MLflow** | http://localhost:5000 | - |
| **Prometheus** | http://localhost:9090 | - |
| **Grafana** | http://localhost:3000 | admin/admin |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin |

## Your First Prediction

### Via Swagger UI (Easiest)

1. Open http://localhost:8000/docs
2. Expand `POST /predict/p01_xgboost`
3. Click "Try it out"
4. Use this example:
```json
{
  "features": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88],
  "metadata": {
    "lot_id": "TC41x_LOT123",
    "wafer_id": "W05"
  }
}
```
5. Click "Execute"

### Via curl

```bash
curl -X POST "http://localhost:8000/predict/p01_xgboost" \
  -H "Content-Type: application/json" \
  -d '{
    "features": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88],
    "metadata": {"lot_id": "TC41x_LOT123", "wafer_id": "W05"}
  }'
```

### Via Python

```python
import requests
import json

response = requests.post(
    "http://localhost:8000/predict/p01_xgboost",
    json={
        "features": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88],
        "metadata": {"lot_id": "TC41x_LOT123", "wafer_id": "W05"}
    }
)

print(json.dumps(response.json(), indent=2))
```

Expected response:
```json
{
  "prediction": 1,
  "confidence": 0.85,
  "model_name": "p01_xgboost_bin_predictor",
  "model_version": "Production/v1.0",
  "latency_ms": 15.3,
  "timestamp": "2024-12-05T10:30:00Z",
  "metadata": {
    "features_count": 11,
    "lot_id": "TC41x_LOT123",
    "wafer_id": "W05"
  }
}
```

## Query Delta Lake Features

Access Spark SQL shell:

```bash
docker-compose exec spark-master pyspark

# Query wafer features
>>> spark.sql("SELECT * FROM delta.`s3a://delta-lake/tables/wafer_features` LIMIT 5").show()

# Aggregate by device
>>> spark.sql("""
    SELECT device, 
           COUNT(*) as wafer_count,
           AVG(wafer_yield) as avg_yield,
           MIN(wafer_yield) as min_yield,
           MAX(wafer_yield) as max_yield
    FROM delta.`s3a://delta-lake/tables/wafer_features`
    GROUP BY device
    """).show()

# Exit
>>> exit()
```

## Trigger Model Training

### Via Airflow UI

1. Open http://localhost:8084 (admin/admin)
2. Find DAG: `p01_xgboost_model_training`
3. Toggle "Paused" to "Active"
4. Click "Trigger DAG" (play button)
5. Monitor execution in "Graph" view

### Via CLI

```bash
# List DAGs
docker-compose exec airflow-scheduler airflow dags list

# Trigger training
docker-compose exec airflow-scheduler airflow dags trigger p01_xgboost_model_training

# Monitor progress
docker-compose exec airflow-scheduler airflow dags list-runs -d p01_xgboost_model_training
```

## View MLflow Experiments

1. Open http://localhost:5000
2. Click "Experiments" in left sidebar
3. Select "p01_xgboost_bin_predictor"
4. Compare runs by:
   - Accuracy
   - F1 Score
   - Training time
5. Click a run to see:
   - Parameters (n_estimators, max_depth)
   - Metrics (train_accuracy, test_accuracy)
   - Artifacts (model files, plots)

## Check Monitoring Dashboards

### Prometheus Metrics

1. Open http://localhost:9090
2. Try these queries:
   - `prediction_latency_seconds` - API latency
   - `predictions_total` - Total predictions
   - `rate(predictions_total[5m])` - Prediction rate

### Grafana Dashboards

1. Open http://localhost:3000 (admin/admin)
2. Go to "Dashboards" > "Import"
3. Upload from `monitoring/grafana/dashboards/`
4. View real-time metrics:
   - FastAPI latency histogram
   - Prediction throughput
   - Error rates
   - Resource usage

## Common Commands

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f fastapi-server
docker-compose logs -f spark-master
docker-compose logs -f airflow-scheduler
```

### Restart Service

```bash
# Restart single service
docker-compose restart fastapi-server

# Restart all
docker-compose restart
```

### Stop Platform

```bash
# Stop all services (keep data)
docker-compose stop

# Stop and remove containers (keep volumes)
docker-compose down

# Stop and remove everything including volumes
docker-compose down -v
```

### Check Resource Usage

```bash
# Docker stats
docker stats

# Disk usage
docker system df

# Container resource usage
docker-compose ps
```

## Troubleshooting

### Services Not Starting

```bash
# Check Docker resources
docker system info | grep Memory

# Increase Docker memory to 16GB in Docker Desktop settings
# Restart Docker

# Clean and restart
docker-compose down -v
docker system prune -a
docker-compose up -d
```

### "Port already in use" Error

```bash
# Find process using port (e.g., 8000)
lsof -i :8000

# Kill process
kill -9 <PID>

# Or change port in docker-compose.yml
```

### Spark Jobs Failing

```bash
# Check Spark logs
docker-compose logs spark-master

# Check Spark UI
open http://localhost:8081

# Increase executor memory in docker-compose.yml
# SPARK_EXECUTOR_MEMORY=4g
```

### API Predictions Slow

```bash
# Check if models loaded
curl http://localhost:8000/models

# Check metrics
curl http://localhost:8000/metrics | grep prediction_latency

# Restart FastAPI to reload models
docker-compose restart fastapi-server
```

## Next Steps

### 1. Explore More Models

Try predictions with other models:
- `/predict/p02_resnet/yield` - Yield prediction
- `/predict/p04_unet/defect` - Defect classification
- `/predict/p06_lstm/anomaly` - Anomaly detection

### 2. Load Real Data

Replace synthetic data with actual STDF files:

```bash
# Copy your .std files
cp /path/to/your/stdf/*.std data/stdf-ingestion/

# Run ingestion
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/jobs/stdf_kafka_ingestion.py batch
```

### 3. Customize Features

Edit `spark-jobs/feature_engineering.py` to add domain-specific features.

### 4. Deploy to Cloud

Follow [MANUAL_TASKS.md](MANUAL_TASKS.md) for Databricks + AWS deployment.

### 5. Integrate with Your Applications

Use the FastAPI endpoints in your applications:
- Test automation systems
- Manufacturing execution systems (MES)
- Quality management systems
- Dashboards and reports

## Getting Help

- **Documentation**: [README.md](README.md), [PRD.md](PRD.md), [MANUAL_TASKS.md](MANUAL_TASKS.md)
- **API Docs**: http://localhost:8000/docs
- **Issues**: [GitHub Issues](https://github.com/your-org/P16_Enterprise_ML_Data_Pipeline/issues)
- **Slack**: #p16-mlops-platform
- **Email**: mlops-team@company.com

## Success Checklist

- [ ] All 12 Docker services running (`docker-compose ps`)
- [ ] FastAPI responds with prediction (`curl http://localhost:8000/health`)
- [ ] Delta Lake tables created (verify with Spark SQL)
- [ ] Synthetic data generated (check MinIO console)
- [ ] Features computed (query wafer_features table)
- [ ] Airflow DAGs visible (http://localhost:8084)
- [ ] MLflow experiments accessible (http://localhost:5000)
- [ ] Grafana dashboards loaded (http://localhost:3000)
- [ ] Prediction latency <100ms (check FastAPI Swagger UI)

**🎉 Congratulations! Your P16 platform is ready for production ML workloads!**
