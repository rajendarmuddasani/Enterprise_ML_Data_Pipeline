# Manual Tasks and Configuration Guide

**Last Updated**: December 7, 2025

## 📌 Quick Navigation

**Looking for next steps?** → Read **[NEXT_STEPS.md](NEXT_STEPS.md)** for immediate actions  
**Need complete context?** → Read **[PROJECT_STATE.md](PROJECT_STATE.md)** for full project state  
**Want to test locally first?** → See **[QUICKSTART.md](QUICKSTART.md)** for 10-minute setup

---

## Overview
This document lists all manual configurations, settings, and tasks needed to deploy the P16 Enterprise ML Data Pipeline Platform.

## Prerequisites

### 1. System Requirements
- **Docker Desktop**: Version 27.0+ with at least 16GB RAM allocated
- **Python**: Version 3.11+
- **Git**: Version 2.40+
- **Disk Space**: Minimum 50GB free space (for Docker volumes and data)

### 2. Software Installation

#### Install Docker Desktop
```bash
# macOS
brew install --cask docker

# Verify installation
docker --version
docker-compose --version
```

#### Install Python Dependencies
```bash
# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install poetry for dependency management
pip install poetry

# Install project dependencies
poetry install
```

## Local Development Setup

### 1. Environment Configuration

Create `.env` file in the project root:
```bash
# Copy template
cp .env.template .env

# Edit .env and configure:
# - AWS credentials (if using S3)
# - Databricks token (for cloud deployment)
# - Email settings (for Airflow alerts)
```

**Required Environment Variables:**
- `AWS_ACCESS_KEY_ID` - AWS access key (for S3 storage)
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_DEFAULT_REGION` - AWS region (e.g., us-west-2)
- `DATABRICKS_HOST` - Databricks workspace URL
- `DATABRICKS_TOKEN` - Databricks personal access token
- `SMTP_HOST` - Email server for alerts
- `SMTP_PORT` - SMTP port (usually 587)
- `SMTP_USER` - Email username
- `SMTP_PASSWORD` - Email password
- `ALERT_EMAIL` - Email address for alerts

### 2. Start Docker Infrastructure

```bash
# Start all services
docker-compose up -d

# Verify all services are running
docker-compose ps

# View logs
docker-compose logs -f
```

**Service URLs (Local):**
- Kafka UI: http://localhost:8080
- Spark Master UI: http://localhost:8081
- Airflow UI: http://localhost:8082 (admin/admin)
- MLflow UI: http://localhost:5000
- FastAPI Swagger: http://localhost:8000/docs
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

### 3. Initialize Databases

```bash
# Initialize Airflow database
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Initialize MLflow backend
docker-compose exec mlflow-server python -c "
from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore
store = SqlAlchemyStore('postgresql://mlflow:mlflow@postgres:5432/mlflow')
"
```

### 4. Create Delta Lake Tables

```bash
# Run initialization script
docker-compose exec spark-master python /app/scripts/init_delta_tables.py

# Verify tables created
docker-compose exec spark-master pyspark
>>> spark.sql("SHOW TABLES").show()
```

## Cloud Deployment (Databricks + AWS)

### 1. AWS Setup

#### Create S3 Buckets
```bash
# Install AWS CLI
brew install awscli

# Configure AWS credentials
aws configure

# Create S3 buckets
aws s3 mb s3://p16-delta-lake-prod
aws s3 mb s3://p16-mlflow-artifacts
aws s3 mb s3://p16-airflow-logs
aws s3 mb s3://p16-stdf-raw

# Set lifecycle policies (optional - for cost optimization)
aws s3api put-bucket-lifecycle-configuration \
    --bucket p16-stdf-raw \
    --lifecycle-configuration file://config/s3-lifecycle.json
```

#### Create IAM Roles
```bash
# Create Databricks access role
aws iam create-role --role-name P16-Databricks-Access \
    --assume-role-policy-document file://config/databricks-trust-policy.json

# Attach S3 access policy
aws iam attach-role-policy --role-name P16-Databricks-Access \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### 2. Databricks Workspace Setup

#### Create Databricks Account
1. Go to https://accounts.cloud.databricks.com/registration.html
2. Sign up for Community Edition (free) or Trial ($0 for 14 days)
3. Create workspace in preferred region (us-west-2 recommended)

#### Configure Databricks CLI
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Enter Databricks host: https://<your-workspace>.cloud.databricks.com
# Enter token: <generate from User Settings > Access Tokens>
```

#### Create Databricks Cluster
```bash
# Create cluster via CLI
databricks clusters create --json-file config/databricks-cluster-config.json

# OR manually via UI:
# - Go to Compute > Create Cluster
# - Cluster name: P16-Production
# - Cluster mode: Standard
# - Databricks runtime: 14.3 LTS (includes Apache Spark 3.5.0)
# - Worker type: m5.large (14 GB Memory, 2 Cores)
# - Workers: 2-8 (Enable autoscaling)
# - Driver type: Same as worker
# - Auto Termination: 120 minutes
```

#### Install Libraries on Cluster
```bash
# Via UI: Compute > <cluster> > Libraries > Install New
# - PyPI: delta-spark==3.0.0
# - PyPI: mlflow==2.12.0
# - PyPI: pystdf==1.4.0
# - PyPI: fastapi==0.110.0
# - PyPI: prometheus-client==0.20.0

# Or via CLI
databricks libraries install --cluster-id <cluster-id> --pypi-package delta-spark==3.0.0
```

### 3. MLflow on Databricks

#### Configure MLflow Tracking
```bash
# MLflow is pre-installed on Databricks
# Set tracking URI in notebooks/jobs:
import mlflow
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment("/Users/<your-email>/p16-experiments")
```

#### Create Model Registry Models
```bash
# Run this in Databricks notebook
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Create model entries for all P01-P15 projects
models = [
    "p01_xgboost_bin_predictor",
    "p02_resnet_yield_predictor",
    "p03_multi_agent_rca",
    "p04_unet_wafer_defect",
    "p05_amsa_ai_rca",
    "p06_lstm_anomaly_detector",
    "p07_gan_synthetic_data",
    "p08_xgboost_limit_change",
    "p09_transfer_learning_field_failure",
    "p10_gnn_failure_propagation",
    "p11_multi_agent_rl_optimizer",
    "p12_edge_ai_validation",
    "p13_dqn_adaptive_test_flow",
    "p14_transformer_scan_localizer",
    "p15_bayesian_shmoo_optimizer"
]

for model_name in models:
    try:
        client.create_registered_model(model_name)
        print(f"Created {model_name}")
    except Exception as e:
        print(f"Model {model_name} already exists or error: {e}")
```

### 4. Airflow Cloud Deployment

#### Option A: Deploy Airflow on Kubernetes (EKS)
```bash
# Install kubectl and helm
brew install kubectl helm

# Create EKS cluster
eksctl create cluster \
    --name p16-airflow \
    --region us-west-2 \
    --nodegroup-name standard-workers \
    --node-type t3.medium \
    --nodes 3 \
    --nodes-min 2 \
    --nodes-max 5 \
    --managed

# Add Airflow Helm repository
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Install Airflow
helm install airflow apache-airflow/airflow \
    --namespace airflow \
    --create-namespace \
    --values config/airflow-helm-values.yaml

# Get Airflow UI URL
kubectl get svc -n airflow
```

#### Option B: Use Managed Airflow (AWS MWAA)
```bash
# Create MWAA environment via AWS Console or CloudFormation
# Cost: ~$0.49/hour = ~$350/month (over budget, use EKS instead)
```

### 5. FastAPI Deployment on Kubernetes

```bash
# Build Docker image
docker build -t p16-fastapi:latest -f docker/Dockerfile.fastapi .

# Push to ECR
aws ecr create-repository --repository-name p16-fastapi
aws ecr get-login-password --region us-west-2 | \
    docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-west-2.amazonaws.com
docker tag p16-fastapi:latest <account-id>.dkr.ecr.us-west-2.amazonaws.com/p16-fastapi:latest
docker push <account-id>.dkr.ecr.us-west-2.amazonaws.com/p16-fastapi:latest

# Deploy to EKS
kubectl apply -f k8s/fastapi-deployment.yaml
kubectl apply -f k8s/fastapi-service.yaml
kubectl apply -f k8s/fastapi-hpa.yaml

# Get FastAPI URL
kubectl get svc p16-fastapi -n default
```

## Monitoring Setup

### 1. Prometheus Configuration
```bash
# Prometheus is already running in Docker Compose
# Add custom scrape targets in config/prometheus.yml

# For cloud deployment, deploy Prometheus to Kubernetes
kubectl apply -f k8s/prometheus-deployment.yaml
kubectl apply -f k8s/prometheus-service.yaml
```

### 2. Grafana Dashboard Import
```bash
# Access Grafana at http://localhost:3000 (admin/admin)

# Import dashboards:
# 1. Go to Dashboards > Import
# 2. Upload JSON files from monitoring/grafana-dashboards/
#    - kafka-metrics.json
#    - spark-metrics.json
#    - airflow-metrics.json
#    - fastapi-metrics.json
#    - mlflow-metrics.json

# 3. Configure Prometheus data source:
#    - Name: Prometheus
#    - URL: http://prometheus:9090 (local) or http://prometheus.monitoring:9090 (k8s)
```

### 3. Alert Configuration
```bash
# Edit monitoring/prometheus/alerts.yml to customize alert rules
# Configure AlertManager in monitoring/prometheus/alertmanager.yml

# Add PagerDuty integration:
# - Get integration key from PagerDuty
# - Add to alertmanager.yml receivers section

# Add Slack integration:
# - Create Slack webhook URL
# - Add to alertmanager.yml receivers section
```

## Data Setup

### 1. Option A: Use Existing 423 Synthetic STDF Files (Fast Development)

```bash
# If you have existing .std files from P15/P07:
# Copy them to the Kafka ingestion directory
cp -r /path/to/existing/stdf/files/* data/stdf-ingestion/

# Or mount volume in docker-compose.yml:
# volumes:
#   - /path/to/existing/stdf/files:/opt/stdf-ingestion
```

### 2. Option B: Generate Fresh Synthetic STDFs via GAN (Realistic Variability)

```bash
# This requires P07 GAN to be running
# Configure Airflow DAG to generate 100-500 files nightly

# Enable GAN generation DAG:
docker-compose exec airflow-webserver airflow dags unpause p07_gan_generate_stdf

# Manually trigger first run:
docker-compose exec airflow-webserver airflow dags trigger p07_gan_generate_stdf

# Monitor generation:
docker-compose exec airflow-webserver airflow dags list-runs -d p07_gan_generate_stdf
```

### 3. Synthetic Data Generation (If No Data Available)

```bash
# Run the synthetic data generator script
docker-compose exec spark-master python scripts/generate_synthetic_stdf.py \
    --num-files 100 \
    --output-dir /opt/stdf-ingestion

# This creates simplified .std files for testing
```

## Security Configuration

### 1. Enable SSL/TLS

#### For Kafka
```bash
# Generate certificates
cd security/kafka-ssl
./generate-certs.sh

# Update docker-compose.yml with SSL listeners
# Uncomment SSL configuration in config/kafka/server.properties
```

#### For Airflow
```bash
# Generate SSL certificate
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout security/airflow-webserver-key.pem \
    -out security/airflow-webserver.pem

# Update airflow.cfg:
# web_server_ssl_cert = /opt/airflow/security/airflow-webserver.pem
# web_server_ssl_key = /opt/airflow/security/airflow-webserver-key.pem
```

### 2. Configure Authentication

#### Airflow RBAC
```bash
# Create users with different roles
docker-compose exec airflow-webserver airflow users create \
    --username data_engineer \
    --firstname Data \
    --lastname Engineer \
    --role Op \
    --email de@example.com \
    --password <password>

docker-compose exec airflow-webserver airflow users create \
    --username ml_engineer \
    --firstname ML \
    --lastname Engineer \
    --role User \
    --email ml@example.com \
    --password <password>
```

#### FastAPI JWT Authentication
```bash
# Generate secret key
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Add to .env:
# JWT_SECRET_KEY=<generated-key>
# JWT_ALGORITHM=HS256
# ACCESS_TOKEN_EXPIRE_MINUTES=30
```

### 3. Secrets Management

#### Using Kubernetes Secrets
```bash
# Create secrets
kubectl create secret generic p16-secrets \
    --from-literal=aws-access-key-id=<key> \
    --from-literal=aws-secret-access-key=<secret> \
    --from-literal=databricks-token=<token> \
    --from-literal=mlflow-tracking-uri=<uri> \
    -n default

# Reference in deployments
# See k8s/fastapi-deployment.yaml for examples
```

#### Using AWS Secrets Manager
```bash
# Store secrets
aws secretsmanager create-secret \
    --name p16/databricks/token \
    --secret-string "<databricks-token>"

aws secretsmanager create-secret \
    --name p16/aws/credentials \
    --secret-string '{"access_key":"<key>","secret_key":"<secret>"}'

# Install External Secrets Operator on EKS
helm repo add external-secrets https://charts.external-secrets.io
helm install external-secrets external-secrets/external-secrets -n external-secrets-system
```

## Testing and Validation

### 1. Verify Local Setup

```bash
# Run integration tests
docker-compose exec spark-master pytest tests/integration/

# Test Kafka ingestion
echo "test message" | docker-compose exec -T kafka-broker kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic stdf_ingestion

# Test Spark processing
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    jobs/test_stdf_parser.py

# Test MLflow logging
docker-compose exec mlflow-server python scripts/test_mlflow_tracking.py

# Test FastAPI endpoints
curl -X POST http://localhost:8000/predict/p01_xgboost \
    -H "Content-Type: application/json" \
    -d '{"features": [0.95, 0.87, 1.23, 0.45]}'
```

### 2. Load Testing

```bash
# Install locust
pip install locust

# Run load test
locust -f tests/load/test_fastapi_load.py --host http://localhost:8000

# Access Locust UI at http://localhost:8089
# Configure: 100 users, 10 users/sec spawn rate, run for 5 minutes
```

### 3. Data Quality Validation

```bash
# Run data quality checks
docker-compose exec spark-master python scripts/validate_data_quality.py

# Check Delta Lake table statistics
docker-compose exec spark-master pyspark
>>> spark.sql("DESCRIBE EXTENDED wafer_features").show(100, False)
>>> spark.sql("SELECT COUNT(*) FROM wafer_features").show()
```

## Cost Monitoring

### 1. AWS Cost Dashboard

```bash
# Enable AWS Cost Explorer
aws ce get-cost-and-usage \
    --time-period Start=2024-12-01,End=2024-12-31 \
    --granularity DAILY \
    --metrics BlendedCost \
    --group-by Type=SERVICE

# Set up budget alerts
aws budgets create-budget \
    --account-id <account-id> \
    --budget file://config/aws-budget.json \
    --notifications-with-subscribers file://config/aws-budget-notifications.json
```

### 2. Databricks Cost Monitoring

```bash
# View cluster usage
databricks clusters list

# Check DBU usage in Databricks UI:
# - Go to Account Console > Usage
# - View DBU consumption per cluster
# - Set up alerts for >50 DBUs/day
```

### 3. Cost Optimization

```bash
# Stop unused Databricks clusters
databricks clusters delete --cluster-id <cluster-id>

# Lifecycle policy for S3 (move to Glacier after 90 days)
aws s3api put-bucket-lifecycle-configuration \
    --bucket p16-stdf-raw \
    --lifecycle-configuration file://config/s3-lifecycle-glacier.json

# Delete old Delta Lake versions
docker-compose exec spark-master pyspark
>>> spark.sql("VACUUM wafer_features RETAIN 7 HOURS")
```

## Troubleshooting

### Common Issues

#### 1. Docker Services Not Starting
```bash
# Check Docker resource allocation (needs 16GB+ RAM)
docker system info | grep Memory

# Increase Docker memory limit:
# Docker Desktop > Settings > Resources > Memory > 16GB

# Restart Docker
docker-compose down
docker system prune -a
docker-compose up -d
```

#### 2. Kafka Connection Errors
```bash
# Check Kafka broker status
docker-compose exec kafka-broker kafka-broker-api-versions \
    --bootstrap-server localhost:9092

# View Kafka logs
docker-compose logs kafka-broker

# Reset Kafka consumer group
docker-compose exec kafka-broker kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group spark-stdf-consumer \
    --reset-offsets --to-earliest --execute --topic stdf_ingestion
```

#### 3. Spark OOM Errors
```bash
# Increase Spark executor memory in docker-compose.yml:
# SPARK_EXECUTOR_MEMORY=4g
# SPARK_DRIVER_MEMORY=2g

# Repartition large datasets
df = df.repartition(100)  # More partitions = less memory per partition

# Enable Spark dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
```

#### 4. MLflow Artifacts Not Saving
```bash
# Check S3 permissions
aws s3 ls s3://p16-mlflow-artifacts/

# Test MLflow connection
docker-compose exec mlflow-server python -c "
import mlflow
mlflow.set_tracking_uri('http://mlflow-server:5000')
with mlflow.start_run():
    mlflow.log_param('test', 'value')
print('MLflow test successful')
"
```

#### 5. Airflow DAGs Not Loading
```bash
# Check Airflow logs
docker-compose logs airflow-scheduler

# Validate DAG syntax
docker-compose exec airflow-scheduler python -c "
from airflow.models import DagBag
dag_bag = DagBag('/opt/airflow/dags')
print(f'Loaded {len(dag_bag.dags)} DAGs')
print(f'Import errors: {dag_bag.import_errors}')
"

# Refresh DAGs
docker-compose exec airflow-scheduler airflow dags reserialize
```

## Production Checklist

Before going to production, verify:

- [ ] All environment variables configured in `.env`
- [ ] SSL/TLS enabled for Kafka, Airflow, FastAPI
- [ ] Authentication enabled (Airflow RBAC, FastAPI JWT)
- [ ] Secrets stored in Kubernetes Secrets or AWS Secrets Manager
- [ ] S3 buckets created with lifecycle policies
- [ ] Databricks cluster created with auto-termination
- [ ] MLflow models registered for all P01-P15 projects
- [ ] Airflow DAGs validated and scheduled
- [ ] FastAPI deployed with HPA (2-20 replicas)
- [ ] Prometheus and Grafana configured with alerts
- [ ] PagerDuty/Slack integration for critical alerts
- [ ] Cost monitoring enabled (AWS Budget, Databricks alerts)
- [ ] Backup strategy configured (Delta Lake VACUUM retention, S3 versioning)
- [ ] Load testing completed (FastAPI can handle 1000+ req/min)
- [ ] Data quality validation passing (>98% STDFs pass checks)
- [ ] Documentation updated (runbooks, API docs, architecture diagrams)
- [ ] Team training completed (onboarding guide, office hours scheduled)

## Support and Resources

### Internal Documentation
- Architecture: `docs/architecture/ARCHITECTURE.md`
- API Docs: http://localhost:8000/docs (local) or https://api.p16.company.com/docs (prod)
- Runbooks: `docs/runbooks/`
- Delta Lake Schemas: `docs/schemas/delta-lake-schemas.md`

### External Resources
- Kafka: https://kafka.apache.org/documentation/
- Spark: https://spark.apache.org/docs/latest/
- Delta Lake: https://docs.delta.io/latest/index.html
- MLflow: https://mlflow.org/docs/latest/index.html
- Airflow: https://airflow.apache.org/docs/
- FastAPI: https://fastapi.tiangolo.com/
- Databricks: https://docs.databricks.com/

### Team Contact
- Slack Channel: #p16-mlops-platform
- Email: mlops-team@company.com
- On-call: Check PagerDuty schedule

---

**Last Updated**: 2024-12-05
**Version**: 1.0
**Author**: MLOps Team
