#!/bin/bash

# P16 Enterprise ML Pipeline - Startup Script
# This script initializes and starts the complete platform

set -e  # Exit on error

echo "============================================"
echo "  P16 Enterprise ML Platform - Startup"
echo "============================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
    echo "Please install Docker Desktop from https://www.docker.com/products/docker-desktop"
    exit 1
fi
print_success "Docker found: $(docker --version)"

# Check Docker Compose (try new 'docker compose' first, then old 'docker-compose')
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
    print_success "Docker Compose found: $(docker compose version)"
elif command -v ${DOCKER_COMPOSE} &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
    print_success "Docker Compose found: $(${DOCKER_COMPOSE} --version)"
else
    print_error "Docker Compose is not installed"
    exit 1
fi

# Check Docker is running
if ! docker info &> /dev/null; then
    print_error "Docker is not running"
    echo "Please start Docker Desktop"
    exit 1
fi
print_success "Docker is running"

# Check Docker resources
DOCKER_MEMORY=$(docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{print int($1/1024/1024/1024)}')
if [ "$DOCKER_MEMORY" -lt 6 ]; then
    print_error "Docker memory is ${DOCKER_MEMORY}GB (minimum: 6GB required)"
    echo "Please increase Docker memory:"
    echo "  1. Open Docker Desktop"
    echo "  2. Go to Settings → Resources"
    echo "  3. Increase Memory to at least 6GB (8GB recommended)"
    echo "  4. Click Apply & Restart"
    exit 1
elif [ "$DOCKER_MEMORY" -lt 8 ]; then
    print_warning "Docker memory is ${DOCKER_MEMORY}GB (recommended: 8GB)"
    echo "Platform will run but may be slower with limited services"
    print_success "Memory check passed (minimum met)"
else
    print_success "Docker memory: ${DOCKER_MEMORY}GB"
fi

echo ""
echo "============================================"
echo "  Step 1: Environment Setup"
echo "============================================"
echo ""

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    print_warning ".env file not found, creating from template..."
    cp .env.template .env
    print_success ".env file created"
    print_warning "Please edit .env and configure your settings"
    echo "Press Enter to continue after editing .env (or Ctrl+C to exit)"
    read
fi

# Create necessary directories
echo "Creating directories..."
mkdir -p data/stdf-ingestion
mkdir -p airflow/logs
mkdir -p airflow/plugins
print_success "Directories created"

echo ""
echo "============================================"
echo "  Step 2: Starting Docker Services"
echo "============================================"
echo ""

# Stop any existing containers
if [ "$(${DOCKER_COMPOSE} ps -q)" ]; then
    print_warning "Stopping existing containers..."
    ${DOCKER_COMPOSE} down
fi

# Start services
echo "Starting all services (this may take a few minutes)..."
${DOCKER_COMPOSE} up -d

# Wait for services to be ready
echo ""
echo "Waiting for services to initialize..."
sleep 30

# Check service health
echo ""
echo "Checking service health..."

services=("zookeeper" "kafka-broker" "postgres" "redis" "minio" "spark-master" "mlflow-server" "airflow-webserver" "airflow-scheduler" "fastapi-server" "prometheus" "grafana")

for service in "${services[@]}"; do
    if ${DOCKER_COMPOSE} ps | grep -q "p16-${service}.*Up"; then
        print_success "${service} is running"
    else
        print_error "${service} failed to start"
        echo "Check logs with: ${DOCKER_COMPOSE} logs ${service}"
    fi
done

echo ""
echo "============================================"
echo "  Step 3: Initialize Databases"
echo "============================================"
echo ""

# Wait for Postgres to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 10

# Initialize Airflow database
echo "Initializing Airflow database..."
${DOCKER_COMPOSE} exec -T airflow-webserver airflow db init || print_warning "Airflow DB already initialized"
print_success "Airflow database initialized"

# Create Airflow admin user
echo "Creating Airflow admin user..."
${DOCKER_COMPOSE} exec -T airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin 2>/dev/null || print_warning "Airflow admin user already exists"
print_success "Airflow admin user created (username: admin, password: admin)"

echo ""
echo "============================================"
echo "  Step 4: Initialize Delta Lake Tables"
echo "============================================"
echo ""

# Wait for Spark to be ready
echo "Waiting for Spark cluster to be ready..."
sleep 15

# Initialize Delta Lake tables
echo "Creating Delta Lake tables..."
${DOCKER_COMPOSE} exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/scripts/init_delta_tables.py

if [ $? -eq 0 ]; then
    print_success "Delta Lake tables created"
else
    print_error "Failed to create Delta Lake tables"
fi

echo ""
echo "============================================"
echo "  Step 5: Generate Synthetic Data"
echo "============================================"
echo ""

# Generate synthetic STDF data
echo "Generating synthetic test data..."
${DOCKER_COMPOSE} exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/jobs/stdf_kafka_ingestion.py batch

if [ $? -eq 0 ]; then
    print_success "Synthetic data generated"
else
    print_warning "Failed to generate synthetic data (you can do this later)"
fi

echo ""
echo "============================================"
echo "  Step 6: Run Feature Engineering"
echo "============================================"
echo ""

# Compute features
echo "Computing wafer-level features..."
${DOCKER_COMPOSE} exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/jobs/feature_engineering.py

if [ $? -eq 0 ]; then
    print_success "Feature engineering completed"
else
    print_warning "Failed to compute features (you can do this later)"
fi

echo ""
echo "============================================"
echo "  ✓ Platform Ready!"
echo "============================================"
echo ""

echo "Access the platform:"
echo ""
echo "  FastAPI (Model Serving):  http://localhost:8000/docs"
echo "  Kafka UI:                 http://localhost:8080"
echo "  Spark Master UI:          http://localhost:8081"
echo "  Airflow UI:               http://localhost:8084 (admin/admin)"
echo "  MLflow UI:                http://localhost:5000"
echo "  Prometheus:               http://localhost:9090"
echo "  Grafana:                  http://localhost:3000 (admin/admin)"
echo "  MinIO Console:            http://localhost:9001 (minioadmin/minioadmin)"
echo ""

echo "Quick test:"
echo ""
echo "  # Make a prediction"
echo "  curl -X POST http://localhost:8000/predict/p01_xgboost \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"features\": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88]}'"
echo ""

echo "View logs:"
echo "  ${DOCKER_COMPOSE} logs -f [service-name]"
echo ""

echo "Stop platform:"
echo "  ${DOCKER_COMPOSE} down"
echo ""

print_success "Platform initialization complete!"
