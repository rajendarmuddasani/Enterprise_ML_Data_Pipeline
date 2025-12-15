#!/bin/bash

# P16 Platform Verification Script
# Checks if all services are running correctly

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

echo "============================================"
echo "  P16 Platform Verification"
echo "============================================"
echo ""

# Test each service
services=(
    "http://localhost:8000/health:FastAPI Model Serving"
    "http://localhost:8080:Kafka UI"
    "http://localhost:8081:Spark Master"
    "http://localhost:8084/health:Airflow"
    "http://localhost:5000:MLflow"
    "http://localhost:9090:Prometheus"
    "http://localhost:3000:Grafana"
    "http://localhost:9001:MinIO"
)

failed=0

for service in "${services[@]}"; do
    IFS=':' read -r url name <<< "$service"
    
    if curl -s -f "$url" > /dev/null 2>&1; then
        print_success "$name is accessible at $url"
    else
        print_error "$name is NOT accessible at $url"
        ((failed++))
    fi
done

echo ""
echo "Testing API endpoint..."

# Test FastAPI prediction
response=$(curl -s -X POST "http://localhost:8000/predict/p01_xgboost" \
    -H "Content-Type: application/json" \
    -d '{"features": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88]}' 2>/dev/null)

if [ $? -eq 0 ] && echo "$response" | grep -q "prediction"; then
    print_success "API prediction endpoint working"
    echo "  Sample response: ${response:0:100}..."
else
    print_error "API prediction endpoint not working"
    ((failed++))
fi

echo ""
echo "Checking Docker containers..."

# Check Docker containers
containers=$(docker-compose ps --services --filter "status=running" | wc -l)
total=$(docker-compose ps --services | wc -l)

if [ "$containers" -eq "$total" ]; then
    print_success "All Docker containers running ($containers/$total)"
else
    print_warning "$containers/$total containers running"
    echo "  Run 'docker-compose ps' to see details"
fi

echo ""
echo "Checking Delta Lake tables..."

# Check Delta Lake tables
table_check=$(docker-compose exec -T spark-master pyspark --quiet 2>/dev/null <<EOF
spark.sql("SELECT COUNT(*) FROM delta.\\\`s3a://delta-lake/tables/raw_stdf\\\`").show()
EOF
)

if [ $? -eq 0 ]; then
    print_success "Delta Lake tables accessible"
else
    print_warning "Could not verify Delta Lake tables"
fi

echo ""
echo "============================================"

if [ $failed -eq 0 ]; then
    print_success "All checks passed! Platform is ready."
    echo ""
    echo "Next steps:"
    echo "  1. Access FastAPI docs: http://localhost:8000/docs"
    echo "  2. Open Airflow UI: http://localhost:8084 (admin/admin)"
    echo "  3. View MLflow experiments: http://localhost:5000"
    echo "  4. Check Grafana dashboards: http://localhost:3000 (admin/admin)"
else
    print_error "$failed checks failed. Please review the errors above."
    exit 1
fi
