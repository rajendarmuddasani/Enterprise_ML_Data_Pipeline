#!/bin/bash
# P16 Platform - Complete API Test Suite
# Tests all models and endpoints

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
API_URL="${API_URL:-http://localhost:9999}"
VERBOSE="${VERBOSE:-0}"

echo ""
echo "============================================================"
echo "  P16 Platform - API Test Suite"
echo "============================================================"
echo ""
echo "Testing API at: $API_URL"
echo ""

# Function to test endpoint
test_endpoint() {
    local name=$1
    local method=$2
    local endpoint=$3
    local data=$4
    
    echo -n "Testing $name... "
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" "$API_URL$endpoint")
    else
        response=$(curl -s -w "\n%{http_code}" -X POST "$API_URL$endpoint" \
            -H "Content-Type: application/json" \
            -d "$data")
    fi
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    
    if [ "$http_code" = "200" ]; then
        echo -e "${GREEN}✓ PASS${NC} (HTTP $http_code)"
        if [ "$VERBOSE" = "1" ]; then
            echo "$body" | python -m json.tool 2>/dev/null || echo "$body"
        fi
    else
        echo -e "${RED}✗ FAIL${NC} (HTTP $http_code)"
        echo "$body"
        return 1
    fi
}

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

run_test() {
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if test_endpoint "$@"; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
}

echo "============================================================"
echo "  1. Health & Status Endpoints"
echo "============================================================"
echo ""

run_test "Root endpoint" "GET" "/"
run_test "Health check" "GET" "/health"
run_test "Models list" "GET" "/models"
run_test "Metrics endpoint" "GET" "/metrics"

echo "============================================================"
echo "  2. Model Predictions"
echo "============================================================"
echo ""

# P01 XGBoost Binary Classification
run_test "P01 XGBoost" "POST" "/predict/p01_xgboost" \
    '{"features": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88], "metadata": {"lot_id": "LOT001", "wafer_id": "W01"}}'

# P02 ResNet Yield
run_test "P02 ResNet Yield" "POST" "/predict/p02_resnet/yield" \
    '{"features": [0.92, 0.88, 0.95, 1.1, 0.12], "metadata": {"wafer_id": "W02", "device": "TC41x"}}'

# P02 ResNet Wafermap
run_test "P02 ResNet Wafermap" "POST" "/predict/p02_resnet/wafermap" \
    '{"features": [0.85, 0.90, 0.88, 0.92], "metadata": {"wafer_id": "W03"}}'

# P03 LSTM Timeseries
run_test "P03 LSTM Timeseries" "POST" "/predict/p03_lstm/timeseries" \
    '{"features": [0.91, 0.89, 0.93, 0.87, 0.95], "metadata": {"equipment_id": "EQP001"}}'

# P04 U-Net Defect
run_test "P04 U-Net Defect" "POST" "/predict/p04_unet/defect" \
    '{"features": [0.1, 0.2, 0.15, 0.3, 0.05], "metadata": {"inspection_type": "AOI"}}'

# P06 LSTM Anomaly
run_test "P06 LSTM Anomaly" "POST" "/predict/p06_lstm/anomaly" \
    '{"features": [0.88, 0.92, 0.85, 0.90], "metadata": {"equipment_id": "EQP002"}}'

echo "============================================================"
echo "  3. Error Handling Tests"
echo "============================================================"
echo ""

# Test with insufficient features
echo -n "Testing error handling (insufficient features)... "
response=$(curl -s -w "\n%{http_code}" -X POST "$API_URL/predict/p01_xgboost" \
    -H "Content-Type: application/json" \
    -d '{"features": [0.5]}')
http_code=$(echo "$response" | tail -n1)

if [ "$http_code" = "400" ]; then
    echo -e "${GREEN}✓ PASS${NC} (correctly returned HTTP 400)"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}✗ FAIL${NC} (expected HTTP 400, got $http_code)"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))
echo ""

echo "============================================================"
echo "  Test Summary"
echo "============================================================"
echo ""

echo "Total Tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ ALL TESTS PASSED${NC}"
    echo ""
    exit 0
else
    echo ""
    echo -e "${RED}✗ SOME TESTS FAILED${NC}"
    echo ""
    exit 1
fi
