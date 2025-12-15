"""
Test FastAPI endpoints
"""

import pytest
from fastapi.testclient import TestClient
import sys
import os

# Add fastapi-app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'fastapi-app'))

from main import app

client = TestClient(app)

def test_root_endpoint():
    """Test root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "P16 Enterprise ML Platform API"
    assert "version" in data

def test_health_endpoint():
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "mlflow_uri" in data

def test_list_models():
    """Test list models endpoint"""
    response = client.get("/models")
    assert response.status_code == 200
    data = response.json()
    assert "models" in data
    assert len(data["models"]) > 0

def test_predict_p01():
    """Test P01 XGBoost prediction"""
    payload = {
        "features": [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88],
        "metadata": {"lot_id": "TEST_LOT", "wafer_id": "W01"}
    }
    response = client.post("/predict/p01_xgboost", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "prediction" in data
    assert "confidence" in data
    assert "latency_ms" in data

def test_predict_invalid_model():
    """Test prediction with invalid model key"""
    payload = {
        "features": [0.95, 0.87],
        "metadata": {}
    }
    response = client.post("/predict/invalid_model", json=payload)
    assert response.status_code == 404

def test_predict_missing_features():
    """Test prediction with missing features"""
    payload = {
        "metadata": {}
    }
    response = client.post("/predict/p01_xgboost", json=payload)
    assert response.status_code == 422  # Validation error

def test_batch_prediction():
    """Test batch prediction endpoint"""
    payload = {
        "features_batch": [
            [0.95, 0.87, 0.91, 1.23, 0.15, 0.7, 0.05, 0.94, 0.89, 0.92, 0.88],
            [0.92, 0.85, 0.88, 1.18, 0.12, 0.68, 0.04, 0.91, 0.86, 0.89, 0.85],
            [0.98, 0.91, 0.95, 1.28, 0.18, 0.72, 0.06, 0.97, 0.92, 0.95, 0.91]
        ]
    }
    response = client.post("/predict/p01_xgboost/batch", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "predictions" in data
    assert len(data["predictions"]) == 3
    assert "batch_size" in data
    assert data["batch_size"] == 3
