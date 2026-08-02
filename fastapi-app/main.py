"""
P16 Enterprise ML Data Pipeline - FastAPI Model Serving
Main application for serving predictions from all P01-P15 models
"""

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
import mlflow
import mlflow.pyfunc
import numpy as np
import time
import logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
PREDICTION_COUNTER = Counter('predictions_total', 'Total predictions', ['model_name', 'status'])
PREDICTION_LATENCY = Histogram('prediction_latency_seconds', 'Prediction latency', ['model_name'])
MODEL_LOAD_COUNTER = Counter('model_loads_total', 'Total model loads', ['model_name'])

# Initialize FastAPI app
app = FastAPI(
    title="P16 Enterprise ML Platform API",
    description="Production MLOps API serving predictions from 15 semiconductor AI/ML models",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MLflow configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Model registry - cache loaded models
MODEL_CACHE = {}

# Model configurations
MODEL_CONFIGS = {
    "p01_xgboost": {
        "name": "p01_xgboost_bin_predictor",
        "description": "XGBoost wafer bin prediction",
        "input_features": ["wafer_yield", "edge_die_yield", "parametric_mean_IDDQ", "parametric_std_VTH"],
        "output_type": "classification"
    },
    "p02_resnet": {
        "name": "p02_resnet_yield_predictor",
        "description": "ResNet transfer learning yield prediction",
        "input_features": ["spatial_features_256"],
        "output_type": "regression"
    },
    "p03_multi_agent": {
        "name": "p03_multi_agent_rca",
        "description": "Multi-agent root cause analysis",
        "input_features": ["failure_pattern", "test_history", "lot_context"],
        "output_type": "text"
    },
    "p04_unet": {
        "name": "p04_unet_wafer_defect",
        "description": "U-Net wafer defect classifier",
        "input_features": ["wafer_map_512x512"],
        "output_type": "classification"
    },
    "p06_lstm": {
        "name": "p06_lstm_anomaly_detector",
        "description": "LSTM parametric anomaly detection",
        "input_features": ["time_series_100"],
        "output_type": "anomaly_score"
    },
    "p08_xgboost": {
        "name": "p08_xgboost_limit_change",
        "description": "XGBoost test limit optimization",
        "input_features": ["current_limits", "yield_impact", "test_correlation"],
        "output_type": "regression"
    },
    "p10_gnn": {
        "name": "p10_gnn_failure_propagation",
        "description": "GNN failure propagation graph",
        "input_features": ["test_graph_embeddings"],
        "output_type": "graph_prediction"
    }
}

# ============= Pydantic Models =============

class PredictionRequest(BaseModel):
    """Standard prediction request"""
    features: List[float] = Field(..., description="Input features for prediction")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Optional metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "features": [0.95, 0.87, 1.23, 0.45],
                "metadata": {"lot_id": "TC41x_LOT123", "wafer_id": "W05"}
            }
        }

class PredictionResponse(BaseModel):
    """Standard prediction response"""
    prediction: Any = Field(..., description="Model prediction (value, class, or text)")
    confidence: Optional[float] = Field(None, description="Prediction confidence score")
    model_name: str = Field(..., description="Model name used for prediction")
    model_version: str = Field(..., description="Model version/stage")
    latency_ms: float = Field(..., description="Prediction latency in milliseconds")
    timestamp: str = Field(..., description="Prediction timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "prediction": 1,
                "confidence": 0.87,
                "model_name": "p01_xgboost_bin_predictor",
                "model_version": "Production/v2.3",
                "latency_ms": 15.3,
                "timestamp": "2024-12-05T10:30:00Z",
                "metadata": {"features_count": 4}
            }
        }

class BatchPredictionRequest(BaseModel):
    """Batch prediction request"""
    features_batch: List[List[float]] = Field(..., description="Batch of feature vectors")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    class Config:
        json_schema_extra = {
            "example": {
                "features_batch": [
                    [0.95, 0.87, 1.23, 0.45],
                    [0.92, 0.85, 1.18, 0.42],
                    [0.98, 0.91, 1.28, 0.48]
                ]
            }
        }

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    mlflow_uri: str
    models_loaded: int
    uptime_seconds: float

# ============= Helper Functions =============

def load_model(model_key: str, stage: str = "Production") -> Any:
    """Load model from MLflow registry with caching"""
    cache_key = f"{model_key}_{stage}"
    
    if cache_key in MODEL_CACHE:
        logger.info(f"Using cached model: {cache_key}")
        return MODEL_CACHE[cache_key]
    
    try:
        model_name = MODEL_CONFIGS[model_key]["name"]
        model_uri = f"models:/{model_name}/{stage}"
        
        logger.info(f"Loading model from MLflow: {model_uri}")
        model = mlflow.pyfunc.load_model(model_uri)
        
        MODEL_CACHE[cache_key] = model
        MODEL_LOAD_COUNTER.labels(model_name=model_key).inc()
        
        logger.info(f"Model loaded successfully: {cache_key}")
        return model
        
    except Exception as e:
        logger.error(f"Failed to load model {model_key}: {str(e)}")
        
        # Fallback to mock model for demo purposes
        logger.warning(f"Using mock model for {model_key}")
        return create_mock_model(model_key)

def create_mock_model(model_key: str):
    """Create a mock model for demo/testing when real model not available"""
    class MockModel:
        def __init__(self, model_key):
            self.model_key = model_key
            
        def predict(self, features):
            """Return dummy predictions"""
            if isinstance(features, np.ndarray):
                batch_size = features.shape[0]
                if MODEL_CONFIGS[model_key]["output_type"] == "classification":
                    return np.random.randint(1, 5, size=batch_size)
                else:
                    return np.random.random(batch_size) * 100
            else:
                if MODEL_CONFIGS[model_key]["output_type"] == "classification":
                    return [1]
                else:
                    return [85.5]
    
    return MockModel(model_key)

# ============= API Endpoints =============

@app.get("/", tags=["General"])
async def root():
    """Root endpoint with API information"""
    return {
        "service": "P16 Enterprise ML Platform API",
        "version": "1.0.0",
        "status": "operational",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics"
    }

@app.get("/health", response_model=HealthResponse, tags=["General"])
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        mlflow_uri=MLFLOW_TRACKING_URI,
        models_loaded=len(MODEL_CACHE),
        uptime_seconds=time.time()
    )

@app.get("/metrics", tags=["General"])
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/models", tags=["Models"])
async def list_models():
    """List all available models"""
    return {
        "models": [
            {
                "key": key,
                "name": config["name"],
                "description": config["description"],
                "input_features": config["input_features"],
                "output_type": config["output_type"],
                "endpoint": f"/predict/{key}"
            }
            for key, config in MODEL_CONFIGS.items()
        ],
        "total": len(MODEL_CONFIGS)
    }

@app.post("/predict/{model_key}", response_model=PredictionResponse, tags=["Predictions"])
async def predict(model_key: str, request: PredictionRequest):
    """
    Make a prediction using the specified model
    
    - **model_key**: Model identifier (e.g., p01_xgboost, p02_resnet)
    - **features**: Input feature vector
    - **metadata**: Optional metadata
    """
    start_time = time.time()
    
    try:
        # Validate model key
        if model_key not in MODEL_CONFIGS:
            raise HTTPException(
                status_code=404,
                detail=f"Model {model_key} not found. Available models: {list(MODEL_CONFIGS.keys())}"
            )
        
        # Load model
        model = load_model(model_key)
        
        # Convert features to numpy array
        features_array = np.array([request.features])
        
        # Make prediction
        prediction = model.predict(features_array)
        
        # Calculate latency
        latency_ms = (time.time() - start_time) * 1000
        
        # Record metrics
        PREDICTION_COUNTER.labels(model_name=model_key, status="success").inc()
        PREDICTION_LATENCY.labels(model_name=model_key).observe(time.time() - start_time)
        
        # Prepare response
        response = PredictionResponse(
            prediction=float(prediction[0]) if isinstance(prediction[0], (np.integer, np.floating)) else int(prediction[0]),
            confidence=0.85,  # TODO: Get from model metadata
            model_name=MODEL_CONFIGS[model_key]["name"],
            model_version="Production/v1.0",  # TODO: Get from MLflow
            latency_ms=latency_ms,
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            metadata={
                **request.metadata,
                "features_count": len(request.features),
                "model_key": model_key
            }
        )
        
        logger.info(f"Prediction successful: {model_key}, latency: {latency_ms:.2f}ms")
        return response
        
    except HTTPException:
        raise  # preserve 404 / 422 from model-not-found check
    except Exception as e:
        PREDICTION_COUNTER.labels(model_name=model_key, status="error").inc()
        logger.error(f"Prediction failed for {model_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")
async def predict_batch(model_key: str, request: BatchPredictionRequest):
    """
    Make batch predictions using the specified model
    
    - **model_key**: Model identifier
    - **features_batch**: List of feature vectors
    """
    start_time = time.time()
    
    try:
        # Validate model key
        if model_key not in MODEL_CONFIGS:
            raise HTTPException(status_code=404, detail=f"Model {model_key} not found")
        
        # Load model
        model = load_model(model_key)
        
        # Convert features to numpy array
        features_array = np.array(request.features_batch)
        
        # Make predictions
        predictions = model.predict(features_array)
        
        # Calculate latency
        latency_ms = (time.time() - start_time) * 1000
        
        # Record metrics
        PREDICTION_COUNTER.labels(model_name=model_key, status="success").inc()
        PREDICTION_LATENCY.labels(model_name=model_key).observe(time.time() - start_time)
        
        # Prepare response
        response = {
            "predictions": [float(p) if isinstance(p, (np.integer, np.floating)) else int(p) for p in predictions],
            "model_name": MODEL_CONFIGS[model_key]["name"],
            "model_version": "Production/v1.0",
            "batch_size": len(request.features_batch),
            "latency_ms": latency_ms,
            "latency_per_sample_ms": latency_ms / len(request.features_batch),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        
        logger.info(f"Batch prediction successful: {model_key}, batch_size: {len(request.features_batch)}, latency: {latency_ms:.2f}ms")
        return response
        
    except Exception as e:
        PREDICTION_COUNTER.labels(model_name=model_key, status="error").inc()
        logger.error(f"Batch prediction failed for {model_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {str(e)}")

# ============= Model-Specific Endpoints =============

@app.post("/predict/p01_xgboost/bin", tags=["P01 - XGBoost Bin Predictor"])
async def predict_p01_bin(request: PredictionRequest):
    """P01: Predict wafer bin using XGBoost"""
    return await predict("p01_xgboost", request)

@app.post("/predict/p02_resnet/yield", tags=["P02 - ResNet Yield Predictor"])
async def predict_p02_yield(request: PredictionRequest):
    """P02: Predict wafer yield using ResNet transfer learning"""
    return await predict("p02_resnet", request)

@app.post("/predict/p04_unet/defect", tags=["P04 - U-Net Wafer Defect"])
async def predict_p04_defect(request: PredictionRequest):
    """P04: Classify wafer defects using U-Net"""
    return await predict("p04_unet", request)

@app.post("/predict/p06_lstm/anomaly", tags=["P06 - LSTM Anomaly Detector"])
async def predict_p06_anomaly(request: PredictionRequest):
    """P06: Detect parametric anomalies using LSTM"""
    return await predict("p06_lstm", request)

@app.post("/predict/p08_xgboost/limits", tags=["P08 - XGBoost Limit Change"])
async def predict_p08_limits(request: PredictionRequest):
    """P08: Optimize test limits using XGBoost"""
    return await predict("p08_xgboost", request)

@app.post("/predict/p10_gnn/propagation", tags=["P10 - GNN Failure Propagation"])
async def predict_p10_propagation(request: PredictionRequest):
    """P10: Predict failure propagation using GNN"""
    return await predict("p10_gnn", request)

# ============= Startup Event =============

@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    logger.info("Starting P16 FastAPI Model Serving")
    logger.info(f"MLflow Tracking URI: {MLFLOW_TRACKING_URI}")
    logger.info(f"Available models: {len(MODEL_CONFIGS)}")
    
    # Pre-load critical models (optional)
    # try:
    #     logger.info("Pre-loading critical models...")
    #     load_model("p01_xgboost")
    #     load_model("p02_resnet")
    #     logger.info("Critical models pre-loaded successfully")
    # except Exception as e:
    #     logger.warning(f"Failed to pre-load models: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down P16 FastAPI Model Serving")
    MODEL_CACHE.clear()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
