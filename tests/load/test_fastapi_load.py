"""
Load testing for FastAPI endpoints using Locust
Run: locust -f test_fastapi_load.py --host http://localhost:8000
"""

from locust import HttpUser, task, between
import random

class P16APIUser(HttpUser):
    """Simulate API user making predictions"""
    
    wait_time = between(1, 3)  # Wait 1-3 seconds between requests
    
    def on_start(self):
        """Called when a simulated user starts"""
        pass
    
    @task(3)
    def predict_p01(self):
        """Make P01 XGBoost predictions (higher weight)"""
        features = [
            random.uniform(0.85, 0.99),  # wafer_yield
            random.uniform(0.80, 0.95),  # edge_die_yield
            random.uniform(0.85, 0.95),  # center_die_yield
            random.uniform(1.0, 1.5),    # parametric_mean_IDDQ
            random.uniform(0.1, 0.2),    # parametric_std_IDDQ
            random.uniform(0.6, 0.8),    # parametric_mean_VTH
            random.uniform(0.03, 0.07),  # parametric_std_VTH
            random.uniform(0.85, 0.99),  # quadrant_q1_yield
            random.uniform(0.85, 0.99),  # quadrant_q2_yield
            random.uniform(0.85, 0.99),  # quadrant_q3_yield
            random.uniform(0.85, 0.99)   # quadrant_q4_yield
        ]
        
        payload = {
            "features": features,
            "metadata": {
                "lot_id": f"TEST_LOT_{random.randint(100, 999)}",
                "wafer_id": f"W{random.randint(1, 25):02d}"
            }
        }
        
        with self.client.post(
            "/predict/p01_xgboost",
            json=payload,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get("latency_ms", 0) > 100:
                    response.failure(f"Latency too high: {data['latency_ms']}ms")
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(2)
    def predict_p02(self):
        """Make P02 ResNet predictions"""
        # Simplified features for P02
        features = [random.uniform(0.8, 1.0) for _ in range(10)]
        
        payload = {
            "features": features,
            "metadata": {}
        }
        
        self.client.post("/predict/p02_resnet", json=payload)
    
    @task(1)
    def batch_predict(self):
        """Make batch predictions"""
        batch_size = random.randint(5, 20)
        features_batch = [
            [random.uniform(0.8, 1.0) for _ in range(11)]
            for _ in range(batch_size)
        ]
        
        payload = {
            "features_batch": features_batch
        }
        
        with self.client.post(
            "/predict/p01_xgboost/batch",
            json=payload,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                data = response.json()
                avg_latency = data.get("latency_per_sample_ms", 0)
                if avg_latency > 50:
                    response.failure(f"Avg latency per sample too high: {avg_latency}ms")
    
    @task(1)
    def check_health(self):
        """Check health endpoint"""
        self.client.get("/health")
    
    @task(1)
    def list_models(self):
        """List available models"""
        self.client.get("/models")
