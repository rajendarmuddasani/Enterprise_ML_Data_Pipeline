# Next Steps - Quick Reference

**Last Updated**: December 7, 2025  
**Current Status**: ✅ Standalone API Tested & Working | ⏳ Docker Deployment Pending

---

## 🚀 What to Do Next (When You Ask "What Next")

### Step 1: Deploy Docker Stack (IMMEDIATE - 15 min)

**Why**: Enable full platform with Kafka, Spark, Airflow, MLflow

**Prerequisites**:
```bash
# Check Docker memory
docker info | grep Memory
# If less than 8GB, go to Docker Desktop → Settings → Resources → Increase to 8GB
```

**Execute**:
```bash
cd /Users/rajendarmuddasani/AIML/47_/P16_Enterprise_ML_Data_Pipeline
./startup.sh
# Wait 10 minutes for services to initialize
./verify.sh
```

**Success Criteria**:
- ✅ All 13 services running
- ✅ FastAPI accessible at http://localhost:8000/docs
- ✅ Airflow accessible at http://localhost:8084 (admin/admin)
- ✅ MLflow accessible at http://localhost:5000

**If Docker Memory Too Low**: Use minimal deployment
```bash
docker-compose -f docker-compose.minimal.yml up -d
```

---

### Step 2: Create P02 Model Training DAG (NEXT - 2 hours)

**Why**: Automate ResNet model training workflow

**Execute**:
```bash
cd airflow/dags
cp p01_model_training.py p02_resnet_training.py
```

**Edit `p02_resnet_training.py`**:
```python
# Change these variables:
DAG_ID = "p02_resnet_yield_training"  # Line 15
MODEL_NAME = "p02_resnet_yield_predictor"  # Line 25
TRAINING_DATA_QUERY = """
    SELECT 
        wafer_yield,
        edge_yield, center_yield,
        param_mean_1, param_std_1,
        spatial_q1_yield, spatial_q2_yield,
        spatial_q3_yield, spatial_q4_yield
    FROM delta.`s3a://delta-lake/tables/wafer_features`
    WHERE process_date >= current_date() - INTERVAL 90 DAYS
"""  # Line 45-55

# In train_model() function:
# Replace RandomForestClassifier with ResNet implementation
# Update hyperparameters for ResNet
```

**Test DAG**:
```bash
docker-compose exec airflow-scheduler airflow dags test p02_resnet_yield_training 2024-01-01
```

**Repeat for P03-P15**: Use same pattern for remaining models

---

### Step 3: Integrate Real STDF Data (AFTER DAGs - 4 hours)

**Why**: Move from synthetic to production data

**Install STDF Parser**:
```bash
pip install pystdf
# or
pip install python-stdf
```

**Update `spark-jobs/stdf_kafka_ingestion.py`**:

Replace `parse_stdf_simple()` function:
```python
from pystdf import Parser

def parse_stdf_simple(stdf_binary):
    """Parse actual STDF file"""
    try:
        parser = Parser(stdf_binary)
        records = []
        
        for record in parser:
            if record.type == 'PTR':  # Parametric Test Record
                records.append({
                    'lot_id': record.lot_id,
                    'wafer_id': record.wafer_id,
                    'die_x': record.x_coord,
                    'die_y': record.y_coord,
                    'test_num': record.test_num,
                    'result': record.result,
                    'pass_fail': 1 if record.test_flg & 0x80 else 0,
                    'timestamp': datetime.now().isoformat()
                })
        
        return records
    except Exception as e:
        print(f"Error parsing STDF: {e}")
        return []
```

**Process Real Files**:
```bash
# Copy your .std files
cp /path/to/production/*.std data/stdf-ingestion/

# Run ingestion
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/jobs/stdf_kafka_ingestion.py batch

# Verify data
docker-compose exec spark-master pyspark
>>> spark.sql("SELECT COUNT(*) FROM delta.`s3a://delta-lake/tables/raw_stdf`").show()
```

---

### Step 4: Build Grafana Dashboards (OPTIONAL - 2 hours)

**Access Grafana**:
```
http://localhost:3000
Username: admin
Password: admin
```

**Create Dashboards**:
1. Add Prometheus data source (http://prometheus:9090)
2. Create dashboard: "FastAPI Performance"
   - Panel 1: Prediction latency (histogram)
   - Panel 2: Request rate (graph)
   - Panel 3: Error rate (graph)
3. Export as JSON
4. Save to `monitoring/grafana/dashboards/fastapi_performance.json`

**Repeat for**:
- Model metrics dashboard
- Infrastructure health dashboard
- Cost monitoring dashboard

---

### Step 5: Deploy to Cloud (FINAL - 6 hours)

**Follow Complete Guide**: [`MANUAL_TASKS.md`](MANUAL_TASKS.md)

**Summary Steps**:
1. Create AWS S3 buckets for Delta Lake
2. Set up Databricks workspace
3. Configure Databricks cluster with Delta Lake
4. Deploy Spark jobs to Databricks
5. Set up MLflow on Databricks
6. Deploy FastAPI to EKS or EC2
7. Configure monitoring and alerts

**Estimated Cost**: $50-100/month

---

## 📋 Quick Decision Tree

```
Ask yourself: "What's my immediate goal?"

┌─────────────────────────────────────────┐
│ Want to see full platform working?      │
│ → Go to Step 1 (Docker Deployment)      │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ Want to automate model training?        │
│ → Go to Step 2 (Create DAGs)            │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ Want to process real test data?         │
│ → Go to Step 3 (STDF Integration)       │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ Want to visualize metrics?              │
│ → Go to Step 4 (Grafana Dashboards)     │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ Ready for production deployment?        │
│ → Go to Step 5 (Cloud Deployment)       │
└─────────────────────────────────────────┘
```

---

## 🔍 Current State Quick Check

```bash
# Is standalone API running?
curl http://localhost:9999/health

# Are Docker services running?
docker-compose ps

# Last test results
cat TEST_RESULTS.md | grep "Total Tests"

# Server PID (to stop)
ps aux | grep test_api_standalone
```

---

## 📚 Documentation Map

| Document | Purpose | When to Read |
|----------|---------|--------------|
| **[PROJECT_STATE.md](PROJECT_STATE.md)** | Complete project state, all context | When asking "what next" |
| **[NEXT_STEPS.md](NEXT_STEPS.md)** | This file - quick action guide | Quick reference |
| **[README.md](README.md)** | Project overview and architecture | First time understanding |
| **[QUICKSTART.md](QUICKSTART.md)** | 10-minute setup guide | Fast testing |
| **[MANUAL_TASKS.md](MANUAL_TASKS.md)** | Cloud deployment details | Production deployment |
| **[TEST_RESULTS.md](TEST_RESULTS.md)** | Test results and examples | Verification |
| **[PRD.md](PRD.md)** | Original requirements | Requirements reference |

---

## 🎯 Success Metrics by Step

### After Step 1 (Docker Deployment)
- [ ] 13 services running: `docker-compose ps | grep Up | wc -l` = 13
- [ ] FastAPI responsive: `curl http://localhost:8000/health` = 200
- [ ] Airflow UI loads: http://localhost:8084
- [ ] MLflow UI loads: http://localhost:5000

### After Step 2 (P02 DAG)
- [ ] DAG visible in Airflow UI
- [ ] Test run succeeds: `airflow dags test p02_resnet_yield_training`
- [ ] Model tracked in MLflow
- [ ] Model metrics logged (accuracy, F1, etc.)

### After Step 3 (Real STDF Data)
- [ ] STDF files parsed: No errors in Spark logs
- [ ] Data in Delta Lake: `SELECT COUNT(*) FROM raw_stdf` > 0
- [ ] Features computed: `SELECT COUNT(*) FROM wafer_features` > 0
- [ ] Data quality checks pass

### After Step 4 (Grafana)
- [ ] 4+ dashboards created
- [ ] All panels showing data
- [ ] Alerts configured
- [ ] JSON files saved

### After Step 5 (Cloud)
- [ ] S3 buckets accessible
- [ ] Databricks cluster running
- [ ] Spark jobs executing on Databricks
- [ ] FastAPI deployed and accessible
- [ ] Monitoring operational

---

## 🚨 Common Issues & Quick Fixes

### "Docker memory too low"
```bash
# Open Docker Desktop → Settings → Resources → Memory → Set to 8GB
docker system prune -a
./startup.sh
```

### "Port 8000 already in use"
```bash
# Find and kill process
lsof -i :8000
kill -9 <PID>
# Or use minimal deployment with different ports
docker-compose -f docker-compose.minimal.yml up -d
```

### "Services not starting"
```bash
# Clean restart
docker-compose down -v
docker system prune -a
./startup.sh
```

### "Can't find project files"
```bash
cd /Users/rajendarmuddasani/AIML/47_/P16_Enterprise_ML_Data_Pipeline
ls -la
```

---

## 💡 Pro Tips

1. **Start Small**: Deploy minimal stack first if Docker memory limited
2. **Test Incrementally**: Verify each component before moving to next step
3. **Use Logs**: `docker-compose logs -f <service>` is your friend
4. **Save State**: Document what works before making changes
5. **Read Errors**: Error messages usually tell you exactly what's wrong

---

## ⚡ Copy-Paste Commands for Next Session

```bash
# Navigate to project
cd /Users/rajendarmuddasani/AIML/47_/P16_Enterprise_ML_Data_Pipeline

# Check current state
cat PROJECT_STATE.md | head -50
cat NEXT_STEPS.md | head -30

# Start Docker
docker info | grep Memory  # Check memory first
./startup.sh

# Test API
./test_api.sh

# View services
docker-compose ps
```

---

**Remember**: Read [`PROJECT_STATE.md`](PROJECT_STATE.md) for complete context when you return! 🎯

**Most Important Next Step**: Docker Deployment (Step 1)
