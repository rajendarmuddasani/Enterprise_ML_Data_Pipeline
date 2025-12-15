# Product Requirements Document (PRD)
# P16: Enterprise ML Data Pipeline Platform

**Project ID**: P16_Enterprise_ML_Data_Pipeline  
**Category**: MLOps / Data Engineering / Distributed Systems  
**Status**: Draft for Review  
**Version**: v1.0  
**Last Updated**: 2025-12-04  
**Data Source**: 200GB synthetic STDF files from P07 GAN (cloud-safe)  
**Infrastructure**: Apache Kafka, Apache Spark (PySpark), Databricks, Apache Airflow, Delta Lake, MLflow, FastAPI  
**Deployment**: Hybrid - Local Docker (development) + Databricks Cloud (production)  
**Cost**: Local $0 (Docker), Cloud <$100/month (AWS S3 + Databricks trial)  

---

## 1. Overview

### 1.1 Executive Summary

The Enterprise ML Data Pipeline Platform (P16) is a production-grade MLOps infrastructure that enables real-time ingestion, distributed processing, feature engineering, experiment tracking, orchestration, and model serving for all 15 semiconductor AI/ML projects (P01-P15) at scale. Unlike fragmented single-project pipelines, P16 provides a unified end-to-end data platform that processes 1,000+ STDF files per day, generates reusable features for multiple models, tracks experiments across all projects, orchestrates automated retraining workflows, and serves predictions via FastAPI endpoints with <100ms latency.

The platform leverages industry-leading open-source technologies orchestrated in a 6-layer architecture: (1) **Apache Kafka** - real-time STDF ingestion with <1 minute latency from test completion to availability for ML models, (2) **Apache Spark (PySpark)** - distributed STDF parsing and feature engineering processing 10-100× faster than single-server Pandas, (3) **Delta Lake on S3** - ACID-compliant feature store with time-travel versioning enabling reproducible training and regulatory audits, (4) **MLflow** - centralized experiment tracking and model registry for all P01-P15 models with automated A/B testing and champion/challenger promotion, (5) **Apache Airflow** - DAG-based orchestration for scheduled retraining, data quality checks, and rollback workflows, and (6) **FastAPI + MLflow** - production model serving with auto-scaling, multi-model hosting, and <100ms p95 latency.

**Key Value Proposition**: Transform P01-P15 from isolated proof-of-concepts running on laptops with Pandas CSVs into production-ready ML systems processing real-time data at scale (1,000s STDFs/day), sharing features across projects via Delta Lake feature store (10-100× faster than re-parsing), tracking all experiments with MLflow (no more lost models), automating retraining with Airflow (weekly schedules, data quality checks), and serving predictions via FastAPI (<100ms latency, auto-scaling). Enable infrastructure cost efficiency (<$100/month cloud for 200GB data) and development velocity (local Docker for free iteration, Databricks for production scale).

### 1.2 Document Purpose

This PRD defines comprehensive requirements for designing, developing, testing, and deploying the P16 Enterprise ML Data Pipeline Platform. It covers:

- **Functional and non-functional requirements** for Kafka ingestion, Spark processing, Delta Lake feature store, MLflow tracking, Airflow orchestration, FastAPI serving
- **System architecture** with 6-layer design, data flow diagrams, integration points with P01-P15 models
- **Data model** including Delta Lake schemas (raw_stdf, wafer_features, parametric_stats, model_predictions), ACID transaction patterns, time-travel use cases
- **Hybrid deployment strategy** combining local Docker (development, free, 44GB actual STDFs from P15) and Databricks Cloud (production, <$100/month, 200GB synthetic STDFs from P07)
- **Phased implementation approach**: Phase 1 (Week 1-4) local development with Option A (replay existing 423 synthetic .std files for fast iteration), Phase 2 (Week 5-14) cloud production with Option B (generate 100-500 fresh .std files nightly via Airflow for realistic variability)
- **Mock model integration strategy**: Phase 1 uses baseline models (RandomForest, linear regression) to develop P16 infrastructure independently of P01-P15 model training, Phase 2 integrates production-trained models via MLflow registry
- **API specifications** for 20+ FastAPI endpoints serving predictions from all P01-P15 models with standardized request/response formats
- **Performance, scalability, security, and testing strategies** for production MLOps systems handling 1,000+ daily STDF ingestions
- **Success metrics and KPIs** validating infrastructure enablement (all 15 projects deployed), cost efficiency (<$100/month), and operational excellence (99.9% uptime)

The document serves as the single source of truth for cross-functional teams (Data Engineering, ML Engineering, MLOps, DevOps, Backend, Frontend, Test Engineering, Yield Engineering) throughout the 14-week development lifecycle.

### 1.3 Product Vision

**Vision Statement**: Establish the industry-leading MLOps infrastructure for semiconductor AI/ML at scale, enabling real-time data pipelines, distributed feature engineering, centralized experiment tracking, automated orchestration, and production model serving that transforms P01-P15 from isolated prototypes into production systems processing 1,000s of STDFs daily with <$100/month cloud cost and 99.9% uptime.

**Long-term Goals** (12-18 months):

- **Infrastructure Enablement**: Deploy all 15 AI/ML projects (P01-P15) into production via P16 platform
- **Real-time Data Processing**: Ingest 1,000+ STDFs per day via Kafka with <5 minute end-to-end latency (test completion → feature availability → model prediction)
- **Distributed Scale**: Process 10,000+ wafers/day with Spark parallel processing (100× faster than sequential Pandas on single server)
- **Feature Store Reuse**: Delta Lake feature store with 500+ engineered features shared across all projects, reducing redundant STDF parsing by 90%
- **Experiment Tracking**: MLflow tracking for 5,000+ experiments across P01-P15 with model registry, versioning, A/B testing, automated promotion
- **Automated Orchestration**: 50+ Airflow DAGs automating scheduled retraining (weekly/monthly), data quality checks, rollback workflows, alert notifications
- **Production Serving**: FastAPI serving predictions from 15 models with <100ms p95 latency, auto-scaling 2-20 replicas, 99.9% uptime
- **Cost Efficiency**: Total cloud cost <$100/month (AWS S3 $5/month for 200GB, Databricks Community Edition trial, local Docker $0)
- **Hybrid Deployment**: Local Docker for development (free, fast iteration, 44GB actual data), Databricks Cloud for production (scalable, managed Spark/Delta/MLflow)

**Differentiation from Alternative Approaches**:

- **Unified Platform vs. Fragmented Pipelines**: Single P16 infrastructure serves all 15 projects vs. 15 isolated pipelines with duplicated code/infrastructure
- **Real-time Kafka Ingestion vs. Batch Processing**: <5 min latency for test data availability vs. next-day batch file transfers
- **Distributed Spark vs. Single-Server Pandas**: 10-100× speedup via parallel processing vs. sequential CSV parsing bottlenecks
- **Delta Lake Feature Store vs. CSV/Parquet Files**: ACID transactions, time-travel versioning, schema evolution vs. fragile file-based storage
- **MLflow Centralized Tracking vs. Scattered Notebooks**: All experiments searchable in one place vs. lost models in local Jupyter notebooks
- **Airflow Orchestration vs. Manual Scripts**: Automated DAG workflows with retries, alerting, monitoring vs. cron jobs and manual triggering
- **FastAPI Auto-scaling Serving vs. Flask on Single VM**: Production-grade APIs with Kubernetes HPA vs. single-point-of-failure deployments
- **Hybrid Local+Cloud vs. Cloud-Only**: $0 local development cost + <$100/month production vs. $1,000+/month cloud-only approaches
- **Mock Models Phase 1 vs. Waiting for Model Training**: P16 infrastructure developed independently using baseline models vs. blocked on P01-P15 completion
- **200GB Synthetic Data from P07 vs. Real Production Data**: Cloud-safe, privacy-compliant development vs. regulatory/legal barriers to cloud deployment

**Strategic Impact on Portfolio**:

- **P01 (XGBoost Bin Predictor)**: Kafka real-time STDF → Spark feature engineering → Delta Lake features → MLflow XGBoost tracking → Airflow weekly retrain → FastAPI `/predict/bin` <100ms
- **P02 (Transfer Learning Yield)**: Kafka wafer complete → Spark ResNet inference → Delta Lake yield predictions → Airflow adaptive test termination → 40 min/lot savings
- **P03 (Multi-Agent RCA)**: Kafka failure spike → Airflow priority RCA spawn → Spark 6-agent parallel → Delta Lake 500K historical RCAs → FastAPI <2 min RCA report
- **P04 (ResNet Wafer Defect)**: Kafka wafer done → Spark ResNet segmentation → Delta Lake versioned wafer images → FastAPI FA alert <90 sec → immediate investigation
- **P05 (AMSA AI RCA)**: Delta Lake parsed AMSA reports → MLflow LangChain RAG tracking → Airflow automated report generation → FastAPI AMSA API
- **P06 (LSTM Anomaly Detector)**: Kafka parametric stream → Spark LSTM inference → Delta Lake anomaly alerts → Airflow drift investigation → FastAPI `/detect/anomaly`
- **P07 (GAN Synthetic Data)**: Airflow nightly GAN generation → 100-500 .std files → Kafka ingestion → Spark validation → Delta Lake storage (enables P16 Option B)
- **P08 (XGBoost Limit Change)**: Delta Lake what-if scenarios → Spark limit sweep → MLflow A/B test results → Airflow auto-apply optimal limits
- **P09 (Field Failure Predictor)**: Kafka field return data → Spark BERT classification → Delta Lake predictions → Airflow proactive recall analysis
- **P10 (GNN Failure Propagation)**: Delta Lake test correlations → Spark GNN training → MLflow graph embeddings → FastAPI `/predict/propagation`
- **P11 (Multi-Agent RL Optimizer)**: Airflow RL training DAG → Spark parallel rollouts → MLflow policy tracking → Delta Lake optimal test sequences
- **P12 (Edge AI Validation)**: Kafka JTAG captures → Spark ONNX/TensorRT validation → MLflow edge model registry → Delta Lake edge inference results
- **P13 (DQN Adaptive Test Flow)**: Kafka live test data → Spark DQN inference → Delta Lake Q-values → Airflow adaptive flow updates
- **P14 (Transformer Scan Localizer)**: Kafka scan chain dumps → Spark BERT inference → Delta Lake bug locations → FastAPI `/localize/bug`
- **P15 (Bayesian Shmoo Optimizer)**: Delta Lake shmoo history → Spark Bayesian optimization → MLflow acquisition function tracking → Airflow optimal voltage/freq

---

## 2. Problem Statement

### 2.1 Current Challenges

**Challenge 1: Fragmented Single-Project Pipelines with No Reuse**

- Each of P01-P15 develops isolated data pipeline: custom STDF parser, unique feature engineering, project-specific storage format
- Massive code duplication: 15 independent STDF parsers, 15 Pandas CSV pipelines, 15 training scripts with similar preprocessing
- No feature sharing across projects: P02 (yield prediction) and P04 (wafer defect) both parse same STDFs and generate spatial statistics independently
- Feature engineering bottleneck: re-parsing 1GB STDF takes 5-10 minutes on laptop, repeated 15 times across projects for same data
- CSV/Parquet file chaos: 500+ scattered CSV files across project folders, no versioning, no schema enforcement, manual cleanup
- Knowledge silos: each project's data engineer reinvents data pipeline patterns (ingestion, validation, feature store, versioning)

**Challenge 2: Laptop-Scale Development with No Production Path**

- All P01-P15 prototyped on individual laptops using Pandas, single-core processing, local Jupyter notebooks
- Pandas single-threaded bottleneck: processing 1,000 STDFs sequentially takes 83 hours (5 min each × 1,000) vs. Spark parallel in <1 hour
- Laptop memory limitations: 16GB RAM insufficient for large datasets (1,000 wafers × 30MB each = 30GB, OOM crashes)
- No horizontal scaling: cannot add more compute to speed up processing when deadlines approach
- Production deployment gap: Jupyter notebooks with hardcoded paths cannot transition to automated production pipelines
- Single-point-of-failure: if laptop crashes/stolen, all experiment history, trained models, feature engineering code lost
- After-hours unavailability: no 24/7 data processing or model serving (blocked on engineer's laptop being online)

**Challenge 3: Lost Experiments and Irreproducible Results**

- Experiment tracking chaos: 500+ Jupyter notebooks across 15 projects named `model_final_v2_FINAL_REALLY.ipynb`, no searchable metadata
- Lost models: "Which XGBoost hyperparameters gave 92% accuracy 2 months ago?" → unknown, notebook overwritten or deleted
- Irreproducible training: cannot recreate model from 6 months ago because training data CSVs were manually edited/deleted, no versioning
- Manual hyperparameter logging: engineers manually copy-paste metrics into Excel spreadsheets (error-prone, incomplete)
- No A/B testing infrastructure: comparing two model versions requires manual metric computation, no automated champion/challenger selection
- Cross-project comparison impossible: cannot compare P01 XGBoost vs. P08 XGBoost because metrics logged differently (accuracy vs. F1 vs. AUC)
- Model registry gap: trained PyTorch/XGBoost models saved as local .pkl/.pth files, no centralized storage, no versioning, no deployment metadata

**Challenge 4: Manual Orchestration and No Automation**

- Manual retraining: engineers manually re-run training notebooks every few weeks when "they remember" or failure rates spike
- No scheduled workflows: no automatic weekly/monthly retraining, no data quality checks before training, no rollback if new model degrades
- Ad-hoc data validation: engineers manually inspect STDF files for corruption, missing tests, outliers (time-consuming, error-prone)
- Reactive alerting: failures discovered after-the-fact when model predictions become inaccurate, no proactive drift detection
- Cron job fragility: some projects use cron jobs on personal laptops (fails when laptop off, no retry logic, no logging, no alerting)
- Manual handoffs: test engineer identifies new data → emails data engineer → data engineer processes files → emails ML engineer → ML engineer retrains → emails back results (72-hour latency)
- No dependency management: if upstream data processing fails, downstream model training proceeds with stale data (silent errors)

**Challenge 5: No Production Model Serving Infrastructure**

- Models trained but not deployed: P01-P15 models exist as Jupyter notebook outputs, not accessible to production test systems
- Flask APIs on laptops: some projects expose models via Flask running on engineer's laptop (unavailable after-hours, crashes, no monitoring)
- No auto-scaling: single Flask instance cannot handle peak loads (100+ simultaneous test requests), no horizontal scaling
- Manual prediction workflow: test engineer emails STDF file → ML engineer runs notebook → emails back prediction (hours of latency)
- No API versioning: when model updated, old API breaks without backward compatibility, client applications fail
- No monitoring/alerting: if model serving crashes, no automated alerts, discovered only when users complain
- Security gaps: Flask APIs have no authentication, CORS issues, no rate limiting, vulnerable to attacks

**Challenge 6: High Cloud Cost Barrier and Privacy Concerns**

- Cloud cost fear: engineers estimate $1,000+/month for AWS/Azure ML services → management rejects cloud deployment → stuck on laptops
- Real production data privacy: cannot upload actual customer STDFs to public cloud due to NDA/GDPR/automotive regulations
- All-or-nothing cloud approach: belief that "cloud means expensive managed services" vs. cost-optimized hybrid architectures
- No cloud budget allocated: P01-P15 have zero cloud budget, all infrastructure must be free or <$100/month to get approval
- Vendor lock-in concerns: worry that using AWS SageMaker or Azure ML locks into proprietary APIs, difficult to migrate later

### 2.2 Impact Analysis

**Business Impact**:

- **Infrastructure Barrier to Production**: Zero of P01-P15 projects deployed to production despite models trained and validated (stuck in Jupyter notebook phase)
- **Delayed ROI Realization**: P01-P15 collectively offer $25M+/year business value, but zero value captured while models remain on laptops
- **Engineering Time Waste**: 200+ hours/month across 4-5 data engineers building redundant data pipelines for each project independently
- **Opportunity Cost**: Engineers spend 70% time on data plumbing (parsing STDFs, feature engineering, manual retraining) vs. 30% on actual ML innovation
- **Scalability Ceiling**: Current laptop-based approach maxes out at processing ~50 STDFs/day vs. production requirement of 1,000+/day
- **Knowledge Attrition Risk**: When data engineer leaves, their project-specific pipeline code becomes unmaintainable by others (no documentation, no standardization)
- **Total Cost Impact**: $3M+/year in lost productivity (engineering time waste) + $25M+/year in unrealized ROI (models not deployed) = $28M+/year opportunity cost

**Technical Impact**:

- **Data Fragmentation**: 500+ CSV/Parquet files scattered across 15 project folders, no centralized data catalog, no schema enforcement
- **Processing Bottleneck**: Pandas single-threaded processing on laptops takes 10-100× longer than distributed Spark for same data
- **Storage Inefficiency**: Same STDF data duplicated 5-10× across projects (raw STDF + P02 CSV + P04 CSV + P10 CSV = 5× storage waste)
- **No ACID Guarantees**: CSV/Parquet files have no transactional guarantees (partial writes on crash, concurrent write conflicts, schema drift)
- **Experiment Tracking Chaos**: 500+ Jupyter notebooks with no searchable metadata, 90% of experiment history effectively lost
- **Model Deployment Gap**: No path from trained Jupyter notebook model to production API (manual Flask deployment, no CI/CD, no versioning)
- **No Reproducibility**: Cannot reproduce model training from 6 months ago (data deleted, code overwritten, environment changed)

**Operational Impact**:

- **Manual Toil**: Data engineers spend 40+ hours/week on manual tasks (data transfer, validation, feature engineering, retraining, deployment)
- **No 24/7 Availability**: All pipelines run on engineer laptops, unavailable after-hours or during PTO/sick leave
- **Reactive vs. Proactive**: Data quality issues, model drift, pipeline failures discovered after-the-fact vs. proactive monitoring/alerting
- **Slow Iteration Cycles**: 72-hour turnaround from "new data available" to "model retrained and predictions ready" due to manual handoffs
- **No Disaster Recovery**: If laptop crashes/stolen, all data, code, models lost (no backups, no cloud replication)
- **Scaling Bottleneck**: Cannot scale to 10× data volume (1,000 → 10,000 STDFs/day) without 10× more engineers (linear scaling, not sustainable)

### 2.3 Opportunity

**MLOps Infrastructure Transformation**:

- **Unified Platform for 15 Projects**: Single P16 infrastructure eliminates 15 fragmented pipelines, enabling code reuse, standardization, and economies of scale
- **Real-time Kafka Ingestion**: <5 min latency from STDF test completion to ML-ready features vs. 72-hour batch processing delays
- **Distributed Spark Processing**: 10-100× speedup via parallel STDF parsing across Spark cluster vs. sequential Pandas on laptop
- **Delta Lake Feature Store**: Centralized ACID-compliant storage with 500+ shared features eliminates 90% redundant STDF re-parsing across projects
- **MLflow Centralized Tracking**: All 5,000+ experiments searchable in one place with automated model registry, versioning, A/B testing
- **Airflow Automated Orchestration**: 50+ DAGs automate retraining, data quality checks, rollbacks vs. manual cron jobs and human triggers
- **FastAPI Production Serving**: Auto-scaling APIs with <100ms latency, 99.9% uptime vs. unreliable Flask on laptops
- **Hybrid Cost Optimization**: $0 local Docker development + <$100/month Databricks Cloud production vs. $1,000+/month all-cloud approaches

**Scalability Benefits**:

- **Horizontal Scaling**: Add Spark workers to process 10× more data without code changes (vs. linear engineer headcount scaling)
- **Feature Reuse**: Parse STDF once → 15 projects consume features from Delta Lake (vs. 15× redundant parsing)
- **Parallel Experiments**: Run 10 model training experiments simultaneously on Databricks cluster (vs. 1 at a time on laptop)
- **Automated Retraining**: Airflow schedules 50+ DAGs weekly/monthly without human intervention (vs. manual ad-hoc retraining)
- **Multi-Model Serving**: Single FastAPI deployment serves 15 models via MLflow registry (vs. 15 independent Flask instances)
- **Global 24/7 Availability**: Cloud-based pipelines and APIs run continuously (vs. laptop-dependent availability)

**ROI Potential**:

- **Infrastructure Enablement**: Unlock $25M+/year business value from deploying all P01-P15 to production (currently $0)
- **Engineering Productivity**: Reduce data engineering time by 70% (200 hrs/month → 60 hrs/month) via automation and reusable components
- **Direct Cost Savings**: $2M+/year in engineering time savings (140 hrs/month × $150/hr × 12 months)
- **Indirect Benefits**: Faster model iteration (72hr → 5min retraining latency), improved model quality (more experiments), better data quality (automated validation)
- **Strategic Value**: Production MLOps capability enables future AI/ML projects beyond P01-P15, competitive advantage in semiconductor industry
- **Cloud Cost Efficiency**: <$100/month cloud infrastructure (200GB S3 + Databricks trial) vs. industry-standard $1,000+/month for equivalent capacity

**Technology Enablers**:

- **Open-Source Ecosystem**: Kafka, Spark, Delta Lake, MLflow, Airflow all Apache 2.0 licensed (no vendor lock-in, active communities)
- **Databricks Managed Platform**: Integrated Spark + Delta Lake + MLflow reduces operational complexity vs. self-managing Hadoop/Kubernetes clusters
- **Hybrid Deployment Model**: Local Docker for free development, cloud for production enables cost-optimized staging (vs. cloud-only or on-prem-only)
- **Synthetic Data from P07**: 200GB GAN-generated .std files enable cloud deployment without privacy/regulatory barriers (vs. blocked on real data)
- **Mock Model Strategy**: P16 developed independently using baseline models, unblocking infrastructure work from P01-P15 model training completion

**Competitive Advantage**:

- **Industry-Leading MLOps**: Few semiconductor companies have production ML pipelines at this scale (most stuck in Jupyter notebook phase like current state)
- **Cost Leadership**: <$100/month cloud cost for 200GB data and 15 models is 10× lower than typical industry solutions ($1,000+/month)
- **Time-to-Market**: 14-week P16 implementation enables deploying all P01-P15 in parallel vs. 2+ years if each project builds own infrastructure
- **Scalability**: Platform designed for 1,000 STDFs/day can scale to 10,000/day without re-architecture (vs. brittle laptop-based prototypes)
- **Knowledge Capture**: Standardized platform with documentation and reusable components survives engineer turnover (vs. tribal knowledge loss)

---

## 2. Problem Statement

### 2.1 Current Challenges

**Challenge 1: Fragmented Data Pipelines Across 15 Projects**
- Each of P01-P15 implements independent STDF parsing (95% duplicate code, no code reuse)
- Same STDF files parsed 15 times by different projects (10× wasted compute, storage, engineering time)
- No shared feature engineering (wafer-level yield, parametric statistics, spatial patterns recomputed by every project)
- Inconsistent data transformations (P02 uses one outlier detection method, P04 uses different method on same data)
- No cross-project data quality validation (missing values handled differently, causing model discrepancies)
- Estimated waste: **$500K+/year** in duplicate engineering effort, 10× data processing redundancy

**Challenge 2: Laptop-Scale Development Bottlenecks Production Deployment**
- All P01-P15 prototypes built on single laptops with Pandas (RAM-limited to 10GB, single-threaded CSV processing)
- Pandas bottlenecks: 10GB STDF data requires 30-60 minutes to parse on laptop vs. <5 minutes with distributed Spark
- No path to production: laptop prototypes cannot scale to 1,000 STDFs/day (current production test volume)
- Manual data extracts: test engineers export STDF subsets for ML teams (days of delay, stale data, manual effort)
- No real-time inference: models run batch predictions overnight on CSV exports (not real-time from test floor)
- Infrastructure gap prevents P01-P15 from delivering **$25M+/year** business value (stuck in POC phase)

**Challenge 3: Lost Experiments and Irreproducible Models**
- 500+ Jupyter notebooks scattered across 15 projects with no centralized tracking
- No MLflow/experiment tracking: hyperparameters, model versions, training data provenance all undocumented
- Champion model identification impossible: "Which XGBoost model from P01 had 0.89 F1-score 6 months ago?" → unknown
- Model reproducibility broken: cannot retrain same model due to missing random seeds, unknown data versions, lost preprocessing steps
- A/B testing infeasible: no framework to compare model versions (champion vs. challenger)
- Knowledge loss: 80% of experiments wasted (good ideas forgotten, bad ideas repeated, no learning accumulation)
- Estimated waste: **$2M+/year** in repeated experiments, debugging irreproducible results, re-discovering known failures

**Challenge 4: Manual Orchestration and Reactive Operations**
- No automated retraining: models deployed once, never updated with new data (drift accumulates)
- Manual triggers: test engineer emails ML engineer "please retrain yield predictor" → 3-day turnaround
- No data quality checks: broken STDFs fed to models causing silent prediction failures
- Reactive alerting: model predictions fail → users complain → engineers debug (no proactive monitoring)
- Cron script brittleness: retraining scheduled via cron with no dependency management, error handling, or retry logic
- Operational burden: **40 engineer-hours/month** spent on manual model updates, debugging pipeline failures, reactive firefighting

**Challenge 5: No Production Model Serving Infrastructure**
- Models "served" via Flask on laptop (single-threaded, no load balancing, downtime during retraining)
- Prediction latency: 5-10 seconds per request (unacceptable for real-time test floor decisions)
- No auto-scaling: fixed capacity regardless of load (weekends idle, peak hours overloaded)
- No multi-model hosting: each P01-P15 model requires separate Flask app (15 apps to manage)
- No versioning: cannot roll back to previous model version if new model performs poorly
- Business impact: **$1M+/year** lost yield optimization opportunities due to slow/unavailable predictions

**Challenge 6: Cloud Cost Fears Block Spark/Databricks Adoption**
- Teams estimate $1,000+/month AWS costs for Spark clusters → abandon distributed processing idea
- No hybrid deployment strategy: "all-or-nothing" cloud migration perceived as too expensive
- Privacy concerns: real STDF files contain proprietary chip data → reluctance to upload to cloud
- Lack of synthetic data: no privacy-safe dataset for cloud experimentation
- Result: P01-P15 remain laptop-bound despite Spark's 100× speedup potential
- Opportunity cost: **$3M+/year** in slower development, delayed production deployment, inability to scale

### 2.2 Business Impact

**Unrealized ROI from P01-P15 Stuck in POC Phase**:
- **P01 XGBoost Bin Predictor**: $2M+/year value (reduce test time via bin prediction) - POC complete, no production deployment
- **P02 Transfer Learning Yield Predictor**: $5M+/year value (wafer-level yield prediction for WIP prioritization) - trained model exists, no serving infrastructure
- **P03 Multi-Agent RCA**: $4M+/year value (automated root cause analysis) - LangGraph prototype, no data pipeline to feed agents
- **P04 ResNet Wafer Defect Classifier**: $2M+/year value (automated defect detection) - model accuracy validated, no real-time wafer map ingestion
- **P05 AMSA AI RCA Reporter**: $3M+/year value (automated report generation) - LLM integration works, no access to production STDF data
- **P06-P15**: Additional $9M+/year value across LSTM anomaly detection, GAN data synthesis, GNN failure propagation, RL test optimization, etc.
- **Total Unrealized ROI**: **$25M+/year** from P01-P15 unable to deploy to production due to missing data infrastructure

**Engineering Waste and Operational Inefficiency**:
- **Duplicate Data Processing**: $500K/year (15 projects re-parsing same STDFs, no feature sharing)
- **Lost Experiments**: $2M/year (500+ notebooks with no tracking, 80% wasted effort, irreproducible results)
- **Manual Orchestration**: $300K/year (40 hrs/month × 12 months × $150/hr fully loaded for reactive ops)
- **Delayed Corrective Actions**: $200K/year (slow model predictions delay test limit changes, lot dispositions)
- **Total Engineering Waste**: **$3M+/year**

**Total Opportunity Cost**: **$28M+/year** ($25M unrealized ROI + $3M engineering waste)

**Competitive Risk**:
- Competitors with mature MLOps platforms deploy AI/ML to production 6-12 months faster
- First-mover advantage in AI-driven yield optimization, automated test, predictive maintenance
- Talent retention risk: ML engineers frustrated by inability to deploy models leave for companies with better infrastructure

### 2.3 Opportunity

**MLOps Transformation Enabled by P16**:
- **10-100× Processing Speedups**: Spark distributed parsing processes 10GB STDF data in <5 minutes (vs. 30-60 min Pandas on laptop)
- **90% Feature Reuse**: Delta Lake feature store eliminates duplicate parsing - wafer-level features computed once, reused by all 15 projects
- **Zero Knowledge Loss**: MLflow tracks 100% of experiments (vs. 80% lost today) with full reproducibility (hyperparameters, data versions, random seeds)
- **Automated Operations**: Airflow DAGs handle retraining (weekly schedules), data quality checks, model promotions (champion/challenger) with no manual intervention
- **Production Serving**: FastAPI endpoints serve predictions in <100ms (vs. 5-10 sec Flask) with auto-scaling (2-20 replicas based on load)
- **<$100/month Cloud Cost**: Hybrid deployment (local Docker dev + Databricks Cloud prod) eliminates $1,000+ cost barrier

**Strategic Infrastructure Enabler**:
- **Production Deployment for All P01-P15**: Unified platform enables deploying all 15 models simultaneously (vs. 2+ years if each builds own infrastructure)
- **Real-time ML on Test Floor**: Kafka ingestion → Spark processing → FastAPI serving pipeline delivers predictions within minutes of test completion (vs. days with manual CSV exports)
- **Scalability Headroom**: Platform designed for 1,000 STDFs/day can scale to 10,000/day without re-architecture (10× growth capacity)
- **Regulatory Compliance**: Delta Lake time-travel provides audit trail for model training data (required for automotive ISO 26262, IATF 16949)
- **Cross-project Intelligence**: Shared feature store enables meta-learning (P02 yield predictor uses features from P10 GNN failure propagation graph)

**Cost Efficiency and Privacy Solution**:
- **Databricks Community Edition**: $0/year for first year (14GB clusters, 15GB storage, sufficient for P16 development)
- **AWS Free Tier**: $0 Year 1 for S3 (5GB free), EC2 (750 hours/month free), sufficient for P16 POC
- **S3 Storage Cost**: 200GB synthetic STDFs @ $0.023/GB/month = **$5-25/month** (depending on access patterns)
- **Databricks Production**: Estimate <$50/month for 14GB cluster running 40 hrs/month (weekly retraining jobs)
- **Total Cloud Cost**: **<$100/month** (vs. $1,000+ fear that blocked adoption)
- **Privacy-Safe Data**: P07 GAN generates 200GB synthetic .std files (realistic test data, no proprietary chip information) → safe for Databricks Cloud

**ROI Calculation**:
- **Investment**: $600K (14 weeks × 4.5 FTE × $10K/FTE/week fully loaded)
- **Benefit Year 1**: $25M unrealized ROI unlocked + $3M engineering waste eliminated = **$28M/year**
- **Payback Period**: <1 month
- **5-Year NPV**: >$100M (assuming $28M/year sustained benefit, conservative)

---

## 3. Goals and Objectives

### 3.1 Primary Goals

**Goal 1: Unified Data Infrastructure for All P01-P15 Projects**
- Deploy single end-to-end MLOps platform serving all 15 AI/ML projects (no fragmented per-project pipelines)
- Ingest 1,000+ STDF files per day via Kafka with <5 minute latency from test completion to feature availability
- Process data 10-100× faster with Spark distributed computing (vs. single-server Pandas bottlenecks)
- Store features in Delta Lake with ACID transactions, time-travel versioning, and schema evolution
- Achieve 90%+ feature reuse across projects (wafer-level yield, parametric statistics, spatial patterns computed once, shared by all)
- Eliminate duplicate STDF parsing (currently 10× redundant processing across 15 projects)

**Goal 2: Production-Grade Model Serving with <100ms Latency**
- Deploy FastAPI endpoints for all P01-P15 models with unified request/response formats
- Achieve <100ms p95 prediction latency (vs. 5-10 sec Flask on laptops today)
- Implement auto-scaling (2-20 replicas based on load) with zero-downtime deployments
- Support multi-model hosting (15 models served from single platform, not 15 separate Flask apps)
- Enable A/B testing and champion/challenger model deployments via MLflow model registry
- Deliver 99.9% API uptime (max 8 hours downtime/year)

**Goal 3: Comprehensive Experiment Tracking and Reproducibility**
- Track 100% of experiments across all P01-P15 projects in centralized MLflow (vs. 80% lost in scattered notebooks today)
- Log hyperparameters, metrics, model artifacts, data versions, random seeds for full reproducibility
- Enable champion model identification (answer "which model version achieved 0.89 F1-score 6 months ago?")
- Support model versioning with rollback capability (revert to previous version if new model performs poorly)
- Provide model lineage tracking (data pipeline version + training code commit + hyperparameters → model artifact)
- Reduce experiment waste from 80% to <20% via systematic tracking and knowledge accumulation

**Goal 4: Automated Orchestration and Proactive Operations**
- Orchestrate weekly retraining workflows via Airflow DAGs (no manual triggers)
- Implement data quality checks (missing values, schema validation, statistical drift detection) before training
- Enable automated model promotion (champion/challenger A/B testing → auto-promote if accuracy improves >3%)
- Deploy proactive alerting (data quality failures, model drift, prediction latency spikes) before users complain
- Reduce manual orchestration time from 40 hrs/month to <5 hrs/month (monitoring only, no manual intervention)
- Achieve <1 hour MTTR (mean time to recovery) for pipeline failures via automated rollback

**Goal 5: Cost-Efficient Hybrid Deployment**
- Enable $0 local development (Docker Compose: Kafka, Spark, Airflow, PostgreSQL, MinIO on laptops)
- Deploy Databricks Cloud production for <$100/month (Community Edition free Year 1, then 14GB cluster @ <$50/month)
- Use 200GB synthetic STDF data from P07 GAN (privacy-safe for cloud, no proprietary chip data)
- Implement cost monitoring and auto-shutdown (Databricks clusters terminate after 2 hours idle)
- Validate cost efficiency: $100/month for 15 models = $6.67/model/month (vs. $1,000+ industry typical)
- Prove business case: <1 month payback period ($28M/year benefit vs. $600K investment)

### 3.2 Business Objectives

**Objective 1: Enable Production Deployment of All P01-P15 Projects**
- **Target**: Deploy all 15 AI/ML projects into production via P16 platform within 18 months
- **Success Criteria**:
  - Phase 1 (Month 1-3): 3 projects deployed (P01 XGBoost, P02 Transfer Learning, P04 ResNet)
  - Phase 2 (Month 4-9): Additional 6 projects (P03 Multi-Agent RCA, P05 AMSA RCA, P06 LSTM, P08 XGBoost Limits, P09 Transfer Learning Field, P10 GNN)
  - Phase 3 (Month 10-18): Remaining 6 projects (P07 GAN, P11 Multi-Agent RL, P12 Edge AI, P13 DQN, P14 Transformer, P15 Bayesian)
- **Business Value**: Unlock $25M+/year unrealized ROI from production AI/ML systems

**Objective 2: Reduce Data Processing Time by 90%**
- **Current State**: Pandas single-server processing: 30-60 minutes for 10GB STDF data
- **Target State**: Spark distributed processing: <5 minutes for 10GB STDF data (10-12× speedup)
- **Success Criteria**:
  - Benchmark: Parse 423 existing synthetic STDFs in <30 minutes total (vs. 8+ hours sequential Pandas)
  - Production: Process 1,000 STDFs/day with <5 minute p95 latency per file
  - Scalability test: Process 10,000 STDFs in single day without performance degradation
- **Business Value**: Free 320+ engineer-hours/month currently spent waiting for data processing

**Objective 3: Achieve 90% Feature Reuse Across Projects**
- **Current State**: Each of 15 projects parses same STDFs independently (10× redundant compute)
- **Target State**: Delta Lake feature store computes features once, reused by all projects
- **Success Criteria**:
  - Shared features: wafer-level yield, parametric statistics (mean/std/p95), spatial pattern embeddings, test correlation matrices
  - Reuse metrics: 90% of P01-P15 model training queries use Delta Lake features (vs. parsing raw STDFs)
  - Cost reduction: Eliminate 90% of duplicate STDF parsing compute (save $450K/year of $500K waste)
- **Business Value**: $450K/year engineering time savings, 10× faster feature engineering for new projects

**Objective 4: Reduce Experiment Waste from 80% to <20%**
- **Current State**: 500+ notebooks scattered, no tracking, 80% of experiments lost/irreproducible
- **Target State**: 100% experiments tracked in MLflow with full reproducibility
- **Success Criteria**:
  - MLflow adoption: 100% of P01-P15 model training runs logged (hyperparameters, metrics, artifacts)
  - Reproducibility: 95% of experiments reproducible within ±2% accuracy (re-run with same data/hyperparameters)
  - Knowledge retention: 100% of champion models identifiable with lineage (data + code + hyperparameters)
- **Business Value**: $1.6M/year savings (80% of $2M experiment waste eliminated), faster ML iteration cycles

**Objective 5: Deliver <100ms Prediction Latency for Real-Time Decisions**
- **Current State**: Flask on laptop: 5-10 seconds per prediction (too slow for test floor decisions)
- **Target State**: FastAPI with auto-scaling: <100ms p95 latency
- **Success Criteria**:
  - Latency benchmarks: P01 XGBoost <20ms, P02 ResNet <80ms, P04 U-Net <100ms, P10 GNN <150ms (graph query overhead)
  - Throughput: Support 1,000+ predictions/minute during peak hours (vs. 6-10/min Flask)
  - Availability: 99.9% uptime (max 8 hours downtime/year)
- **Business Value**: Enable real-time ML decisions on test floor (wafer disposition, test limit changes, lot prioritization worth $1M+/year)

**Objective 6: Maintain Cloud Cost <$100/month**
- **Target**: Total cloud cost (Databricks + S3) <$100/month for 200GB data, 15 models, 1,000 STDFs/day
- **Cost Breakdown**:
  - Databricks Community Edition: $0/month (free for 1 year, 14GB cluster)
  - Databricks Production (Year 2+): <$50/month (14GB cluster, 40 hours/month usage for weekly retraining)
  - AWS S3: $5-25/month (200GB @ $0.023/GB/month Standard tier, depends on access patterns)
  - Data transfer: <$10/month (ingress free, egress minimal for model artifacts)
- **Success Criteria**:
  - Month 1-12: $0-25/month (Community Edition + S3 only)
  - Month 13+: <$100/month total
  - Cost monitoring: Grafana dashboard tracks daily spend, alerts if exceeding $3/day average
- **Business Value**: Overcome $1,000+/month cost barrier, prove cloud MLOps is affordable for mid-size companies

### 3.3 Success Metrics

**Infrastructure Performance Metrics**:
- **Ingestion Latency**: <5 minutes p95 from STDF arrival to Kafka to Delta Lake (target: <1 minute p50)
- **Processing Throughput**: 1,000+ STDFs processed per day via Spark (scalable to 10,000/day)
- **Feature Engineering Speed**: <5 minutes p95 to parse 10GB STDF and compute 500+ features (vs. 30-60 min Pandas)
- **Serving Latency**: <100ms p95 prediction latency for all models (breakdowns: P01 <20ms, P02 <80ms, P04 <100ms)
- **API Uptime**: >99.9% (max 8 hours downtime/year, target: 99.95% = 4 hours/year)
- **Auto-scaling Response**: Scale from 2 to 20 replicas within 2 minutes when load increases 10×

**MLOps Quality Metrics**:
- **Experiment Tracking Coverage**: 100% of P01-P15 training runs logged to MLflow (vs. 20% today)
- **Model Reproducibility**: 95% of experiments reproducible within ±2% accuracy (re-run with logged hyperparameters/data)
- **Feature Reuse Rate**: 90% of model training queries use Delta Lake features (vs. 0% raw STDF parsing)
- **Data Quality Pass Rate**: >98% of ingested STDFs pass quality checks (schema validation, completeness, statistical sanity)
- **Retraining Success Rate**: >95% of automated Airflow retraining DAGs complete successfully (no manual intervention)
- **Model Promotion Rate**: 30% of challenger models promoted to champion (vs. 0% today - no A/B testing framework)

**Cost Efficiency Metrics**:
- **Cloud Cost**: <$100/month total (Databricks + S3 + data transfer)
- **Cost per Model**: <$10/month per deployed model (15 models = $100-150/month including shared infra)
- **Cost per Prediction**: <$0.001 per prediction (1M predictions/month = $1,000/month at scale, well under budget)
- **Development Cost**: $0/month for local Docker development (vs. cloud-only alternatives requiring $500+/month)

**Business Impact Metrics**:
- **P01-P15 Production Deployment**: 15/15 projects deployed within 18 months
- **Realized ROI**: $25M+/year unlocked from P01-P15 production systems (measured via project-specific KPIs: test time reduction, yield improvement, RCA time savings)
- **Engineering Time Savings**: 320+ hours/month freed (10× STDF parsing eliminated, 40 hrs/month manual orchestration eliminated)
- **Experiment Waste Reduction**: 80% → <20% (measured via MLflow experiment success rate, reproducibility)
- **Payback Period**: <1 month ($600K investment / $28M annual benefit × 12 months)

**Adoption and User Satisfaction Metrics**:
- **Active Users**: 50+ engineers using P16 platform weekly (Data Engineers, ML Engineers, Test Engineers, Yield Engineers)
- **API Request Volume**: 10,000+ prediction requests/day across all P01-P15 models
- **Platform Satisfaction**: >4.0/5.0 user satisfaction rating (quarterly surveys)
- **Documentation Completeness**: 100% of APIs, DAGs, schemas documented with examples
- **Onboarding Time**: New project teams onboarded to P16 in <2 weeks (vs. 2+ months to build own infrastructure)

---

## 4. Target Users/Audience

### 4.1 Primary Users

**Data Engineers** (20+ users):
- Build and maintain Kafka ingestion pipelines, Spark ETL jobs, Delta Lake schemas
- Optimize data processing performance (Spark query plans, partition strategies, caching)
- Monitor data quality (schema drift, missing values, statistical anomalies)
- Troubleshoot pipeline failures (Kafka lag, Spark OOM errors, Delta Lake transaction conflicts)
- Manage hybrid deployment (Docker Compose local dev, Databricks Cloud production)
- Define data SLAs (ingestion latency, processing throughput, feature freshness)

**ML Engineers** (30+ users from P01-P15 teams):
- Train models using Delta Lake features (no raw STDF parsing needed)
- Track experiments in MLflow (log hyperparameters, metrics, model artifacts)
- Deploy models to FastAPI serving layer (upload to MLflow registry → auto-deploy)
- Monitor model performance (prediction latency, accuracy drift, A/B test results)
- Implement retraining logic (Airflow DAGs for weekly/monthly model updates)
- Collaborate across projects via shared feature store (reuse wafer-level features from other teams)

**Platform Engineers** (10+ users):
- Provision Databricks clusters, configure Spark settings, optimize resource allocation
- Manage Kubernetes deployments (FastAPI pods, Airflow workers, monitoring stack)
- Implement security controls (OAuth2 authentication, RBAC, TLS encryption, secrets management)
- Monitor infrastructure health (CPU/memory/disk utilization, Kafka consumer lag, Spark job failures)
- Scale platform capacity (add Spark executors, increase Kafka partitions, expand Delta Lake storage)
- Optimize cloud costs (auto-shutdown idle clusters, S3 lifecycle policies, spot instances)

**DevOps Engineers** (5+ users):
- Build CI/CD pipelines (GitHub Actions: lint → test → build Docker images → deploy to K8s)
- Automate deployment workflows (Helm charts for Kafka/Spark/Airflow/FastAPI)
- Implement blue-green deployments (zero-downtime model updates, rollback on failures)
- Manage monitoring stack (Prometheus metrics, Grafana dashboards, OpenSearch logs, alerts)
- Configure backup/restore procedures (Delta Lake snapshots, MLflow registry backups, disaster recovery)
- Enforce infrastructure-as-code practices (Terraform for cloud resources, version-controlled configs)

### 4.2 Secondary Users

**Test Engineers** (150+ users):
- Submit STDFs for processing via Kafka (manual upload or automated ATE integration)
- Query Delta Lake features for ad-hoc analysis (wafer yield trends, parametric distributions)
- Consume model predictions via FastAPI (bin prediction, yield forecast, defect detection)
- Validate model accuracy against actual test results (close feedback loop for model improvement)
- Report data quality issues (missing STDFs, schema mismatches, invalid test codes)

**Yield Engineers** (50+ users):
- Analyze wafer-level features from Delta Lake (spatial patterns, parametric trends, correlations)
- Review MLflow experiment results (compare model accuracy across projects)
- Provide domain expertise for feature engineering (suggest new features: edge die yield, test correlation metrics)
- Validate model predictions against fab data (cross-check yield forecasts with actual lot outcomes)
- Contribute to data quality definitions (acceptable ranges for parametric measurements, outlier thresholds)

**ML Researchers/Scientists** (10+ users):
- Experiment with novel architectures using Delta Lake features (no data engineering overhead)
- Compare model performance via MLflow (XGBoost vs. LightGBM vs. neural networks on same features)
- Prototype new projects (P17, P18, ...) using P16 infrastructure (fast time-to-first-model)
- Publish research findings citing P16 capabilities (industry thought leadership, conference papers)

**Management/Leadership** (10+ users):
- Monitor platform ROI via Grafana dashboards (cost, usage, P01-P15 production deployment status)
- Review quarterly business metrics (realized ROI from deployed models, engineering time savings)
- Approve cloud budget (monthly AWS/Databricks cost reports, variance explanations)
- Prioritize platform enhancements (new features, performance optimizations, additional projects)
- Communicate success stories to executives (case studies: P01 saved $2M/year via P16-enabled deployment)

### 4.3 User Personas

**Persona 1: Emily - Senior Data Engineer**
- **Background**: 8 years data engineering experience (Hadoop, Spark, Kafka), expert in distributed systems and performance optimization
- **Current Pain Points**:
  - Maintains 3 separate STDF parsing pipelines for P01, P02, P04 (95% duplicate code, nightmare to keep in sync)
  - Pandas bottlenecks: 10GB STDF takes 45 minutes to parse on her laptop (blocks ML team from experimenting)
  - No production path: prototypes work on laptop but cannot deploy to production (no Kafka/Spark infrastructure)
  - Cloud cost fears: estimated $2,000/month for AWS EMR Spark cluster → management rejected proposal
  - Manual operations: restarts failed parsing jobs at 2am when test engineers report missing data
- **Goals with P16**:
  - Build ONE unified STDF parsing pipeline serving all P01-P15 (eliminate duplicate code)
  - Achieve <5 minute parsing for 10GB STDFs with Spark (10× faster than Pandas, unblock ML teams)
  - Deploy to production with <$100/month cloud cost (prove to management it's affordable)
  - Automate failure recovery with Airflow (no more 2am manual restarts, self-healing pipelines)
  - Enable self-service for ML teams (engineers query Delta Lake directly, no data request tickets)
- **Success Criteria**:
  - Single codebase for STDF parsing (replace 3 divergent pipelines with 1 shared pipeline)
  - 10× parsing speedup validated (Pandas 45min → Spark <5min on same 10GB STDF)
  - Production deployed to Databricks <$100/month (prove cloud affordability)
  - Zero 2am pages for failed pipelines (Airflow auto-retry + alerting reduces manual intervention 95%)
  - ML team satisfaction >4.5/5.0 (fast feature availability, self-service queries)

**Persona 2: Alex - ML Engineer (P02 Transfer Learning Yield Predictor)**
- **Background**: 5 years ML experience (PyTorch, transformers, computer vision), PhD in ML, domain expert in wafer yield prediction
- **Current Pain Points**:
  - Spends 40% of time on data engineering (parsing STDFs, cleaning data, feature engineering) vs. 60% preferred on modeling
  - Lost 50+ experiments (notebooks overwritten, hyperparameters forgotten, champion model irreproducible)
  - Cannot deploy P02 yield predictor to production (no serving infrastructure, Flask on laptop too slow/unreliable)
  - Model retraining manual (every 3 months, test engineer emails "please retrain" → 2-week turnaround)
  - No A/B testing framework (cannot compare ResNet-18 vs. ResNet-50, must pick one and hope)
- **Goals with P16**:
  - Focus on modeling (Delta Lake provides pre-computed features, no STDF parsing needed)
  - Track ALL experiments in MLflow (never lose hyperparameters, always reproducible)
  - Deploy P02 to production FastAPI (real-time yield predictions for test floor, <100ms latency)
  - Automate weekly retraining via Airflow (fresh data from Delta Lake, no manual intervention)
  - Enable A/B testing (champion ResNet-18 vs. challenger ResNet-50, auto-promote if accuracy improves)
- **Success Criteria**:
  - 80%+ time on modeling vs. 20% data engineering (vs. 60/40 today)
  - 100% experiments logged to MLflow (vs. 50+ lost experiments today)
  - P02 serving <100ms p95 latency (measured via FastAPI metrics, vs. 5-10 sec Flask)
  - Zero manual retraining requests (Airflow DAG runs weekly, checks data quality, trains, validates, deploys champion)
  - 3+ successful A/B tests per year (compare architectures, hyperparameters, features with statistical significance)

**Persona 3: Raj - Platform Engineer**
- **Background**: 10 years infrastructure experience (Kubernetes, Terraform, AWS), expert in cloud cost optimization and SRE practices
- **Current Pain Points**:
  - Manages 15 separate Flask apps for P01-P15 (different ports, inconsistent configs, manual deployments)
  - No auto-scaling (fixed EC2 instances, over-provisioned for peak → wasted 70% of compute during off-hours)
  - Poor observability (scattered logs, no centralized metrics, reactive debugging when users report "API is slow")
  - Manual deployments (SSH to servers, git pull, restart Flask, pray it works, no rollback mechanism)
  - Cloud cost uncertainty (no usage tracking, surprise $1,500 AWS bill last month, management unhappy)
- **Goals with P16**:
  - Consolidate to single FastAPI platform (15 models, 1 deployment, consistent config/monitoring)
  - Implement auto-scaling (2-20 replicas based on load, save 70% compute cost during off-hours)
  - Comprehensive observability (Prometheus metrics, Grafana dashboards, OpenSearch logs, PagerDuty alerts)
  - Automated CI/CD (GitHub Actions: commit → test → build Docker → deploy to K8s → smoke tests)
  - Predictable cloud costs (<$100/month with monitoring, no surprise bills, cost attribution per project)
- **Success Criteria**:
  - 15 models served from single FastAPI platform (vs. 15 separate Flask apps)
  - 70% compute cost savings (auto-scaling: 2 replicas off-hours, 20 replicas peak)
  - <5 minute MTTD (mean time to detect issues via Grafana alerts vs. 30+ min user complaints)
  - <1 hour MTTR (mean time to recover via automated rollback vs. 4+ hours manual debugging)
  - 100% cloud cost transparency (daily Grafana dashboard, cost per project, <$100/month total validated)

**Persona 4: Sarah - Test Engineer (P01 XGBoost User)**
- **Background**: 3 years test engineering on Advantest V93000, responsible for 500+ lots/month, limited ML experience
- **Current Pain Points**:
  - Waits 3 days for P01 bin predictions (emails ML team → manual STDF extract → model runs overnight → results emailed back)
  - Predictions often stale (model trained 6 months ago, test floor conditions changed, accuracy degraded)
  - Cannot validate predictions (no confidence scores, no feature explanations, "black box" model)
  - Manual data exports (every week, extracts STDF subsets for ML teams, 4 hours wasted)
  - No real-time decisions (bin predictions arrive too late to adjust test flow for current lot)
- **Goals with P16**:
  - Real-time bin predictions (<1 minute from STDF upload to FastAPI response)
  - Automatic model updates (weekly retraining with fresh data, predictions stay accurate)
  - Prediction explanations (SHAP values show why die predicted Bin 3 vs. Bin 1)
  - Self-service (upload STDF via web UI, get predictions, no emails to ML team)
  - Confidence scores (know when to trust predictions: 95% confident vs. 60% confident)
- **Success Criteria**:
  - <1 minute prediction turnaround (upload STDF → FastAPI → results, vs. 3 days email roundtrip)
  - Model freshness: retrained weekly (vs. 6-month staleness), prediction accuracy maintained >85%
  - 100% predictions include confidence scores and top-3 SHAP feature importances
  - Zero manual data export requests (Kafka auto-ingestion, ML teams self-serve from Delta Lake)
  - Test floor decisions enabled: adjust test limits, skip tests, prioritize lots based on real-time predictions

**Persona 5: David - Junior ML Engineer (New to Company)**
- **Background**: Fresh PhD graduate (1 month at company), assigned to P14 Transformer Scan Chain Localizer, learning semiconductor domain
- **Current Pain Points**:
  - Overwhelmed by data complexity (STDF binary format, 1,000+ test codes, no documentation)
  - Cannot reproduce senior engineer's results (notebook runs fail with missing dependencies, unknown hyperparameters)
  - Wastes 2 weeks setting up local environment (install Pandas, parse STDFs, discover data quality issues)
  - No guidance on feature engineering (which parametric tests matter for scan chain failures? unknown)
  - Afraid to experiment (might overwrite important notebooks, break production code)
- **Goals with P16**:
  - Fast onboarding (<1 week from zero to training first model using Delta Lake features)
  - Pre-computed features (no STDF parsing expertise needed, domain experts already engineered relevant features)
  - Experiment safely (MLflow tracks everything, cannot lose work, easy rollback)
  - Learn from others (see P02/P04 MLflow experiments, understand what features/hyperparameters work)
  - Standardized workflows (follow P16 best practices: Delta Lake → MLflow → Airflow → FastAPI)
- **Success Criteria**:
  - Train first P14 model within 1 week (vs. 2+ weeks wasted on setup today)
  - 100% experiments tracked (no fear of losing work, safe to experiment)
  - Feature reuse from P02/P04 (leverage wafer-level features, no re-inventing wheel)
  - Learning via MLflow (browse 500+ experiments from senior engineers, understand best practices)
  - Deploy P14 to production within 3 months (vs. 6+ months without P16 infrastructure)

---

## 5. User Stories

**US-01: Real-time STDF Ingestion via Kafka**
- **As a** test engineer
- **I want to** automatically ingest STDFs from ATE to Kafka upon test completion
- **So that** ML models have fresh data within minutes (vs. manual CSV exports taking days)
- **Acceptance Criteria**:
  - ATE (Advantest V93000) pushes STDF to network share upon lot completion
  - Kafka connector watches network share, ingests new STDFs within <1 minute
  - STDF metadata published to Kafka topic: lot_id, wafer_id, device, test_program, timestamp
  - Kafka UI dashboard shows ingestion rate (STDFs/hour), lag (time since last STDF), errors
  - Email/Slack alert if no STDFs ingested for >2 hours (potential ATE integration issue)
  - Data lineage: every STDF tracked from ATE → Kafka → Spark → Delta Lake with timestamps

**US-02: Distributed STDF Parsing with Spark**
- **As a** data engineer
- **I want to** parse 10GB STDF files in <5 minutes using Spark distributed processing
- **So that** ML teams have fast access to features (vs. 30-60 min Pandas bottleneck)
- **Acceptance Criteria**:
  - Spark job reads STDF from Kafka, parses die-level test results (Bin, HBin, X, Y, 1,000+ parametric tests)
  - Distributed parsing: 10GB STDF across 8 Spark executors = <5 minutes total (vs. 45 min single-threaded Pandas)
  - Output: Parquet files in Delta Lake with schema (lot_id, wafer_id, die_x, die_y, bin, hbin, test_001, test_002, ..., test_1000)
  - Data quality checks: flag missing values (die without bin), outliers (parametric test >5 sigma), schema mismatches
  - Spark UI metrics: job duration, shuffle read/write, executor utilization, memory usage
  - Benchmark: Parse 423 existing synthetic STDFs (<30 min total, vs. 8+ hours sequential Pandas)

**US-03: Delta Lake Feature Store with Time-Travel**
- **As an** ML engineer
- **I want to** query pre-computed wafer-level features from Delta Lake with time-travel versioning
- **So that** I can train reproducible models without re-parsing STDFs (90% faster feature engineering)
- **Acceptance Criteria**:
  - Delta Lake tables: `raw_stdf` (die-level), `wafer_features` (wafer-level aggregates), `parametric_stats` (test distributions), `spatial_patterns` (edge die yield, center cluster metrics)
  - Time-travel queries: `SELECT * FROM wafer_features VERSION AS OF 30 DAYS AGO` (reproduce training data from 1 month ago)
  - ACID transactions: concurrent reads/writes, no corrupt data during Spark ETL runs
  - Feature catalog: documented schemas with descriptions (wafer_yield = passed_die / total_die, edge_die_yield = passed_die on outer 2mm / total_edge_die)
  - SQL/Python/R access: Databricks notebooks, Spark SQL, pandas.read_sql for Delta Lake queries
  - Query performance: <10 seconds to fetch 1 year of wafer features (1M+ rows), leveraging Delta Lake partitioning (partition by year/month/day)

**US-04: MLflow Experiment Tracking for All P01-P15 Projects**
- **As an** ML engineer
- **I want to** log 100% of model training runs to MLflow with hyperparameters, metrics, and artifacts
- **So that** I never lose experiments and can reproduce champion models (vs. 80% lost today)
- **Acceptance Criteria**:
  - MLflow UI shows all P01-P15 experiments with run history (hyperparameters, accuracy, F1-score, training time)
  - Logged artifacts: model pickle/ONNX, feature importance plots, confusion matrix, training/validation curves
  - Experiment comparison: side-by-side table comparing 10+ runs (sort by F1-score, filter by hyperparameter ranges)
  - Model registry: promote champion model with tags (Production, Staging, Archived), version history (v1.0, v1.1, v2.0)
  - Reproducibility: `mlflow.log_params({"random_seed": 42, "train_data_version": "2024-12-01"})` → re-run achieves ±2% accuracy
  - Auto-logging: MLflow auto-logs XGBoost/scikit-learn/PyTorch hyperparameters (no manual logging code needed)

**US-05: Airflow DAGs for Automated Weekly Retraining**
- **As a** platform engineer
- **I want to** schedule weekly model retraining workflows via Airflow DAGs
- **So that** models stay fresh without manual intervention (vs. 40 hrs/month manual orchestration)
- **Acceptance Criteria**:
  - Airflow DAG: `retrain_p01_xgboost` runs every Sunday 2am (low-traffic period)
  - DAG tasks: (1) Query Delta Lake for last 90 days data, (2) Train XGBoost model, (3) Validate accuracy on holdout set, (4) Compare with current champion in MLflow, (5) Promote to Production if accuracy improves >3%, (6) Send Slack notification with results
  - Data quality gate: DAG fails if <1,000 wafers in training set or >10% missing values
  - Retry logic: transient failures (Spark OOM, MLflow API timeout) retry 3× with exponential backoff
  - Rollback: if new model accuracy degrades >5% on validation → auto-rollback to previous champion, alert on-call engineer
  - Airflow UI: visualize DAG graph, task duration, success rate (>95% target), historical runs

**US-06: FastAPI Multi-Model Serving with <100ms Latency**
- **As a** test engineer
- **I want to** call FastAPI endpoint for real-time bin predictions with <100ms response
- **So that** I can make immediate test floor decisions (vs. 5-10 sec Flask too slow)
- **Acceptance Criteria**:
  - FastAPI endpoint: `POST /predict/p01_xgboost` with JSON body `{"lot_id": "TC41x_LOT123", "wafer_id": "W05", "features": [...]}`
  - Response: `{"bin_prediction": 3, "confidence": 0.87, "latency_ms": 45, "model_version": "v2.3"}` within <100ms p95
  - Multi-model hosting: 15 models served from single FastAPI app (P01-P15 endpoints: `/predict/p01_xgboost`, `/predict/p02_resnet`, ...)
  - Auto-scaling: 2 replicas during off-hours (midnight-6am), scale to 20 replicas if request rate >500/min
  - Model versioning: FastAPI loads latest Production model from MLflow registry, auto-reloads when new version promoted
  - Prometheus metrics: request rate, latency histogram (p50/p95/p99), error rate, model version

**US-07: A/B Testing Champion vs. Challenger Models**
- **As an** ML engineer
- **I want to** deploy champion and challenger models simultaneously with traffic split (90/10)
- **So that** I can validate new model performance on real traffic before full rollout (reduce risk)
- **Acceptance Criteria**:
  - MLflow model registry: Champion model tagged `Production`, Challenger tagged `Staging`
  - FastAPI routing: 90% requests → Champion model, 10% requests → Challenger model (random sampling)
  - Metrics tracking: separate Prometheus histograms for Champion vs. Challenger (accuracy, latency, error rate)
  - A/B test dashboard: Grafana panel comparing Champion (F1=0.87) vs. Challenger (F1=0.89) with statistical significance test
  - Auto-promotion logic: if Challenger outperforms Champion by >3% accuracy over 7 days → Airflow DAG promotes Challenger to Production, retires old Champion to Archived
  - Rollback safety: if Challenger error rate >5% → immediate traffic switch to 100% Champion, alert ML team

**US-08: Hybrid Deployment - Local Docker Dev + Databricks Cloud Prod**
- **As a** data engineer
- **I want to** develop pipelines locally on Docker Compose for $0 cost, then deploy to Databricks Cloud for production
- **So that** I iterate fast without cloud spend, then scale to production <$100/month
- **Acceptance Criteria**:
  - Local Docker Compose: Kafka, Spark (standalone mode), Airflow, PostgreSQL, MinIO (S3-compatible), MLflow server
  - Local development: test engineer uploads 44GB STDF (423 files from P15), Spark parses to Delta Lake on MinIO, MLflow logs experiment
  - Code portability: same PySpark/Airflow code runs on Docker (local) and Databricks (cloud) with environment variable config
  - Cloud deployment: GitHub Actions CI/CD deploys to Databricks (Spark jobs), AWS S3 (Delta Lake storage), EKS (FastAPI/Airflow)
  - Cost validation: Databricks Community Edition free Year 1, then 14GB cluster <$50/month, S3 200GB <$25/month, total <$100/month
  - Migration path: Week 1-4 local dev with 423 STDFs, Week 5+ generate 100-500 fresh STDFs/day via P07 GAN on Databricks

**US-09: Self-Service Feature Queries for ML Teams**
- **As an** ML engineer
- **I want to** query Delta Lake features via SQL without requesting data extracts from data engineering
- **So that** I experiment faster (self-service vs. 2-day data request turnaround)
- **Acceptance Criteria**:
  - Databricks SQL editor: ML engineers write queries `SELECT * FROM wafer_features WHERE device='TC41x' AND date>'2024-01-01'`
  - Query performance: <10 seconds for 1M+ rows (Delta Lake partitioned by year/month/day, Z-order by device)
  - Feature documentation: Delta Lake table schemas documented with descriptions (wafer_yield definition, parametric_mean formula)
  - Sample queries: template notebooks showing common patterns (join wafer_features with parametric_stats, filter outliers, split train/val/test)
  - Access control: ML engineers read-only access to Delta Lake, data engineers read-write
  - Usage analytics: track top-10 queries, popular features (inform data engineering optimization priorities)

**US-10: Proactive Data Quality Monitoring and Alerting**
- **As a** platform engineer
- **I want to** detect data quality issues (missing STDFs, schema drift, statistical anomalies) before model training
- **So that** models train on clean data (vs. garbage-in-garbage-out failures)
- **Acceptance Criteria**:
  - Airflow data quality DAG runs hourly: (1) Check Kafka lag <5 min, (2) Validate Delta Lake row count (expect 1,000+ wafers/day), (3) Schema validation (all expected columns present), (4) Statistical checks (parametric test means within ±3 sigma of baseline)
  - Alerts: PagerDuty page if Kafka lag >30 min, email if <500 wafers/day (expected 1,000+), Slack if schema mismatch
  - Data quality dashboard: Grafana panel showing daily ingestion volume, schema drift count, outlier percentage, data freshness
  - Automated remediation: if STDF fails schema validation → quarantine to `quarantine_stdf` table, log error, skip from model training
  - Historical tracking: Delta Lake stores data quality metrics (daily row count, missing value %, outlier count) for trend analysis

**US-11: Model Lineage and Audit Trail for Regulatory Compliance**
- **As a** quality engineer
- **I want to** trace model predictions back to training data, code version, and hyperparameters
- **So that** I comply with automotive regulations (ISO 26262, IATF 16949 audit requirements)
- **Acceptance Criteria**:
  - MLflow model metadata: `train_data_version: "delta_lake_2024-12-01"`, `code_commit: "a3f5b9c"`, `hyperparameters: {"n_estimators": 100, "max_depth": 6}`
  - Delta Lake time-travel: query exact training dataset used 6 months ago (`SELECT * FROM wafer_features VERSION AS OF '2024-06-01'`)
  - Prediction logs: every FastAPI prediction logged with (timestamp, model_version, input_features, output_prediction, latency_ms)
  - Audit report: generate CSV showing all predictions from model v2.3 between 2024-06-01 and 2024-12-01 with traceability to training data
  - Retention policy: prediction logs stored 3 years (regulatory requirement), model artifacts 5 years, training data 7 years
  - Compliance dashboard: Grafana panel showing audit trail completeness (100% predictions traceable to model+data)

---

## 6. Functional Requirements

### 6.1 Core Features

**FR-001: Kafka Real-Time STDF Ingestion**
- Kafka cluster (3 brokers) for high-availability STDF streaming
- Kafka topic: `stdf_ingestion` with 12 partitions (support 1,000 STDFs/day with parallelism)
- Kafka Connect FileStreamSource connector watches ATE network share, auto-ingests new .std files
- STDF metadata extraction: lot_id, wafer_id, device, test_program, timestamp published to Kafka
- Kafka retention: 7 days (1 week buffer for reprocessing, then auto-delete to save storage)
- Monitoring: Kafka lag metrics (consumer group lag <5 minutes), ingestion rate (STDFs/hour)
- Error handling: malformed STDF → publish to `stdf_dead_letter_queue` topic, alert data engineers

**FR-002: Spark Distributed STDF Parsing**
- Spark Structured Streaming reads from Kafka `stdf_ingestion` topic
- Custom STDF parser (PySpark UDF) extracts: die-level bin/hbin, X/Y coordinates, 1,000+ parametric tests
- Distributed processing: 8 Spark executors process 10GB STDF in <5 minutes (vs. 30-60 min Pandas)
- Output schema: `(lot_id, wafer_id, die_x, die_y, bin, hbin, test_001, test_002, ..., test_1000, timestamp)`
- Write to Delta Lake: ACID transactions, partition by `year/month/day` for query performance
- Data quality checks: validate schema, flag missing values (>10% null → alert), detect outliers (>5 sigma)
- Spark UI metrics: job duration, shuffle size, executor memory/CPU, stage-level DAG visualization

**FR-003: Delta Lake ACID Feature Store**
- Delta Lake tables on S3 (production) or MinIO (local dev):
  - `raw_stdf`: Die-level test results (lot_id, wafer_id, die_x, die_y, bin, hbin, test_001...test_1000)
  - `wafer_features`: Wafer-level aggregates (wafer_yield, edge_die_yield, center_yield, parametric_mean, parametric_std)
  - `parametric_stats`: Test-level statistics (test_id, mean, std, p5, p95, outlier_count, timestamp)
  - `spatial_patterns`: Wafer map embeddings (wafer_id, pattern_type, embedding_vector_512d, confidence)
  - `model_predictions`: Inference results (prediction_id, model_name, model_version, input_features, output, confidence, latency_ms)
- ACID transactions: concurrent reads/writes, no corrupt data during Spark ETL
- Time-travel versioning: `SELECT * FROM wafer_features VERSION AS OF '2024-06-01'` (reproduce training data)
- Schema evolution: add new columns without breaking existing queries (backward compatible)
- Z-ordering: optimize query performance (`OPTIMIZE wafer_features ZORDER BY (device, lot_id)`)
- Vacuum: clean up old versions after 30 days (`VACUUM wafer_features RETAIN 720 HOURS`)

**FR-004: MLflow Centralized Experiment Tracking**
- MLflow Tracking Server (PostgreSQL backend, S3 artifact store)
- Auto-logging: XGBoost, scikit-learn, PyTorch, TensorFlow hyperparameters/metrics logged automatically
- Custom logging: `mlflow.log_params({"n_estimators": 100})`, `mlflow.log_metrics({"f1_score": 0.87})`
- Artifact storage: model pickle/ONNX, feature importance plots, confusion matrix, training curves
- Experiment organization: projects (P01-P15), experiments (XGBoost_BinPredictor_v1, ResNet_YieldPredictor_v2)
- Model registry: champion model tagged `Production`, challenger tagged `Staging`, old versions `Archived`
- Model versioning: semantic versioning (v1.0.0, v1.1.0, v2.0.0), Git commit hash, training data version
- MLflow UI: compare 10+ runs side-by-side, filter by hyperparameter ranges, sort by F1-score
- API access: programmatic model loading (`mlflow.pyfunc.load_model("models:/p01_xgboost/Production")`)

**FR-005: Airflow DAG Orchestration**
- Airflow scheduler (2 replicas for HA) orchestrates retraining workflows
- DAG templates for each P01-P15 project:
  - **Retraining DAG**: (1) Query Delta Lake for last 90 days data, (2) Train model, (3) Validate accuracy, (4) Compare with champion, (5) Promote if accuracy improves >3%, (6) Notify Slack
  - **Data Quality DAG**: (1) Check Kafka lag <5 min, (2) Validate Delta Lake row count (expect 1,000+ wafers/day), (3) Schema validation, (4) Statistical checks (parametric means within ±3 sigma)
  - **Feature Engineering DAG**: (1) Read raw_stdf from Delta Lake, (2) Compute wafer_features aggregates, (3) Update parametric_stats, (4) Generate spatial_patterns embeddings
- Scheduling: cron expressions (weekly Sunday 2am, daily 1am, hourly)
- Dependencies: TaskFlow API (`@task` decorators), task dependencies via `>>` operator
- Retry logic: exponential backoff (1min, 5min, 15min), max 3 retries
- SLA monitoring: alert if DAG exceeds expected duration (retraining >2 hours, data quality >10 min)
- Airflow UI: DAG graph visualization, task duration gantt chart, historical run success rate

**FR-006: FastAPI Multi-Model Serving**
- FastAPI app (Uvicorn ASGI server) serves all P01-P15 models from single deployment
- Endpoints: `/predict/p01_xgboost`, `/predict/p02_resnet`, `/predict/p04_unet`, ..., `/predict/p15_bayesian`
- Request format: `POST /predict/{model_name}` with JSON `{"features": [...], "metadata": {"lot_id": "X123"}}`
- Response format: `{"prediction": 3, "confidence": 0.87, "model_version": "v2.3", "latency_ms": 45}`
- Model loading: FastAPI loads models from MLflow registry on startup, auto-reloads on new Production tag
- Batch inference: `/batch_predict` endpoint accepts array of feature vectors (100+ predictions in single request)
- Async processing: FastAPI async endpoints for non-blocking I/O (concurrent prediction requests)
- Auto-scaling: Kubernetes HPA scales 2-20 replicas based on CPU >70% or request rate >500/min
- Health checks: `/health` endpoint (liveness), `/ready` endpoint (readiness for K8s probes)

**FR-007: Hybrid Deployment (Docker Local + Databricks Cloud)**
- **Local Development (Docker Compose)**:
  - Services: Kafka (1 broker), Zookeeper, Spark (standalone mode, 1 master + 2 workers), Airflow (scheduler + webserver), PostgreSQL, MinIO (S3-compatible), MLflow server, FastAPI
  - Data: 44GB real STDFs from P15 (423 files) on local disk, or replay 423 synthetic STDFs from P07
  - Cost: $0 (all open-source, runs on developer laptop/workstation)
  - Use case: Fast iteration, unit testing, debugging Spark jobs, Airflow DAGs without cloud spend
- **Cloud Production (Databricks + AWS)**:
  - Databricks Runtime 14.3 LTS (managed Spark, Delta Lake, MLflow)
  - AWS S3: Delta Lake storage (200GB synthetic STDFs from P07 GAN)
  - Databricks Jobs: scheduled Spark ETL, model training via Airflow integration
  - EKS (Elastic Kubernetes Service): FastAPI serving, Airflow scheduler/workers, Kafka Connect
  - Cost: Databricks Community Edition free Year 1, then 14GB cluster <$50/month, S3 <$25/month
- **Code Portability**: Same PySpark/Airflow/FastAPI code runs in both environments with config variables (`STORAGE_BACKEND=minio` vs `s3`, `SPARK_MASTER=local` vs `databricks`)

**FR-008: Mock Model Integration (Phase 1: Infrastructure Development)**
- Baseline models for P16 infrastructure testing (no dependency on P01-P15 training completion):
  - **P01 XGBoost Bin Predictor**: RandomForestClassifier (scikit-learn) with 10 features, 100 trees
  - **P02 Transfer Learning Yield**: LinearRegression (scikit-learn) on wafer-level aggregates
  - **P04 ResNet Wafer Defect**: Dummy CNN (3-layer ConvNet) with random weights, 70% accuracy
  - **P06 LSTM Anomaly**: SimpleRNN (Keras) with 2 LSTM layers, 50 units each
  - **P10 GNN Failure Propagation**: NetworkX graph with shortest path algorithm (no actual GNN)
- Mock model training: synthetic data from Delta Lake (423 STDFs from P15 or P07 generated data)
- Mock model serving: FastAPI endpoints return predictions with random confidence scores (80-95%)
- Validation: end-to-end pipeline works (Kafka → Spark → Delta Lake → MLflow → Airflow → FastAPI)
- Phase 2 integration: replace mock models with production-trained P01-P15 models via MLflow registry swap

**FR-009: Feature Engineering Pipelines**
- **Wafer-level features** (computed from raw_stdf, stored in wafer_features table):
  - `wafer_yield`: passed_die / total_die
  - `edge_die_yield`: passed_die in outer 2mm ring / total_edge_die
  - `center_yield`: passed_die in center 20% area / total_center_die
  - `parametric_mean`, `parametric_std`, `parametric_p5`, `parametric_p95` for each of 1,000 tests
  - `bin_distribution`: JSON histogram of bin counts `{"Bin1": 1500, "Bin2": 200, "Bin3": 50}`
- **Spatial pattern features** (embedded from wafer maps):
  - ResNet-18 feature extractor (pre-trained on 10,000 wafer maps) generates 512-dim embeddings
  - Pattern classification: edge effect, center cluster, ring, quadrant, random, scratch (via softmax)
  - Store in `spatial_patterns` table: `(wafer_id, pattern_type, embedding_vector, confidence)`
- **Test correlation features** (Pearson correlation matrix for 1,000 tests → 500K pairs):
  - Top-100 correlated test pairs (|r| > 0.7) stored for each lot
  - Test clusters identified via community detection (networkx Louvain algorithm)
- **Reusability**: All features computed once by Spark, reused by all P01-P15 models (90% feature reuse goal)

**FR-010: Data Quality Validation Framework**
- Schema validation: enforce Delta Lake schema contracts (column types, not-null constraints)
- Completeness checks: flag if >10% missing values in critical columns (bin, hbin, parametric tests)
- Statistical sanity checks: parametric test means within ±3 sigma of historical baseline (detect ATE drift)
- Business logic validation: wafer_yield between 0-100%, total_die matches expected die count for device
- Anomaly detection: Isolation Forest flags outlier lots (unusually low yield, parametric shifts)
- Data quality metrics: published to Prometheus, visualized in Grafana dashboard (daily row count, null %, outlier count)
- Quarantine: failed validation → move to `quarantine_stdf` table, exclude from model training, alert data engineers

**FR-011: Monitoring and Observability**
- **Prometheus metrics**:
  - Kafka: ingestion rate (STDFs/hour), consumer lag (seconds), partition distribution
  - Spark: job duration (seconds), shuffle read/write (GB), executor memory/CPU utilization
  - Delta Lake: table size (GB), row count, version count, time-travel queries/day
  - MLflow: experiments logged/day, model downloads, registry model count (Production/Staging/Archived)
  - Airflow: DAG success rate (%), task duration (seconds), SLA misses
  - FastAPI: request rate (req/min), latency histogram (p50/p95/p99), error rate (%), model version distribution
- **Grafana dashboards**:
  - **Platform Overview**: ingestion rate, processing throughput, API uptime, cost (daily AWS spend)
  - **Data Quality**: row count trends, null %, outlier %, schema violations
  - **ML Metrics**: experiment count, champion model accuracy, prediction latency, A/B test results
  - **Cost Dashboard**: Databricks DBU usage, S3 storage cost, data transfer cost, projected monthly spend
- **OpenSearch logs**: structured JSON logs with correlation IDs (trace requests across Kafka → Spark → Delta Lake → FastAPI)
- **Alerts**: PagerDuty/Slack notifications for: Kafka lag >30 min, Spark job failures, API error rate >5%, cost exceeds $5/day

**FR-012: Security and Access Control**
- OAuth2/OIDC authentication: users authenticate via corporate SSO (Azure AD, Okta)
- RBAC (Role-Based Access Control):
  - **Data Engineers**: Read/write Delta Lake, Kafka, Spark jobs
  - **ML Engineers**: Read Delta Lake, write MLflow experiments, deploy models to Staging
  - **Platform Engineers**: Admin access (Databricks clusters, K8s, monitoring)
  - **Test/Yield Engineers**: Read-only Delta Lake, query FastAPI endpoints
- TLS encryption: all communications encrypted (Kafka TLS, HTTPS FastAPI, MLflow HTTPS)
- Secrets management: Kubernetes Secrets / AWS Secrets Manager (Databricks tokens, S3 credentials, database passwords)
- Data privacy: synthetic STDFs only in cloud (no real chip data), comply with GDPR/CCPA
- Audit logs: all data access logged (who queried which Delta Lake table, when, what columns)

### 6.2 Advanced Features

**FR-013: A/B Testing Champion vs. Challenger Models**
- MLflow model registry: Champion tagged `Production`, Challenger tagged `Staging`
- FastAPI routing: 90% traffic → Champion, 10% traffic → Challenger (random sampling)
- Metrics tracking: separate Prometheus histograms for Champion vs. Challenger (accuracy, latency, error rate)
- Statistical significance: Chi-square test for accuracy difference (p < 0.05 required for promotion)
- Auto-promotion logic: Airflow DAG compares Champion vs. Challenger over 7 days → promotes Challenger if accuracy improves >3%
- Rollback safety: if Challenger error rate >5% → immediate traffic switch to 100% Champion, retire Challenger

**FR-014: Delta Lake Time-Travel for Reproducible Training**
- Version history: Delta Lake stores all table versions (snapshots every MERGE/UPDATE/DELETE operation)
- Time-travel queries: `SELECT * FROM wafer_features VERSION AS OF '2024-06-01'` (exact data from 6 months ago)
- MLflow integration: log Delta Lake version with experiment (`mlflow.log_param("train_data_version", "delta_wafer_features_v123")`)
- Reproducibility: re-train model with same data version → ±2% accuracy (validate reproducibility)
- Audit trail: compliance reports show exact data used for each model version (ISO 26262 requirement)
- Performance: time-travel queries use Delta Lake snapshots (no full table scan, O(1) lookup)

**FR-015: Airflow Dynamic DAG Generation**
- Generate retraining DAGs programmatically for all P01-P15 projects (DRY principle, no copy-paste)
- Template DAG factory: `create_retraining_dag(project_id, model_type, schedule, data_query, validation_threshold)`
- Example: `create_retraining_dag("P01", "xgboost", "0 2 * * 0", "SELECT * FROM wafer_features WHERE device='TC41x'", 0.85)`
- Benefits: consistent DAG structure, easy to add P17/P18, centralized configuration
- Validation: DAG factory validates parameters (schedule cron syntax, SQL query syntax) before deploying

**FR-016: Feature Store Versioning and Lineage**
- Feature versioning: wafer_features table versioned (v1: 50 features, v2: 75 features, v3: 100 features)
- Lineage tracking: feature_id → raw_stdf → Spark job commit hash → Delta Lake version
- Feature catalog: documented feature definitions, data types, computation logic, update frequency
- Example: `wafer_yield = COUNT(die WHERE bin=1) / COUNT(die)` (definition), `Updated: Daily via Airflow DAG feature_engineering` (frequency)
- Deprecation: old feature versions marked deprecated, warning if model uses deprecated features

**FR-017: Spark Adaptive Query Execution (AQE)**
- Enable Spark AQE for dynamic query optimization (auto-tune partition count, join strategies)
- AQE features: dynamic partition pruning, dynamic coalescing (reduce shuffle partitions), dynamic join selection (broadcast vs. sort-merge)
- Performance gains: 20-40% faster Spark jobs on skewed data (uneven partition sizes)
- Configuration: `spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.coalescePartitions.enabled=true`

**FR-018: Cost Optimization Strategies**
- Databricks auto-termination: clusters auto-shutdown after 2 hours idle (save compute cost)
- S3 lifecycle policies: move old Delta Lake versions to S3 Glacier after 90 days (save storage cost 90%)
- Spot instances: Spark executors run on AWS spot instances (70% cheaper than on-demand, tolerate interruptions)
- Cost monitoring: Grafana dashboard shows daily Databricks DBU usage, S3 storage cost, projected monthly spend
- Budget alerts: email if daily cost exceeds $5/day (monthly $150 vs. target $100)

---

## 7. Non-Functional Requirements

### 7.1 Performance

**NFR-P1: Ingestion Latency**
- **Target**: <5 minutes p95 from STDF arrival at ATE to availability in Delta Lake
- **Breakdown**: Kafka ingestion <1 min, Spark processing <3 min, Delta Lake write <1 min
- **Benchmark**: 423 synthetic STDFs processed in <30 minutes total (vs. 8+ hours sequential Pandas)
- **Scalability**: 1,000 STDFs/day sustained, bursts up to 2,000 STDFs/day (weekend catch-up)

**NFR-P2: Query Performance**
- **Delta Lake queries**: <10 seconds p95 for 1M+ rows (1 year wafer features)
- **Optimization techniques**: Partition by `year/month/day`, Z-order by `device`, `lot_id`
- **Caching**: Databricks Delta Cache for hot tables (wafer_features, parametric_stats)
- **Example query latency**: `SELECT * FROM wafer_features WHERE device='TC41x' AND date>'2024-01-01'` → <5 seconds

**NFR-P3: Model Serving Latency**
- **Target**: <100ms p95 prediction latency for all models
- **Breakdowns**:
  - P01 XGBoost: <20ms (lightweight tree ensemble, 100 trees)
  - P02 ResNet Yield: <80ms (GPU inference via TensorRT, batch size 32)
  - P04 U-Net Wafer Defect: <100ms (GPU inference, 512×512 input)
  - P10 GNN Failure Propagation: <150ms (graph query overhead, Neo4j 10K nodes)
- **Optimization**: ONNX model format, TensorRT GPU acceleration, model quantization (FP16), batch inference
- **Horizontal scaling**: auto-scale 2-20 FastAPI replicas, target <100ms maintained under load

**NFR-P4: Throughput**
- **Kafka ingestion**: 1,000+ STDFs/day sustained (40-50 STDFs/hour average)
- **Spark processing**: 10GB STDF in <5 minutes (distributed across 8 executors)
- **FastAPI serving**: 10,000+ predictions/day (420 predictions/hour, 7 predictions/min)
- **Scalability**: platform designed for 10× growth (10,000 STDFs/day, 100K predictions/day without re-architecture)

### 7.2 Reliability

**NFR-R1: System Uptime**
- **Target**: >99.9% uptime (max 8 hours downtime per year)
- **High availability**:
  - Kafka: 3 brokers, replication factor 3 (tolerate 2 broker failures)
  - Spark: Databricks cluster auto-restart on failure, checkpoint/WAL for Structured Streaming
  - Airflow: 2 scheduler replicas (active/standby), PostgreSQL replication
  - FastAPI: Kubernetes deployment 2-20 replicas, rolling updates (zero downtime)
- **Disaster recovery**: full backup/restore within 4 hours (RTO), RPO <1 hour (Delta Lake transaction log)

**NFR-R2: Data Durability**
- Delta Lake: ACID guarantees, atomic commits, transaction log on S3 (11 9's durability)
- PostgreSQL: 2 replicas (master-slave), synchronous replication, daily backups to S3
- Kafka: replication factor 3, min.insync.replicas=2 (guarantee 2 copies before ack)
- MLflow artifacts: S3 storage (11 9's durability), versioned model files, 5-year retention
- Backup retention: 7 days for operational data, 3 years for regulatory compliance (STDFs, predictions)

**NFR-R3: Fault Tolerance**
- Spark: automatic task retry on executor failures, checkpoint for Structured Streaming (resume from last offset)
- Airflow: task retry with exponential backoff (1min, 5min, 15min), max 3 retries
- Kafka: consumer group auto-rebalancing on consumer failures, offset management for exactly-once processing
- FastAPI: circuit breaker pattern (fail fast if MLflow/Delta Lake down), graceful degradation (return cached predictions)

**NFR-R4: Error Handling and Alerting**
- Comprehensive error logging: structured JSON logs with stack traces, correlation IDs
- Alert channels: PagerDuty (critical), Slack (warnings), email (daily summaries)
- Alert rules:
  - **Critical**: Kafka lag >30 min, Spark job failures, API error rate >10%, cost >$10/day
  - **Warning**: Kafka lag 5-30 min, data quality issues (>10% null values), API latency >200ms p95
  - **Info**: Daily summary (STDFs processed, experiments logged, predictions served, cost spent)
- On-call rotation: 24/7 coverage for production incidents, escalation to senior engineers if unresolved in 1 hour

### 7.3 Usability

**NFR-U1: Self-Service for ML Engineers**
- Databricks SQL editor: ML engineers write queries without requesting data extracts from data engineers
- MLflow UI: browse experiments, compare runs, download models without needing backend access
- FastAPI Swagger UI: test endpoints interactively (upload features, see predictions) without writing code
- Documentation: comprehensive guides for common tasks (train model, query Delta Lake, deploy to FastAPI)

**NFR-U2: Onboarding Time**
- **Target**: New project teams (P17, P18) onboarded to P16 in <2 weeks
- Onboarding checklist: (1) Access granted (Databricks, MLflow, Airflow), (2) Tutorial notebook (query Delta Lake, log experiment), (3) Deploy mock model to FastAPI, (4) Schedule retraining DAG
- Templates: reusable code snippets (Spark ETL, MLflow logging, Airflow DAG, FastAPI endpoint)
- Support: Slack channel for P16 questions, office hours (weekly 1-hour session)

**NFR-U3: Developer Experience**
- Local development: full-stack Docker Compose (Kafka, Spark, Airflow, MLflow, FastAPI) runs on laptop
- Fast iteration: code changes → rebuild Docker image → restart service <2 minutes
- Debugging tools: Spark UI for job profiling, Airflow logs for DAG failures, MLflow UI for experiment comparison
- CI/CD: GitHub Actions runs tests on every commit (unit tests, integration tests, linting)

### 7.4 Maintainability

**NFR-M1: Code Quality**
- Python: type hints (mypy), linting (ruff), formatting (black), test coverage >80%
- PySpark: modular ETL functions, unit tests with pyspark.testing
- Airflow: DAG validation (no circular dependencies, valid cron syntax), task documentation
- FastAPI: OpenAPI schema auto-generation, request/response validation (Pydantic), unit tests (pytest)

**NFR-M2: Observability**
- Structured logging: JSON logs with correlation IDs, log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Distributed tracing: OpenTelemetry traces for end-to-end request flows (Kafka → Spark → Delta Lake → FastAPI)
- Metrics: Prometheus instrumentation for all services (custom metrics: stdf_processing_duration, model_prediction_latency)
- Dashboards: Grafana visualizations for all NFRs (latency, throughput, uptime, error rate, cost)

**NFR-M3: Infrastructure as Code**
- Terraform: AWS resources (S3, EKS, VPC, security groups), Databricks workspaces, IAM roles
- Helm charts: Kubernetes deployments (Kafka, Airflow, FastAPI, Prometheus, Grafana)
- Version control: all infrastructure code in Git, pull request reviews, automated plan/apply via GitHub Actions
- Reproducibility: destroy and recreate entire environment from code (disaster recovery, testing)

**NFR-M4: Documentation**
- Architecture diagrams: 6-layer architecture (ASCII art + Mermaid diagrams), data flow diagrams
- API documentation: FastAPI auto-generated Swagger UI, MLflow REST API examples
- Runbooks: troubleshooting guides (Kafka lag remediation, Spark OOM errors, Airflow DAG failures)
- Delta Lake schemas: documented table definitions, column descriptions, sample queries
- MLflow best practices: naming conventions, experiment organization, model versioning guidelines

### 7.5 Scalability

**NFR-S1: Horizontal Scaling**
- Kafka: add brokers to increase partition parallelism (12 partitions → 24 partitions for 2,000 STDFs/day)
- Spark: Databricks cluster auto-scaling (2-8 workers based on queue depth)
- FastAPI: Kubernetes HPA auto-scaling (2-20 replicas based on CPU >70% or request rate >500/min)
- Airflow: scale workers horizontally (Celery executor with Redis, add worker pods)

**NFR-S2: Vertical Scaling**
- Databricks clusters: upgrade from 14GB to 56GB nodes if memory bottlenecks (larger executor memory)
- PostgreSQL: upgrade RDS instance from db.t3.medium to db.r5.large if high query load
- Redis: upgrade to larger instance for Airflow task queue if task backlog >1,000

**NFR-S3: Data Growth**
- Delta Lake storage: S3 auto-scales to petabytes (no capacity planning needed)
- Partitioning strategy: partition by `year/month/day` prevents full table scans (query only relevant partitions)
- Table optimization: run `OPTIMIZE` weekly to compact small files, `VACUUM` monthly to delete old versions
- Projected growth: 200GB Year 1 → 500GB Year 2 → 1TB Year 3 (linear with STDF ingestion volume)

### 7.6 Security

**NFR-SEC1: Authentication and Authorization**
- OAuth2/OIDC SSO integration (Azure AD, Okta)
- RBAC: Data Engineers (write), ML Engineers (read Delta Lake + write MLflow), Test Engineers (read-only)
- API authentication: JWT tokens for FastAPI endpoints, OAuth2 client credentials flow
- Service accounts: Kubernetes service accounts for pod-to-pod communication (Spark → Delta Lake, Airflow → MLflow)

**NFR-SEC2: Data Encryption**
- At-rest: S3 server-side encryption (SSE-S3 AES-256), PostgreSQL TDE (Transparent Data Encryption)
- In-transit: TLS 1.2+ for all communications (Kafka TLS, HTTPS FastAPI, MLflow HTTPS, Databricks HTTPS)
- Secrets: Kubernetes Secrets / AWS Secrets Manager (Databricks tokens, S3 credentials, database passwords)
- Key rotation: secrets rotated every 90 days, automated via AWS Secrets Manager

**NFR-SEC3: Compliance**
- GDPR/CCPA: synthetic STDFs only (no real chip data in cloud), data deletion API (delete user data on request)
- ISO 26262: model lineage (training data version, code commit, hyperparameters), audit trail (all predictions logged)
- IATF 16949: quality management (data quality checks, retraining validation, champion/challenger testing)
- Audit logs: all data access logged (who, what, when, where), retained 3 years

---

## 8. Technical Requirements

### 8.1 Technical Stack

**Data Ingestion Layer (Kafka)**:
- Apache Kafka 3.6+ (3 brokers, replication factor 3, 12 partitions for `stdf_ingestion` topic)
- Kafka Connect 3.6+ (FileStreamSource connector for ATE network share)
- Zookeeper 3.8+ (Kafka metadata, cluster coordination)
- Kafka UI: Kafdrop 4.0+ or Kafka UI 0.7+ (topic monitoring, consumer lag visualization)
- Schema Registry: Confluent Schema Registry 7.5+ (Avro schema evolution for STDF metadata)

**Data Processing Layer (Spark)**:
- Apache Spark 3.5+ with PySpark 3.5+ (distributed STDF parsing, feature engineering)
- Spark Structured Streaming 3.5+ (real-time Kafka → Delta Lake pipeline)
- Databricks Runtime 14.3 LTS (managed Spark clusters, auto-scaling, GPU support)
- Local development: Spark Standalone mode (1 master + 2 workers in Docker Compose)
- Libraries: pystdf 1.4+ (STDF parsing), pandas 2.2+, numpy 1.26+, pyarrow 15.0+ (Parquet I/O)

**Data Storage Layer (Delta Lake)**:
- Delta Lake 3.0+ (ACID transactions, time-travel, schema evolution, MERGE/UPDATE/DELETE)
- Storage backends:
  - **Local**: MinIO 2024-01 (S3-compatible object storage in Docker)
  - **Cloud**: AWS S3 (production, 200GB synthetic STDFs from P07, 11 9's durability)
- Delta Lake optimizations: Z-ordering, partition pruning, data skipping (via min/max statistics)
- Query engines: Spark SQL 3.5+, Databricks SQL (serverless), pandas.read_sql() via Delta Sharing

**Experiment Tracking Layer (MLflow)**:
- MLflow 2.12+ (tracking server, model registry, model serving)
- Backend store: PostgreSQL 16+ (experiment metadata, run metrics, model versions)
- Artifact store: S3 (model artifacts, plots, training curves, 5-year retention)
- MLflow UI: experiment comparison, run visualization, model registry management
- MLflow APIs: Python SDK (`mlflow.log_params()`, `mlflow.log_metrics()`), REST API (external integrations)
- Auto-logging: XGBoost, scikit-learn, PyTorch, TensorFlow, Keras (hyperparameters auto-detected)

**Orchestration Layer (Airflow)**:
- Apache Airflow 2.8+ (scheduler, webserver, workers via CeleryExecutor)
- Executor: CeleryExecutor with Redis 7.2+ (distributed task queue, horizontal worker scaling)
- DAG storage: Git repository (version-controlled DAGs, GitHub sync)
- Connections: Databricks (Spark job submission), MLflow (model registry), PostgreSQL (metadata), S3 (logs)
- Plugins: Databricks operator, MLflow operator, custom operators (Delta Lake quality checks)
- Airflow UI: DAG graph, Gantt chart, task logs, historical run analysis

**Model Serving Layer (FastAPI)**:
- FastAPI 0.110+ (async ASGI framework, OpenAPI auto-generation, Pydantic validation)
- ASGI server: Uvicorn 0.29+ (production-grade, high-performance, auto-reload in dev)
- Model formats: Python pickle (scikit-learn, XGBoost), ONNX 1.16+ (PyTorch/TensorFlow), TensorRT 8.6+ (GPU acceleration)
- Model loading: `mlflow.pyfunc.load_model()` from MLflow registry (auto-reload on Production tag change)
- Batch inference: accept arrays of feature vectors, vectorized NumPy operations
- Kubernetes deployment: 2-20 replicas (HPA), rolling updates, health checks

**Database & Caching**:
- PostgreSQL 16+ (Airflow metadata, MLflow backend, user management, audit logs)
- Redis 7.2+ (Airflow Celery broker, FastAPI response caching, rate limiting)
- Replication: PostgreSQL master-slave (2 replicas), Redis Sentinel (HA)

**Monitoring & Observability**:
- Prometheus 2.53+ (metrics collection, 15-day retention, AlertManager for alerts)
- Grafana 11.1+ (dashboards, visualization, annotations, alerting)
- OpenSearch 2.13+ (log aggregation, Lucene queries, 30-day retention)
- OpenTelemetry 1.25+ (distributed tracing, spans for Kafka → Spark → Delta Lake → FastAPI)
- Exporters: Kafka Exporter, Spark metrics, Airflow StatsD, FastAPI Prometheus middleware

**Deployment & Infrastructure**:
- **Local**: Docker 27+, Docker Compose 2.27+ (Kafka, Spark, Airflow, PostgreSQL, MinIO, MLflow, FastAPI, Prometheus, Grafana)
- **Cloud**: AWS EKS 1.30+ (Kubernetes), Databricks workspace (managed Spark/Delta Lake/MLflow), AWS S3, RDS PostgreSQL
- **IaC**: Terraform 1.8+ (AWS resources), Helm 3.15+ (Kubernetes charts)
- **CI/CD**: GitHub Actions (lint → test → build Docker → push to ECR → deploy to EKS)

**Development & Testing**:
- Python 3.11+ (language runtime, type hints, async/await)
- Poetry 1.8+ or pip-tools 7.4+ (dependency management, lock files)
- Testing: pytest 8.2+, pytest-cov 5.0+ (unit tests, >80% coverage), pyspark.testing (Spark job tests)
- Linting: ruff 0.4+ (fast linter/formatter, replaces flake8+black+isort), mypy 1.10+ (type checking)
- Pre-commit hooks: run linting, type checking, unit tests before git commit

### 8.2 AI/ML Components

**Mock Models (Phase 1: P16 Infrastructure Development)**:

```python
# P01 XGBoost Bin Predictor Mock (RandomForest)
from sklearn.ensemble import RandomForestClassifier
import mlflow
import mlflow.sklearn

# Generate synthetic training data from Delta Lake
df = spark.sql("SELECT * FROM wafer_features LIMIT 10000").toPandas()
X = df[['wafer_yield', 'edge_die_yield', 'parametric_mean_IDDQ', 'parametric_std_VTH']]
y = df['majority_bin']  # Most common bin on wafer

# Train mock model
model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
model.fit(X, y)

# Log to MLflow
with mlflow.start_run():
    mlflow.log_params({"n_estimators": 100, "max_depth": 10, "model_type": "mock_baseline"})
    mlflow.log_metrics({"accuracy": 0.75, "f1_score": 0.72})
    mlflow.sklearn.log_model(model, "model", registered_model_name="p01_xgboost_bin_predictor")
    mlflow.set_tag("stage", "mock_phase1")

# Deploy to FastAPI
from fastapi import FastAPI
import mlflow.pyfunc

app = FastAPI()
model_p01 = mlflow.pyfunc.load_model("models:/p01_xgboost_bin_predictor/Production")

@app.post("/predict/p01_xgboost")
async def predict_p01(features: list[float]):
    import numpy as np
    prediction = model_p01.predict(np.array([features]))
    return {"bin_prediction": int(prediction[0]), "confidence": 0.85, "model_version": "mock_v1.0"}
```

**Spark STDF Parsing ETL**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pystdf

# Spark session with Delta Lake
spark = SparkSession.builder \
    .appName("STDF_Parser") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stdf_ingestion") \
    .option("startingOffsets", "latest") \
    .load()

# Parse STDF (UDF)
@udf(returnType=StructType([
    StructField("lot_id", StringType()),
    StructField("wafer_id", StringType()),
    StructField("die_x", IntegerType()),
    StructField("die_y", IntegerType()),
    StructField("bin", IntegerType()),
    StructField("hbin", IntegerType()),
    # ... 1000+ parametric tests
]))
def parse_stdf(stdf_bytes):
    # pystdf parsing logic (simplified)
    # In production: parse binary STDF, extract PTR/FTR/PRR records
    return {"lot_id": "TC41x_LOT123", "wafer_id": "W05", "die_x": 10, "die_y": 15, "bin": 1, "hbin": 1}

# Transform and write to Delta Lake
parsed_df = kafka_df.select(
    col("key").cast("string").alias("stdf_filename"),
    parse_stdf(col("value")).alias("stdf_data"),
    current_timestamp().alias("ingestion_timestamp")
)

# Write to Delta Lake with partitioning
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://my-bucket/checkpoints/raw_stdf") \
    .partitionBy("year", "month", "day") \
    .start("s3://my-bucket/delta/raw_stdf")

query.awaitTermination()
```

**Airflow Retraining DAG (P01 XGBoost)**:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
import mlflow

default_args = {
    'owner': 'mlops_team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['mlops@company.com']
}

with DAG(
    'p01_xgboost_retraining',
    default_args=default_args,
    description='Weekly retraining for P01 XGBoost Bin Predictor',
    schedule_interval='0 2 * * 0',  # Sunday 2am
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['p01', 'xgboost', 'retraining']
) as dag:

    def query_training_data(**context):
        """Query last 90 days from Delta Lake"""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.sql("""
            SELECT * FROM wafer_features 
            WHERE device='TC41x' 
            AND date > current_date() - INTERVAL 90 DAYS
        """)
        df.write.mode("overwrite").parquet("s3://my-bucket/temp/p01_train_data")
        return df.count()

    def train_model(**context):
        """Train XGBoost model"""
        import pandas as pd
        import xgboost as xgb
        import mlflow.xgboost
        
        df = pd.read_parquet("s3://my-bucket/temp/p01_train_data")
        X = df[['wafer_yield', 'edge_die_yield', 'parametric_mean_IDDQ']]
        y = df['majority_bin']
        
        with mlflow.start_run():
            model = xgb.XGBClassifier(n_estimators=200, max_depth=8)
            model.fit(X, y)
            
            accuracy = model.score(X_test, y_test)
            mlflow.log_metrics({"accuracy": accuracy, "f1_score": 0.87})
            mlflow.xgboost.log_model(model, "model", registered_model_name="p01_xgboost_bin_predictor")
            
            return accuracy

    def promote_if_better(**context):
        """Compare with champion, promote if accuracy improves >3%"""
        import mlflow
        client = mlflow.tracking.MlflowClient()
        
        # Get champion model
        champion_versions = client.get_latest_versions("p01_xgboost_bin_predictor", stages=["Production"])
        champion_accuracy = champion_versions[0].run_data.metrics["accuracy"]
        
        # Get challenger (latest Staging)
        challenger_accuracy = context['task_instance'].xcom_pull(task_ids='train_model')
        
        if challenger_accuracy > champion_accuracy * 1.03:  # 3% improvement
            # Promote challenger to Production
            client.transition_model_version_stage(
                name="p01_xgboost_bin_predictor",
                version=challenger_versions[0].version,
                stage="Production"
            )
            return "PROMOTED"
        else:
            return "CHAMPION_RETAINED"

    task_query = PythonOperator(task_id='query_training_data', python_callable=query_training_data)
    task_train = PythonOperator(task_id='train_model', python_callable=train_model)
    task_promote = PythonOperator(task_id='promote_if_better', python_callable=promote_if_better)
    
    task_query >> task_train >> task_promote
```

**Delta Lake Feature Engineering**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Read raw die-level data
raw_df = spark.read.format("delta").load("s3://my-bucket/delta/raw_stdf")

# Compute wafer-level features
wafer_features = raw_df.groupBy("lot_id", "wafer_id", "device").agg(
    (count(when(col("bin") == 1, 1)) / count("*")).alias("wafer_yield"),
    count("*").alias("total_die"),
    count(when(col("bin") == 1, 1)).alias("passed_die"),
    avg("test_IDDQ").alias("parametric_mean_IDDQ"),
    stddev("test_IDDQ").alias("parametric_std_IDDQ"),
    percentile_approx("test_VTH", 0.05).alias("parametric_p5_VTH"),
    percentile_approx("test_VTH", 0.95).alias("parametric_p95_VTH"),
    current_timestamp().alias("feature_timestamp")
)

# Compute edge die yield (die in outer 2mm ring)
edge_features = raw_df.filter(
    (col("die_x") < 2) | (col("die_x") > col("wafer_max_x") - 2) |
    (col("die_y") < 2) | (col("die_y") > col("wafer_max_y") - 2)
).groupBy("lot_id", "wafer_id").agg(
    (count(when(col("bin") == 1, 1)) / count("*")).alias("edge_die_yield")
)

# Join and write to Delta Lake
final_features = wafer_features.join(edge_features, ["lot_id", "wafer_id"], "left")

final_features.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .option("mergeSchema", "true") \
    .save("s3://my-bucket/delta/wafer_features")

# Enable time-travel
spark.sql("""
    ALTER TABLE delta.`s3://my-bucket/delta/wafer_features`
    SET TBLPROPERTIES (delta.logRetentionDuration = '30 days')
""")
```

**FastAPI Multi-Model Serving**:

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow.pyfunc
import numpy as np
from typing import Dict, List
import time

app = FastAPI(title="P16 MLOps Platform API", version="1.0.0")

# Load all models from MLflow registry
models = {
    "p01_xgboost": mlflow.pyfunc.load_model("models:/p01_xgboost_bin_predictor/Production"),
    "p02_resnet": mlflow.pyfunc.load_model("models:/p02_resnet_yield_predictor/Production"),
    "p04_unet": mlflow.pyfunc.load_model("models:/p04_unet_wafer_defect/Production"),
    # ... load all P01-P15 models
}

class PredictionRequest(BaseModel):
    features: List[float]
    metadata: Dict[str, str] = {}

class PredictionResponse(BaseModel):
    prediction: float
    confidence: float
    model_version: str
    latency_ms: float

@app.post("/predict/{model_name}", response_model=PredictionResponse)
async def predict(model_name: str, request: PredictionRequest):
    start_time = time.time()
    
    if model_name not in models:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    
    model = models[model_name]
    features_array = np.array([request.features])
    prediction = model.predict(features_array)[0]
    
    latency_ms = (time.time() - start_time) * 1000
    
    return PredictionResponse(
        prediction=float(prediction),
        confidence=0.87,  # From model metadata
        model_version="v2.3",
        latency_ms=latency_ms
    )

@app.get("/health")
async def health():
    return {"status": "healthy", "models_loaded": len(models)}

@app.get("/models")
async def list_models():
    return {"models": list(models.keys()), "count": len(models)}
```

---

## 9. System Architecture

### 9.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     P16 ENTERPRISE ML DATA PIPELINE                          │
│                        6-Layer MLOps Architecture                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1: INGESTION (Apache Kafka)                                          │
│  ┌──────────────┐      ┌─────────────────┐      ┌──────────────────┐       │
│  │ ATE Network  │─────▶│ Kafka Connect   │─────▶│ Kafka Topic:     │       │
│  │ Share        │      │ FileStreamSrc   │      │ stdf_ingestion   │       │
│  │ (STDF files) │      │ (Watch dir)     │      │ (12 partitions)  │       │
│  └──────────────┘      └─────────────────┘      └──────────────────┘       │
│                                                    │                         │
│  Ingestion Rate: 1,000 STDFs/day  |  Latency: <1 min  |  Retention: 7 days │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 2: PROCESSING (Apache Spark)                                         │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │ Spark Structured Streaming (8 executors, 16 cores, 64GB RAM)  │         │
│  │                                                                 │         │
│  │  ┌──────────────┐   ┌───────────────┐   ┌─────────────────┐  │         │
│  │  │ STDF Parser  │──▶│ Data Quality  │──▶│ Feature Eng     │  │         │
│  │  │ (PySpark UDF)│   │ Validation    │   │ (Aggregations)  │  │         │
│  │  └──────────────┘   └───────────────┘   └─────────────────┘  │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                    │                         │
│  Processing: <5 min per 10GB STDF  |  Schema: 1,000+ columns  |  ACID ✓   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 3: STORAGE (Delta Lake on S3/MinIO)                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐      │
│  │ raw_stdf         │  │ wafer_features   │  │ parametric_stats     │      │
│  │ (Die-level data) │  │ (Aggregates)     │  │ (Test distributions) │      │
│  │ Partition: y/m/d │  │ Partition: y/m/d │  │ Partition: y/m/d     │      │
│  │ Size: 150GB      │  │ Size: 30GB       │  │ Size: 10GB           │      │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘      │
│                                                                              │
│  ┌──────────────────┐  ┌──────────────────────────────────────────┐        │
│  │spatial_patterns  │  │ model_predictions (inference results)    │        │
│  │(Wafer embeddings)│  │ Partition: year/month/day, model_name    │        │
│  │ Size: 5GB        │  │ Size: 5GB                                │        │
│  └──────────────────┘  └──────────────────────────────────────────┘        │
│                                                                              │
│  ACID Transactions ✓  |  Time-Travel ✓  |  Schema Evolution ✓  |  200GB   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 4: TRACKING (MLflow)                                                 │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │ MLflow Tracking Server (PostgreSQL backend, S3 artifacts)    │           │
│  │                                                               │           │
│  │  Experiments: P01-P15 projects (500+ runs)                   │           │
│  │  Model Registry: Champion (Production), Challenger (Staging) │           │
│  │  Auto-logging: XGBoost, scikit-learn, PyTorch, TensorFlow   │           │
│  │  Artifacts: model.pkl, plots, metrics, feature_importance    │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                              │
│  Experiments Tracked: 100%  |  Model Versions: v1.0, v1.1, v2.0  |  5-year │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 5: ORCHESTRATION (Apache Airflow)                                    │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │ Airflow Scheduler (CeleryExecutor, Redis broker, 5 workers) │           │
│  │                                                               │           │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐ │           │
│  │  │ Retraining DAGs  │  │ Data Quality    │  │ Feature    │ │           │
│  │  │ (Weekly Sun 2am) │  │ DAGs (Hourly)   │  │ Eng DAGs   │ │           │
│  │  │ P01-P15 models   │  │ Schema, stats   │  │ (Daily 1am)│ │           │
│  │  └──────────────────┘  └──────────────────┘  └────────────┘ │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                              │
│  DAGs: 30+  |  Success Rate: >95%  |  Retry: 3× exponential backoff        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 6: SERVING (FastAPI + Kubernetes)                                    │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │ FastAPI Multi-Model Serving (Uvicorn ASGI, 2-20 replicas)   │           │
│  │                                                               │           │
│  │  /predict/p01_xgboost  │  /predict/p02_resnet               │           │
│  │  /predict/p04_unet     │  /predict/p10_gnn  ...             │           │
│  │  /batch_predict        │  /health  │  /metrics              │           │
│  │                                                               │           │
│  │  Kubernetes HPA: Auto-scale 2-20 pods (CPU >70%)            │           │
│  │  Load Balancer: NGINX Ingress (round-robin)                 │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                              │
│  Latency: <100ms p95  |  Throughput: 10K predictions/day  |  Uptime: 99.9% │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  CROSS-CUTTING CONCERNS                                                      │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────┐  ┌───────────────┐  │
│  │ Prometheus    │  │ Grafana       │  │ OpenSearch  │  │ OpenTelemetry │  │
│  │ (Metrics)     │  │ (Dashboards)  │  │ (Logs)      │  │ (Traces)      │  │
│  └───────────────┘  └───────────────┘  └─────────────┘  └───────────────┘  │
│                                                                              │
│  Security: OAuth2/OIDC, RBAC, TLS 1.2+, Secrets Manager                     │
│  Cost: <$100/month (Databricks Community + S3 + EKS)                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 9.2 Component Details

**Kafka Cluster (Ingestion Layer)**:
- **Architecture**: 3 brokers (high availability), Zookeeper ensemble (3 nodes)
- **Topics**: 
  - `stdf_ingestion`: 12 partitions (parallel processing), replication factor 3
  - `stdf_dead_letter_queue`: 3 partitions (malformed STDFs)
- **Connectors**: Kafka Connect FileStreamSource (watch ATE network share, auto-ingest .std files)
- **Performance**: 1,000 STDFs/day = 40-50/hour, <1 minute ingestion latency
- **Monitoring**: Consumer lag (target <5 min), partition distribution, broker disk usage

**Spark Cluster (Processing Layer)**:
- **Local Development**: Spark Standalone (1 master + 2 workers in Docker, 4 cores, 8GB RAM each)
- **Cloud Production**: Databricks Runtime 14.3 LTS (auto-scaling 2-8 workers, 14GB each, 4 cores)
- **Jobs**:
  - STDF Parsing: Structured Streaming job (reads Kafka, parses binary STDF, writes Delta Lake)
  - Feature Engineering: Batch job (daily 1am, computes wafer aggregates from raw_stdf)
  - Data Quality: Batch job (hourly, validates schema, detects outliers)
- **Optimization**: Adaptive Query Execution (AQE), partition pruning, broadcast joins (<10MB tables)
- **Checkpointing**: S3 checkpoints for Structured Streaming (exactly-once processing)

**Delta Lake (Storage Layer)**:
- **Tables** (5 core tables):
  1. `raw_stdf`: Die-level test results (lot_id, wafer_id, die_x, die_y, bin, hbin, test_001...test_1000)
  2. `wafer_features`: Wafer-level aggregates (wafer_yield, edge_die_yield, parametric_mean, parametric_std)
  3. `parametric_stats`: Test-level statistics (test_id, mean, std, p5, p95, outlier_count)
  4. `spatial_patterns`: Wafer map embeddings (wafer_id, pattern_type, embedding_vector_512d)
  5. `model_predictions`: Inference results (prediction_id, model_name, model_version, input, output, confidence)
- **Partitioning**: All tables partitioned by `year/month/day` (query only relevant dates)
- **Optimization**: Z-ordering on `(device, lot_id)`, OPTIMIZE weekly, VACUUM monthly (30-day retention)
- **ACID**: Concurrent reads/writes, transaction log, versioning (time-travel queries)

**MLflow (Tracking Layer)**:
- **Components**:
  - Tracking Server: FastAPI backend (PostgreSQL metadata store, S3 artifact store)
  - Model Registry: Champion/Challenger versioning, Production/Staging/Archived tags
  - Auto-logging: Integrations for XGBoost, scikit-learn, PyTorch, TensorFlow
- **Deployment**: Kubernetes pod (2 replicas for HA), NGINX Ingress (HTTPS)
- **Storage**: PostgreSQL 16 (experiments, runs, metrics), S3 (model artifacts, plots)
- **API Access**: Python SDK (`mlflow.log_params()`), REST API (`/api/2.0/mlflow/experiments/list`)

**Airflow (Orchestration Layer)**:
- **Architecture**: 
  - Scheduler: 2 replicas (active/standby), CeleryExecutor
  - Workers: 5 Celery workers (auto-scale to 10 based on queue depth)
  - Webserver: 2 replicas (HA), NGINX Ingress
  - Broker: Redis 7.2 (Celery task queue)
- **DAG Storage**: Git repository (GitHub sync every 5 min, version-controlled DAGs)
- **Connections**: Databricks (Spark job submission), MLflow (model registry), PostgreSQL, S3
- **Scheduler**: Cron expressions (weekly Sunday 2am, daily 1am, hourly), SLA monitoring

**FastAPI (Serving Layer)**:
- **Deployment**: Kubernetes Deployment (2-20 replicas, HPA based on CPU >70% or request rate >500/min)
- **Load Balancer**: NGINX Ingress Controller (round-robin, sticky sessions optional)
- **Model Loading**: Load all P01-P15 models from MLflow registry on startup, watch for Production tag changes
- **Endpoints**: 15+ endpoints (`/predict/p01_xgboost`, `/predict/p02_resnet`, ...), `/batch_predict`, `/health`, `/metrics`
- **Performance**: <100ms p95 latency, 10K predictions/day, async request handling

### 9.3 Data Flow

**End-to-End Pipeline (ATE → Prediction)**:

```
1. STDF Generation (ATE)
   Advantest V93000 completes lot test → writes STDF to network share
   └─▶ Timestamp: T+0 min

2. Kafka Ingestion
   Kafka Connect FileStreamSource detects new .std file → publishes to stdf_ingestion topic
   └─▶ Timestamp: T+1 min  |  Latency: <1 min

3. Spark Processing
   Structured Streaming job reads Kafka → parses STDF (PySpark UDF) → writes Delta Lake
   └─▶ Timestamp: T+4 min  |  Latency: <3 min (10GB STDF parsed by 8 executors)

4. Delta Lake Storage
   raw_stdf table updated (ACID transaction) → trigger downstream feature engineering
   └─▶ Timestamp: T+5 min  |  Latency: <1 min (write + commit)

5. Feature Engineering (Daily Batch)
   Airflow DAG (daily 1am) reads raw_stdf → computes wafer_features aggregates → writes back
   └─▶ Timestamp: T+1 day (next 1am)  |  Latency: 10-15 min (batch processing)

6. Model Training (Weekly)
   Airflow retraining DAG (Sunday 2am) queries Delta Lake → trains model → logs to MLflow
   └─▶ Timestamp: T+7 days (next Sunday)  |  Latency: 30-60 min (training + validation)

7. Model Deployment
   Champion model promoted to Production in MLflow → FastAPI auto-reloads model
   └─▶ Timestamp: T+7 days + 5 min  |  Latency: <5 min (model download + reload)

8. Prediction Serving
   Test engineer calls /predict/p01_xgboost → FastAPI loads features from Delta Lake → inference
   └─▶ Timestamp: Real-time  |  Latency: <100ms p95 (feature query + model inference)
```

**Data Lineage (Traceability)**:

```
STDF File (lot_id=TC41x_LOT123, wafer_id=W05, timestamp=2024-12-01T10:00:00Z)
  │
  ├─▶ Kafka Message (topic=stdf_ingestion, partition=3, offset=12345)
  │     │
  │     └─▶ Spark Job (job_id=spark-12345, commit=a3f5b9c, timestamp=2024-12-01T10:02:00Z)
  │           │
  │           └─▶ Delta Lake raw_stdf (version=456, timestamp=2024-12-01T10:04:00Z)
  │                 │
  │                 ├─▶ Spark Feature Eng (job_id=spark-67890, timestamp=2024-12-02T01:00:00Z)
  │                 │     │
  │                 │     └─▶ Delta Lake wafer_features (version=123, timestamp=2024-12-02T01:15:00Z)
  │                 │           │
  │                 │           └─▶ MLflow Experiment (experiment_id=p01, run_id=abc123, timestamp=2024-12-08T02:00:00Z)
  │                 │                 │
  │                 │                 └─▶ MLflow Model v2.3 (model_uri=s3://mlflow/p01/v2.3/model.pkl, timestamp=2024-12-08T02:30:00Z)
  │                 │                       │
  │                 │                       └─▶ FastAPI Prediction (prediction_id=pred-99999, model_version=v2.3, timestamp=2024-12-10T14:30:00Z)
  │                 │
  │                 └─▶ model_predictions table (logged for audit trail, 3-year retention)
  │
  └─▶ Audit Trail: Full lineage from raw STDF → model training → prediction (ISO 26262 compliance)
```

**Deployment Topologies**:

**Local Development (Docker Compose)**:
```
Docker Host (Laptop/Workstation)
├── kafka (1 broker, 1 zookeeper)
├── spark-master (1 master)
├── spark-worker-1, spark-worker-2 (2 workers, 4 cores each)
├── airflow-scheduler, airflow-webserver, airflow-worker (3 containers)
├── postgres (Airflow metadata + MLflow backend)
├── redis (Airflow Celery broker)
├── minio (S3-compatible storage for Delta Lake + MLflow artifacts)
├── mlflow-server (tracking server)
├── fastapi (model serving)
├── prometheus, grafana (monitoring)
└── Data: 44GB real STDFs (423 files from P15) OR replay 423 synthetic from P07

Cost: $0 (all open-source, local execution)
Performance: Limited by laptop resources (8-16 cores, 16-32GB RAM)
Use case: Fast iteration, debugging, unit testing, Spark job development
```

**Cloud Production (Databricks + AWS EKS)**:
```
AWS Cloud
├── Databricks Workspace (Spark + Delta Lake + MLflow managed)
│   ├── Databricks Runtime 14.3 LTS cluster (2-8 workers, 14GB each, auto-scaling)
│   ├── Delta Lake storage on S3 (200GB synthetic STDFs from P07)
│   ├── MLflow Tracking Server (managed, PostgreSQL + S3)
│   └── Databricks Jobs (scheduled Spark ETL, model training via Airflow)
│
├── AWS EKS Cluster (Kubernetes for stateless services)
│   ├── Kafka StatefulSet (3 pods, Zookeeper 3 pods)
│   ├── Airflow (scheduler 2 pods, webserver 2 pods, workers 5 pods, Redis 1 pod)
│   ├── FastAPI Deployment (2-20 pods, HPA auto-scaling)
│   ├── Prometheus StatefulSet (1 pod, 15-day retention)
│   ├── Grafana Deployment (2 pods)
│   └── OpenSearch StatefulSet (3 pods, 30-day log retention)
│
├── AWS S3 (object storage)
│   ├── Delta Lake tables (200GB, partitioned by year/month/day)
│   ├── MLflow artifacts (model files, plots, 5-year retention)
│   └── Airflow logs, Spark checkpoints
│
├── AWS RDS PostgreSQL (metadata)
│   ├── Airflow metadata (DAG runs, task instances)
│   └── MLflow tracking (experiments, runs, metrics)
│
└── Networking: VPC, private subnets, NAT gateway, ALB (Application Load Balancer)

Cost: <$100/month
  - Databricks Community Edition: $0 (Year 1), then ~$50/month (14GB cluster, 40 hrs/month)
  - AWS S3: $5-25/month (200GB Standard tier, data transfer)
  - EKS: ~$70/month (control plane $72/month, 2-3 t3.medium nodes $30-45/month)
  - RDS: ~$20/month (db.t3.micro PostgreSQL)
  - Total: $95-165/month (optimize with spot instances, auto-shutdown to <$100)
```

---

## 10. Data Model

### 10.1 Entity Relationships

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           P16 DATA MODEL                                     │
│                     Delta Lake Schema (5 Core Tables)                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ raw_stdf (Die-level test results)                                        │
│ Partition: year/month/day  |  Z-order: (device, lot_id)  |  Size: 150GB │
├──────────────────────────────────────────────────────────────────────────┤
│ PK: (lot_id, wafer_id, die_x, die_y)                                    │
│                                                                           │
│ - lot_id: STRING                    (TC41x_LOT123)                       │
│ - wafer_id: STRING                  (W05)                                │
│ - die_x: INT                        (10)                                 │
│ - die_y: INT                        (15)                                 │
│ - bin: INT                          (1=Pass, 2-99=Fail)                  │
│ - hbin: INT                         (Hardware bin)                       │
│ - device: STRING                    (TC41x, TC42x)                       │
│ - test_program: STRING              (FT_v1.2.3)                          │
│ - test_001: DOUBLE ... test_1000   (Parametric test results)            │
│ - timestamp: TIMESTAMP              (2024-12-01T10:04:00Z)               │
│ - kafka_offset: LONG                (12345, for exactly-once)            │
│                                                                           │
│ Relationships:                                                            │
│   1:N → wafer_features (aggregated by lot_id, wafer_id)                 │
│   1:N → parametric_stats (aggregated by test_id)                        │
└──────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ GROUP BY lot_id, wafer_id
                                  │ (Spark Feature Engineering DAG)
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ wafer_features (Wafer-level aggregates)                                  │
│ Partition: year/month/day  |  Z-order: (device, lot_id)  |  Size: 30GB  │
├──────────────────────────────────────────────────────────────────────────┤
│ PK: (lot_id, wafer_id)                                                   │
│                                                                           │
│ - lot_id: STRING                                                         │
│ - wafer_id: STRING                                                       │
│ - device: STRING                                                         │
│ - wafer_yield: DOUBLE               (passed_die / total_die)            │
│ - edge_die_yield: DOUBLE            (passed_die in outer 2mm ring)      │
│ - center_yield: DOUBLE              (passed_die in center 20%)          │
│ - total_die: INT                    (1500)                               │
│ - passed_die: INT                   (1350)                               │
│ - failed_die: INT                   (150)                                │
│ - bin_distribution: MAP<INT,INT>    ({1:1350, 2:100, 3:50})            │
│ - parametric_mean_IDDQ: DOUBLE      (Mean IDDQ across all die)         │
│ - parametric_std_IDDQ: DOUBLE       (Std dev IDDQ)                      │
│ - parametric_p5_VTH: DOUBLE         (5th percentile VTH)                │
│ - parametric_p95_VTH: DOUBLE        (95th percentile VTH)               │
│ - timestamp: TIMESTAMP                                                   │
│ - feature_version: STRING           (v1.0, for schema evolution)        │
│                                                                           │
│ Relationships:                                                            │
│   N:1 ← raw_stdf (source data)                                          │
│   1:1 → spatial_patterns (wafer map embeddings)                         │
│   N:M → model_predictions (input features for P01-P15 models)           │
└──────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │ Used by P01-P15 models
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ parametric_stats (Test-level statistics)                                 │
│ Partition: year/month/day  |  Z-order: (device, test_id)  |  Size: 10GB │
├──────────────────────────────────────────────────────────────────────────┤
│ PK: (test_id, device, date)                                             │
│                                                                           │
│ - test_id: STRING                   (test_IDDQ, test_VTH)               │
│ - device: STRING                    (TC41x)                              │
│ - date: DATE                        (2024-12-01)                         │
│ - mean: DOUBLE                      (1.23e-6 for IDDQ)                  │
│ - std: DOUBLE                       (2.5e-7)                             │
│ - p5: DOUBLE                        (5th percentile)                     │
│ - p95: DOUBLE                       (95th percentile)                    │
│ - outlier_count: INT                (Die >5 sigma from mean)            │
│ - sample_size: INT                  (10,000 die measured)                │
│ - timestamp: TIMESTAMP                                                   │
│                                                                           │
│ Relationships:                                                            │
│   N:1 ← raw_stdf (source data)                                          │
│   Used for: Data quality checks, baseline comparisons, drift detection  │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ spatial_patterns (Wafer map embeddings & classification)                 │
│ Partition: year/month/day  |  Z-order: (device, wafer_id)  |  Size: 5GB │
├──────────────────────────────────────────────────────────────────────────┤
│ PK: (lot_id, wafer_id)                                                   │
│                                                                           │
│ - lot_id: STRING                                                         │
│ - wafer_id: STRING                                                       │
│ - device: STRING                                                         │
│ - pattern_type: STRING              (edge, center, ring, quadrant, ...)│
│ - pattern_confidence: DOUBLE        (0.95 = 95% confident)              │
│ - embedding_vector: ARRAY<DOUBLE>   (512-dim ResNet-18 embeddings)     │
│ - wafer_map_s3_path: STRING         (s3://bucket/wafer_maps/W05.png)   │
│ - timestamp: TIMESTAMP                                                   │
│                                                                           │
│ Relationships:                                                            │
│   1:1 ← wafer_features (FK: lot_id, wafer_id)                          │
│   Used by: P04 ResNet Wafer Defect Classifier, P03 Multi-Agent RCA     │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ model_predictions (Inference results & audit trail)                      │
│ Partition: year/month/day/model_name  |  Z-order: prediction_id         │
├──────────────────────────────────────────────────────────────────────────┤
│ PK: prediction_id (UUID)                                                 │
│                                                                           │
│ - prediction_id: STRING             (UUID: abc-123-def-456)             │
│ - model_name: STRING                (p01_xgboost, p02_resnet, ...)      │
│ - model_version: STRING             (v2.3)                               │
│ - input_features: MAP<STRING,DOUBLE>({wafer_yield:0.90, edge:0.85})    │
│ - output_prediction: DOUBLE         (3.0 for bin prediction)            │
│ - confidence: DOUBLE                (0.87)                               │
│ - latency_ms: DOUBLE                (45.2)                               │
│ - user_id: STRING                   (engineer@company.com)               │
│ - timestamp: TIMESTAMP              (2024-12-10T14:30:00Z)               │
│ - lot_id: STRING (optional)         (TC41x_LOT123, for traceability)    │
│ - wafer_id: STRING (optional)       (W05)                                │
│                                                                           │
│ Relationships:                                                            │
│   N:1 → wafer_features (FK: lot_id, wafer_id, optional)                │
│   Used for: Audit trail (ISO 26262), A/B testing, model monitoring      │
│   Retention: 3 years (compliance requirement)                            │
└──────────────────────────────────────────────────────────────────────────┘
```

### 10.2 Database Schema

**Delta Lake Schema Definitions (SQL DDL)**:

```sql
-- Table 1: raw_stdf (Die-level test results)
CREATE TABLE IF NOT EXISTS raw_stdf (
    lot_id STRING NOT NULL COMMENT 'Lot identifier (e.g., TC41x_LOT123)',
    wafer_id STRING NOT NULL COMMENT 'Wafer identifier (e.g., W05)',
    die_x INT NOT NULL COMMENT 'Die X coordinate (0-based)',
    die_y INT NOT NULL COMMENT 'Die Y coordinate (0-based)',
    bin INT NOT NULL COMMENT 'Software bin (1=Pass, 2-99=Fail)',
    hbin INT NOT NULL COMMENT 'Hardware bin',
    device STRING NOT NULL COMMENT 'Device type (TC41x, TC42x, etc.)',
    test_program STRING COMMENT 'Test program version (e.g., FT_v1.2.3)',
    -- 1,000 parametric tests (dynamic columns)
    test_IDDQ DOUBLE COMMENT 'IDDQ leakage current (Amps)',
    test_VTH DOUBLE COMMENT 'Threshold voltage (Volts)',
    test_FMAX DOUBLE COMMENT 'Maximum frequency (Hz)',
    -- ... test_001 to test_1000 ...
    timestamp TIMESTAMP NOT NULL COMMENT 'STDF parsing timestamp (UTC)',
    kafka_offset LONG NOT NULL COMMENT 'Kafka message offset (exactly-once processing)',
    year INT COMMENT 'Partition column: year',
    month INT COMMENT 'Partition column: month',
    day INT COMMENT 'Partition column: day'
)
USING DELTA
PARTITIONED BY (year, month, day)
LOCATION 's3://p16-datalake/delta/raw_stdf'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = '30 days',
    'delta.deletedFileRetentionDuration' = '7 days'
);

-- Z-ordering for query performance
OPTIMIZE raw_stdf ZORDER BY (device, lot_id);

-- Table 2: wafer_features (Wafer-level aggregates)
CREATE TABLE IF NOT EXISTS wafer_features (
    lot_id STRING NOT NULL,
    wafer_id STRING NOT NULL,
    device STRING NOT NULL,
    wafer_yield DOUBLE COMMENT 'Wafer yield = passed_die / total_die',
    edge_die_yield DOUBLE COMMENT 'Yield of die in outer 2mm ring',
    center_yield DOUBLE COMMENT 'Yield of die in center 20% area',
    total_die INT,
    passed_die INT,
    failed_die INT,
    bin_distribution MAP<INT, INT> COMMENT 'Histogram of bin counts {1:1350, 2:100}',
    parametric_mean_IDDQ DOUBLE,
    parametric_std_IDDQ DOUBLE,
    parametric_p5_VTH DOUBLE COMMENT '5th percentile VTH',
    parametric_p95_VTH DOUBLE COMMENT '95th percentile VTH',
    timestamp TIMESTAMP NOT NULL,
    feature_version STRING DEFAULT 'v1.0' COMMENT 'Schema version for evolution',
    year INT,
    month INT,
    day INT
)
USING DELTA
PARTITIONED BY (year, month, day)
LOCATION 's3://p16-datalake/delta/wafer_features'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.logRetentionDuration' = '30 days'
);

OPTIMIZE wafer_features ZORDER BY (device, lot_id);

-- Table 3: parametric_stats (Test-level statistics)
CREATE TABLE IF NOT EXISTS parametric_stats (
    test_id STRING NOT NULL COMMENT 'Test identifier (test_IDDQ, test_VTH)',
    device STRING NOT NULL,
    date DATE NOT NULL,
    mean DOUBLE,
    std DOUBLE,
    p5 DOUBLE COMMENT '5th percentile',
    p95 DOUBLE COMMENT '95th percentile',
    outlier_count INT COMMENT 'Count of die >5 sigma from mean',
    sample_size INT COMMENT 'Number of die measured',
    timestamp TIMESTAMP NOT NULL,
    year INT,
    month INT,
    day INT
)
USING DELTA
PARTITIONED BY (year, month, day)
LOCATION 's3://p16-datalake/delta/parametric_stats';

OPTIMIZE parametric_stats ZORDER BY (device, test_id);

-- Table 4: spatial_patterns (Wafer map embeddings)
CREATE TABLE IF NOT EXISTS spatial_patterns (
    lot_id STRING NOT NULL,
    wafer_id STRING NOT NULL,
    device STRING NOT NULL,
    pattern_type STRING COMMENT 'edge, center, ring, quadrant, random, scratch',
    pattern_confidence DOUBLE COMMENT 'Confidence score 0-1',
    embedding_vector ARRAY<DOUBLE> COMMENT '512-dim ResNet-18 feature vector',
    wafer_map_s3_path STRING COMMENT 's3://bucket/wafer_maps/W05.png',
    timestamp TIMESTAMP NOT NULL,
    year INT,
    month INT,
    day INT
)
USING DELTA
PARTITIONED BY (year, month, day)
LOCATION 's3://p16-datalake/delta/spatial_patterns';

-- Table 5: model_predictions (Inference audit trail)
CREATE TABLE IF NOT EXISTS model_predictions (
    prediction_id STRING NOT NULL COMMENT 'UUID for each prediction',
    model_name STRING NOT NULL,
    model_version STRING NOT NULL,
    input_features MAP<STRING, DOUBLE> COMMENT 'Input feature values',
    output_prediction DOUBLE,
    confidence DOUBLE,
    latency_ms DOUBLE,
    user_id STRING,
    timestamp TIMESTAMP NOT NULL,
    lot_id STRING COMMENT 'Optional: link to source wafer',
    wafer_id STRING,
    year INT,
    month INT,
    day INT,
    model_name_partition STRING COMMENT 'Partition by model for separate storage'
)
USING DELTA
PARTITIONED BY (year, month, day, model_name_partition)
LOCATION 's3://p16-datalake/delta/model_predictions'
TBLPROPERTIES (
    'delta.logRetentionDuration' = '1095 days',  -- 3 years (compliance)
    'delta.deletedFileRetentionDuration' = '30 days'
);
```

### 10.3 Data Flow Diagrams

**Feature Engineering Pipeline (ASCII Diagram)**:

```
┌──────────────────────────────────────────────────────────────────────────┐
│ SPARK FEATURE ENGINEERING DAG (Daily 1am)                                │
└──────────────────────────────────────────────────────────────────────────┘

INPUT: raw_stdf (150GB, partitioned by year/month/day)
  │
  │ SELECT * FROM raw_stdf WHERE date = current_date() - INTERVAL 1 DAY
  │ (Read yesterday's data, ~400MB/day)
  │
  ├─────────────────┬──────────────────┬─────────────────┐
  │                 │                  │                 │
  ▼                 ▼                  ▼                 ▼
┌────────────┐  ┌────────────┐  ┌─────────────┐  ┌───────────────┐
│ Wafer-level│  │ Test-level │  │ Spatial     │  │ Correlation   │
│ Aggregates │  │ Statistics │  │ Pattern     │  │ Features      │
│            │  │            │  │ Embeddings  │  │               │
│ GROUP BY   │  │ GROUP BY   │  │ Wafer Map   │  │ Pearson Corr  │
│ lot_id,    │  │ test_id,   │  │ Generator   │  │ Matrix        │
│ wafer_id   │  │ device     │  │ ResNet-18   │  │ (1K tests)    │
│            │  │            │  │ Extractor   │  │               │
│ Features:  │  │ Metrics:   │  │ Output:     │  │ Output:       │
│ - yield    │  │ - mean     │  │ - pattern   │  │ - top 100     │
│ - edge_die │  │ - std      │  │ - embedding │  │   correlated  │
│ - center   │  │ - p5, p95  │  │ - confidence│  │   test pairs  │
│ - bin dist │  │ - outliers │  │             │  │               │
└────────────┘  └────────────┘  └─────────────┘  └───────────────┘
  │                 │                  │                 │
  │ WRITE           │ WRITE            │ WRITE           │ (Optional)
  │ Delta Lake      │ Delta Lake       │ Delta Lake      │ Compute on-demand
  │                 │                  │                 │
  ▼                 ▼                  ▼                 │
┌────────────┐  ┌────────────┐  ┌─────────────┐         │
│wafer_feat  │  │parametric_ │  │spatial_     │         │
│ures        │  │stats       │  │patterns     │         │
│(30GB)      │  │(10GB)      │  │(5GB)        │         │
└────────────┘  └────────────┘  └─────────────┘         │
  │                                                      │
  │ MERGED BY lot_id, wafer_id                          │
  │                                                      │
  ▼                                                      │
┌───────────────────────────────────────────────────────┴──┐
│ UNIFIED FEATURE VIEW (For ML model training)             │
│                                                           │
│ SELECT wf.*, sp.pattern_type, sp.embedding_vector        │
│ FROM wafer_features wf                                   │
│ LEFT JOIN spatial_patterns sp                           │
│   ON wf.lot_id = sp.lot_id AND wf.wafer_id = sp.wafer_id│
│                                                           │
│ Used by: P01-P15 models for training/inference           │
└───────────────────────────────────────────────────────────┘
```

### 10.4 Input Data & Dataset Requirements

**Primary Data Source: STDF Files**:
- **Format**: Standard Test Data Format (STDF V4, binary)
- **Volume**: 
  - Development: 423 synthetic .std files (44GB total, from P15 or P07 GAN)
  - Production: 1,000 STDFs/day (average 550KB each = 550MB/day = 200GB/year)
- **Structure**:
  - Header: Lot ID, Wafer ID, Test Program, ATE Config
  - PTR Records: Parametric Test Results (1,000+ tests per die, floating-point values)
  - FTR Records: Functional Test Results (Pass/Fail flags)
  - PRR Records: Part Result Record (Die X/Y, Bin, HBin, Site)
- **Parsing**: pystdf library extracts PTR/FTR/PRR records → Pandas DataFrame → PySpark DataFrame

**Synthetic Data Strategy (P07 GAN Integration)**:
- **Phase 1 (Weeks 1-4)**: Replay existing 423 synthetic STDFs from P07 for fast development iteration
- **Phase 2 (Weeks 5-14)**: Generate fresh STDFs nightly (100-500 files/night) via P07 GAN Airflow DAG
- **Privacy Advantage**: Synthetic STDFs safe for Databricks Cloud (no proprietary chip data exposure)
- **Realism**: P07 GAN trained on 10 years of real STDF patterns → realistic variability (bin distributions, parametric distributions, spatial patterns)

**Data Quality Requirements**:
- **Completeness**: <10% missing values in critical columns (bin, hbin, parametric tests)
- **Schema Validation**: All 1,000 parametric tests present, correct data types (DOUBLE for measurements)
- **Statistical Sanity**: Parametric test means within ±3 sigma of historical baseline (detect ATE drift)
- **Outlier Handling**: Flag die with parametric values >5 sigma, exclude from model training
- **Deduplication**: Kafka offset tracking ensures exactly-once processing (no duplicate die records)

**Dataset Splits (Model Training)**:
- **Training**: 70% of wafers (random split, stratified by device and bin distribution)
- **Validation**: 15% of wafers (for hyperparameter tuning, early stopping)
- **Test**: 15% of wafers (held-out, never seen during training, final accuracy evaluation)
- **Time-based Split**: Training (data from months 1-9), Validation (month 10-11), Test (month 12) for time-series validation

**Feature Store Schema Evolution**:
- **v1.0** (Initial): 50 features (wafer_yield, edge_die_yield, parametric_mean/std for 20 critical tests)
- **v1.1** (Month 3): +25 features (spatial pattern embeddings, test correlation features)
- **v2.0** (Month 6): +25 features (shmoo plot features, scan chain coverage metrics)
- **Backward Compatibility**: Delta Lake schema evolution, models specify `feature_version` in MLflow metadata

---

## 11. API Specifications

### 11.1 REST Endpoints

**FastAPI Model Serving Endpoints (Layer 6)**:

**Base URL**: `https://api.p16.company.com/v1`

**Authentication**: OAuth2 Bearer Token (JWT from Azure AD/Okta SSO)

**1. Prediction Endpoints (P01-P15 Models)**:

```http
POST /predict/{model_name}
Content-Type: application/json
Authorization: Bearer <jwt_token>

Request Body:
{
  "features": [0.90, 0.85, 1.23e-6, 0.65, ...],  // Feature vector (length depends on model)
  "metadata": {
    "lot_id": "TC41x_LOT123",  // Optional: for audit trail
    "wafer_id": "W05",
    "user_id": "engineer@company.com"
  }
}

Response (200 OK):
{
  "prediction": 3.0,           // Predicted value (bin number, yield %, etc.)
  "confidence": 0.87,          // Confidence score 0-1
  "model_version": "v2.3",     // Model version from MLflow registry
  "latency_ms": 45.2,          // Prediction latency
  "timestamp": "2024-12-10T14:30:00Z",
  "prediction_id": "pred-abc-123"  // UUID for audit trail
}

Error Response (400 Bad Request):
{
  "detail": "Invalid feature vector length: expected 50, got 45"
}

Error Response (404 Not Found):
{
  "detail": "Model p99_invalid not found. Available models: p01_xgboost, p02_resnet, ..."
}
```

**Model-Specific Endpoints**:
- `POST /predict/p01_xgboost` - XGBoost Bin Predictor (input: 10 features, output: bin 1-99)
- `POST /predict/p02_resnet` - ResNet Yield Predictor (input: wafer features, output: yield 0-1)
- `POST /predict/p04_unet` - U-Net Wafer Defect Classifier (input: 512×512 wafer map image, output: defect class)
- `POST /predict/p10_gnn` - GNN Failure Propagation (input: test failure graph, output: root cause probability distribution)
- ... (15 total endpoints for P01-P15)

**2. Batch Prediction Endpoint**:

```http
POST /batch_predict/{model_name}
Content-Type: application/json
Authorization: Bearer <jwt_token>

Request Body:
{
  "features_batch": [
    [0.90, 0.85, 1.23e-6, ...],  // Feature vector 1
    [0.88, 0.82, 1.45e-6, ...],  // Feature vector 2
    ...  // Up to 1000 feature vectors
  ],
  "metadata": {
    "batch_id": "batch-2024-12-10-001"
  }
}

Response (200 OK):
{
  "predictions": [3.0, 2.0, ...],  // Array of predictions
  "confidences": [0.87, 0.92, ...],
  "batch_latency_ms": 450.5,       // Total batch processing time
  "batch_size": 100,
  "model_version": "v2.3",
  "timestamp": "2024-12-10T14:30:00Z"
}
```

**3. Model Metadata Endpoints**:

```http
GET /models
Authorization: Bearer <jwt_token>

Response (200 OK):
{
  "models": [
    {
      "name": "p01_xgboost",
      "version": "v2.3",
      "status": "Production",
      "accuracy": 0.89,
      "latency_ms_p95": 18.5,
      "last_updated": "2024-12-08T02:30:00Z"
    },
    {
      "name": "p02_resnet",
      "version": "v1.5",
      "status": "Production",
      "accuracy": 0.91,
      "latency_ms_p95": 75.2,
      "last_updated": "2024-11-25T10:15:00Z"
    },
    ...
  ],
  "total_models": 15
}

GET /models/{model_name}/metadata
Authorization: Bearer <jwt_token>

Response (200 OK):
{
  "model_name": "p01_xgboost",
  "current_version": "v2.3",
  "production_version": "v2.3",
  "staging_version": "v2.4",  // Challenger model in A/B test
  "feature_names": ["wafer_yield", "edge_die_yield", "parametric_mean_IDDQ", ...],
  "feature_count": 10,
  "training_data_version": "delta_wafer_features_v123",
  "training_date": "2024-12-08",
  "metrics": {
    "accuracy": 0.89,
    "f1_score": 0.87,
    "precision": 0.88,
    "recall": 0.86
  },
  "mlflow_run_id": "abc123def456",
  "git_commit": "a3f5b9c"
}
```

**4. Health & Monitoring Endpoints**:

```http
GET /health
Response (200 OK):
{
  "status": "healthy",
  "models_loaded": 15,
  "uptime_seconds": 345600,  // 4 days uptime
  "timestamp": "2024-12-10T14:30:00Z"
}

GET /metrics
Response (200 OK - Prometheus format):
# HELP fastapi_requests_total Total number of requests
# TYPE fastapi_requests_total counter
fastapi_requests_total{method="POST",endpoint="/predict/p01_xgboost",status="200"} 12345

# HELP fastapi_request_duration_seconds Request latency
# TYPE fastapi_request_duration_seconds histogram
fastapi_request_duration_seconds_bucket{le="0.01",model="p01_xgboost"} 5000
fastapi_request_duration_seconds_bucket{le="0.05",model="p01_xgboost"} 11000
fastapi_request_duration_seconds_bucket{le="0.1",model="p01_xgboost"} 12200
...

GET /ready
Response (200 OK if all models loaded, 503 Service Unavailable otherwise):
{
  "status": "ready",
  "models": {
    "p01_xgboost": "loaded",
    "p02_resnet": "loaded",
    ...
  }
}
```

### 11.2 Request/Response Examples

**Example 1: P01 XGBoost Bin Prediction**:

```bash
# Request
curl -X POST "https://api.p16.company.com/v1/predict/p01_xgboost" \
  -H "Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json" \
  -d '{
    "features": [0.90, 0.85, 1.23e-6, 0.65, 2.5e-7, 0.88, 0.92, 500, 450, 50],
    "metadata": {
      "lot_id": "TC41x_LOT123",
      "wafer_id": "W05",
      "user_id": "john.doe@company.com"
    }
  }'

# Response (Success)
{
  "prediction": 3.0,
  "confidence": 0.87,
  "model_version": "v2.3",
  "latency_ms": 18.5,
  "timestamp": "2024-12-10T14:30:15.234Z",
  "prediction_id": "pred-f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "feature_importance": {
    "wafer_yield": 0.35,
    "edge_die_yield": 0.28,
    "parametric_mean_IDDQ": 0.22,
    ...
  }
}
```

**Example 2: P02 ResNet Yield Prediction (Batch)**:

```bash
# Request (Batch of 3 wafers)
curl -X POST "https://api.p16.company.com/v1/batch_predict/p02_resnet" \
  -H "Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json" \
  -d '{
    "features_batch": [
      [0.90, 0.85, 1.23e-6, 0.65, 2.5e-7, 0.88, 0.92, 500, 450, 50],
      [0.88, 0.82, 1.45e-6, 0.60, 3.1e-7, 0.85, 0.90, 480, 430, 50],
      [0.92, 0.87, 1.10e-6, 0.68, 2.2e-7, 0.90, 0.94, 510, 460, 50]
    ],
    "metadata": {
      "batch_id": "batch-2024-12-10-001",
      "lot_id": "TC41x_LOT123"
    }
  }'

# Response
{
  "predictions": [0.905, 0.885, 0.925],  // Predicted yields for 3 wafers
  "confidences": [0.91, 0.88, 0.93],
  "batch_latency_ms": 125.8,
  "batch_size": 3,
  "model_version": "v1.5",
  "timestamp": "2024-12-10T14:31:00.123Z",
  "predictions_detail": [
    {
      "prediction": 0.905,
      "confidence": 0.91,
      "prediction_id": "pred-123"
    },
    {
      "prediction": 0.885,
      "confidence": 0.88,
      "prediction_id": "pred-124"
    },
    {
      "prediction": 0.925,
      "confidence": 0.93,
      "prediction_id": "pred-125"
    }
  ]
}
```

**Example 3: Error Handling**:

```bash
# Request (Invalid feature vector)
curl -X POST "https://api.p16.company.com/v1/predict/p01_xgboost" \
  -H "Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json" \
  -d '{
    "features": [0.90, 0.85]  // Only 2 features, expected 10
  }'

# Response (400 Bad Request)
{
  "detail": "Invalid feature vector length: expected 10, got 2",
  "error_code": "INVALID_FEATURE_LENGTH",
  "model_name": "p01_xgboost",
  "expected_features": 10,
  "provided_features": 2,
  "timestamp": "2024-12-10T14:32:00.000Z"
}
```

### 11.3 Authentication

**OAuth2 / OIDC Integration (Azure AD / Okta)**:

```python
# FastAPI OAuth2 Setup
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Decode JWT token
        payload = jwt.decode(
            token, 
            PUBLIC_KEY,  # Azure AD public key (fetched from JWKS endpoint)
            algorithms=["RS256"],
            audience="api://p16-mlops-platform"
        )
        username: str = payload.get("sub")
        roles: list = payload.get("roles", [])
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    return {"username": username, "roles": roles}

# Protected endpoint
@app.post("/predict/{model_name}")
async def predict(
    model_name: str,
    request: PredictionRequest,
    current_user: dict = Depends(get_current_user)
):
    # Check RBAC permissions
    if "ml_engineer" not in current_user["roles"] and "test_engineer" not in current_user["roles"]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Prediction logic...
    ...
```

**RBAC Roles**:
- `data_engineer`: Read/write Delta Lake, Kafka, Spark jobs
- `ml_engineer`: Read Delta Lake, write MLflow experiments, deploy models to Staging, call prediction APIs
- `platform_engineer`: Admin access (Databricks clusters, K8s, monitoring)
- `test_engineer`: Read-only Delta Lake, call prediction APIs
- `yield_engineer`: Read Delta Lake, call prediction APIs, provide model feedback

---

## 12. UI/UX Requirements

### 12.1 User Interface

**Web-Based Dashboards**:

**1. MLflow Tracking UI** (Primary interface for ML Engineers):
- **Experiments View**: 
  - List all P01-P15 experiments with run counts, last updated timestamp
  - Search/filter by project, date range, hyperparameters, metrics
  - Compare runs side-by-side (metrics charts, parallel coordinates plot)
- **Runs Detail View**:
  - Hyperparameters table (learning_rate, max_depth, n_estimators, etc.)
  - Metrics charts (accuracy, F1-score, loss over epochs)
  - Artifacts browser (model.pkl, feature_importance.png, confusion_matrix.png)
  - Code version (git commit hash), tags, notes
- **Model Registry**:
  - List registered models with Production/Staging/Archived status
  - Model versioning (v1.0, v1.1, v2.0) with lineage graph
  - A/B test status (Champion vs. Challenger traffic split)
  - Transition buttons (Stage → Production, Production → Archived)
- **Accessibility**: WCAG 2.1 AA compliant, keyboard navigation, high contrast mode

**2. Databricks SQL Analytics** (For Data/Yield Engineers):
- **Query Editor**:
  - SQL autocomplete for Delta Lake tables (raw_stdf, wafer_features, parametric_stats)
  - Syntax highlighting, error detection, query history
  - Saved queries library (common aggregations, feature engineering templates)
- **Dashboards**:
  - Wafer Yield Trends: Line chart (yield % over time by device)
  - Bin Distribution: Stacked bar chart (bin counts by lot)
  - Parametric Outliers: Scatter plot (IDDQ vs. VTH with 5-sigma boundaries)
  - Spatial Patterns: Wafer map heatmaps (edge failures, center failures, etc.)
- **Sharing**: Public links (read-only), scheduled email reports (daily/weekly)
- **Responsive Design**: Desktop (1920×1080), tablet (1024×768), mobile view (basic)

**3. Airflow Web UI** (For Platform Engineers):
- **DAG Graph View**:
  - Visual DAG topology (nodes = tasks, edges = dependencies)
  - Task status colors (green=success, red=failed, yellow=running, gray=scheduled)
  - Click task → view logs, XCom data, retry/clear
- **Gantt Chart**:
  - Timeline view of task executions (identify bottlenecks)
  - Filter by DAG, date range, execution duration
- **Logs Viewer**:
  - Real-time streaming logs (WebSocket connection)
  - Search/filter by keyword, log level (INFO, WARNING, ERROR)
  - Download logs as .txt file

**4. Grafana Monitoring Dashboards** (For All Users):
- **Infrastructure Health Dashboard**:
  - Kafka: Consumer lag gauge (target <5 min, alert >10 min)
  - Spark: Active jobs count, executor memory usage
  - Delta Lake: Table sizes (GB), daily growth rate
  - FastAPI: Request rate (req/min), p50/p95/p99 latency, error rate %
- **Model Performance Dashboard**:
  - Prediction count by model (line chart, last 24 hours)
  - Latency distribution histogram (P01-P15 models)
  - Confidence score distribution (0-1 range, detect low-confidence predictions)
  - Drift detection: Feature distribution changes (KL divergence)
- **Cost Dashboard**:
  - Databricks DBU consumption (daily, weekly, monthly trends)
  - AWS S3 storage costs (GB stored, data transfer)
  - EKS compute costs (node hours, spot vs. on-demand)
  - Total monthly spend gauge (target <$100, alert >$120)
- **Alerts Panel**: Recent alerts (last 7 days), acknowledged/resolved status

**5. FastAPI Swagger UI** (For Developers):
- **Interactive API Documentation**:
  - Auto-generated from Pydantic models (request/response schemas)
  - "Try it out" feature (send test requests directly from browser)
  - Example requests/responses for each endpoint
  - Authentication: JWT token input field (test with real Azure AD tokens)
- **Endpoint Explorer**:
  - List all 20+ endpoints (/predict/p01_xgboost, /batch_predict, /health, /metrics)
  - HTTP method badges (POST, GET, PUT, DELETE)
  - Response status codes (200, 400, 401, 404, 500) with descriptions

### 12.2 User Experience

**Onboarding Flow (New ML Engineer)**:

1. **Day 1: Environment Setup**
   - Docker Compose quickstart: `docker-compose up -d` (all services running locally in <5 min)
   - Access checklist: MLflow UI (http://localhost:5000), Airflow (http://localhost:8080), Databricks (cloud login)
   - Tutorial notebook: `notebooks/01_quickstart.ipynb` (query Delta Lake, log experiment to MLflow, trigger prediction)

2. **Day 2-3: Data Exploration**
   - Databricks SQL tutorial: Query `wafer_features` (10 sample queries provided)
   - Delta Lake time-travel: Reproduce training data from 3 months ago (`VERSION AS OF 123`)
   - Feature engineering playground: Modify `spark_jobs/feature_engineering.py`, run locally, see results in <10 min

3. **Day 4-7: Model Training**
   - MLflow auto-logging setup: `mlflow.sklearn.autolog()` (track experiments automatically)
   - Train P01 mock model: `python scripts/train_p01_mock.py` (logs to MLflow, registers model)
   - Model promotion workflow: Stage model → manual validation → promote to Production (via UI button)

4. **Day 8-14: Production Deployment**
   - FastAPI integration: Load model from MLflow, test `/predict/p01_xgboost` endpoint
   - Airflow retraining DAG: Schedule weekly Sunday 2am, configure email notifications
   - Monitoring setup: Create Grafana alert (P01 accuracy <85% → Slack notification)

**Error Recovery Workflows**:

**Scenario 1: Kafka Lag >10 min** (Data ingestion bottleneck):
- **Detection**: Grafana alert "Kafka consumer lag >10 min for partition 5"
- **Resolution Steps**:
  1. Check Airflow UI → locate stuck Spark Structured Streaming job
  2. View Spark job logs (Airflow task logs or Databricks job UI)
  3. Identify error (e.g., schema mismatch, OOM in executor)
  4. Fix schema issue in `spark_jobs/stdf_parser.py`, commit to Git
  5. Airflow auto-syncs DAG from Git (5 min), restart failed task
  6. Monitor Kafka lag in Grafana (should decrease to <5 min within 15 min)
- **Documentation**: Runbook at `docs/runbooks/kafka_lag_troubleshooting.md`

**Scenario 2: Model Accuracy Drop >5%** (Model drift):
- **Detection**: Weekly automated validation job logs accuracy 83% (down from 89% baseline)
- **Resolution Steps**:
  1. MLflow UI → compare recent runs (identify metrics degradation)
  2. Check Delta Lake data quality dashboard (detect distribution shift in features)
  3. Query `parametric_stats` table (find outlier spike in test_IDDQ mean +20%)
  4. Airflow data quality DAG already flagged anomaly → investigate upstream ATE calibration
  5. Retrain model with outlier filtering (`outlier_threshold = 5 sigma`), log to MLflow
  6. Stage new model → validate accuracy 88% → promote to Production
- **Notification**: Slack alert to #ml-alerts channel with model comparison link

**Self-Service Capabilities**:

- **Feature Queries**: ML Engineers can SQL Delta Lake directly (no dependency on Data Engineers)
  ```sql
  SELECT wafer_yield, edge_die_yield, parametric_mean_IDDQ
  FROM wafer_features
  WHERE device = 'TC41x' AND date BETWEEN '2024-11-01' AND '2024-11-30'
  ORDER BY wafer_yield ASC
  LIMIT 100
  ```
  
- **Model Deployment**: Stage → Production promotion via MLflow UI (no DevOps involvement for model updates)

- **Experiment Reproduction**: MLflow run ID → one-click download of exact code/data/hyperparameters

- **Data Quality Monitoring**: Airflow hourly DAG generates alerts → self-serve investigation via Databricks SQL

### 12.3 Accessibility

**WCAG 2.1 Level AA Compliance**:

- **Keyboard Navigation**: All UI elements accessible via Tab key (MLflow, Grafana, Airflow)
- **Screen Reader Support**: Semantic HTML, ARIA labels for charts/graphs
- **Color Contrast**: 4.5:1 minimum ratio (text on background), high contrast mode available
- **Text Scaling**: Support 200% zoom without horizontal scrolling
- **Alternative Text**: All charts/images have descriptive alt text

**Responsive Design**:
- Desktop (1920×1080): Full feature set (multi-panel dashboards, side-by-side comparisons)
- Tablet (1024×768): Simplified layout (single-panel dashboards, collapsible sidebars)
- Mobile (375×667): Essential features only (view alerts, query status, basic charts)

**Internationalization (Future)**:
- UI text externalized to i18n files (English v1.0, German/Chinese planned for v2.0)
- Date/time formatting: UTC displayed, local timezone conversion optional

---

## 13. Security Requirements

### 13.1 Authentication

**OAuth2 / OpenID Connect (OIDC) SSO**:

- **Identity Providers**: Azure Active Directory (primary), Okta (backup)
- **Protocol**: OAuth2 Authorization Code flow with PKCE (Proof Key for Code Exchange)
- **Token Management**:
  - JWT access tokens (1-hour expiration, RS256 signature)
  - Refresh tokens (7-day expiration, secure HttpOnly cookies)
  - Token rotation: New refresh token issued on each refresh
- **Multi-Factor Authentication (MFA)**: Enforced for all users (TOTP, SMS, or biometric)
- **Session Management**:
  - Idle timeout: 30 minutes (auto-logout)
  - Max session duration: 8 hours (force re-authentication)
  - Concurrent sessions: Limited to 3 devices per user

**Service-to-Service Authentication**:
- **Kubernetes ServiceAccounts**: Pods use K8s ServiceAccount tokens for inter-service communication
- **Databricks Personal Access Tokens (PATs)**: Rotated every 90 days, stored in Kubernetes Secrets
- **MLflow API Keys**: Generated per user/service, scoped to specific experiments

**Token Validation**:
```python
# FastAPI JWT validation middleware
from fastapi import Request, HTTPException
from jose import jwt, JWTError
import requests

# Fetch Azure AD public keys (JWKS) on startup
JWKS_URL = "https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
public_keys = requests.get(JWKS_URL).json()

async def validate_jwt(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    
    token = auth_header.split(" ")[1]
    try:
        # Validate signature, expiration, audience
        payload = jwt.decode(
            token,
            public_keys,
            algorithms=["RS256"],
            audience="api://p16-mlops-platform",
            issuer="https://login.microsoftonline.com/{tenant_id}/v2.0"
        )
        return payload
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")
```

### 13.2 Authorization

**Role-Based Access Control (RBAC)**:

| Role                  | Delta Lake          | MLflow                    | Kafka/Spark           | FastAPI Predict      | Airflow DAGs         |
|-----------------------|---------------------|---------------------------|-----------------------|----------------------|----------------------|
| **data_engineer**     | Read/Write          | Read Experiments          | Read/Write            | Read-only            | Read/Write           |
| **ml_engineer**       | Read                | Read/Write Experiments    | Read                  | Read/Write           | Read                 |
|                       |                     | Stage models              |                       |                      |                      |
| **platform_engineer** | Admin               | Admin                     | Admin                 | Admin                | Admin                |
| **test_engineer**     | Read                | Read Experiments          | No Access             | Read/Write Predict   | Read                 |
| **yield_engineer**    | Read                | Read Experiments          | No Access             | Read/Write Predict   | Read                 |

**Delta Lake Row-Level Security (RLS)**:
```sql
-- Create secure view for test_engineer role (only their assigned devices)
CREATE OR REPLACE VIEW wafer_features_secure AS
SELECT *
FROM wafer_features
WHERE device IN (
    SELECT device FROM user_device_permissions 
    WHERE user_id = current_user()
);

GRANT SELECT ON wafer_features_secure TO test_engineer;
```

**MLflow Model Registry Permissions**:
- **Stage to Staging**: ml_engineer, platform_engineer
- **Promote to Production**: platform_engineer only (requires approval workflow)
- **Archive models**: platform_engineer only
- **Delete models**: platform_engineer only (with audit trail)

**Airflow DAG Permissions**:
- **View DAGs**: All roles
- **Trigger DAGs**: data_engineer, platform_engineer
- **Edit DAGs**: platform_engineer only (via Git repository, protected main branch)
- **Delete DAG runs**: platform_engineer only

### 13.3 Data Protection

**Encryption at Rest**:
- **AWS S3 (Delta Lake, MLflow artifacts)**: SSE-S3 (AES-256 encryption, AWS-managed keys)
- **PostgreSQL (Airflow, MLflow metadata)**: Transparent Data Encryption (TDE), AES-256
- **Kubernetes Secrets**: Encrypted etcd datastore (AES-CBC with 32-byte keys)
- **Local Development**: MinIO server-side encryption (SSE-C, customer-provided keys)

**Encryption in Transit**:
- **TLS 1.2+ Required**: All HTTP traffic (FastAPI, MLflow, Airflow, Grafana)
- **Certificate Management**: Let's Encrypt auto-renewal (cert-manager in K8s)
- **Kafka Encryption**: TLS for broker-to-broker and client-to-broker communication
- **Spark Shuffle Encryption**: AES-256 encryption for shuffle data (prevent network sniffing)

**Data Masking (Synthetic Data Strategy)**:
- **Production STDF Files**: NEVER uploaded to cloud (proprietary chip data)
- **Cloud Deployment**: 200GB synthetic STDFs from P07 GAN (privacy-safe, realistic patterns)
- **Local Development**: 44GB real STDFs (on-premise laptops/workstations only)
- **PII Removal**: Synthetic STDFs have anonymized lot_id, wafer_id (TC41x_LOT_SYNTH_001, W_SYNTH_05)

**Secret Management**:
- **Kubernetes Secrets**: Store DB passwords, API keys, OAuth client secrets
- **External Secrets Operator (ESO)**: Sync secrets from AWS Secrets Manager (90-day rotation)
- **Databricks Secrets**: dbutils.secrets.get() for PATs, S3 credentials (scoped to workspace)
- **Gitignore Enforcement**: Pre-commit hook blocks commits with hardcoded secrets

**Data Retention & Deletion**:
- **Delta Lake raw_stdf**: 1-year retention (VACUUM older versions after 365 days)
- **Delta Lake wafer_features**: 3-year retention (compliance requirement)
- **MLflow experiments**: 5-year retention (model lineage for regulatory audits)
- **model_predictions**: 3-year retention (ISO 26262 traceability)
- **Deletion API** (GDPR compliance):
  ```python
  DELETE FROM model_predictions WHERE lot_id = 'TC41x_LOT_TO_DELETE';
  DELETE FROM wafer_features WHERE lot_id = 'TC41x_LOT_TO_DELETE';
  DELETE FROM raw_stdf WHERE lot_id = 'TC41x_LOT_TO_DELETE';
  ```

### 13.4 Compliance

**Regulatory Requirements**:

**1. GDPR (General Data Protection Regulation)**:
- **Data Minimization**: Synthetic STDFs in cloud eliminate PII exposure
- **Right to Deletion**: Deletion API removes all traces of lot_id (Delta Lake supports DELETE)
- **Data Portability**: Export API returns user's prediction history in JSON format
- **Audit Trail**: 3-year retention of model_predictions table (who predicted what, when)

**2. ISO 26262 (Automotive Functional Safety)**:
- **Model Lineage**: MLflow tracks training data version, code commit, hyperparameters
- **Traceability**: prediction_id → model_version → training_data_version → raw_stdf (full chain)
- **Validation Evidence**: Test set accuracy, confusion matrix, SHAP explanations stored in MLflow
- **Change Management**: Git-based DAG changes, pull request reviews, CI/CD pipeline

**3. IATF 16949 (Automotive Quality Management)**:
- **Statistical Process Control (SPC)**: parametric_stats table tracks test means/stds (detect drift)
- **Corrective Action**: Airflow data quality DAG alerts on anomalies → investigation workflow
- **Continuous Improvement**: Monthly model retraining, accuracy trend monitoring in Grafana

**4. CCPA (California Consumer Privacy Act)**:
- **Data Disclosure**: Privacy policy explains synthetic STDF usage, ML model purposes
- **Opt-Out**: Users can request prediction deletion via deletion API

**Compliance Dashboards**:
- **Audit Log Viewer** (Grafana dashboard):
  - Filter by user, action (predict, delete, export), date range
  - Export audit logs as CSV (3-year retention)
- **Data Lineage Graph** (MLflow UI):
  - Visual graph: STDF file → features → model training → prediction
  - Click any node → view full metadata (timestamps, versions, users)

**Security Scanning**:
- **Dependency Scanning**: Snyk scans Python packages weekly (alert on CVE with CVSS >7.0)
- **Container Image Scanning**: Trivy scans Docker images on push (block deployment if critical vulnerabilities)
- **Secret Scanning**: GitGuardian monitors Git commits (prevent accidental credential leaks)
- **SAST (Static Application Security Testing)**: Bandit scans Python code for security issues

---

## 14. Performance Requirements

### 14.1 Response Times

**Ingestion Pipeline** (Kafka → Spark → Delta Lake):
- **Kafka Ingestion**: <1 min p95 (STDF file detected on network share → published to Kafka topic)
- **Spark Parsing**: <3 min p95 (10GB STDF parsed by 8 executors → written to Delta Lake)
- **Delta Lake Write**: <1 min p95 (ACID transaction commit + metadata update)
- **End-to-End Ingestion**: <5 min p95 (STDF file → queryable in Delta Lake)

**Query Performance** (Delta Lake reads):
- **Point Query**: <500ms p95 (SELECT * FROM wafer_features WHERE wafer_id = 'W05')
- **Aggregation Query**: <10 sec p95 (SELECT AVG(wafer_yield) FROM wafer_features WHERE device = 'TC41x' AND date >= '2024-01-01', 1M+ rows)
- **Join Query**: <30 sec p95 (JOIN wafer_features + spatial_patterns + parametric_stats, 100K wafers)
- **Time-Travel Query**: <15 sec p95 (SELECT * FROM wafer_features VERSION AS OF 123, reconstruct historical data)

**Model Serving Latency** (FastAPI predictions):
- **P01 XGBoost**: <20ms p95 (simple tree ensemble, 10 features)
- **P02 ResNet**: <80ms p95 (CNN inference on CPU, 50 features)
- **P04 U-Net**: <100ms p95 (image segmentation, 512×512 wafer map)
- **P10 GNN**: <150ms p95 (graph neural network, 100-node test dependency graph)
- **Overall Target**: <100ms p95 (weighted average across all P01-P15 models)

**Batch Prediction**:
- **100 predictions**: <1 sec (amortized model loading, vectorized inference)
- **1,000 predictions**: <5 sec (parallelized across 4 Uvicorn workers)
- **10,000 predictions**: <30 sec (Kubernetes HPA scales to 10 pods)

**MLflow Operations**:
- **Log Experiment**: <2 sec (write hyperparameters, metrics, artifacts to PostgreSQL + S3)
- **Model Download**: <10 sec (retrieve 50MB model.pkl from S3, decompress)
- **Registry Update**: <1 sec (transition model Staging → Production, update metadata)

**Airflow DAG Execution**:
- **Retraining DAG**: <60 min p95 (query Delta Lake 5 min + train model 30 min + validate 15 min + deploy 10 min)
- **Feature Engineering DAG**: <15 min p95 (Spark batch job, process 1 day of raw_stdf)
- **Data Quality DAG**: <5 min p95 (schema validation + statistical checks)

### 14.2 Throughput

**Data Ingestion**:
- **Sustained Load**: 1,000 STDFs/day = 40-50 files/hour (550MB/day)
- **Burst Load**: 2,000 STDFs/day (2× normal, ATE running 24/7 during ramp)
- **Kafka Topic Throughput**: 12 partitions × 100 msgs/sec/partition = 1,200 msgs/sec (headroom for spikes)

**Prediction API**:
- **Daily Predictions**: 10,000 predictions/day (416 predictions/hour, 7 predictions/min)
- **Peak Load**: 500 predictions/min (during shift change, multiple test engineers querying)
- **Concurrent Requests**: 50 concurrent users (Kubernetes HPA scales to 10 FastAPI pods)

**Feature Store Queries**:
- **Concurrent SQL Queries**: 20 simultaneous Databricks SQL queries (10 ML engineers + 10 yield engineers)
- **Query Queue Depth**: <5 queued queries (Databricks cluster auto-scales to 8 workers if queue >5)

**MLflow Tracking**:
- **Concurrent Experiments**: 30 ML engineers × 5 experiments/week = 150 experiments/week = 20-25 experiments/day
- **Artifact Storage Rate**: 100 GB/month (model files, plots, feature importance)

### 14.3 Resource Usage

**Kafka Cluster** (3 brokers):
- **CPU**: 2 cores/broker = 6 cores total (50-60% utilization at sustained load)
- **Memory**: 8 GB/broker = 24 GB total
- **Disk**: 500 GB/broker = 1.5 TB total (7-day retention × 550 MB/day × replication factor 3)

**Spark Cluster** (Databricks Runtime 14.3 LTS):
- **Driver**: 1 node, 14 GB RAM, 4 cores (orchestration, no heavy computation)
- **Workers**: 2-8 nodes (auto-scaling), 14 GB RAM, 4 cores each
- **Typical Load**: 4 workers (56 GB RAM, 16 cores total, 70% utilization during hourly data quality jobs)
- **Peak Load**: 8 workers (112 GB RAM, 32 cores, 90% utilization during weekly retraining)

**Delta Lake Storage** (AWS S3):
- **Initial**: 200 GB (synthetic STDFs from P07)
- **Growth**: +550 MB/day × 365 days = +200 GB/year
- **Year 1**: 200 GB (existing) + 200 GB (new) = 400 GB
- **Year 3**: 800 GB (projected, with VACUUM and partitioning)

**PostgreSQL** (Airflow + MLflow metadata):
- **Instance**: AWS RDS db.t3.medium (2 vCPUs, 4 GB RAM)
- **Storage**: 100 GB (Airflow DAG runs, MLflow experiments metadata)
- **IOPS**: 3,000 provisioned IOPS (handle 100+ transactions/sec)

**FastAPI Serving** (Kubernetes Deployment):
- **Min Replicas**: 2 pods (high availability)
- **Max Replicas**: 20 pods (HPA scaling based on CPU >70% or request rate >500/min)
- **Per Pod**: 2 cores, 4 GB RAM (load 15 models × 50 MB each = 750 MB, headroom for inference)
- **Typical Load**: 3-5 pods (50% CPU, 2 GB RAM, 100-200 predictions/min)

**Total Cloud Cost** (AWS + Databricks):
- **Databricks**: $50/month (14 GB cluster, 40 hrs/month, Community Edition Year 1 then paid)
- **AWS S3**: $10-15/month (400 GB Standard tier, data transfer)
- **EKS**: $72/month (control plane) + $30-45/month (2-3 t3.medium nodes, spot instances)
- **RDS**: $20/month (db.t3.micro PostgreSQL)
- **Data Transfer**: $5-10/month (inter-AZ, CloudFront)
- **Total**: **$95-165/month** (optimize with auto-shutdown, spot instances to <$100)

**Performance Optimization Strategies**:
- **Delta Lake Z-ordering**: 30-50% speedup on filtered queries (`ZORDER BY device, lot_id`)
- **Spark Adaptive Query Execution (AQE)**: 20-40% speedup on complex joins
- **FastAPI ONNX/TensorRT**: 2-5× speedup on P02 ResNet inference (CPU → GPU optional)
- **Kubernetes HPA**: Auto-scale 2-20 pods based on load (handle 10× traffic spikes)
- **Databricks Cluster Auto-termination**: Shut down after 2 hours idle (save $20-30/month)

---

## 15. Scalability Requirements

### 15.1 Horizontal Scaling

**Kafka Cluster Scaling**:
- **Current**: 3 brokers, 12 partitions per topic (stdf_ingestion)
- **Scale Trigger**: Consumer lag >10 min sustained for 2+ hours
- **Scaling Action**: Add 3 brokers (6 total), increase partitions 12 → 24 (rebalance existing data)
- **Capacity**: 24 partitions × 100 msgs/sec = 2,400 msgs/sec (2× current throughput)
- **Timeline**: 2-hour maintenance window (add brokers, partition rebalance)

**Spark Workers (Databricks Auto-Scaling)**:
- **Current**: 2-8 workers (14 GB RAM, 4 cores each, auto-scaling based on queue depth)
- **Scale Trigger**: 
  - Queue depth >10 pending jobs for 5+ min → scale up
  - CPU utilization >80% sustained for 10+ min → scale up
  - Queue empty for 15+ min → scale down
- **Scaling Action**: Add/remove workers in increments of 2 (max 16 workers = 224 GB RAM, 64 cores)
- **Capacity**: 16 workers can process 40 GB STDF in <5 min (4× current capacity)
- **Cost**: Auto-termination after 2 hours idle (minimize cost during off-hours)

**FastAPI Replicas (Kubernetes HPA)**:
- **Current**: 2-20 replicas (HPA based on CPU >70% or request rate >500/min)
- **Scale Trigger**: 
  - CPU >70% average across all pods for 2 min → scale up
  - Request rate >500/min → add 2 replicas
  - CPU <30% for 5 min → scale down
- **Scaling Action**: Add/remove replicas (max 50 replicas for extreme events, e.g., lot release rush)
- **Capacity**: 50 replicas × 100 predictions/min = 5,000 predictions/min (50× current capacity)
- **Load Balancing**: NGINX Ingress round-robin (sticky sessions for stateful requests)

**Airflow Celery Workers**:
- **Current**: 5 Celery workers (handle 10 concurrent tasks)
- **Scale Trigger**: Queued tasks >20 for 10+ min
- **Scaling Action**: Add workers in increments of 5 (max 20 workers = 40 concurrent tasks)
- **Capacity**: 20 workers × 2 tasks/worker = 40 concurrent DAG tasks (4× current)

**PostgreSQL Read Replicas**:
- **Current**: 1 primary (read/write), 2 replicas (read-only, sync replication)
- **Scale Trigger**: Read query latency >1 sec p95 for 10+ min
- **Scaling Action**: Add read replica (max 5 replicas, route read queries via pgpool)
- **Capacity**: 5 replicas = 5× read throughput (write throughput unchanged, limited by primary)

### 15.2 Vertical Scaling

**Databricks Cluster Node Upgrade**:
- **Current**: 14 GB RAM, 4 cores (Standard_D4ds_v5 Azure, or i3.xlarge AWS)
- **Scale Trigger**: Memory OOM errors >5 per day OR CPU throttling >20% of time
- **Upgrade Path**: 
  - Small → Medium: 14 GB → 28 GB RAM, 4 → 8 cores (Standard_D8ds_v5, i3.2xlarge)
  - Medium → Large: 28 GB → 56 GB RAM, 8 → 16 cores (Standard_D16ds_v5, i3.4xlarge)
- **Use Case**: Large STDF files >20 GB, complex feature engineering with 500+ joins

**PostgreSQL Instance Upgrade**:
- **Current**: db.t3.medium (2 vCPUs, 4 GB RAM, 3,000 IOPS)
- **Scale Trigger**: 
  - CPU >80% sustained for 1 hour
  - Disk IOPS >80% of provisioned (2,400+ IOPS)
  - Connection count >80 (max 100)
- **Upgrade Path**:
  - Small → Medium: db.t3.medium → db.r5.large (2 vCPUs, 16 GB RAM, more memory for caching)
  - Medium → Large: db.r5.large → db.r5.xlarge (4 vCPUs, 32 GB RAM, 12,000 IOPS)
- **Cost**: +$50-100/month per tier upgrade

**FastAPI Pod Resources**:
- **Current**: 2 cores, 4 GB RAM per pod (handles 15 models × 50 MB = 750 MB)
- **Scale Trigger**: Memory usage >85% (OOM risk) OR model loading time >30 sec
- **Upgrade Path**: 2 cores → 4 cores, 4 GB → 8 GB RAM (accommodate larger models, e.g., P02 ResNet 500 MB)
- **Use Case**: Deploy P02 with larger ResNet-50 (100M params, 500 MB) instead of ResNet-18 (11M params, 50 MB)

### 15.3 Load Handling

**Traffic Patterns**:

**Daily Pattern**:
```
00:00-06:00: Low (10 predictions/hour, 1 STDF/hour) - Night shift
06:00-08:00: Spike (100 predictions/hour) - Morning lot release review
08:00-12:00: Medium (50 predictions/hour, 30 STDFs/hour) - Day shift
12:00-14:00: Low (20 predictions/hour) - Lunch break
14:00-18:00: High (80 predictions/hour, 50 STDFs/hour) - Afternoon shift
18:00-00:00: Medium (40 predictions/hour, 20 STDFs/hour) - Evening shift
```

**Weekly Pattern**:
```
Monday: High (80 predictions/hour, 60 STDFs/hour) - Week start, backlog processing
Tuesday-Thursday: Medium (50 predictions/hour, 40 STDFs/hour) - Normal production
Friday: Spike (120 predictions/hour, 80 STDFs/hour) - Week-end lot analysis
Saturday-Sunday: Low (20 predictions/hour, 10 STDFs/hour) - Weekend skeleton crew
```

**Seasonal Pattern**:
```
Q1 (Jan-Mar): Medium (1,000 STDFs/day) - Normal production
Q2 (Apr-Jun): High (1,500 STDFs/day) - Product ramp for automotive season
Q3 (Jul-Sep): Low (700 STDFs/day) - Summer slowdown, maintenance
Q4 (Oct-Dec): Peak (2,000 STDFs/day) - Year-end push, new product validation
```

**Load Shedding & Circuit Breaker**:

**Circuit Breaker Pattern** (FastAPI):
```python
from circuitbreaker import circuit

@circuit(failure_threshold=5, recovery_timeout=60)
async def predict_with_mlflow_model(model_name: str, features: List[float]):
    """Load model from MLflow with circuit breaker"""
    try:
        model = mlflow.pyfunc.load_model(f"models:/{model_name}/Production")
        prediction = model.predict([features])[0]
        return prediction
    except Exception as e:
        # Circuit opens after 5 consecutive failures
        # Recovery attempted after 60 sec
        logger.error(f"MLflow model loading failed: {e}")
        raise

@app.post("/predict/{model_name}")
async def predict(model_name: str, request: PredictionRequest):
    try:
        prediction = await predict_with_mlflow_model(model_name, request.features)
        return {"prediction": prediction, ...}
    except CircuitBreakerError:
        # Graceful degradation: return cached prediction or fallback model
        return {
            "prediction": get_cached_prediction(request.features),
            "confidence": 0.5,
            "status": "degraded_mode",
            "message": "MLflow unavailable, using cached model"
        }
```

**Rate Limiting** (FastAPI):
```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.post("/predict/{model_name}")
@limiter.limit("100/minute")  # Max 100 predictions per user per minute
async def predict(request: Request, model_name: str, data: PredictionRequest):
    # Prediction logic...
    pass
```

**Queue-Based Load Management** (Airflow):
- **Priority Queues**: 
  - High priority: Weekly retraining DAGs (critical for model freshness)
  - Medium priority: Daily feature engineering DAGs
  - Low priority: Adhoc analysis DAGs (yield engineer experiments)
- **Queue Depth Monitoring**: Alert if queue >50 tasks for 30+ min (indicates capacity issue)
- **Task Timeout**: 2-hour max per task (prevent runaway Spark jobs)

**Data Growth Planning** (3-Year Projection):

| Metric                   | Year 1 (2025) | Year 2 (2026) | Year 3 (2027) |
|--------------------------|---------------|---------------|---------------|
| **Daily STDF Volume**    | 1,000 files   | 1,500 files   | 2,000 files   |
| **Daily Data Ingestion** | 550 MB        | 825 MB        | 1.1 GB        |
| **Delta Lake raw_stdf**  | 200 GB        | 500 GB        | 800 GB        |
| **Delta Lake Features**  | 50 GB         | 125 GB        | 200 GB        |
| **MLflow Artifacts**     | 100 GB        | 250 GB        | 400 GB        |
| **Total Storage**        | 400 GB        | 1 TB          | 1.5 TB        |
| **Kafka Partitions**     | 12            | 24            | 36            |
| **Spark Workers (avg)**  | 4             | 6             | 8             |
| **FastAPI Replicas (avg)** | 3-5         | 5-10          | 8-15          |
| **Monthly Cost**         | <$100         | $150-200      | $250-300      |

**Capacity Planning Triggers**:
- **Storage**: Migrate to S3 Glacier after 90 days (reduce cost 70%), yearly VACUUM (reclaim space)
- **Compute**: If Spark job duration >2× SLA for 3+ consecutive days → add workers permanently
- **Network**: If data transfer >500 GB/month → enable S3 Transfer Acceleration (+$0.04/GB)

---

## 16. Testing Strategy

### 16.1 Unit Testing

**Python Unit Tests** (pytest):

**Coverage Target**: >80% code coverage (pytest-cov)

**Test Structure**:
```
tests/
├── unit/
│   ├── test_stdf_parser.py          # STDF parsing logic
│   ├── test_feature_engineering.py  # Feature computation
│   ├── test_data_quality.py         # Schema validation, outlier detection
│   ├── test_model_inference.py      # Mock model predictions
│   ├── test_api_endpoints.py        # FastAPI request/response validation
│   └── test_mlflow_integration.py   # MLflow logging, model registry
├── integration/
├── load/
└── conftest.py                      # Pytest fixtures
```

**Example Unit Test** (STDF Parser):
```python
# tests/unit/test_stdf_parser.py
import pytest
from pyspark.sql import SparkSession
from src.ingestion.stdf_parser import parse_stdf_to_dataframe

@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("unittest") \
        .getOrCreate()

def test_parse_stdf_valid_file(spark):
    """Test parsing valid STDF file"""
    stdf_path = "tests/data/sample_valid.std"
    
    df = parse_stdf_to_dataframe(spark, stdf_path)
    
    assert df.count() == 1500  # Expected 1500 die
    assert "lot_id" in df.columns
    assert "wafer_id" in df.columns
    assert "bin" in df.columns
    assert df.filter("bin IS NULL").count() == 0  # No null bins

def test_parse_stdf_malformed_file(spark):
    """Test error handling for malformed STDF"""
    stdf_path = "tests/data/sample_malformed.std"
    
    with pytest.raises(ValueError, match="Invalid STDF header"):
        parse_stdf_to_dataframe(spark, stdf_path)

def test_feature_engineering_wafer_yield():
    """Test wafer yield calculation"""
    from src.analysis.statistics import compute_wafer_yield
    
    total_die = 1500
    passed_die = 1350
    
    yield_pct = compute_wafer_yield(total_die, passed_die)
    
    assert yield_pct == 0.90  # 1350/1500 = 0.90
    assert 0.0 <= yield_pct <= 1.0  # Valid range
```

**Mock Model Testing**:
```python
# tests/unit/test_model_inference.py
import pytest
import numpy as np
from src.ml.models.p01_mock import P01MockModel

def test_p01_mock_prediction():
    """Test P01 XGBoost mock model inference"""
    model = P01MockModel()
    
    # Sample wafer features (10 features)
    features = np.array([[0.90, 0.85, 1.23e-6, 0.65, 2.5e-7, 0.88, 0.92, 500, 450, 50]])
    
    prediction = model.predict(features)
    
    assert prediction.shape == (1,)  # Single prediction
    assert 1 <= prediction[0] <= 99  # Valid bin range
    assert isinstance(prediction[0], (int, np.integer))

def test_p01_batch_prediction():
    """Test batch prediction (100 wafers)"""
    model = P01MockModel()
    
    features = np.random.rand(100, 10)  # 100 wafers, 10 features each
    
    predictions = model.predict(features)
    
    assert predictions.shape == (100,)
    assert all(1 <= p <= 99 for p in predictions)
```

**FastAPI Unit Tests**:
```python
# tests/unit/test_api_endpoints.py
import pytest
from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_health_endpoint():
    """Test /health endpoint"""
    response = client.get("/health")
    
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert "models_loaded" in response.json()

def test_predict_endpoint_valid_request():
    """Test /predict/p01_xgboost with valid request"""
    request_data = {
        "features": [0.90, 0.85, 1.23e-6, 0.65, 2.5e-7, 0.88, 0.92, 500, 450, 50],
        "metadata": {"lot_id": "TC41x_LOT123"}
    }
    
    response = client.post("/predict/p01_xgboost", json=request_data)
    
    assert response.status_code == 200
    assert "prediction" in response.json()
    assert "confidence" in response.json()
    assert 0.0 <= response.json()["confidence"] <= 1.0

def test_predict_endpoint_invalid_features():
    """Test /predict with invalid feature count"""
    request_data = {
        "features": [0.90, 0.85]  # Only 2 features, expected 10
    }
    
    response = client.post("/predict/p01_xgboost", json=request_data)
    
    assert response.status_code == 400
    assert "Invalid feature vector length" in response.json()["detail"]
```

**CI/CD Unit Test Execution** (GitHub Actions):
```yaml
# .github/workflows/unit_tests.yml
name: Unit Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python 3.11
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-cov
    
    - name: Run unit tests
      run: |
        pytest tests/unit/ \
          --cov=src \
          --cov-report=xml \
          --cov-report=html \
          --cov-fail-under=80
    
    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        files: ./coverage.xml
```

### 16.2 Integration Testing

**Integration Test Scenarios**:

**Scenario 1: End-to-End Ingestion Pipeline** (Kafka → Spark → Delta Lake):
```python
# tests/integration/test_ingestion_pipeline.py
import pytest
import time
from kafka import KafkaProducer
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def kafka_producer():
    """Kafka producer for test STDF files"""
    return KafkaProducer(bootstrap_servers=['localhost:9092'])

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .getOrCreate()

def test_end_to_end_ingestion(kafka_producer, spark_session):
    """Test STDF file → Kafka → Spark → Delta Lake"""
    
    # Step 1: Publish STDF to Kafka
    with open("tests/data/sample.std", "rb") as f:
        stdf_bytes = f.read()
    
    kafka_producer.send("stdf_ingestion", value=stdf_bytes)
    kafka_producer.flush()
    
    # Step 2: Wait for Spark Structured Streaming to process (max 30 sec)
    time.sleep(30)
    
    # Step 3: Query Delta Lake to verify ingestion
    df = spark_session.read.format("delta").load("s3://test-bucket/delta/raw_stdf")
    
    assert df.count() > 0  # Data ingested
    assert df.filter("lot_id = 'TC41x_LOT123'").count() == 1500  # Expected die count
    
    # Step 4: Verify data quality
    assert df.filter("bin IS NULL").count() == 0  # No null bins
    assert df.filter("wafer_yield < 0 OR wafer_yield > 1").count() == 0  # Valid yield
```

**Scenario 2: MLflow Model Training & Registry**:
```python
# tests/integration/test_mlflow_workflow.py
import pytest
import mlflow
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

def test_mlflow_training_and_registry():
    """Test model training, logging, and registration in MLflow"""
    
    # Step 1: Start MLflow run
    with mlflow.start_run():
        # Train mock P01 model
        X_train = [[0.9, 0.8], [0.85, 0.75], [0.92, 0.88]]
        y_train = [1, 2, 1]
        
        model = RandomForestClassifier(n_estimators=100, max_depth=10)
        model.fit(X_train, y_train)
        
        # Log hyperparameters
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("max_depth", 10)
        
        # Log metrics
        y_pred = model.predict(X_train)
        accuracy = accuracy_score(y_train, y_pred)
        mlflow.log_metric("accuracy", accuracy)
        
        # Log model
        mlflow.sklearn.log_model(model, "model")
        
        run_id = mlflow.active_run().info.run_id
    
    # Step 2: Register model
    model_uri = f"runs:/{run_id}/model"
    mlflow.register_model(model_uri, "p01_xgboost_test")
    
    # Step 3: Transition to Production
    client = mlflow.MlflowClient()
    client.transition_model_version_stage(
        name="p01_xgboost_test",
        version=1,
        stage="Production"
    )
    
    # Step 4: Load model from registry
    loaded_model = mlflow.pyfunc.load_model("models:/p01_xgboost_test/Production")
    
    # Step 5: Verify prediction
    prediction = loaded_model.predict([[0.9, 0.8]])
    assert prediction.shape == (1,)
```

**Scenario 3: Airflow DAG Execution**:
```python
# tests/integration/test_airflow_dags.py
import pytest
from airflow.models import DagBag
from datetime import datetime

def test_airflow_retraining_dag():
    """Test retraining DAG execution"""
    
    # Load DAG
    dagbag = DagBag(dag_folder="dags/")
    dag = dagbag.get_dag("p01_weekly_retraining")
    
    assert dag is not None
    assert len(dag.tasks) == 4  # query_data, train, validate, promote
    
    # Test DAG structure
    assert "query_training_data" in dag.task_ids
    assert "train_model" in dag.task_ids
    assert "promote_if_better" in dag.task_ids
    
    # Test task dependencies
    train_task = dag.get_task("train_model")
    assert "query_training_data" in [t.task_id for t in train_task.upstream_list]
```

**Docker Compose Integration Tests** (Local Environment):
```bash
# tests/integration/docker_integration_test.sh
#!/bin/bash

# Start all services
docker-compose up -d

# Wait for services to be healthy
echo "Waiting for services to start..."
sleep 60

# Test Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep stdf_ingestion

# Test Spark
docker exec spark-master /opt/spark/bin/spark-submit --version

# Test MLflow
curl http://localhost:5000/api/2.0/mlflow/experiments/list

# Test FastAPI
curl http://localhost:8000/health

# Test Airflow
curl http://localhost:8080/health

# Cleanup
docker-compose down
```

### 16.3 Performance Testing

**Load Testing** (Locust):

```python
# tests/load/locustfile.py
from locust import HttpUser, task, between

class FastAPIPredictionUser(HttpUser):
    wait_time = between(1, 3)  # 1-3 sec between requests
    
    @task(3)  # 3× weight (75% of traffic)
    def predict_p01(self):
        """Test P01 XGBoost prediction"""
        self.client.post("/predict/p01_xgboost", json={
            "features": [0.90, 0.85, 1.23e-6, 0.65, 2.5e-7, 0.88, 0.92, 500, 450, 50]
        })
    
    @task(1)  # 1× weight (25% of traffic)
    def predict_p02(self):
        """Test P02 ResNet prediction"""
        self.client.post("/predict/p02_resnet", json={
            "features": [0.88] * 50  # 50 features
        })
    
    @task(1)
    def health_check(self):
        """Test health endpoint"""
        self.client.get("/health")
```

**Run Load Test**:
```bash
# Test with 100 concurrent users, ramp up over 60 sec
locust -f tests/load/locustfile.py \
  --host=http://localhost:8000 \
  --users=100 \
  --spawn-rate=10 \
  --run-time=10m \
  --headless \
  --csv=load_test_results
```

**Performance Test Assertions**:
- **p95 Latency**: <100ms for /predict/p01_xgboost (target <20ms)
- **Throughput**: >100 requests/sec with 10 FastAPI replicas
- **Error Rate**: <1% (HTTP 5xx errors)
- **Resource Usage**: CPU <80%, Memory <85% (headroom for spikes)

**Spark Performance Testing** (PySpark Test Suite):
```python
# tests/load/test_spark_performance.py
import pytest
import time
from pyspark.sql import SparkSession

def test_stdf_parsing_10gb():
    """Test parsing 10GB STDF file (target <5 min)"""
    spark = SparkSession.builder.master("local[8]").getOrCreate()
    
    start_time = time.time()
    
    # Parse 10GB STDF (or 20× 500MB files)
    df = spark.read.format("stdf").load("tests/data/large_stdf/")
    
    # Force execution
    row_count = df.count()
    
    elapsed_time = time.time() - start_time
    
    assert elapsed_time < 300  # <5 minutes (300 sec)
    assert row_count > 1_000_000  # >1M die expected
```

### 16.4 Security Testing

**Vulnerability Scanning**:

**Dependency Scanning** (Snyk):
```bash
# Scan Python dependencies for CVEs
snyk test --file=requirements.txt --severity-threshold=high

# Scan Docker images
snyk container test p16-fastapi:latest --severity-threshold=critical
```

**SAST (Static Application Security Testing)** (Bandit):
```bash
# Scan Python code for security issues
bandit -r src/ -f json -o bandit_report.json

# Common issues detected:
# - Hardcoded secrets (B105)
# - SQL injection risks (B608)
# - Insecure cryptography (B501)
```

**Penetration Testing Checklist**:
- [ ] SQL Injection: Test Delta Lake queries with malicious input (`'; DROP TABLE --`)
- [ ] JWT Token Tampering: Modify token signature, verify rejection (401 Unauthorized)
- [ ] RBAC Bypass: Test engineer attempts to promote model to Production (403 Forbidden)
- [ ] Rate Limiting: Send 1,000 requests/min, verify 429 Too Many Requests after 100
- [ ] CORS: Cross-origin request from unauthorized domain, verify CORS rejection

**Security Test Automation** (pytest):
```python
# tests/security/test_authentication.py
import pytest
from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_missing_auth_token():
    """Test request without Authorization header"""
    response = client.post("/predict/p01_xgboost", json={"features": [0.9]*10})
    
    assert response.status_code == 401
    assert "Missing or invalid Authorization header" in response.json()["detail"]

def test_expired_jwt_token():
    """Test request with expired JWT token"""
    expired_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."  # Expired 1 hour ago
    
    response = client.post(
        "/predict/p01_xgboost",
        json={"features": [0.9]*10},
        headers={"Authorization": f"Bearer {expired_token}"}
    )
    
    assert response.status_code == 401
    assert "token expired" in response.json()["detail"].lower()

def test_rbac_test_engineer_cannot_promote_model():
    """Test RBAC: test_engineer cannot promote model to Production"""
    test_engineer_token = get_test_engineer_jwt()  # Role: test_engineer
    
    response = client.post(
        "/mlflow/models/p01_xgboost/promote",
        headers={"Authorization": f"Bearer {test_engineer_token}"}
    )
    
    assert response.status_code == 403
    assert "Insufficient permissions" in response.json()["detail"]
```

---

## 17. Deployment Strategy

### 17.1 Deployment Pipeline

**CI/CD Pipeline** (GitHub Actions):

```yaml
# .github/workflows/deploy.yml
name: Deploy P16 MLOps Platform

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python 3.11
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: pip install -r requirements.txt pytest pytest-cov
    
    - name: Run unit tests
      run: pytest tests/unit/ --cov=src --cov-fail-under=80
    
    - name: Run security scan (Bandit)
      run: bandit -r src/ -f json -o bandit_report.json
    
    - name: Run dependency scan (Snyk)
      run: snyk test --severity-threshold=high
      env:
        SNYK_TOKEN: ${{ secrets.SNYK_TOKEN }}
  
  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Build Docker images
      run: |
        docker build -t p16-fastapi:${{ github.sha }} -f Dockerfile.fastapi .
        docker build -t p16-spark:${{ github.sha }} -f Dockerfile.spark .
    
    - name: Scan Docker images (Trivy)
      run: |
        trivy image --severity CRITICAL,HIGH p16-fastapi:${{ github.sha }}
        trivy image --severity CRITICAL,HIGH p16-spark:${{ github.sha }}
    
    - name: Push to Docker Hub
      run: |
        echo ${{ secrets.DOCKER_PASSWORD }} | docker login -u ${{ secrets.DOCKER_USERNAME }} --password-stdin
        docker tag p16-fastapi:${{ github.sha }} company/p16-fastapi:latest
        docker push company/p16-fastapi:latest
        docker push company/p16-fastapi:${{ github.sha }}
  
  deploy-staging:
    needs: build
    runs-on: ubuntu-latest
    environment: staging
    steps:
    - name: Deploy to Kubernetes (Staging)
      run: |
        kubectl config use-context staging-cluster
        kubectl set image deployment/fastapi fastapi=company/p16-fastapi:${{ github.sha }}
        kubectl rollout status deployment/fastapi
    
    - name: Run smoke tests
      run: |
        curl https://staging.p16.company.com/health
        pytest tests/integration/smoke_tests.py --env=staging
  
  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production
    if: github.ref == 'refs/heads/main'
    steps:
    - name: Deploy to Kubernetes (Production)
      run: |
        kubectl config use-context prod-cluster
        kubectl set image deployment/fastapi fastapi=company/p16-fastapi:${{ github.sha }}
        kubectl rollout status deployment/fastapi --timeout=10m
    
    - name: Verify deployment
      run: |
        curl https://api.p16.company.com/health
        pytest tests/integration/smoke_tests.py --env=production
    
    - name: Notify Slack
      run: |
        curl -X POST -H 'Content-type: application/json' \
          --data '{"text":"P16 deployed to production: ${{ github.sha }}"}' \
          ${{ secrets.SLACK_WEBHOOK_URL }}
```

### 17.2 Environments

**Development Environment** (Local Docker Compose):
- **Purpose**: Fast iteration, debugging, unit testing
- **Services**: All 6 layers (Kafka, Spark, Delta Lake/MinIO, MLflow, Airflow, FastAPI)
- **Data**: 44GB real STDFs (423 files) OR replay 423 synthetic from P07
- **Cost**: $0 (all open-source, local execution)
- **Setup**: `docker-compose up -d` (5-min startup)
- **Access**:
  - MLflow: http://localhost:5000
  - Airflow: http://localhost:8080 (admin/admin)
  - FastAPI: http://localhost:8000/docs (Swagger UI)
  - Grafana: http://localhost:3000 (admin/admin)

**Staging Environment** (AWS EKS + Databricks Community):
- **Purpose**: Pre-production validation, integration testing, performance testing
- **Services**: Same as production (EKS cluster, Databricks workspace, RDS PostgreSQL)
- **Data**: 10GB synthetic STDFs (subset of 200GB production dataset)
- **Cost**: $50-80/month (smaller EKS nodes, Databricks Community free)
- **Differences from Production**:
  - Single replica for FastAPI (vs. 2-20 in prod)
  - 2 Spark workers max (vs. 8 in prod)
  - 7-day data retention (vs. 1-year in prod)
- **Access**:
  - https://staging.p16.company.com (OAuth2 SSO)
  - Databricks: https://community.cloud.databricks.com/staging-workspace

**Production Environment** (AWS EKS + Databricks Paid):
- **Purpose**: Serve P01-P15 models to 200+ test/yield engineers
- **Services**: Full 6-layer architecture (HA, auto-scaling, monitoring)
- **Data**: 200GB synthetic STDFs (Year 1), growing to 800GB (Year 3)
- **Cost**: <$100/month (Year 1 with Databricks Community), $150-200/month (Year 2-3)
- **SLAs**:
  - Uptime: 99.9% (max 8 hours downtime/year)
  - Latency: <100ms p95 for predictions
  - Ingestion: <5 min p95 for STDF → Delta Lake
- **Access**:
  - https://api.p16.company.com (OAuth2 SSO, rate limiting)
  - Databricks: https://company.cloud.databricks.com/prod-workspace

### 17.3 Rollout Plan

**Phase 1: Infrastructure Setup** (Weeks 1-2):
- **Week 1**:
  - Day 1-2: Provision AWS resources (EKS cluster, RDS PostgreSQL, S3 buckets)
  - Day 3-4: Deploy Kafka cluster (3 brokers, Zookeeper ensemble, Kafdrop UI)
  - Day 5: Deploy Databricks workspace, create Unity Catalog (Delta Lake schema)
- **Week 2**:
  - Day 1-2: Deploy Airflow (scheduler, webserver, Celery workers, Redis broker)
  - Day 3-4: Deploy MLflow tracking server (PostgreSQL backend, S3 artifacts)
  - Day 5: Configure monitoring (Prometheus, Grafana dashboards, OpenSearch logs)

**Phase 2: Data Ingestion & Storage** (Weeks 3-4):
- **Week 3**:
  - Implement Kafka Connect FileStreamSource (watch ATE network share)
  - Develop Spark Structured Streaming job (STDF parser → Delta Lake)
  - Test with 423 synthetic STDFs (verify end-to-end ingestion <5 min)
- **Week 4**:
  - Create Delta Lake tables (raw_stdf, wafer_features, parametric_stats, spatial_patterns)
  - Implement data quality validation (schema checks, outlier detection)
  - Airflow DAG: Data quality monitoring (hourly runs)

**Phase 3: Feature Engineering** (Weeks 5-6):
- **Week 5**:
  - Develop wafer-level aggregation Spark job (yield, edge_die_yield, parametric stats)
  - Develop spatial pattern extraction (wafer map heatmap generation, ResNet-18 embeddings)
  - Test with 10GB STDF dataset (verify <5 min processing)
- **Week 6**:
  - Airflow DAG: Daily feature engineering (1am scheduled run)
  - Optimize with Z-ordering (`ZORDER BY device, lot_id`), partition pruning
  - Verify query performance (<10 sec for 1M+ rows)

**Phase 4: Model Training & Tracking** (Weeks 7-9):
- **Week 7**:
  - Train mock models (P01 RandomForest, P02 LinearRegression, P04 dummy CNN)
  - MLflow auto-logging integration (sklearn, PyTorch)
  - Model registry setup (Production, Staging, Archived tags)
- **Week 8**:
  - Airflow retraining DAG (Sunday 2am, query Delta Lake → train → validate → promote)
  - Implement auto-promotion logic (if new_accuracy > baseline_accuracy + 3%, promote to Production)
  - Test weekly retraining cycle (mock 4-week simulation)
- **Week 9**:
  - Model lineage documentation (training_data_version, git_commit, hyperparameters)
  - A/B testing setup (90/10 Champion/Challenger traffic split)
  - Integrate real P01-P15 models (swap mock models with production-trained versions)

**Phase 5: Model Serving** (Weeks 10-11):
- **Week 10**:
  - Deploy FastAPI application (load 15 models from MLflow registry on startup)
  - Implement 20+ endpoints (/predict/p01_xgboost, /predict/p02_resnet, ..., /batch_predict)
  - OAuth2/OIDC integration (Azure AD SSO, JWT validation)
- **Week 11**:
  - Kubernetes HPA setup (auto-scale 2-20 pods based on CPU >70%)
  - Load testing (Locust: 100 concurrent users, verify <100ms p95 latency)
  - RBAC enforcement (test_engineer can predict, cannot promote models)

**Phase 6: Production Launch & Optimization** (Weeks 12-14):
- **Week 12**:
  - Production cutover (migrate 10 pilot users from manual workflows to P16 API)
  - Monitor dashboards (Grafana: Kafka lag, Spark duration, FastAPI latency)
  - Incident response runbooks (Kafka lag troubleshooting, model accuracy drop)
- **Week 13**:
  - Gradual rollout: 50 users (week 13), 100 users (week 14), 200+ users (week 15)
  - Cost monitoring (track daily spend, optimize Databricks cluster auto-termination)
  - Documentation (user guides, API reference, video tutorials)
- **Week 14**:
  - Retrospective (lessons learned, performance tuning, backlog grooming)
  - Cost validation (<$100/month target, Databricks Community + S3 + EKS optimized)
  - Celebrate launch 🎉 (demo to leadership, collect user feedback)

### 17.4 Rollback Procedures

**Rollback Triggers**:
1. **Critical Bug**: Prediction API error rate >10% for 5+ min
2. **Performance Degradation**: p95 latency >500ms (5× baseline) for 10+ min
3. **Data Corruption**: Delta Lake schema mismatch, null bins >10%
4. **Security Incident**: Unauthorized access detected, JWT validation bypass

**Rollback Steps** (Kubernetes Deployment):

**Step 1: Immediate Rollback** (FastAPI):
```bash
# Rollback to previous deployment (within 10 min of issue detection)
kubectl rollout undo deployment/fastapi

# Verify rollback
kubectl rollout status deployment/fastapi
curl https://api.p16.company.com/health
```

**Step 2: Model Rollback** (MLflow):
```python
# Revert model to previous Production version
import mlflow

client = mlflow.MlflowClient()

# Transition current Production model to Archived
client.transition_model_version_stage(
    name="p01_xgboost",
    version=3,  # Current Production version (buggy)
    stage="Archived"
)

# Promote previous version back to Production
client.transition_model_version_stage(
    name="p01_xgboost",
    version=2,  # Previous stable version
    stage="Production"
)

# FastAPI auto-reloads model from MLflow registry (5-min polling)
```

**Step 3: Data Rollback** (Delta Lake Time-Travel):
```sql
-- Rollback wafer_features table to version 3 hours ago
RESTORE TABLE wafer_features TO VERSION AS OF 123;

-- Or rollback to timestamp
RESTORE TABLE wafer_features TO TIMESTAMP AS OF '2024-12-10 12:00:00';

-- Verify data integrity
SELECT COUNT(*), AVG(wafer_yield) 
FROM wafer_features 
WHERE date = current_date();
```

**Step 4: Airflow DAG Pause**:
```bash
# Pause retraining DAG to prevent further issues
airflow dags pause p01_weekly_retraining

# Clear failed task instances
airflow tasks clear p01_weekly_retraining --start-date 2024-12-10 --end-date 2024-12-10
```

**Post-Rollback Checklist**:
- [ ] Verify all 15 models serving correctly (/models endpoint)
- [ ] Check Grafana dashboards (latency, error rate, Kafka lag)
- [ ] Query Delta Lake (validate row counts, schema, sample data)
- [ ] Notify users (Slack #ml-alerts, incident postmortem document)
- [ ] Root cause analysis (review logs, Git diff, debug locally)
- [ ] Hotfix PR (fix bug, add unit test, deploy to staging → production)

**Rollback SLA**: <15 min from issue detection to stable state (rollback complete, services healthy)

---

## 18. Monitoring & Observability

### 18.1 Metrics

**Infrastructure Metrics** (Prometheus):

**Kafka Metrics**:
```
# Consumer lag (critical: >10 min = alert)
kafka_consumer_lag_seconds{topic="stdf_ingestion", partition="3"} 

# Broker disk usage
kafka_broker_disk_usage_percent{broker="kafka-0"}

# Message throughput
kafka_topic_messages_per_second{topic="stdf_ingestion"}

# Replication lag
kafka_replica_lag_milliseconds{topic="stdf_ingestion", replica="kafka-1"}
```

**Spark Metrics**:
```
# Active Spark jobs
spark_active_jobs_count{cluster="databricks-prod"}

# Executor memory usage
spark_executor_memory_used_bytes{executor_id="1"}

# Shuffle data size
spark_shuffle_write_bytes_total{job_id="12345"}

# Job duration
spark_job_duration_seconds{job_name="stdf_parsing"}
```

**Delta Lake Metrics**:
```
# Table size (GB)
delta_table_size_gb{table="raw_stdf", partition="2024-12-10"}

# Daily row count growth
delta_table_rows_added_daily{table="wafer_features"}

# Transaction commit duration
delta_transaction_commit_duration_seconds{table="raw_stdf"}

# VACUUM reclaimed space
delta_vacuum_reclaimed_gb{table="raw_stdf"}
```

**MLflow Metrics**:
```
# Total experiments
mlflow_experiments_count{status="active"}

# Model registry size
mlflow_registered_models_count{}

# Artifact storage size
mlflow_artifacts_size_gb{}

# Runs logged per day
mlflow_runs_logged_daily{experiment="p01_xgboost"}
```

**FastAPI Metrics**:
```
# Request count by endpoint
fastapi_requests_total{method="POST", endpoint="/predict/p01_xgboost", status="200"}

# Request latency histogram
fastapi_request_duration_seconds{le="0.02", model="p01_xgboost"}  # <20ms bucket
fastapi_request_duration_seconds{le="0.1", model="p01_xgboost"}   # <100ms bucket

# Active connections
fastapi_active_connections{pod="fastapi-7d9f5b8c-abc12"}

# Error rate
fastapi_requests_total{status=~"5.."} / fastapi_requests_total
```

**System Metrics**:
```
# CPU usage by pod
container_cpu_usage_seconds_total{pod="fastapi-7d9f5b8c-abc12"}

# Memory usage
container_memory_usage_bytes{pod="airflow-scheduler-5f6d8a-xyz34"}

# Disk I/O
node_disk_io_time_seconds_total{device="nvme0n1"}

# Network throughput
node_network_transmit_bytes_total{device="eth0"}
```

### 18.2 Logging

**Structured Logging Format** (JSON):

```json
{
  "timestamp": "2024-12-10T14:30:15.234Z",
  "level": "INFO",
  "service": "fastapi",
  "pod": "fastapi-7d9f5b8c-abc12",
  "correlation_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "user_id": "john.doe@company.com",
  "message": "Prediction request received",
  "context": {
    "model_name": "p01_xgboost",
    "model_version": "v2.3",
    "feature_count": 10,
    "lot_id": "TC41x_LOT123",
    "wafer_id": "W05"
  },
  "duration_ms": 18.5
}
```

**Log Levels**:
- **DEBUG**: Detailed diagnostic info (disabled in production, enabled via env var)
- **INFO**: Normal operations (prediction requests, model loading, DAG runs)
- **WARNING**: Recoverable issues (high Kafka lag, slow queries, low confidence predictions)
- **ERROR**: Errors requiring investigation (model loading failed, Spark job crash)
- **CRITICAL**: System failures (Kafka broker down, Delta Lake unavailable, MLflow unreachable)

**Logging Infrastructure** (OpenSearch):

**Log Aggregation**:
- **Fluent Bit**: Deployed as DaemonSet in Kubernetes (collect logs from all pods)
- **OpenSearch**: 3-node cluster (30-day retention, 100GB storage)
- **Kibana**: Visualization UI (log search, filtering, dashboards)

**Log Search Examples**:
```
# Find all errors in last 24 hours
level:ERROR AND timestamp:[now-24h TO now]

# Find predictions for specific lot
lot_id:"TC41x_LOT123" AND service:fastapi

# Find slow queries (>10 sec)
duration_ms:>10000 AND service:spark

# Find 5xx errors by user
status:5xx AND user_id:* | terms field=user_id
```

**Log Retention**:
- **Hot tier** (OpenSearch): 30 days (fast queries, <1 sec search)
- **Warm tier** (S3 Glacier): 90 days (slower queries, <1 min search)
- **Cold tier** (S3 Glacier Deep Archive): 3 years (compliance, <12 hour retrieval)

### 18.3 Alerting

**Alert Rules** (Prometheus Alertmanager):

**Critical Alerts** (PagerDuty + Slack #ml-critical):

```yaml
# Kafka consumer lag >10 min
- alert: KafkaHighConsumerLag
  expr: kafka_consumer_lag_seconds > 600
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Kafka consumer lag >10 min on topic {{ $labels.topic }}"
    description: "Consumer lag {{ $value }}s on partition {{ $labels.partition }}"
    runbook_url: "https://docs.p16.company.com/runbooks/kafka_lag"

# FastAPI error rate >5%
- alert: FastAPIHighErrorRate
  expr: (sum(rate(fastapi_requests_total{status=~"5.."}[5m])) / sum(rate(fastapi_requests_total[5m]))) > 0.05
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "FastAPI error rate >5% (current: {{ $value | humanizePercentage }})"
    description: "Check logs for HTTP 5xx errors"

# Delta Lake unavailable
- alert: DeltaLakeUnavailable
  expr: up{job="delta-lake"} == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Delta Lake unavailable"
    description: "Cannot connect to Delta Lake storage (S3 or MinIO)"
```

**Warning Alerts** (Slack #ml-alerts only):

```yaml
# Model accuracy drop >3%
- alert: ModelAccuracyDrop
  expr: mlflow_model_accuracy{model="p01_xgboost"} < 0.86
  for: 1h
  labels:
    severity: warning
  annotations:
    summary: "P01 XGBoost accuracy dropped to {{ $value }}"
    description: "Baseline accuracy: 0.89, current: {{ $value }}, check for data drift"

# Databricks cluster idle >2 hours
- alert: DatabricksClusterIdle
  expr: databricks_cluster_idle_minutes > 120
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Databricks cluster idle for {{ $value }} minutes"
    description: "Consider auto-termination to save costs ($0.30/hour)"

# Disk usage >80%
- alert: HighDiskUsage
  expr: (node_filesystem_avail_bytes / node_filesystem_size_bytes) < 0.2
  for: 30m
  labels:
    severity: warning
  annotations:
    summary: "Disk usage >80% on {{ $labels.instance }}"
    description: "Free space: {{ $value | humanize1024 }}B, consider cleanup"
```

**Alert Routing**:
- **Critical** → PagerDuty (24/7 on-call rotation) + Slack #ml-critical
- **Warning** → Slack #ml-alerts (business hours review)
- **Info** → Grafana dashboard annotations (no active notification)

**Alert Grouping** (Reduce noise):
- Group by `cluster, service` (consolidate multiple pod alerts into one)
- Wait 2 min before sending (avoid transient spikes)
- Repeat every 4 hours if unresolved (prevent alert fatigue)

### 18.4 Dashboards

**Grafana Dashboard Catalog** (4 core dashboards):

**1. Infrastructure Health Dashboard**:

**Panels**:
- **Kafka Consumer Lag** (Gauge): Current lag in seconds, color-coded (green <5 min, yellow 5-10 min, red >10 min)
- **Spark Active Jobs** (Graph): Active jobs count over time, stacked by job type
- **Delta Lake Table Sizes** (Bar chart): GB per table (raw_stdf, wafer_features, parametric_stats, spatial_patterns)
- **FastAPI Request Rate** (Graph): Requests/min by endpoint, 7-day trend
- **System CPU/Memory** (Heatmap): CPU/memory usage per pod, color intensity

**Queries**:
```promql
# Kafka lag gauge
max(kafka_consumer_lag_seconds{topic="stdf_ingestion"})

# Spark active jobs
sum(spark_active_jobs_count) by (cluster)

# Delta Lake sizes
sum(delta_table_size_gb) by (table)

# FastAPI request rate
rate(fastapi_requests_total[1m]) * 60
```

**2. Model Performance Dashboard**:

**Panels**:
- **Prediction Count** (Graph): Predictions/day by model (P01-P15), last 30 days
- **Latency Distribution** (Histogram): p50/p95/p99 latency per model
- **Confidence Score Distribution** (Histogram): 0-1 range, identify low-confidence predictions
- **Accuracy Trend** (Graph): Weekly model accuracy, baseline comparison
- **Drift Detection** (Heatmap): KL divergence for top 20 features, color = drift severity

**Queries**:
```promql
# Prediction count per model
sum(increase(fastapi_requests_total{endpoint=~"/predict/.*"}[1d])) by (model)

# p95 latency per model
histogram_quantile(0.95, rate(fastapi_request_duration_seconds_bucket[5m])) by (model)

# Model accuracy (from MLflow custom metric)
mlflow_model_accuracy{model=~"p.*"}
```

**3. Cost Dashboard**:

**Panels**:
- **Databricks DBU Consumption** (Graph): DBU/day, weekly/monthly trends, forecast
- **AWS S3 Storage Costs** (Stacked area): GB stored by table, data transfer costs
- **EKS Compute Costs** (Pie chart): Node hours by instance type (on-demand vs. spot)
- **Total Monthly Spend** (Gauge): Current month spend, target <$100, color-coded
- **Cost per Prediction** (Graph): Total cost / total predictions, track efficiency

**Queries** (Custom CloudWatch metrics):
```promql
# Databricks DBU consumption
databricks_dbu_consumed_total{workspace="prod"}

# S3 storage costs
aws_s3_storage_gb{bucket="p16-datalake"} * 0.023  # $0.023/GB/month

# Total monthly spend
sum(aws_billing_estimated_charges{service=~".*"})
```

**4. Data Quality Dashboard**:

**Panels**:
- **Null Value Percentage** (Graph): % null values per critical column, alert if >10%
- **Outlier Count** (Graph): Die with parametric values >5 sigma, daily trend
- **Schema Validation Failures** (Counter): Failed validations, grouped by error type
- **Bin Distribution** (Stacked bar): Bin counts over time, detect anomalies
- **Wafer Yield Distribution** (Histogram): Yield 0-1 range, compare to baseline

**Queries** (Custom Spark job metrics):
```promql
# Null percentage
(delta_null_count{table="raw_stdf", column="bin"} / delta_row_count{table="raw_stdf"}) * 100

# Outlier count
delta_outlier_count{table="parametric_stats", threshold="5_sigma"}

# Schema validation failures
increase(airflow_task_failures{dag="data_quality_validation"}[1d])
```

**Dashboard Access Control**:
- **All users**: View all dashboards (read-only)
- **Platform Engineers**: Edit dashboards, create new panels
- **Embed**: Public links for stakeholder reporting (weekly email to management)

**Auto-Refresh**: 30-second refresh for real-time monitoring, 5-min refresh for historical analysis

---

## 19. Risk Assessment

### 19.1 Technical Risks

**Risk 1: Databricks Cloud Cost Overrun**:
- **Probability**: Medium (40%)
- **Impact**: High (budget exceeded by 2-5×, $100 → $500/month)
- **Root Cause**: 
  - Cluster left running 24/7 (vs. 40 hrs/month budgeted)
  - Large Spark jobs (>20 GB STDF) require 8+ workers
  - Unexpected data transfer costs (S3 egress)
- **Mitigation**:
  - **Auto-termination**: Mandatory 2-hour idle timeout (saves $20-30/month)
  - **Cost alerts**: Slack notification if daily spend >$5 (monthly projected >$150)
  - **Databricks Community Edition**: Free for Year 1 (delay paid tier until Year 2)
  - **Spot instances**: Use spot workers (70% cheaper) for non-critical jobs
  - **Monitoring**: Cost dashboard with daily/weekly/monthly trends
- **Contingency**: If cost >$200/month, migrate to local Spark Standalone cluster (on-premise servers, $0 cloud cost)

**Risk 2: Model Performance Degradation (Data Drift)**:
- **Probability**: High (70%)
- **Impact**: Medium (model accuracy drops 89% → 83%, user trust eroded)
- **Root Cause**:
  - Parametric test distributions shift (ATE calibration drift, new product mix)
  - Training data becomes stale (6 months old, doesn't reflect current production)
  - Feature engineering bugs (incorrect aggregations)
- **Mitigation**:
  - **Weekly retraining**: Airflow DAG retrains models with latest 90 days of data
  - **Drift detection**: Monitor feature KL divergence (alert if >0.3 for critical features)
  - **Baseline comparison**: Alert if accuracy drops >3% from baseline
  - **A/B testing**: Deploy new model to 10% traffic, compare to Champion before full rollout
  - **Data quality checks**: Hourly validation of parametric means/stds (±3 sigma baseline)
- **Contingency**: If accuracy drops >5%, pause deployment, investigate root cause, retrain with feature selection

**Risk 3: Kafka Message Loss (Exactly-Once Failure)**:
- **Probability**: Low (10%)
- **Impact**: Critical (STDF data lost, wafer yield analysis incomplete)
- **Root Cause**:
  - Kafka broker crash during message commit
  - Spark Structured Streaming checkpoint corruption
  - Network partition (Kafka → Spark connection lost)
- **Mitigation**:
  - **Kafka replication**: Replication factor 3 (tolerates 2 broker failures)
  - **min.insync.replicas=2**: Require 2 replicas to acknowledge write
  - **Spark checkpointing**: S3 checkpoints (11 9's durability)
  - **Idempotent producer**: Kafka producer with retries, deduplication
  - **Monitoring**: Consumer lag alert (lag >10 min = potential data loss)
- **Contingency**: If message loss detected, replay STDFs from ATE network share (7-day retention)

**Risk 4: Security Breach (Unauthorized Model Access)**:
- **Probability**: Low (5%)
- **Impact**: Critical (proprietary models leaked, IP theft)
- **Root Cause**:
  - Weak authentication (no MFA, default passwords)
  - RBAC misconfiguration (test_engineer can promote models)
  - Exposed Kubernetes API server (public internet access)
- **Mitigation**:
  - **OAuth2/OIDC SSO**: Azure AD with MFA enforced
  - **RBAC audits**: Monthly review of permissions (principle of least privilege)
  - **Network policies**: Kubernetes NetworkPolicy (isolate namespaces, restrict egress)
  - **Secret rotation**: 90-day rotation for DB passwords, API keys
  - **Security scanning**: Snyk, Bandit, Trivy (block deployment on critical CVE)
- **Contingency**: If breach detected, rotate all secrets, audit logs (identify attacker), notify security team

### 19.2 Business Risks

**Risk 5: Low User Adoption (P16 Not Used)**:
- **Probability**: Medium (30%)
- **Impact**: High (project ROI unrealized, $600K investment wasted)
- **Root Cause**:
  - Steep learning curve (ML engineers prefer Jupyter notebooks over Databricks SQL)
  - Prediction API too slow (>1 sec vs. <100ms expected)
  - Missing features (users need shmoo plot analysis, not just bin prediction)
- **Mitigation**:
  - **Onboarding**: 2-week tutorial program (Day 1: Docker Compose, Day 14: Production deployment)
  - **Performance SLAs**: <100ms p95 latency, monitored 24/7, alerts if >200ms
  - **User feedback**: Monthly surveys (NPS score, feature requests), prioritize backlog
  - **Quick wins**: Deploy P01 first (simplest model, immediate value), then P02-P15
  - **Documentation**: Video tutorials, Slack support channel, runbooks for common tasks
- **Contingency**: If adoption <50% after 6 months, conduct user interviews, pivot to hybrid model (P16 + Jupyter notebooks)

**Risk 6: Regulatory Compliance Failure (ISO 26262 Audit)**:
- **Probability**: Low (10%)
- **Impact**: High (product launch delayed, $5M+ revenue impact)
- **Root Cause**:
  - Incomplete model lineage (cannot trace prediction to training data)
  - Audit trail gaps (3-year retention not enforced)
  - Data deletion API not GDPR-compliant
- **Mitigation**:
  - **MLflow lineage**: Track training_data_version, git_commit, hyperparameters (100% coverage)
  - **Audit logs**: 3-year retention in model_predictions table, export API for auditors
  - **GDPR deletion API**: Hard delete from Delta Lake (not just soft delete)
  - **Compliance dashboard**: Grafana dashboard showing lineage coverage, audit log retention
  - **Dry-run audit**: Internal audit at 6 months (practice for real audit)
- **Contingency**: If audit fails, prioritize compliance fixes over new features (freeze feature development)

### 19.3 Mitigation Strategies

**Risk Mitigation Matrix**:

| Risk ID | Risk Name                  | Probability | Impact   | Mitigation Priority | Owner                |
|---------|----------------------------|-------------|----------|---------------------|----------------------|
| R1      | Databricks Cost Overrun    | Medium      | High     | High                | Platform Engineer    |
| R2      | Model Performance Degradation | High     | Medium   | High                | ML Engineer          |
| R3      | Kafka Message Loss         | Low         | Critical | Medium              | Data Engineer        |
| R4      | Security Breach            | Low         | Critical | High                | Security Engineer    |
| R5      | Low User Adoption          | Medium      | High     | High                | Product Manager      |
| R6      | Compliance Failure         | Low         | High     | Medium              | Compliance Officer   |

**Continuous Risk Monitoring**:
- **Weekly**: Review cost dashboard (R1), model accuracy trends (R2), consumer lag (R3)
- **Monthly**: Security audit (R4), user adoption metrics (R5), compliance checklist (R6)
- **Quarterly**: Risk register update (re-assess probabilities/impacts), update mitigation plans

**Escalation Path**:
1. **Team Level**: ML/Data/Platform engineers resolve tactical issues (cost alerts, model retraining)
2. **Manager Level**: Engineering Manager escalates if risk probability >50% or impact >$50K
3. **Executive Level**: VP Engineering notified if risk impacts product launch or $500K+ budget

---

## 20. Timeline & Milestones

### 20.1 Phase Breakdown

**14-Week Development Timeline**:

**Phase 1: Infrastructure Setup** (Weeks 1-2):

**Week 1: Cloud Provisioning & Kafka Deployment**
- **Day 1-2**: AWS account setup, EKS cluster provisioning (3 nodes, t3.medium)
- **Day 3-4**: Kafka cluster deployment (3 brokers, Zookeeper, Kafdrop UI)
- **Day 5**: Kafka topic creation (stdf_ingestion, 12 partitions, replication factor 3)
- **Deliverables**: 
  - ✅ EKS cluster operational (kubectl access)
  - ✅ Kafka cluster healthy (3 brokers, <100ms latency)
  - ✅ Kafdrop UI accessible (http://kafdrop.p16.company.com)

**Week 2: Databricks, Airflow, MLflow**
- **Day 1-2**: Databricks workspace setup, Unity Catalog creation, IAM roles
- **Day 3-4**: Airflow deployment (scheduler, webserver, Celery workers, Redis)
- **Day 5**: MLflow tracking server (PostgreSQL backend, S3 artifacts, model registry)
- **Deliverables**:
  - ✅ Databricks cluster auto-scaling (2-8 workers)
  - ✅ Airflow UI accessible (http://airflow.p16.company.com)
  - ✅ MLflow tracking 100 test experiments

**Phase 2: Data Ingestion & Storage** (Weeks 3-4):

**Week 3: Kafka → Spark → Delta Lake Pipeline**
- **Day 1-2**: Kafka Connect FileStreamSource (watch ATE network share)
- **Day 3-4**: Spark Structured Streaming STDF parser (pystdf → DataFrame)
- **Day 5**: End-to-end test (423 synthetic STDFs → Delta Lake in <5 min)
- **Deliverables**:
  - ✅ Kafka ingestion <1 min p95
  - ✅ Spark parsing <3 min p95 (10GB STDF)
  - ✅ Delta Lake raw_stdf table (1M+ rows)

**Week 4: Delta Lake Schema & Data Quality**
- **Day 1-2**: Create Delta Lake tables (raw_stdf, wafer_features, parametric_stats, spatial_patterns)
- **Day 3-4**: Data quality validation (schema checks, null checks, outlier detection)
- **Day 5**: Airflow data quality DAG (hourly runs, Slack alerts)
- **Deliverables**:
  - ✅ 4 Delta Lake tables operational
  - ✅ Data quality dashboard (Grafana)
  - ✅ Hourly validation <10% null values

**Phase 3: Feature Engineering** (Weeks 5-6):

**Week 5: Wafer-Level Aggregations**
- **Day 1-2**: Wafer yield calculation (passed_die / total_die)
- **Day 3-4**: Edge die yield, center yield, bin distribution
- **Day 5**: Parametric statistics (mean, std, p5, p95 per test)
- **Deliverables**:
  - ✅ wafer_features table (50+ features)
  - ✅ Feature engineering Spark job <15 min for 1 day of data

**Week 6: Spatial Patterns & Optimization**
- **Day 1-2**: Wafer map heatmap generation (matplotlib → S3)
- **Day 3-4**: ResNet-18 feature extraction (512-dim embeddings)
- **Day 5**: Z-ordering optimization (ZORDER BY device, lot_id)
- **Deliverables**:
  - ✅ spatial_patterns table (5GB embeddings)
  - ✅ Query performance <10 sec for 1M+ rows
  - ✅ Airflow daily feature engineering DAG

**Phase 4: Model Training & Tracking** (Weeks 7-9):

**Week 7: Mock Models & MLflow Integration**
- **Day 1-2**: Train P01 RandomForest mock model (100 trees, max_depth=10)
- **Day 3-4**: MLflow auto-logging (hyperparameters, metrics, artifacts)
- **Day 5**: Model registry setup (Production, Staging, Archived tags)
- **Deliverables**:
  - ✅ P01/P02/P04/P10 mock models trained
  - ✅ MLflow registry with 15 models
  - ✅ Model versioning (v1.0, v1.1)

**Week 8: Airflow Retraining DAG**
- **Day 1-2**: Retraining DAG implementation (query Delta Lake → train → validate)
- **Day 3-4**: Auto-promotion logic (if accuracy >baseline + 3%, promote to Production)
- **Day 5**: Weekly retraining schedule (Sunday 2am, email notifications)
- **Deliverables**:
  - ✅ Retraining DAG operational (15 DAGs for P01-P15)
  - ✅ Auto-promotion tested (mock accuracy improvement)
  - ✅ Email notifications (success/failure)

**Week 9: Real Model Integration & A/B Testing**
- **Day 1-2**: Integrate real P01-P15 models (swap mock models)
- **Day 3-4**: A/B testing setup (90/10 Champion/Challenger split)
- **Day 5**: Model lineage documentation (training_data_version, git_commit)
- **Deliverables**:
  - ✅ 15 real models deployed to Staging
  - ✅ A/B test framework operational
  - ✅ Model lineage 100% coverage

**Phase 5: Model Serving** (Weeks 10-11):

**Week 10: FastAPI Deployment**
- **Day 1-2**: FastAPI application development (15 endpoints /predict/p01_xgboost, ...)
- **Day 3-4**: OAuth2/OIDC integration (Azure AD SSO, JWT validation)
- **Day 5**: RBAC enforcement (test_engineer can predict, cannot promote)
- **Deliverables**:
  - ✅ FastAPI application deployed (2 replicas)
  - ✅ 20+ endpoints operational
  - ✅ OAuth2 SSO functional

**Week 11: Performance Tuning & Load Testing**
- **Day 1-2**: Kubernetes HPA setup (auto-scale 2-20 pods, CPU >70%)
- **Day 3-4**: Load testing (Locust: 100 concurrent users, 10K predictions)
- **Day 5**: Performance optimization (ONNX conversion, caching)
- **Deliverables**:
  - ✅ <100ms p95 latency (all models)
  - ✅ HPA tested (scale to 10 pods under load)
  - ✅ 10K predictions/day sustained

**Phase 6: Production Launch & Optimization** (Weeks 12-14):

**Week 12: Pilot Launch**
- **Day 1-2**: Migrate 10 pilot users (test engineers → P16 API)
- **Day 3-4**: Monitoring dashboards (Grafana: Kafka, Spark, FastAPI, Cost)
- **Day 5**: Incident response runbooks (Kafka lag, model accuracy drop)
- **Deliverables**:
  - ✅ 10 pilot users active
  - ✅ 4 Grafana dashboards operational
  - ✅ 5 runbooks documented

**Week 13: Gradual Rollout**
- **Day 1-2**: Expand to 50 users (25% of target)
- **Day 3-4**: Cost monitoring (daily spend <$5, monthly <$100)
- **Day 5**: User feedback collection (NPS survey, feature requests)
- **Deliverables**:
  - ✅ 50 users active (NPS >40)
  - ✅ Cost <$100/month validated
  - ✅ Top 5 feature requests prioritized

**Week 14: Full Launch & Retrospective**
- **Day 1-2**: Full rollout (200+ users, all test/yield engineers)
- **Day 3-4**: Documentation finalization (user guides, API reference, video tutorials)
- **Day 5**: Retrospective (lessons learned, celebrate launch 🎉)
- **Deliverables**:
  - ✅ 200+ users active
  - ✅ <100ms p95 latency sustained
  - ✅ $28M/year ROI path validated
  - ✅ Project retrospective complete

### 20.2 Key Milestones

**Milestone Tracker**:

| Milestone | Week | Description | Success Criteria | Status |
|-----------|------|-------------|------------------|--------|
| **M1: Infrastructure Ready** | 2 | EKS, Kafka, Databricks, Airflow, MLflow operational | All services healthy, kubectl access | 🔲 Not Started |
| **M2: Data Ingestion Live** | 4 | STDF → Kafka → Spark → Delta Lake pipeline functional | 1,000 STDFs/day ingested, <5 min p95 | 🔲 Not Started |
| **M3: Features Available** | 6 | wafer_features, spatial_patterns tables populated | 50+ features, <10 sec queries | 🔲 Not Started |
| **M4: Models Trained** | 9 | 15 real P01-P15 models trained, logged to MLflow | 100% experiment tracking, v1.0 registered | 🔲 Not Started |
| **M5: API Deployed** | 11 | FastAPI serving 15 models, <100ms p95 latency | 10K predictions/day, HPA tested | 🔲 Not Started |
| **M6: Production Launch** | 14 | 200+ users active, <$100/month cost, $28M ROI path | NPS >40, uptime >99%, cost validated | 🔲 Not Started |

**Go/No-Go Decision Points**:

**Week 4 Review**: 
- **Go Criteria**: Kafka ingestion <5 min, Delta Lake tables operational, data quality <10% null
- **No-Go**: If Kafka lag >10 min sustained OR data quality >20% null → investigate root cause, add 1-2 weeks buffer

**Week 9 Review**:
- **Go Criteria**: 15 models trained, MLflow tracking 100%, model lineage documented
- **No-Go**: If model accuracy <80% OR MLflow unavailable → delay FastAPI deployment, focus on model quality

**Week 11 Review**:
- **Go Criteria**: FastAPI <100ms p95, HPA tested, 10K predictions/day
- **No-Go**: If latency >200ms OR error rate >5% → optimize models (ONNX), add replicas, delay launch 1-2 weeks

**Week 14 Review**:
- **Success Criteria**: 200+ users active, <$100/month cost, NPS >40, uptime >99%
- **Partial Success**: If 100-200 users OR cost $100-150 → continue gradual rollout, optimize costs
- **Failure**: If <50 users OR cost >$200 → conduct postmortem, pivot strategy

---

## 21. Success Metrics & KPIs

### 21.1 Measurable Targets

**Infrastructure Performance KPIs**:

| Metric | Target | Current Baseline | Measurement | Dashboard |
|--------|--------|------------------|-------------|-----------|
| **STDF Ingestion Latency** | <5 min p95 | 30-60 min (Pandas) | Prometheus: `kafka_consumer_lag_seconds` | Infrastructure Health |
| **Spark Processing Time** | <5 min for 10GB | 30-60 min (laptop) | Prometheus: `spark_job_duration_seconds` | Infrastructure Health |
| **Delta Lake Query Time** | <10 sec for 1M+ rows | N/A (no existing feature store) | Databricks SQL query logs | Data Quality |
| **FastAPI Prediction Latency** | <100ms p95 | 5-10 sec (Flask) | Prometheus: `fastapi_request_duration_seconds_bucket` | Model Performance |
| **Kafka Throughput** | 1,000 STDFs/day sustained | N/A (manual uploads) | Prometheus: `kafka_topic_messages_per_second` | Infrastructure Health |
| **System Uptime** | 99.9% (max 8 hrs/year) | N/A | Prometheus: `up` metric | Infrastructure Health |

**MLOps Quality KPIs**:

| Metric | Target | Current Baseline | Measurement | Dashboard |
|--------|--------|------------------|-------------|-----------|
| **Experiment Tracking Coverage** | 100% of ML runs | 0% (no tracking) | MLflow API: `mlflow.search_runs()` | Model Performance |
| **Model Lineage Coverage** | 100% (training data → prediction) | 0% (no lineage) | MLflow tags: `training_data_version`, `git_commit` | Model Performance |
| **Feature Reuse Rate** | 90% (eliminate duplicate parsing) | 5% (each project parses STDFs independently) | Delta Lake query count / total queries | Data Quality |
| **Model Reproducibility** | 95% (same code/data/hyperparams → same accuracy ±1%) | 10% (manual notebook runs) | MLflow experiment comparison | Model Performance |
| **Automated Retraining Frequency** | Weekly (Sunday 2am) | Manual (every 3-6 months) | Airflow DAG run count | Infrastructure Health |
| **Model Deployment Time** | <15 min (Staging → Production) | 2-4 weeks (manual validation) | MLflow registry transition timestamp | Model Performance |

**Cost Efficiency KPIs**:

| Metric | Target | Current Baseline | Measurement | Dashboard |
|--------|--------|------------------|-------------|-----------|
| **Monthly Cloud Cost** | <$100 | $0 (laptop-only, no cloud) | AWS Cost Explorer, Databricks billing | Cost |
| **Cost per Prediction** | <$0.01 | N/A | Total cost / total predictions | Cost |
| **Cost per Model** | <$6.67/month ($100 / 15 models) | N/A | Databricks DBU / model count | Cost |
| **Storage Cost Growth Rate** | <10% month-over-month | N/A | S3 storage GB × $0.023/GB | Cost |
| **Compute Cost Optimization** | >70% savings (spot vs. on-demand) | N/A | EKS spot instance hours / total hours | Cost |

**Business Impact KPIs**:

| Metric | Target | Current Baseline | Measurement | Dashboard |
|--------|--------|------------------|-------------|-----------|
| **P01-P15 Projects Deployed** | 15/15 within 18 months | 0/15 in production | Project status tracker | Custom |
| **ROI Realization** | $28M/year opportunity unlocked | $0 (POC stage) | Revenue from P01-P15 deployed projects | Custom |
| **Engineering Time Saved** | 320 hrs/month (data eng + ML eng) | 0 hrs | Time tracking: data pipeline setup → feature engineering | Custom |
| **User Adoption Rate** | 200+ active users (test/yield engineers) | 0 users | FastAPI request logs: unique `user_id` count | Model Performance |
| **Net Promoter Score (NPS)** | >40 (promoters > detractors) | N/A | Monthly user survey | Custom |
| **Model Accuracy Maintenance** | >85% accuracy sustained (P01-P15 average) | 89% baseline (POC) | MLflow metrics: `accuracy` per model | Model Performance |

**Adoption & Engagement KPIs**:

| Metric | Target | Current Baseline | Measurement | Dashboard |
|--------|--------|------------------|-------------|-----------|
| **Daily Active Users (DAU)** | 50+ users/day | 0 | FastAPI logs: unique `user_id` per day | Model Performance |
| **Daily Predictions** | 10,000 predictions/day | 0 | Prometheus: `fastapi_requests_total{endpoint=~"/predict/.*"}` | Model Performance |
| **Self-Service Feature Queries** | 100 queries/week (Databricks SQL) | 0 (all queries via data engineers) | Databricks query logs | Data Quality |
| **MLflow Experiment Runs** | 150 runs/week (30 engineers × 5 runs) | 0 | MLflow API: `mlflow.search_runs()` count | Model Performance |
| **Average Time to First Prediction** | <1 hour (new user onboarding) | N/A | User survey + FastAPI first request timestamp | Custom |
| **Feature Request Rate** | <10/month (low = good UX) | N/A | GitHub issues labeled "feature-request" | Custom |

**Reliability & Availability KPIs**:

| Metric | Target | Current Baseline | Measurement | Dashboard |
|--------|--------|------------------|-------------|-----------|
| **API Uptime** | 99.9% (max 8 hrs downtime/year) | N/A | Prometheus: `up{job="fastapi"}` | Infrastructure Health |
| **Kafka Availability** | 99.95% (3-broker HA) | N/A | Prometheus: `up{job="kafka"}` | Infrastructure Health |
| **Delta Lake ACID Success Rate** | >99.99% (no data corruption) | N/A | Delta Lake transaction logs | Data Quality |
| **MLflow Availability** | 99.9% | N/A | Prometheus: `up{job="mlflow"}` | Model Performance |
| **Mean Time to Recovery (MTTR)** | <15 min (rollback procedures) | N/A | Incident postmortem analysis | Custom |
| **Alert Noise Ratio** | <20% false positives | N/A | Resolved alerts / total alerts | Infrastructure Health |

**KPI Tracking Frequency**:
- **Real-time** (Grafana dashboards): Latency, uptime, error rate, Kafka lag
- **Daily** (automated reports): Predictions count, cost, DAU, experiment runs
- **Weekly** (team review): Model accuracy, user adoption, feature requests
- **Monthly** (stakeholder review): ROI progress, cost trends, NPS, project deployment status
- **Quarterly** (executive review): $28M ROI realization, strategic pivots, risk register

**Success Thresholds** (90-Day Post-Launch):

**Tier 1: Minimum Viable Success**
- ✅ 50+ daily active users
- ✅ <$150/month cost (50% over budget acceptable)
- ✅ 5+ P01-P15 projects deployed
- ✅ 99% uptime (3 nines)
- ✅ <200ms p95 latency (2× target acceptable)

**Tier 2: Target Success** (Original Goals):
- ✅ 100+ daily active users
- ✅ <$100/month cost
- ✅ 10+ P01-P15 projects deployed
- ✅ 99.9% uptime (3.5 nines)
- ✅ <100ms p95 latency

**Tier 3: Exceptional Success** (Stretch Goals):
- ✅ 200+ daily active users
- ✅ <$80/month cost (20% under budget)
- ✅ 15/15 P01-P15 projects deployed
- ✅ 99.95% uptime (4 nines)
- ✅ <50ms p95 latency (2× faster than target)

---

## 22. Appendices & Glossary

### 22.1 Technical Background

**Apache Kafka**:
- **Definition**: Distributed event streaming platform for high-throughput, fault-tolerant data ingestion
- **P16 Usage**: Ingest STDF files from ATE network share, publish to `stdf_ingestion` topic (12 partitions)
- **Key Concepts**:
  - **Broker**: Kafka server instance (P16 runs 3 brokers for HA)
  - **Topic**: Logical channel for messages (e.g., `stdf_ingestion`, `stdf_dead_letter_queue`)
  - **Partition**: Ordered, immutable sequence of messages (enables parallel processing)
  - **Consumer Lag**: Delay between message production and consumption (target <5 min)
  - **Replication Factor**: Number of message copies (3 = tolerates 2 broker failures)

**Apache Spark**:
- **Definition**: Unified analytics engine for large-scale data processing (batch + streaming)
- **P16 Usage**: Parse STDF binary files (PySpark UDFs), aggregate wafer features (Spark SQL), write to Delta Lake
- **Key Concepts**:
  - **RDD (Resilient Distributed Dataset)**: Immutable distributed collection (low-level API)
  - **DataFrame**: Distributed table with schema (high-level API, similar to Pandas)
  - **Structured Streaming**: Continuous data processing (reads Kafka, writes Delta Lake)
  - **Executor**: Worker process that runs Spark tasks (P16 uses 8 executors × 4 cores)
  - **Adaptive Query Execution (AQE)**: Dynamic query optimization (20-40% speedup)

**Delta Lake**:
- **Definition**: Open-source storage layer that adds ACID transactions to data lakes (Parquet + transaction log)
- **P16 Usage**: Feature store (raw_stdf, wafer_features, parametric_stats, spatial_patterns, model_predictions)
- **Key Concepts**:
  - **ACID Transactions**: Atomicity, Consistency, Isolation, Durability (no partial writes, no data corruption)
  - **Time-Travel**: Query historical versions (`VERSION AS OF 123` or `TIMESTAMP AS OF '2024-12-01'`)
  - **Z-Ordering**: Data clustering optimization (30-50% speedup on filtered queries)
  - **OPTIMIZE**: Compact small files into larger ones (improves query performance)
  - **VACUUM**: Delete old file versions (reclaim storage, 30-day default retention)

**MLflow**:
- **Definition**: Open-source platform for managing ML lifecycle (tracking, projects, models, registry)
- **P16 Usage**: Track 100% of P01-P15 experiments, model registry (Production/Staging/Archived), lineage
- **Key Concepts**:
  - **Experiment**: Collection of runs for a single ML project (e.g., `p01_xgboost`)
  - **Run**: Single execution of training code (logs hyperparameters, metrics, artifacts)
  - **Artifact**: File produced by run (model.pkl, plots, feature_importance.csv)
  - **Model Registry**: Central repository for models (versioning, staging, deployment)
  - **Auto-logging**: Automatically log hyperparameters/metrics (sklearn, PyTorch, TensorFlow)

**Apache Airflow**:
- **Definition**: Workflow orchestration platform (DAGs = Directed Acyclic Graphs of tasks)
- **P16 Usage**: Schedule weekly retraining, hourly data quality checks, daily feature engineering
- **Key Concepts**:
  - **DAG**: Directed Acyclic Graph (defines task dependencies, e.g., query_data → train → validate → deploy)
  - **Task**: Single unit of work (Python function, Bash command, Spark job submission)
  - **Scheduler**: Airflow component that triggers DAG runs based on schedule (cron expressions)
  - **Executor**: How tasks run (LocalExecutor, CeleryExecutor, KubernetesExecutor)
  - **XCom**: Cross-communication mechanism (pass data between tasks)

**FastAPI**:
- **Definition**: Modern Python web framework for building APIs (async, auto-generated docs)
- **P16 Usage**: Serve 15 P01-P15 models via REST endpoints (/predict/p01_xgboost, /predict/p02_resnet, ...)
- **Key Concepts**:
  - **Pydantic**: Data validation using Python type hints (request/response schemas)
  - **Uvicorn**: ASGI server (async, supports WebSockets, 10K+ req/sec)
  - **Dependency Injection**: Reusable dependencies (OAuth2 authentication, database connections)
  - **OpenAPI**: Auto-generated API documentation (Swagger UI at /docs)
  - **CORS**: Cross-Origin Resource Sharing (allow browser requests from different domains)

**Kubernetes (K8s)**:
- **Definition**: Container orchestration platform (automate deployment, scaling, management)
- **P16 Usage**: Deploy FastAPI (2-20 replicas, HPA), Kafka, Airflow, Prometheus, Grafana on AWS EKS
- **Key Concepts**:
  - **Pod**: Smallest deployable unit (1+ containers, shared network/storage)
  - **Deployment**: Desired state for pods (e.g., 5 FastAPI replicas)
  - **HPA (Horizontal Pod Autoscaler)**: Auto-scale pods based on CPU/memory/custom metrics
  - **Service**: Stable network endpoint for pods (load balancing, service discovery)
  - **Ingress**: HTTP(S) routing to services (NGINX Ingress Controller)

**Databricks**:
- **Definition**: Unified analytics platform built on Spark (managed Spark clusters, notebooks, SQL editor)
- **P16 Usage**: Spark Structured Streaming (STDF parsing), Delta Lake (feature store), SQL analytics
- **Key Concepts**:
  - **Workspace**: Isolated environment (notebooks, jobs, clusters, data)
  - **Cluster**: Managed Spark cluster (auto-scaling, auto-termination, spot instances)
  - **Runtime**: Optimized Spark distribution (Runtime 14.3 LTS = Spark 3.5 + Delta Lake 3.0)
  - **Unity Catalog**: Centralized metadata and governance (tables, schemas, permissions)
  - **DBU (Databricks Unit)**: Billing unit (1 DBU = 1 hour of compute, varies by instance type)

### 22.2 References

**Industry Standards**:
1. **JEDEC JESD22-A100**: Test Methods for Semiconductor Devices
2. **SEMI E5**: Specification for STDF (Standard Test Data Format)
3. **ISO 26262**: Functional Safety for Automotive Systems (Part 6: Software Development)
4. **IATF 16949**: Automotive Quality Management System Standard
5. **GDPR (General Data Protection Regulation)**: EU data privacy regulation (Right to Deletion, Data Portability)
6. **CCPA (California Consumer Privacy Act)**: California data privacy law

**Technical Documentation**:
1. **Apache Kafka Documentation**: https://kafka.apache.org/documentation/
2. **Apache Spark Programming Guide**: https://spark.apache.org/docs/latest/
3. **Delta Lake Documentation**: https://docs.delta.io/latest/index.html
4. **MLflow Documentation**: https://mlflow.org/docs/latest/index.html
5. **Apache Airflow Documentation**: https://airflow.apache.org/docs/
6. **FastAPI Documentation**: https://fastapi.tiangolo.com/
7. **Kubernetes Documentation**: https://kubernetes.io/docs/home/
8. **Databricks Documentation**: https://docs.databricks.com/

**Cloud Provider Documentation**:
1. **AWS EKS User Guide**: https://docs.aws.amazon.com/eks/
2. **AWS S3 Developer Guide**: https://docs.aws.amazon.com/s3/
3. **AWS RDS PostgreSQL Guide**: https://docs.aws.amazon.com/rds/
4. **Databricks on AWS**: https://docs.databricks.com/aws/index.html

**Best Practices & Patterns**:
1. **Databricks Delta Lake Best Practices**: https://docs.databricks.com/delta/best-practices.html
2. **Kubernetes Production Best Practices**: https://kubernetes.io/docs/setup/best-practices/
3. **MLOps Principles (Google)**: https://cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning
4. **12-Factor App Methodology**: https://12factor.net/ (cloud-native application design)
5. **Microservices Architecture Patterns**: https://microservices.io/patterns/

**Academic Papers**:
1. **Zaharia, M. et al. (2016)**: "Apache Spark: A Unified Engine for Big Data Processing", Communications of the ACM
2. **Armbrust, M. et al. (2020)**: "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores", VLDB
3. **Sculley, D. et al. (2015)**: "Hidden Technical Debt in Machine Learning Systems", NeurIPS

**Open-Source Projects**:
1. **pystdf**: Python library for parsing STDF files (https://github.com/cmars/pystdf)
2. **pytest-spark**: Pytest plugin for PySpark testing (https://github.com/malexer/pytest-spark)
3. **Locust**: Load testing framework (https://locust.io/)
4. **Prometheus**: Monitoring and alerting toolkit (https://prometheus.io/)
5. **Grafana**: Observability platform (https://grafana.com/)

**Internal Documentation** (P16-specific):
1. **Architecture Decision Records (ADRs)**: `docs/adr/` (Why Kafka? Why Delta Lake? Why Databricks?)
2. **Runbooks**: `docs/runbooks/` (Kafka lag troubleshooting, model accuracy drop, cost overrun)
3. **API Reference**: `docs/api_reference.md` (20+ FastAPI endpoints with examples)
4. **Deployment Guide**: `docs/deployment.md` (Docker Compose setup, AWS EKS provisioning)
5. **User Guides**: `docs/user_guides/` (Onboarding tutorial, Databricks SQL cheat sheet, MLflow quickstart)

### 22.3 Glossary

**A**
- **ACID**: Atomicity, Consistency, Isolation, Durability (database transaction properties)
- **ATE (Automatic Test Equipment)**: Semiconductor test platform (e.g., Advantest V93000, Teradyne)
- **Airflow**: Workflow orchestration platform (Apache Airflow)
- **A/B Testing**: Compare two model versions (Champion vs. Challenger) with traffic split

**B**
- **Bin**: Software test result category (1=Pass, 2-99=Fail with different failure modes)
- **Broker**: Kafka server instance (P16 runs 3 brokers)
- **Batch Prediction**: Predict on multiple samples in one API call (e.g., 100 wafers)

**C**
- **Consumer Lag**: Delay between Kafka message production and consumption (target <5 min)
- **CNN (Convolutional Neural Network)**: Deep learning architecture for image processing (P04 wafer maps)
- **CORS (Cross-Origin Resource Sharing)**: Browser security mechanism (allow API requests from different domains)

**D**
- **DAG (Directed Acyclic Graph)**: Airflow workflow definition (task dependencies)
- **DBU (Databricks Unit)**: Databricks billing unit (1 DBU = 1 hour compute, $0.30-0.60/DBU)
- **Delta Lake**: ACID storage layer for data lakes (Parquet + transaction log)
- **Die**: Single semiconductor chip on wafer (e.g., 1,500 die per wafer)
- **Drift**: Change in data distribution over time (model accuracy degradation)

**E**
- **Edge Die**: Die located in outer 2mm ring of wafer (higher failure rate, thermal stress)
- **Experiment**: MLflow collection of training runs for one project (e.g., `p01_xgboost`)
- **Executor**: Spark worker process (P16 uses 8 executors × 4 cores)

**F**
- **Feature Store**: Centralized repository for ML features (P16 uses Delta Lake)
- **Feature Engineering**: Derive features from raw data (e.g., wafer_yield from die-level bins)
- **FTR (Functional Test Record)**: STDF record for pass/fail tests

**G**
- **GNN (Graph Neural Network)**: Deep learning for graph-structured data (P10 test failure propagation)
- **Grafana**: Open-source observability platform (dashboards, alerts)

**H**
- **HBin (Hardware Bin)**: Physical sorting bin for tested die (separate from software bin)
- **HPA (Horizontal Pod Autoscaler)**: Kubernetes auto-scaling (2-20 FastAPI pods based on CPU)

**I**
- **Ingestion**: Process of importing data into pipeline (STDF → Kafka → Spark → Delta Lake)
- **IDDQ**: Quiescent supply current test (leakage current, critical automotive parameter)

**J**
- **JWT (JSON Web Token)**: Compact authentication token (OAuth2, Azure AD SSO)

**K**
- **Kafka**: Distributed event streaming platform (ingestion layer)
- **KPI (Key Performance Indicator)**: Measurable metric for success (e.g., <100ms latency)

**L**
- **Lineage**: Traceability from prediction → model → training data → raw STDF (ISO 26262 compliance)
- **Lot**: Collection of wafers processed together (e.g., TC41x_LOT123 = 25 wafers)

**M**
- **MLflow**: ML lifecycle platform (tracking, registry, deployment)
- **MTTR (Mean Time to Recovery)**: Average time to fix incident (target <15 min)

**N**
- **NPS (Net Promoter Score)**: User satisfaction metric (promoters % - detractors %, target >40)
- **NER (Named Entity Recognition)**: NLP task to extract entities (P03 RCA reports)

**O**
- **ONNX (Open Neural Network Exchange)**: Model format for optimized inference (2-5× speedup)
- **Outlier**: Data point >5 sigma from mean (detected in parametric tests)

**P**
- **Partition**: Kafka message sequence or Delta Lake data organization (P16 partitions by year/month/day)
- **PTR (Parametric Test Record)**: STDF record with measurement value (e.g., IDDQ=1.23e-6A)
- **Prometheus**: Monitoring system (metrics collection, alerting)

**Q**
- **Query**: SQL statement to retrieve data (Databricks SQL, Delta Lake)

**R**
- **RBAC (Role-Based Access Control)**: Permission system (data_engineer, ml_engineer, platform_engineer, test_engineer, yield_engineer)
- **Replica**: Kafka message copy (replication factor 3 = 3 replicas)
- **Rollback**: Revert to previous deployment (Kubernetes, MLflow model version)

**S**
- **SLA (Service Level Agreement)**: Uptime guarantee (99.9% = max 8 hours downtime/year)
- **STDF (Standard Test Data Format)**: Binary format for semiconductor test data (SEMI E5)
- **Synthetic Data**: Artificially generated data (P07 GAN creates 200GB synthetic STDFs)

**T**
- **Time-Travel**: Delta Lake feature to query historical data (`VERSION AS OF 123`)
- **TLS (Transport Layer Security)**: Encryption protocol (HTTPS, Kafka)

**U**
- **Uptime**: Percentage of time service is available (target 99.9%)
- **Unity Catalog**: Databricks metadata and governance layer

**V**
- **VACUUM**: Delta Lake operation to delete old file versions (reclaim storage)

**W**
- **Wafer**: Silicon disc with semiconductor chips (e.g., 300mm diameter, 1,500 die)
- **Wafer Yield**: Percentage of passing die (passed_die / total_die, e.g., 0.90 = 90%)

**X**
- **XGBoost**: Gradient boosting library (P01 bin predictor, P08 limit-change predictor)

**Y**
- **Yield**: Percentage of passing die (wafer-level, lot-level, or die-level)

**Z**
- **Z-Ordering**: Delta Lake optimization technique (cluster data by columns, 30-50% speedup)

---

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-12-04 | P16 Project Team | Initial PRD release (22 sections, 4,300+ lines) |

---

## Approval Signatures

**Product Owner**: _________________________ Date: _____________

**Engineering Manager**: _________________________ Date: _____________

**VP Engineering**: _________________________ Date: _____________

---

**END OF PRODUCT REQUIREMENTS DOCUMENT**

**Total Sections**: 22
**Total Pages**: 140+ (estimated at standard formatting)
**Total Lines**: 4,300+
**Estimated Reading Time**: 2-3 hours

**Document Status**: ✅ **COMPLETE**

---

