# PRD_next: Enterprise ML Data Pipeline Improvements

## Project Overview
This repository showcases a sophisticated 6-layer enterprise ML data pipeline, implementing a Lambda architecture to process data efficiently. It has dramatically reduced processing time from 83 hours to under 1 hour and manages over 5000 MLflow experiments. The project features automated Airflow DAGs for orchestration and a FastAPI service for model serving with sub-100ms response times. Existing components include Docker for containerization and AWS S3 for storage, making it a robust foundation for MLOps practices.

## Current State Assessment

| Feature | Status | Details |
|---|---|---|
| **Model Serving** | ✅ Present | FastAPI serving with <100ms response times. |
| **Containerization** | ✅ Present | Docker and likely `docker-compose.yml`, `Dockerfile` exist. |
| **MLflow Integration** | ✅ Present | Over 5000 MLflow experiments, likely includes experiment tracking, model registry, and artifact logging. |
| **Model Monitoring** | ❌ Missing | No explicit mention of Evidently AI or similar model monitoring solutions. |
| **Kubernetes Deployment** | ❌ Missing | No mention of Kubernetes/Helm charts. |
| **Cloud Deployment** | ✅ Present (Partial) | AWS S3 is used, but full cloud deployment (e.g., AWS EKS) is not explicitly configured. |
| **CI/CD for ML** | ❌ Missing | No mention of GitHub Actions CI/CD workflows. |
| **Airflow/Orchestration** | ✅ Present | Automated Airflow DAGs for pipeline orchestration. |
| **Big Data** | ✅ Present | Kafka and Spark integration. |
| **API Testing** | ❌ Missing | No explicit mention of `pytest` or integration tests for the FastAPI. |

## Gap Analysis with Priority

| Gap | Priority | Rationale |
|---|---|---|
| **Kubernetes Deployment** | High | Essential for scalable, resilient, and portable MLOps deployments. Demonstrates advanced infrastructure skills. |
| **CI/CD for ML** | High | Automates the entire ML lifecycle, ensuring consistent and rapid deployment of models and pipelines. Crucial for production-grade MLOps. |
| **Model Monitoring (Evidently AI)** | High | Critical for detecting data drift, concept drift, and model performance degradation in production, ensuring model reliability and maintainability. |
| **API Testing** | Medium | Ensures the reliability and correctness of the FastAPI endpoints, preventing regressions and improving code quality. |
| **Full Cloud Deployment (AWS EKS)** | Medium | While AWS S3 is used, a complete EKS deployment would showcase end-to-end cloud MLOps capabilities. |

## Recommended Improvements with Detailed Implementation Steps

### 1. Implement Kubernetes Deployment with Helm Charts
**Estimated Effort:** 3-5 days
**Description:** Containerize the entire application stack (FastAPI, Kafka, Spark, MLflow, PostgreSQL) and deploy it to a Kubernetes cluster using Helm charts for simplified management and scalability.
**Implementation Steps:**
1.  **Containerize Services:** Ensure all components (FastAPI, Kafka, Spark, MLflow, PostgreSQL) have optimized Dockerfiles. Create separate Docker images for each service if not already done.
2.  **Kubernetes Manifests:** Write `deployment.yaml`, `service.yaml`, `ingress.yaml` for each service. Configure resource limits, liveness/readiness probes, and scaling policies.
3.  **Helm Chart Creation:** Develop a Helm chart for the entire application. This includes `Chart.yaml`, `values.yaml`, and templates for all Kubernetes resources. Parameterize configurations for different environments.
4.  **Local Testing:** Deploy the Helm chart to a local Kubernetes cluster (e.g., Minikube or Kind) to verify functionality.
5.  **Integration with CI/CD:** Prepare for integration with GitHub Actions for automated deployment.

### 2. Establish CI/CD for ML with GitHub Actions
**Estimated Effort:** 2-3 days
**Description:** Automate the build, test, and deployment process for the ML pipeline and FastAPI service using GitHub Actions.
**Implementation Steps:**
1.  **Workflow for FastAPI:** Create a `.github/workflows/fastapi_ci.yml` to build the Docker image, run API tests, and push to a container registry (e.g., ECR).
2.  **Workflow for ML Pipeline:** Create a `.github/workflows/ml_pipeline_ci.yml` to trigger Airflow DAGs for training, run data validation, and register models in MLflow.
3.  **Deployment Workflow:** Create a `.github/workflows/cd.yml` to deploy the Helm chart to the Kubernetes cluster upon successful CI runs.
4.  **Secrets Management:** Securely store AWS credentials and other sensitive information as GitHub Secrets.

### 3. Integrate Model Monitoring with Evidently AI
**Estimated Effort:** 2-3 days
**Description:** Implement Evidently AI to monitor data drift, concept drift, and model performance in production, providing alerts and dashboards.
**Implementation Steps:**
1.  **Evidently AI Setup:** Install Evidently AI and integrate it into the ML pipeline. This might involve adding a step in Airflow DAGs to generate Evidently reports.
2.  **Data Collection:** Collect production inference data and ground truth data to feed into Evidently AI for analysis.
3.  **Report Generation:** Configure Evidently AI to generate HTML reports or JSON metrics for data quality, data drift, and model performance.
4.  **Dashboard Integration:** Integrate Evidently reports into a dashboard (e.g., using Streamlit or a custom web interface) or send alerts to a communication channel (e.g., Slack).

### 4. Implement Comprehensive API Testing
**Estimated Effort:** 1-2 days
**Description:** Develop unit and integration tests for the FastAPI endpoints to ensure their correctness and robustness.
**Implementation Steps:**
1.  **Pytest Setup:** Install `pytest` and `httpx` (or `requests`) for testing the FastAPI application.
2.  **Unit Tests:** Write unit tests for individual functions and components of the FastAPI application.
3.  **Integration Tests:** Create integration tests to verify the end-to-end flow of API endpoints, including interactions with MLflow, Kafka, and PostgreSQL.
4.  **Test Automation:** Integrate API tests into the GitHub Actions CI workflow.

### 5. Full Cloud Deployment to AWS EKS
**Estimated Effort:** 3-4 days
**Description:** Deploy the Kubernetes cluster and all services to AWS Elastic Kubernetes Service (EKS), leveraging AWS managed services for scalability and reliability.
**Implementation Steps:**
1.  **EKS Cluster Creation:** Use `eksctl` or AWS CloudFormation to provision an EKS cluster.
2.  **IAM Roles and Policies:** Configure appropriate IAM roles and policies for EKS and associated AWS services (e.g., S3, ECR).
3.  **Container Registry:** Push Docker images to AWS Elastic Container Registry (ECR).
4.  **Helm Deployment:** Deploy the Helm chart to the EKS cluster. Configure `values.yaml` for AWS-specific settings (e.g., AWS Load Balancer Controller for Ingress).
5.  **Monitoring and Logging:** Set up AWS CloudWatch for EKS cluster monitoring and logging.

## Best Interview Topics This Project Demonstrates After Improvements
This project, with the proposed improvements, would be an excellent demonstration of **End-to-End MLOps on AWS**, specifically highlighting: **Kubernetes Deployment with Helm**, **CI/CD for ML with GitHub Actions**, **Model Monitoring with Evidently AI**, **Scalable Data Pipelines (Kafka/Spark/Delta Lake)**, and **High-Performance Model Serving (FastAPI)**.

## Cloud Deployment Plan (AWS Specific)

**Target Service:** AWS Elastic Kubernetes Service (EKS)

**Architecture:**
*   **EKS Cluster:** A managed Kubernetes cluster for orchestrating containerized applications.
*   **ECR:** Elastic Container Registry for storing Docker images.
*   **S3:** Existing S3 buckets for data storage and MLflow artifacts.
*   **RDS (PostgreSQL):** Managed PostgreSQL database for metadata and application data.
*   **MSK (Kafka):** Managed Streaming for Kafka for the Kafka cluster.
*   **EMR (Spark):** Elastic MapReduce for Spark workloads (or Spark on Kubernetes).
*   **ALB/NLB:** AWS Load Balancer for Ingress to the FastAPI service.
*   **CloudWatch/Prometheus/Grafana:** For comprehensive monitoring and logging.

**Deployment Steps:**
1.  **Infrastructure Provisioning:** Use `terraform` or `eksctl` to provision the EKS cluster, VPC, subnets, and other necessary AWS resources.
2.  **Service Deployment:** Deploy the Helm chart containing all application components (FastAPI, MLflow, Airflow, etc.) to the EKS cluster.
3.  **Data Services:** Configure MSK for Kafka and RDS for PostgreSQL. Integrate Spark with EMR or deploy Spark on Kubernetes.
4.  **Networking:** Set up AWS Load Balancer Controller for Ingress to expose the FastAPI service externally.
5.  **Monitoring:** Deploy Prometheus and Grafana within the EKS cluster for application and infrastructure monitoring. Integrate with CloudWatch for centralized logging.
6.  **CI/CD Integration:** Configure GitHub Actions to automatically deploy updates to the EKS cluster.

## Quick Win (1-2 days)

The single most impactful improvement achievable in 1-2 days is to **implement comprehensive API testing for the FastAPI service using `pytest`**. This will immediately improve the reliability of the model serving endpoint and provide a solid foundation for future CI/CD integration. It involves creating a `tests/` directory, writing unit and integration tests for the FastAPI routes, and ensuring all endpoints behave as expected. This quick win directly addresses a critical quality assurance gap and provides immediate value by increasing confidence in the existing model serving infrastructure.
