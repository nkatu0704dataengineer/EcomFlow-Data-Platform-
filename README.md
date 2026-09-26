# 🚀 EcomFlow Data Platform

![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=influxdb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## 📌 Executive Summary
**EcomFlow Data Platform** is a modern, fully automated E-commerce Data Pipeline designed to track, process, and extract insights from customer data. The system features a decoupled architecture where **Apache Airflow** acts as the orchestrator and **Databricks (Spark)** handles heavy compute workloads. 

To prevent "blind execution" and ensure operational excellence, the platform is equipped with a comprehensive **TIG Stack (Telegraf, InfluxDB, Grafana)** for end-to-end data pipeline observability.

---

## 🏛️ High-Level Architecture

The platform operates on a strict **Medallion Architecture**, seamlessly ingesting raw tracking data and transforming it into aggregated business metrics.

```mermaid
graph TD
    subgraph Data Sources
        SRC1[Web Tracking]
        SRC2[App Events]
        SRC3[Transactions]
    end

    subgraph Medallion Architecture (Databricks / Spark)
        BRONZE[(🥉 Bronze<br/>Raw / Immutable)]
        SILVER[(🥈 Silver<br/>Cleansed / Filtered)]
        GOLD[(🥇 Gold<br/>Business Metrics)]
    end

    subgraph Orchestration
        AIRFLOW((Apache Airflow<br/>Orchestrator))
    end

    subgraph TIG Observability Stack
        TELEGRAF(Telegraf<br/>StatsD Receiver)
        INFLUX[(InfluxDB)]
        GRAFANA(Grafana Dashboards)
    end

    %% Data Flow
    SRC1 --> BRONZE
    SRC2 --> BRONZE
    SRC3 --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD

    %% Orchestration Flow
    AIRFLOW -.Triggers.-> BRONZE
    AIRFLOW -.Triggers.-> SILVER
    AIRFLOW -.Triggers.-> GOLD

    %% Observability Flow
    AIRFLOW -- UDP :8125 --> TELEGRAF
    TELEGRAF --> INFLUX
    INFLUX --> GRAFANA
```

---

## 🛠️ Technology Stack

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Compute & Processing** | Databricks (Apache Spark) | Heavy lifting for big data transformations. |
| **Orchestration** | Apache Airflow (Astronomer) | Dependency management and pipeline scheduling. |
| **Observability Agent** | Telegraf | Receives StatsD metrics from Airflow over UDP. |
| **Time-Series Database** | InfluxDB | High-performance metrics storage. |
| **Visualization** | Grafana | Operational dashboards (Airflow Health, Pipeline Performance). |
| **Infrastructure** | Docker & Docker Compose | Containerized environments for local development. |

---

## 🔄 Data Pipeline Workflow (The Medallion Approach)

The system relies on 4 independent Directed Acyclic Graphs (DAGs) orchestrated by a master controller (`ecomflow_master`).

1. 🥉 **Bronze Layer (`ecomflow_bronze`)**: Ingestion of raw e-commerce tracking data. Data is immutable, serving as the ultimate historical source of truth.
2. 🥈 **Silver Layer (`ecomflow_silver`)**: Schema enforcement, data cleansing, deduplication, and dropping corrupted records. Ready for Data Analysts.
3. 🥇 **Gold Layer (`ecomflow_gold`)**: Transformation and aggregation to produce key business metrics (e.g., Daily Active Users, Conversion Rates, GMV). Ready for BI tools, ML models, and stakeholders.

---

## 📂 Repository Structure

```text
EcomFlow/
├── dags/                        # Airflow DAG definitions
│   ├── ecomflow_master.py
│   ├── ...
├── include/
│   └── framework/               # PySpark transformation logic
│       ├── bronze_spark/
│       ├── silver_spark/
│       └── gold_spark/
├── infra/
│   └── observability/
│       └── tig/                 # TIG Stack configuration
│           ├── docker-compose-observability-tig.yml
│           ├── grafana/         # Pre-provisioned dashboards
│           ├── statsd.yml
│           └── telegraf.yml
├── notebooks/                   # Databricks notebook execution scripts
├── Dockerfile                   # Astronomer Runtime configuration
├── requirements.txt             # Python dependencies
└── .env                         # Environment variables (Credentials)
```

---

## 🚀 Setup & Installation Guide

### Step 1: Prerequisites
- **Docker Desktop** installed and running.
- **Astro CLI** (Astronomer CLI) installed (`curl -sL https://install.astronomer.io | sudo bash`).
- A **Databricks** Workspace (with a generated Personal Access Token).

### Step 2: Spin Up the Infrastructure

**1. Start the TIG Observability Stack:**
```bash
cd infra/observability/tig
docker-compose -f docker-compose-observability-tig.yml up -d
```
*Wait for the containers to initialize. Grafana will be available at `http://localhost:3000`.*

**2. Start Apache Airflow (Astro CLI):**
```bash
# Return to the project root
cd ../../../
astro dev start
```
*Airflow UI will be available at `http://localhost:8080` (admin/admin).*

### Step 3: Configure Databricks Connection
In the Airflow UI:
1. Navigate to **Admin -> Connections**.
2. Add a new connection:
   - **Connection Id**: `databricks_default`
   - **Connection Type**: `Databricks`
   - **Host**: `https://<your-databricks-instance>.cloud.databricks.com`
   - **Password**: `<your-databricks-personal-access-token>`

### Step 4: Trigger the Pipeline
1. In the Airflow UI, locate the `ecomflow_master` DAG.
2. Toggle the switch to **Unpause**.
3. Click the **Trigger DAG** (Play) button to start the end-to-end Medallion pipeline.

---

## 📈 Observability & Monitoring (TIG Stack)

EcomFlow integrates a zero-touch monitoring setup. Airflow emits internal metrics via **StatsD (UDP 8125)** directly into **Telegraf**. 

Grafana comes pre-provisioned with **3 Core Dashboards** to monitor system health:

1. 🌍 **EcomFlow Overview**: A holistic view of task successes, queue depths, and general workload.
2. 🫀 **Airflow Health**: Monitors the scheduler heartbeat, critical section durations, and parsing performance to prevent orchestration bottlenecks.
3. ⏱️ **Pipeline Performance**: Advanced SLA tracking featuring separated DAG Run Duration metrics (Bronze vs. Silver vs. Gold vs. Master) and task throughput rates.

> **Note:** Dashboards are managed via code (`infra/observability/tig/grafana/provisioning/dashboards/json/`) ensuring they are version-controlled and reproducible.
