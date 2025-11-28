# LiDAR Lake
**Real-time LiDAR Processing & Analytics Lakehouse**

---

## 📌 1. Executive Summary

This project implements a **Lambda Architecture** for processing autonomous vehicle LiDAR data. It handles the high-velocity ingestion of 3D point clouds, performs real-time cleaning, transforms raw points into structured **Voxels** (3D cubes), and stores them in an ACID-compliant **Data Lakehouse** (Apache Iceberg).

Beyond the core data pipeline, the project includes a robust **Observability Layer** (Prometheus & Grafana) to monitor system health and a **Visualization Layer** (CloudCompare) to validate data quality and semantic segmentation accuracy.

---

## 🏗️ 2. Architecture Overview

The system consists of **eight layers** covering the full lifecycle from sensor to insight:

1. **Ingestion Layer**: Kafka (Streaming Storage & Decoupling)
2. **Processing Layer**: Flink (Stateful Stream Computation)
3. **Storage Layer (Data Lake)**: HDFS (Raw, Distributed File Storage)
4. **Lakehouse Layer**: Spark + Iceberg (Voxelization & ACID Tables)
5. **Orchestration Layer**: Airflow (Workflow Management)
6. **Intelligence Layer**: TensorFlow/Keras (Voxel Classification)
7. **Observability Layer**: Prometheus & Grafana (System Monitoring)
8. **Visualization Layer**: CloudCompare (Quality Control)

---

## 🚀 3. Core Pipeline Implementation

### 🔹 Layer 1: Ingestion (Kafka)

- **Component**: Apache Kafka 3.9.0 (with Zookeeper)
- **Role**: Ingestion & Durable Buffer
- **Script**: `src/producer.py`

**Technical Implementation**:
- Utilized the `plyfile` library to parse binary `.ply` headers
- Serialized raw points into JSON payloads:
```json
  {"x": 1.2, "y": 3.4, "z": 0.5, "intensity": 10}
```
- Implemented a delay (`time.sleep(0.01)`) to simulate ~10Hz LiDAR rotation

---

### 🔹 Layer 2: Stream Processing (Flink)

- **Component**: Apache Flink 1.18.1
- **Role**: The Computational Streaming Layer
- **Script**: `src/flink_cleaning.py`

**Technical Implementation**:
- **Connector**: `flink-sql-connector-kafka-3.1.0-1.18.jar`
- Enabled checkpointing (5 seconds):
```python
  env.enable_checkpointing(5000)
```
- Noise filtering via Flink SQL (`WHERE label <> 0`)

---

### 🔹 Layer 3: The Data Lake (HDFS)

- **Component**: Hadoop HDFS (Raw Storage)

**Technical Implementation**:
- Flink writes rolling files based on size (128MB) or time (1 minute)
- Acts as the **Landing Zone** for clean data before Spark batch processing
- Chosen for high throughput with Spark

---

### 🔹 Layer 4: The Lakehouse (Spark & Iceberg)

- **Component**: Apache Spark 3.5 + Apache Iceberg
- **Script**: `src/spark_voxel_job.py`

**Technical Implementation**:
- **Voxelization Logic**: Rounded coordinates to integers using:
```python
  withColumn("voxel_x", round(col("x"), 0).cast("int"))
```
  representing 1m³ grids
- **Schema Inference**: Used `createOrReplace()` to infer Iceberg schema
- **Partitioning**: Configured:
```python
  partitionedBy("majority_label")
```
  enabling partition pruning (`label=7/`, etc.)

---

### 🔹 Layer 5: Orchestration (Airflow)

- **Component**: Apache Airflow
- **Script**: `src/lidar_pipeline.py`

**Technical Implementation**:
- **DAG**:
```
  flink_cleaning >> spark_voxelization >> export_to_qgis
```
- Used `BashOperator` to run Python scripts
- Injected `JAVA_HOME` explicitly due to worker environment differences

---

### 🔹 Layer 6: Machine Learning (TensorFlow)

- **Component**: TensorFlow/Keras
- **Script**: `src/ml_voxel_classifier.py`

**Technical Implementation**:
- Loaded Iceberg tables using Spark:
```python
  spark.read.table("local.lidar.voxels")
```
- Normalized intensity and color values using `StandardScaler`
- One-hot encoded labels (0–8)
- **Architecture**: Dense network (64 → 128 → 64, ReLU)
- Excluded `x` and `y` features to avoid spatial overfitting

---

## 📊 4. Observability & Monitoring Layer (Prometheus + Grafana)

To ensure stability, the system includes a complete monitoring stack.

### A. Infrastructure Setup

- **Node Exporter**: Installed on host VM; opened port 9100
- **Prometheus**: Configured via `prometheus.yml` for 15s scraping (port 9090)
- **Grafana**: Connected as data source on port 3000

### B. Key Technical Metrics (PromQL)

**CPU Usage**:
```promql
100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
```

**Available Memory**:
```promql
node_memory_MemAvailable_bytes
```

**Disk Space**:
```promql
node_filesystem_avail_bytes
```

---

## 🛰️ 5. Visualization & Quality Control Layer

**CloudCompare** is used to verify sensor data and AI model outputs.

### A. Technical Workflow

- **Global Shift**: Applied due to large UTM coordinates causing jitter
- **Scalar Fields**: Imported `Mavericks_classes_9.txt` to map label IDs → semantic class names
- **Clipping Fix**: Adjusted the Near Clipping Plane to avoid geometry disappearing in close-up views

### B. Value for AI Team

- Enabled visual validation of the voxel classification model
- Allowed overlay of true vs. predicted labels to identify misclassifications (e.g., sidewalk edges misidentified as fences)

---

## 🎯 Project Structure
```
lidar-lake/
├── src/
│   ├── producer.py              # Kafka ingestion
│   ├── flink_cleaning.py        # Stream processing
│   ├── spark_voxel_job.py       # Voxelization & Iceberg
│   ├── lidar_pipeline.py        # Airflow DAG
│   └── ml_voxel_classifier.py   # ML classification
├── config/
│   └── prometheus.yml           # Monitoring configuration
└── README.md
```

---

## 🚦 Getting Started

### Prerequisites

- Apache Kafka 3.9.0
- Apache Flink 1.18.1
- Apache Spark 3.5
- Apache Iceberg
- Apache Airflow
- TensorFlow/Keras
- Prometheus & Grafana
- CloudCompare

### Installation

1. Clone the repository
2. Configure Kafka, Flink, and Spark clusters
3. Set up Airflow DAGs
4. Configure monitoring stack (Prometheus + Grafana)
5. Run the scripts / DAGS:

---

## 📈 Monitoring

Access the monitoring dashboards:
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000
- **Node Exporter Metrics**: http://localhost:9100/metrics

---

## 👥 Contributors

Seif Alaa Sedky

Salma Ashraf

Mostafa Atef
