LiDAR Lake: Comprehensive Technical Documentation

Project: Real-time LiDAR Processing & Analytics Lakehouse

1. Executive Summary

This project implements a Lambda Architecture for processing autonomous vehicle LiDAR data. It handles the high-velocity ingestion of 3D point clouds, performs real-time cleaning, transforms raw points into structured Voxels (3D cubes), and stores them in an ACID-compliant Data Lakehouse (Apache Iceberg).

Beyond the core data pipeline, the project includes a robust Observability Layer (Prometheus & Grafana) to monitor system health and a Visualization Layer (CloudCompare) to validate data quality and semantic segmentation accuracy.

2. Architecture Overview

The system is composed of eight distinct layers, covering the lifecycle of data from sensor to insight:

Ingestion Layer: Kafka (Streaming Storage & Decoupling).

Processing Layer: Flink (Stateful Stream Computation).

Storage Layer (Data Lake): HDFS (Raw, Distributed File Storage).

Lakehouse Layer: Spark + Iceberg (Voxelization & ACID Tables).

Orchestration Layer: Airflow (Workflow Management).

Intelligence Layer: TensorFlow/Keras (Voxel Classification).

Observability Layer: Prometheus & Grafana (System Monitoring).

Visualization Layer: CloudCompare (Quality Control).

3. Core Pipeline Implementation

Layer 1: Ingestion (Kafka)

Component: Apache Kafka 3.9.0 (with Zookeeper)

Role: While Kafka is a streaming platform, its primary role here is Ingestion and Durable Buffer. It stores the raw stream to ensure no data is lost if the processing layer (Flink) goes down.

Script: src/producer.py

Technical Implementation:

Utilized the plyfile library to parse binary .ply headers.

Serialized raw points into JSON payloads: {"x": 1.2, "y": 3.4, "z": 0.5, "intensity": 10...}.

Simulation: Implemented a thread sleep delay (time.sleep(0.01)) between chunks to mimic the ~10Hz frequency of a rotating LiDAR sensor, avoiding a "batch dump" effect.

Layer 2: Stream Processing (Flink)

Component: Apache Flink 1.18.1

Role: The Computational Streaming Layer. It acts on the data while it is in motion.

Script: src/flink_cleaning.py

Technical Implementation:

Connector: Used flink-sql-connector-kafka-3.1.0-1.18.jar to consume the lidar-raw topic.

Checkpointing: Crucial technical fix. We enabled env.enable_checkpointing(5000) (5 seconds). Without this, Flink's file sink kept files in an open .inprogress state (hidden), preventing downstream tools from reading them.

Logic: Filtered out noise points (Label 0) using Flink SQL (WHERE label <> 0).

Layer 3: The Data Lake (HDFS)

Component: Hadoop HDFS (Raw Storage)

Technical Implementation:

Flink writes Rolling Files to this layer based on size (128MB) or time (1 minute).

Serves as the "Landing Zone" for clean data before heavy batch processing.

Why HDFS: Provides high throughput for the Spark batch jobs in the next layer.

Layer 4: The Lakehouse (Spark & Iceberg)

Component: Apache Spark 3.5 + Apache Iceberg

Script: src/spark_voxel_job.py

Technical Implementation:

Voxelization Logic: We used Spark SQL functions to round coordinates to integers: withColumn("voxel_x", round(col("x"), 0).cast("int")). This aggregates points into $1m^3$ grids.

Schema Inference: Utilized createOrReplace() to automatically infer the Iceberg schema from the Spark DataFrame, avoiding manual DDL statements.

Partitioning: Configured partitionedBy("majority_label"). This physically separates data folders (e.g., label=7/), allowing partition pruning.

Layer 5: Orchestration (Airflow)

Component: Apache Airflow (DAG Definition)

Script: src/lidar_pipeline.py

Technical Implementation:

Defined a DAG (Directed Acyclic Graph) with dependencies: flink_cleaning >> spark_voxelization >> export_to_qgis (or cloud compare).

Used BashOperator to execute the Python scripts.

Environment Management: We had to explicitly inject JAVA_HOME into the Airflow command strings (export JAVA_HOME=... && python3 ...) because the Airflow worker environment often differs from the user shell.

Layer 6: Machine Learning (TensorFlow)

Component: TensorFlow/Keras

Script: src/ml_voxel_classifier.py

Technical Implementation:

Data Loading: Instead of using file paths, the script connects to the Iceberg Catalog: spark.read.table("local.lidar.voxels").

Preprocessing: Used StandardScaler to normalize intensity and color values (0-255 -> 0-1) and to_categorical for one-hot encoding labels (0-8).

Model Architecture: A Feed-Forward Neural Network (Dense Layers 64 -> 128 -> 64) with ReLU activation.

Constraint: We explicitly excluded x and y coordinates from training to prevent the model from overfitting to specific locations (Spatial Invariance).

4. Observability & Monitoring Layer (The Team Contribution)

To ensure system stability, we implemented a monitoring stack using Prometheus and Grafana.

A. Infrastructure Setup

Node Exporter: Installed on the host VM to expose hardware metrics. We had to open firewall port 9100 using sudo firewall-cmd --add-port=9100/tcp.

Prometheus: Configured via prometheus.yml to scrape targets every 15s. Accessed on port 9090.

Grafana: Connected to Prometheus as a data source on port 3000.

B. Key Technical Metrics (PromQL)

We crafted specific queries to monitor pipeline health:

CPU Usage:

100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)


Insight: Used to detect when Spark jobs spiked CPU to 100%.

Available Memory:

node_memory_MemAvailable_bytes


Insight: Critical for preventing OOM (Out of Memory) kills during Flink processing.

Disk Space: node_filesystem_avail_bytes to monitor the HDFS/MinIO volume growth.

5. Visualization & Quality Control Layer

We utilized CloudCompare as a ground-truth verification tool for both the raw sensor data and the AI model outputs.

A. Technical Workflow

Global Shift: Upon import, we had to apply a "Global Shift" to the coordinates because the raw UTM values were too large for standard 32-bit float rendering (causing the "jittering" effect).

Scalar Fields: We manually imported the Mavericks_classes_9.txt label file to map integer IDs (0-8) to human-readable names (Road, Car, etc.).

Clipping Fix: We adjusted the camera "Near Clipping Plane" in display settings to prevent close-up geometry from disappearing during inspection.

B. Value for AI Team

This layer was not just for pretty pictures; it was a debugging tool for the AI team:

Visual Validation: By overlaying the "Predicted Label" color map against the "True Label" color map, we could visually identify edge cases where the model failed (e.g., misclassifying the edge of a sidewalk as a fence).