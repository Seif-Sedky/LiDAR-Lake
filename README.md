# LiDAR Lake  
### **Real-time LiDAR Processing & Analytics Lakehouse**

---

## 📌 1. Executive Summary

This project implements a Lambda Architecture for processing autonomous vehicle LiDAR data. It handles the high-velocity ingestion of 3D point clouds, performs real-time cleaning, transforms raw points into structured Voxels (3D cubes), and stores them in an ACID-compliant Data Lakehouse (Apache Iceberg).

Beyond the core data pipeline, the project includes a robust Observability Layer (Prometheus & Grafana) to monitor system health and a Visualization Layer (CloudCompare) to validate data quality and semantic segmentation accuracy.

---

## 🏗️ 2. Architecture Overview

The system consists of eight layers covering the full lifecycle from sensor to insight:

- **Ingestion Layer:** Kafka (Streaming Storage & Decoupling)  
- **Processing Layer:** Flink (Stateful Stream Computation)  
- **Storage Layer (Data Lake):** HDFS (Raw, Distributed File Storage)  
- **Lakehouse Layer:** Spark + Iceberg (Voxelization & ACID Tables)  
- **Orchestration Layer:** Airflow (Workflow Management)  
- **Intelligence Layer:** TensorFlow/Keras (Voxel Classification)  
- **Observability Layer:** Prometheus & Grafana (System Monitoring)  
- **Visualization Layer:** CloudCompare (Quality Control)

---

## 🚀 3. Core Pipeline Implementation

---

### **🔹 Layer 1: Ingestion (Kafka)**

**Component:** Apache Kafka 3.9.0 (with Zookeeper)  
**Role:** Ingestion & Durable Buffer  
**Script:** `src/producer.py`

**Technical Details**
- Parsed binary `.ply` headers using `plyfile`.  
- Serialized raw points into JSON:
  ```json
  {"x": 1.2, "y": 3.4, "z": 0.5, "intensity": 10}
