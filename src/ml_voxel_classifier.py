import os
import sys
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers, models, utils
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import classification_report

# --- 1. SETUP ENVIRONMENT (Using your known working configuration) ---
java_home = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["JAVA_HOME"] = java_home
os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

# Configuration
WAREHOUSE_PATH = os.path.expanduser('~/lidar_project/iceberg_warehouse')
ICEBERG_JAR = "/home/seiflinux/.ivy2/cache/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"

def get_data_from_lakehouse():
    """
    Connects to Iceberg and fetches the clean Voxel data.
    """
    print("[INFO] Connecting to Iceberg Lakehouse...")
    spark = SparkSession.builder \
        .appName("LiDAR ML Trainer") \
        .config("spark.jars", ICEBERG_JAR) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH) \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Read the Voxel Table
    # We select only the features relevant for classification + the label
    # ADDED: voxel_z (Height) because it helps distinguish Ground/Cars/Poles
    df = spark.read.table("local.lidar.voxels").select(
        "voxel_z",
        "avg_intensity", 
        "point_count", 
        "avg_red", 
        "avg_green", 
        "avg_blue", 
        "majority_label"
    )
    
    print(f"[INFO] Loading data into Memory (Pandas)...")
    # Since Voxels are highly compressed (1000x smaller than raw points), 
    # we can safely convert to Pandas for Keras.
    pdf = df.toPandas()
    print(f"[INFO] Loaded {len(pdf)} voxels for training.")
    return pdf

def train_voxel_classifier(df):
    """
    Trains a Neural Network to classify voxels based on their properties.
    """
    # --- 2. PREPROCESSING ---
    # Features: Height, Intensity, Density, Color (RGB)
    # Excluded: voxel_x, voxel_y (To prevent overfitting to location)
    X = df[["voxel_z", "avg_intensity", "point_count", "avg_red", "avg_green", "avg_blue"]].values
    
    # Target: The Class Label (Car, Ground, etc.)
    y = df["majority_label"].values

    # Normalize Features (Crucial for Neural Networks)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # One-Hot Encode Labels (e.g., Class 7 -> [0,0,0,0,0,0,0,1])
    encoder = LabelEncoder()
    y_encoded = encoder.fit_transform(y)
    y_categorical = utils.to_categorical(y_encoded)
    
    # Split Data
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y_categorical, test_size=0.2, random_state=42)

    print(f"[INFO] Training on {X_train.shape[0]} samples, Validating on {X_test.shape[0]}...")

    # --- 3. MODEL ARCHITECTURE ---
    # We use a simple Feed-Forward Neural Network
    model = models.Sequential([
        # Input Layer: 6 Features (Height, Intensity, Count, R, G, B)
        layers.Dense(64, activation='relu', input_shape=(X_train.shape[1],)),
        layers.Dropout(0.2), # Prevents overfitting
        layers.Dense(32, activation='relu'),
        # Output Layer: One neuron per class, Softmax for probability
        layers.Dense(y_categorical.shape[1], activation='softmax')
    ])

    model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    # --- 4. TRAINING ---
    history = model.fit(
        X_train, y_train, 
        epochs=10,  # Voxel data converges fast
        batch_size=32, 
        validation_split=0.1,
        verbose=1
    )

    # --- 5. EVALUATION ---
    print("\n[INFO] Evaluating Model...")
    loss, accuracy = model.evaluate(X_test, y_test)
    print(f"✅ Test Accuracy: {accuracy:.4f}")
    
    # Save for future use
    model.save("voxel_classifier.keras")
    print("[INFO] Model saved as 'voxel_classifier.keras'")

if __name__ == "__main__":
    # 1. Get Data from the Lake
    data = get_data_from_lakehouse()
    
    # 2. Train the AI
    if not data.empty:
        train_voxel_classifier(data)
    else:
        print("[ERROR] No data found in Iceberg. Run the Spark Voxel Job first!")