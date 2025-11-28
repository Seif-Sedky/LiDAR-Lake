import time
import json
import sys
import os
from kafka import KafkaProducer
from plyfile import PlyData

# --- CONFIGURATION ---
KAFKA_TOPIC = "lidar-raw"
KAFKA_SERVER = "localhost:9092"
# We only pick ONE file to keep it light
PLY_FILE_PATH = os.path.expanduser("~/lidar_project/data/L001.ply")
MAX_POINTS_TO_SEND = 5000  # Limit to prevent crashing

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"[ERROR] Could not connect to Kafka: {e}")
        sys.exit(1)

def stream_data():
    if not os.path.exists(PLY_FILE_PATH):
        print(f"[ERROR] File not found: {PLY_FILE_PATH}")
        print("Please ensure your .ply files are in ~/lidar_project/data/")
        return

    print(f"[INFO] Reading {PLY_FILE_PATH}...")
    try:
        plydata = PlyData.read(PLY_FILE_PATH)
        vertex_data = plydata['vertex'].data
    except Exception as e:
        print(f"[ERROR] Could not read PLY file: {e}")
        return
    
    producer = create_producer()
    count = 0
    
    print(f"[INFO] Streaming first {MAX_POINTS_TO_SEND} points to topic '{KAFKA_TOPIC}'...")
    print("[INFO] Press Ctrl+C to stop manually.")
    
    try:
        for i, point in enumerate(vertex_data):
            if count >= MAX_POINTS_TO_SEND:
                break

            # --- MAPPING SCHEMA ---
            # We explicitly map the .ply property names to our JSON keys here.
            record = {
                # Geometric Data
                "x": float(point['x']),
                "y": float(point['y']),
                "z": float(point['z']),
                
                # Color Data (uchar -> int)
                "red": int(point['red']),
                "green": int(point['green']),
                "blue": int(point['blue']),
                
                # LiDAR Specific Attributes
                "intensity": float(point['scalar_Intensity']),
                "timestamp": float(point['scalar_GPSTime']),
                "scan_angle": float(point['scalar_ScanAngleRank']),
                "label": int(point['scalar_Label']) # Cast float label to int (e.g. 1.0 -> 1)
            }
            
            producer.send(KAFKA_TOPIC, value=record)
            count += 1
            
            # Send in batches of 100 for visual feedback
            if count % 100 == 0:
                print(f" -> Sent {count} points...", end='\r')
                time.sleep(0.05) # 50ms delay per batch

        producer.flush()
        print(f"\n[DONE] Successfully streamed {count} points.")
        
    except KeyboardInterrupt:
        print("\n[STOPPED] Stream stopped by user.")

if __name__ == "__main__":
    stream_data()
