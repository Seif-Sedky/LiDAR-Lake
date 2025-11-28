import os
import sys
import glob
import shutil

# --- CRITICAL FIX: Force Java 17 ---
# Spark 3.5 binaries require Java 17 (Class version 61)
# We point to the standard Ubuntu location for OpenJDK 17
java_home = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["JAVA_HOME"] = java_home

# 2. PREPEND Java 17 bin to PATH
os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"

# 3. Unset SPARK_HOME to avoid conflicts with other installations
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# Ensure PySpark uses the same Python version as the script
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# --- CONFIGURATION ---
# 1. PATHS
# Input Directory (We removed *.json so we can point to the folder)
DATA_DIR = os.path.expanduser('~/lidar_project/hdfs_sim/cleaned_data')
INPUT_PATH = f"file://{DATA_DIR}"
WAREHOUSE_PATH = os.path.expanduser('~/lidar_project/iceberg_warehouse')

# 2. ICEBERG JAR
ICEBERG_JAR = "/home/seiflinux/.ivy2/cache/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"

def fix_flink_output_files():
    """
    Flink writes hidden files like '.part-0-0.inprogress...'.
    This function renames them to normal 'part-0-0.json' files so Spark can read them.
    """
    print("[INFO] Checking for hidden Flink files...")
    if not os.path.exists(DATA_DIR):
        print(f"[WARNING] Data directory does not exist: {DATA_DIR}")
        return

    # Find all files starting with .part
    hidden_files = glob.glob(os.path.join(DATA_DIR, ".part*"))
    
    renamed_count = 0
    for hidden_file in hidden_files:
        # logic: /path/.part-uuid.inprogress -> /path/part-uuid.json
        directory, filename = os.path.split(hidden_file)
        
        # Remove leading dot
        new_filename = filename.lstrip('.')
        
        # Remove .inprogress and trailing garbage if present
        if '.inprogress' in new_filename:
            new_filename = new_filename.split('.inprogress')[0]
        
        # Ensure it ends in .json
        if not new_filename.endswith('.json'):
            new_filename += ".json"
            
        new_path = os.path.join(directory, new_filename)
        
        try:
            shutil.move(hidden_file, new_path)
            renamed_count += 1
        except Exception as e:
            print(f"[WARN] Could not rename {filename}: {e}")

    if renamed_count > 0:
        print(f"[SUCCESS] Renamed {renamed_count} hidden Flink files to readable JSON.")
    else:
        print("[INFO] No hidden files found (or already renamed).")

def create_spark_session():
    print(f"[INFO] Using JAVA_HOME: {os.environ.get('JAVA_HOME')}")
    print("[INFO] Starting Spark Session with Iceberg...")
    
    if not os.path.exists(ICEBERG_JAR):
        print(f"[WARNING] Iceberg JAR not found at: {ICEBERG_JAR}")

    return SparkSession.builder \
        .appName("LiDAR Voxelizer") \
        .config("spark.jars", ICEBERG_JAR) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH) \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

def run_job():
    # 1. Fix the data first!
    fix_flink_output_files()

    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
    except Exception as e:
        print("\n[CRITICAL ERROR] Spark failed to start.")
        print(f"Error details: {e}")
        return

    # 2. Define Schema for JSON Input
    input_schema = StructType([
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("z", DoubleType(), True),
        StructField("red", IntegerType(), True),
        StructField("green", IntegerType(), True),
        StructField("blue", IntegerType(), True),
        StructField("intensity", DoubleType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("scan_angle", DoubleType(), True),
        StructField("label", IntegerType(), True)
    ])

    print(f"[INFO] Reading JSON data from {INPUT_PATH}...")
    try:
        raw_df = spark.read.schema(input_schema).json(INPUT_PATH)
        count = raw_df.count()
        if count == 0:
            print("[ERROR] No data found! Did Flink finish writing files?")
            return
        print(f"[INFO] Loaded {count} raw points.")
    except Exception as e:
        print(f"[ERROR] Could not read files: {e}")
        return

    # 3. The "Value Add" Transformation: Voxelization
    print("[INFO] Performing Voxelization (Aggregation)...")
    voxel_df = raw_df.withColumn("voxel_x", F.round(F.col("x"), 0).cast("int")) \
                     .withColumn("voxel_y", F.round(F.col("y"), 0).cast("int")) \
                     .withColumn("voxel_z", F.round(F.col("z"), 0).cast("int"))

    # 4. Aggregation Logic
    final_df = voxel_df.groupBy("voxel_x", "voxel_y", "voxel_z") \
        .agg(
            F.avg("intensity").alias("avg_intensity"),
            F.expr("mode(label)").alias("majority_label"),
            F.count("*").alias("point_count"),
            F.avg("red").cast("int").alias("avg_red"),
            F.avg("green").cast("int").alias("avg_green"),
            F.avg("blue").cast("int").alias("avg_blue")
        )

    # 5. Write to Iceberg
    print("[INFO] Writing to Iceberg Table: local.lidar.voxels")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lidar")
    
    try:
        final_df.writeTo("local.lidar.voxels") \
            .using("iceberg") \
            .partitionedBy("majority_label") \
            .createOrReplace()
        
        print("[SUCCESS] Data successfully written to Iceberg!")
        print("------------------------------------------------")
        print("Sample Voxel Data from Iceberg:")
        spark.read.table("local.lidar.voxels").show(5)
        
    except Exception as e:
        print(f"[ERROR] Failed to write to Iceberg: {e}")

if __name__ == "__main__":
    run_job()