from datetime import datetime, timedelta
import os
import sys
import subprocess

# --- CONFIGURATION ---
# We force the environment variables here so Airflow (or the script) finds Java
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"
PROJECT_DIR = os.path.expanduser("~/lidar_project")
SRC_DIR = os.path.join(PROJECT_DIR, "src")

# Command template to run a python script with the correct Java environment
def get_pyspark_cmd(script_name):
    script_path = os.path.join(SRC_DIR, script_name)
    # We construct a shell command that sets JAVA_HOME before running
    return f"export JAVA_HOME={JAVA_HOME} && export PATH=$JAVA_HOME/bin:$PATH && python3 {script_path}"

# --- AIRFLOW DAG DEFINITION ---
# This block is what Airflow reads.
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator

    default_args = {
        'owner': 'lidar_team',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    dag = DAG(
        'lidar_processing_pipeline',
        default_args=default_args,
        description='Orchestrate LiDAR Voxelization and Export',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2025, 11, 26),
        catchup=False,
    )

    # Task 1: Run Spark Job (Voxelization)
    t1_spark_voxel = BashOperator(
        task_id='spark_voxelization',
        bash_command=get_pyspark_cmd("spark_voxel_job.py"),
        dag=dag,
    )

    # Task 2: Run Export (QGIS CSV)
    t2_export_qgis = BashOperator(
        task_id='export_to_vis',
        bash_command=get_pyspark_cmd("export_for_vis.py"),
        dag=dag,
    )

    # Task 3: Cleanup (Optional - just showing structure)
    t3_notify = BashOperator(
        task_id='notify_success',
        bash_command='echo "Pipeline Finished Successfully! Data ready for QGIS."',
        dag=dag,
    )

    # Define Dependencies (The Arrow Operator)
    t1_spark_voxel >> t2_export_qgis >> t3_notify

except ImportError:
    print("[WARN] Apache Airflow not found. Running in Standalone Simulation Mode.")

# --- LOCAL SIMULATION MODE (For your VM Demo) ---
# If you run this file directly, it executes the tasks sequentially just like Airflow would.
if __name__ == "__main__":
    print("==================================================")
    print("   AIRFLOW ORCHESTRATION SIMULATION (MANUAL RUN)  ")
    print("==================================================")
    
    # 1. Execute Spark Voxelization
    print(f"\n[ORCHESTRATOR] Starting Task 1: Spark Voxelization...")
    cmd1 = get_pyspark_cmd("spark_voxel_job.py")
    ret1 = subprocess.call(cmd1, shell=True)
    
    if ret1 != 0:
        print("[ORCHESTRATOR] Task 1 Failed! Stopping pipeline.")
        sys.exit(1)
        
    # 2. Execute QGIS Export
    print(f"\n[ORCHESTRATOR] Starting Task 2: Export to QGIS...")
    cmd2 = get_pyspark_cmd("export_for_qgis.py")
    ret2 = subprocess.call(cmd2, shell=True)

    if ret2 != 0:
        print("[ORCHESTRATOR] Task 2 Failed! Stopping pipeline.")
        sys.exit(1)

    # 3. Finish
    print("\n[ORCHESTRATOR] Pipeline Finished Successfully!")
    print(f"Data is ready in {os.path.join(PROJECT_DIR, 'qgis_output')}")
    print("==================================================")