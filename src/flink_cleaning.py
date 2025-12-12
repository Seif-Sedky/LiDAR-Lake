import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# --- CONFIGURATION ---
LIB_JAR_PATH = f"file://{os.path.expanduser('~/lidar_project/lib/flink-sql-connector-kafka-3.1.0-1.18.jar')}"
OUTPUT_PATH = f"file://{os.path.expanduser('~/lidar_project/hdfs_sim/cleaned_data')}"

def run_cleaning_job():
    # 1. Setup Flink Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    env.enable_checkpointing(5000) # Checkpoint every 5s
    
    # 2. Setup Table Environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # 3. Add Kafka Connector JAR
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", LIB_JAR_PATH
    )

    print("[INFO] Flink Job Started...")
    print(f"[INFO] Writing ALL data (No Filter) to: {OUTPUT_PATH}")

    # 4. Define Source Table
    t_env.execute_sql("""
        CREATE TABLE raw_lidar_stream (
            x DOUBLE,
            y DOUBLE,
            z DOUBLE,
            red INT,
            green INT,
            blue INT,
            intensity DOUBLE,
            `timestamp` DOUBLE,
            scan_angle DOUBLE,
            label INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'lidar-raw',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-cleaning-group-v2', -- New group to force re-read
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 5. Define Sink Table
    t_env.execute_sql(f"""
        CREATE TABLE hdfs_sink (
            x DOUBLE,
            y DOUBLE,
            z DOUBLE,
            red INT,
            green INT,
            blue INT,
            intensity DOUBLE,
            `timestamp` DOUBLE,
            scan_angle DOUBLE,
            label INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{OUTPUT_PATH}',
            'format' = 'json' 
        )
    """)

    # 6. Transformation 
    t_env.execute_sql("""
        INSERT INTO hdfs_sink
        SELECT 
            x, y, z, 
            red, green, blue, 
            intensity, `timestamp`, scan_angle, 
            label
        FROM raw_lidar_stream
        -- WHERE label <> 0  <-- COMMENTED OUT TO DEBUG
    """).wait()

if __name__ == "__main__":
    run_cleaning_job()
