import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# --- CONFIGURATION ---
LIB_JAR_PATH = f"file://{os.path.expanduser('~/lidar_project/lib/flink-sql-connector-kafka-3.1.0-1.18.jar')}"

def run_debug_job():
    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # 2. Add Kafka Connector JAR
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", LIB_JAR_PATH
    )

    print("[INFO] Starting Debug Job...")
    print("[INFO] Watch the terminal. If you see data here, Flink is reading correctly.")

    # 3. Define Source (Kafka)
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
            'properties.group.id' = 'flink-debug-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 4. Define Sink (PRINT to Terminal)
    t_env.execute_sql("""
        CREATE TABLE print_sink (
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
            'connector' = 'print'
        )
    """)

    # 5. Execute
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT * FROM raw_lidar_stream
    """).wait()

if __name__ == "__main__":
    run_debug_job()
