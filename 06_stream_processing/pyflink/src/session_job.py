from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_aggregated_sink(t_env):
    table_name = 'taxi_session_aggregation'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        PULocationID INT,
        DOLocationID INT,
        trip_count BIGINT,
        PRIMARY KEY (session_start, session_end, PULocationID, DOLocationID) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/ny_taxi',
        'table-name' = 'taxi_session_aggregation',
        'username' = 'carrie',
        'password' = 'carrie',
        'driver' = 'org.postgresql.Driver'
    )
    """

    t_env.execute_sql(sink_ddl)
    return table_name

def create_source_table(t_env):
    source_ddl = """
    CREATE TABLE green_taxi_source (
        lpep_pickup_datetime STRING,
        lpep_dropoff_datetime STRING,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance DOUBLE,
        tip_amount DOUBLE,
        event_watermark AS 
            CASE 
                WHEN lpep_dropoff_datetime IS NOT NULL THEN 
                    TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') 
                ELSE 
                    NULL 
            END,
        WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda-1:29092',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """
    t_env.execute_sql(source_ddl)
    return "green_taxi_source"

def process_taxi_trip_sessions():
    print("Starting the sessionization job...")
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)
    env.add_jars(
        "file:///opt/flink/lib/flink-json-1.16.0.jar",
        "file:///opt/flink/lib/flink-sql-connector-kafka-1.16.0.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-1.16.0.jar",
        "file:///opt/flink/lib/postgresql-42.2.24.jar"
    )
    
    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    print("Done setting up the environment.")

    try:
        source_table = create_source_table(t_env)
        print("Source table created.")
        sink_table = create_events_aggregated_sink(t_env)
        print("Sink table created.")
        print("Processing data...")
        t_env.execute_sql(f"""
            INSERT INTO {sink_table}
            SELECT
                SESSION_START(event_watermark, INTERVAL '5' MINUTE) AS session_start,
                SESSION_END(event_watermark, INTERVAL '5' MINUTE) AS session_end,
                PULocationID,
                DOLocationID,
                COUNT(*) AS trip_count
            FROM {source_table}
            GROUP BY
                SESSION(event_watermark, INTERVAL '5' MINUTE),
                PULocationID,
                DOLocationID
        """).wait()

    except Exception as e:
        print("Processing failed:", str(e))


if __name__ == '__main__':
    print("2025-03-17 22:35")
    process_taxi_trip_sessions()