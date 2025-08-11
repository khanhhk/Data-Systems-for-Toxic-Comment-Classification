import os

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col, call

JARS_PATH = f"{os.getcwd()}/data_ingestion/kafka_connect/jars/"

# Environment configuration
t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_streaming_mode()
)
t_env.get_config().set(
    "pipeline.jars",
    f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-table-api-java-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-avro-confluent-registry-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-avro-1.17.1.jar;"
    + f"file://{JARS_PATH}/avro-1.11.1.jar;"
    + f"file://{JARS_PATH}/jackson-databind-2.14.2.jar;"
    + f"file://{JARS_PATH}/jackson-core-2.14.2.jar;"
    + f"file://{JARS_PATH}/jackson-annotations-2.14.2.jar;"
    + f"file://{JARS_PATH}/kafka-schema-registry-client-7.5.0.jar;"
    + f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
)

# Register `device` and `sink_device` tables in table environment
source_ddl = """
    CREATE TABLE device (
        schema ROW(
            type STRING,
            fields ARRAY<ROW(name STRING, type STRING)>
        ),
        payload ROW(
            created STRING,
            device_id INT,
            feature_1 FLOAT,
            feature_3 FLOAT,
            feature_5 FLOAT,
            feature_8 FLOAT,
            feature_6 FLOAT,
            feature_0 FLOAT,
            feature_4 FLOAT
        )
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'device_0',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'table-api-consumer-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
"""

t_env.execute_sql(source_ddl)

sink_ddl = f"""
    CREATE TABLE sink_device (
        device_id INT,
        created STRING,
        feature_1 FLOAT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sink_device_0',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json'
    )
    """

t_env.execute_sql(sink_ddl)

# Specify table program
device = t_env.from_path("device")

# # Debug the devices table to see if they are read correctly
# print("Source Table Schema and Sample Data:")
# device.print_schema()
# device.limit(5).execute().print()

selected_records = device.select(
    col('payload').get('device_id').alias('device_id'),
    col('payload').get('created').alias('created'),
    col('payload').get('feature_1').alias('feature_1')
)

selected_records.execute_insert(
    "sink_device"
).wait()
