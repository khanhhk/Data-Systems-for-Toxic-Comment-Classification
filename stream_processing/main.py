import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from model.config import Config
import json
from loguru import logger
from pyflink.table import (
    EnvironmentSettings, TableEnvironment, DataTypes
)
from pyflink.table.expressions import col, call
from pyflink.table.udf import udf
from pyflink.common import Row
from transformers import AutoTokenizer

JARS_PATH = f"{os.getcwd()}/jars/"

logger.info(f"Loading tokenizer for model: {Config.MODEL_NAME}")
tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)

@udf(result_type=DataTypes.ROW([
    DataTypes.FIELD("input_ids", DataTypes.STRING()),
    DataTypes.FIELD("attention_mask", DataTypes.STRING())
]))
def hf_tokenize(text: str):
    if text is None:
        text = ""
    enc = tokenizer(text, max_length=Config.MAX_LENGTH, truncation=True)
    return Row(
        input_ids=json.dumps(enc["input_ids"], ensure_ascii=False),
        attention_mask=json.dumps(enc["attention_mask"], ensure_ascii=False)
    )

def main():
    # Streaming mode
    t_env = TableEnvironment.create(
        environment_settings=EnvironmentSettings.in_streaming_mode()
    )

    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-sql-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/flink-connector-jdbc-core-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/flink-connector-jdbc-postgres-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/postgresql-42.7.7.jar",
    )
    
    # Đăng ký UDF
    t_env.create_temporary_system_function("hf_tokenize", hf_tokenize)

    # ---- Kafka source (Debezium JSON) ----
    t_env.execute_sql(f"""
        CREATE TABLE m2_streaming_src (
        comment_text STRING,
        labels INT
        ) WITH (
        'connector' = 'kafka',
        'topic' = 'test.m2.streaming',
        'properties.bootstrap.servers' = '127.0.0.1:9092',
        'properties.group.id' = 'flink-staging-consumer-001',
        'scan.startup.mode' = 'earliest-offset',
        'value.format' = 'debezium-json',
        'value.debezium-json.schema-include' = 'true',
        'value.debezium-json.ignore-parse-errors' = 'false'
        );
    """)

    # ---- JDBC sink ----
    t_env.execute_sql("""
        CREATE TABLE staging_streaming_sink (
          id STRING NOT NULL,
          labels INT,
          input_ids STRING,
          attention_mask STRING,
          PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
          'connector' = 'jdbc',
          'url' = 'jdbc:postgresql://localhost:5432/k6',
          'table-name' = 'staging.streaming',
          'username' = 'k6',
          'password' = 'k6',
          'driver' = 'org.postgresql.Driver'
        )
    """)

    # Table API: select + tokenize + insert
    src = t_env.from_path("m2_streaming_src")
    # src.execute().print()
    logger.info("Source schema:")
    src.print_schema()

    # logger.info("Preview 5 records from Kafka (if available):")
    # try:
    #     src.select(col("comment_text"), col("labels")) \
    #     .limit(5).execute().print()
    # except Exception as e:
    #     logger.warning(f"Preview failed (maybe no data yet?): {e}")

    tok = src.select(
        call('uuid').alias('id'),
        col("labels"),
        hf_tokenize(col("comment_text")).alias("tok")
    ).select(
        col("id"),
        col("labels"),
        col("tok").get("input_ids").alias("input_ids"),
        col("tok").get("attention_mask").alias("attention_mask")
    )

    # Execute continuous insert
    logger.info("Waiting for the job (Ctrl+C to stop)...")
    tok.execute_insert("staging_streaming_sink").wait()

if __name__ == "__main__":
    main()
