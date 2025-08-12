import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from model.config import Config
import json
from utils.helpers import load_cfg
from loguru import logger
from transformers import AutoTokenizer

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.typeinfo import Types
from pyflink.common.typeinfo import RowTypeInfo
JARS_PATH = f"{os.getcwd()}/jars/"
SCHEMA_FILE = "./configs/schema_config.json"
CFG_FILE = "./configs/config.yaml"
logger.info("Loading application configurations...")
datalake_cfg = load_cfg(CFG_FILE)["datalake"]
spark_cfg = load_cfg(CFG_FILE)["spark"]
postgres_cfg = load_cfg(CFG_FILE)["dw_postgres"]


logger.info(f"Loading tokenizer for model: {Config.MODEL_NAME}")
tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)

# Map Spark-like -> Flink SQL type strings
TYPE_MAP = {
    "StringType": "STRING",
    "IntegerType": "INT",
    "JsonType": "STRING"     # Flink side STRING -> Postgres JSONB column
}

# Map Spark-like -> PyFlink TypeInformation
PYFLINK_TYPEINFO_MAP = {
    "StringType": Types.STRING(),
    "IntegerType": Types.INT(),
    "JsonType": Types.STRING()
}

def build_field_ddl(fields):
    return ",\n  ".join([f"`{f['name']}` {TYPE_MAP[f['type']]}" for f in fields])

def build_row_typeinfo(fields):
    names = [f["name"] for f in fields]
    types = [PYFLINK_TYPEINFO_MAP[f["type"]] for f in fields]
    return RowTypeInfo(types, names)

class TokenizeMap(MapFunction):
    def map(self, row):
        """
        row: (comment_text:str, labels:int)
        return: (labels:int, input_ids_json:str, attention_mask_json:str)
        """
        comment_text, labels = row
        enc = tokenizer(
            comment_text,
            max_length=128,
            truncation=True,
        )
        input_ids = enc["input_ids"]
        attention_mask = enc["attention_mask"]
        return (
            labels,
            json.dumps(input_ids, ensure_ascii=False),
            json.dumps(attention_mask, ensure_ascii=False),
        )

def main():
    with open(SCHEMA_FILE, "r") as f:
        schema_cfg = json.load(f)

    source_fields = schema_cfg["source_fields"]
    sink_fields = schema_cfg["sink_fields"]

    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    env.enable_checkpointing(30000)

    t_env.get_config().set(
        "pipeline.jars",
        f"file://{JARS_PATH}/flink-sql-connector-kafka-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/flink-connector-jdbc-core-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/flink-connector-jdbc-postgres-4.0.0-2.0.jar;"
        + f"file://{JARS_PATH}/postgresql-42.7.7.jar",
    )

    # ---- Kafka source (Table) ----
    t_env.execute_sql(f"""
    CREATE TABLE kafka_src (
      {build_field_ddl(source_fields)}
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'test.m2.streaming',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'flink-consumer',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true'
    )
    """)

    source_table = t_env.from_path("kafka_src")
    src_row_typeinfo = build_row_typeinfo(source_fields)
    ds = t_env.to_append_stream(source_table, src_row_typeinfo)

    # ---- tokenize ----
    sink_row_typeinfo = build_row_typeinfo(sink_fields)
    tokenized_ds = ds.map(TokenizeMap(), output_type=sink_row_typeinfo)
    tokenized_table = t_env.from_data_stream(tokenized_ds)

    # ---- JDBC sink (PostgreSQL) ----
    jdbc_url = f"jdbc:postgresql://{postgres_cfg['host']}:{postgres_cfg['port']}/{postgres_cfg['database']}"

    t_env.execute_sql(f"""
    CREATE TABLE pg_sink (
      {build_field_ddl(sink_fields)}
    )
    WITH (
      'connector' = 'jdbc',
      'url' = '{jdbc_url}',
      'table-name' = '{postgres_cfg['staging_schema']}.streaming',
      'username' = '{postgres_cfg['user']}',
      'password' = '{postgres_cfg['password']}',
      'driver' = 'org.postgresql.Driver',
      'sink.buffer-flush.max-rows' = '1000',
      'sink.buffer-flush.interval' = '2s'
    )
    """)

    # insert stream → postgres
    tokenized_table.execute_insert("pg_sink").wait()

if __name__ == "__main__":
    main()