import os
import logging
import traceback

from utils.helpers import load_cfg
from etl.core.spark_session import create_spark_session
from etl.core.minio_config import load_minio_config
from etl import transform

from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def apply_transform(table_name: str, df: DataFrame) -> DataFrame:
    normalized_name = table_name.replace("olist_", "").replace("_dataset", "")
    func_name = f"transform_{normalized_name}"
    try:
        transform_func = getattr(transform, func_name)
        return transform_func(df)
    except AttributeError:
        logging.warning(f"No transform function found for: {func_name}. Using original data.")
        return df

def write_to_postgres(df: DataFrame, table_name: str):
    import os
    from dotenv import load_dotenv
    load_dotenv(".env")
    jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:5432/{os.getenv('POSTGRES_DB')}"
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", os.getenv("POSTGRES_USER")) \
        .option("password", os.getenv("POSTGRES_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def main():
    cfg = load_cfg("config/datalake.yaml")
    spark_cfg = load_cfg("config/spark.yaml")["spark_config"]
    minio_cfg = cfg["datalake"]
    delta_folder_path = cfg["data"]["deltalake_folder_path"]
    memory = spark_cfg["executor_memory"]
    jar_paths = [
        "jars/postgresql-42.4.3.jar",
        "jars/aws-java-sdk-bundle-1.12.262.jar",
        "jars/hadoop-aws-3.3.4.jar"
    ]

    spark = create_spark_session(memory, jar_paths)
    load_minio_config(spark.sparkContext, minio_cfg)

    for table_folder in os.listdir(delta_folder_path):
        full_path = os.path.join(delta_folder_path, table_folder)
        if not os.path.isdir(full_path):
            continue
        try:
            logging.info(f"Processing: {table_folder}")
            delta_path = f"s3a://{minio_cfg['bucket_name']}/{minio_cfg['folder_name']}/{table_folder}"
            df = spark.read.format("delta").load(delta_path)
            df_transformed = apply_transform(table_folder, df)
            write_to_postgres(df_transformed, f"staging.{table_folder}")
            logging.info(f"Finished: {table_folder}\\n")
        except Exception as e:
            logging.error(f"Error processing {table_folder}: {e}")
            traceback.print_exc()

    spark.stop()

if __name__ == "__main__":
    main()
