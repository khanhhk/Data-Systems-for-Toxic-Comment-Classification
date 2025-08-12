import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from minio import Minio
from loguru import logger
from model.config import Config
from transformers import AutoTokenizer
from utils.helpers import load_cfg
from spark_session import create_spark_session
from minio_config import load_minio_config
from sqlalchemy import create_engine, text

CFG_FILE = "./configs/config.yaml"

logger.info("Loading application configurations...")
datalake_cfg = load_cfg(CFG_FILE)["datalake"]
spark_cfg = load_cfg(CFG_FILE)["spark"]
postgres_cfg = load_cfg(CFG_FILE)["dw_postgres"]

logger.info(f"Loading tokenizer for model: {Config.MODEL_NAME}")
tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)

def processing_dataframe(spark_df):
    """
    Convert a Spark DataFrame to Pandas and tokenize text data.

    Args:
        spark_df (pyspark.sql.DataFrame): Spark DataFrame containing
                                          a 'comment_text' column.

    Returns:
        pandas.DataFrame: DataFrame with tokenized 'input_ids' and 'attention_mask'.
    """
    logger.info("Converting Spark DataFrame to Pandas...")
    pdf = spark_df.toPandas()

    logger.info("Tokenizing comment text...")
    tokenized = tokenizer(
        pdf["comment_text"].tolist(),
        max_length=Config.MAX_LENGTH,
        truncation=True,
    )

    pdf["input_ids"] = tokenized["input_ids"]
    pdf["attention_mask"] = tokenized["attention_mask"]
    logger.success("Data tokenization completed.")
    return pdf.drop(columns=["comment_text"])

def _count_rows(conn, schema: str, table: str) -> int:
    try:
        sql = text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return int(conn.execute(sql).scalar() or 0)
    except Exception as e:
        logger.warning(f"Count failed for {schema}.{table}: {e}")
        return 0
    
def load_to_staging_table(pdf, table_name):
    """
    Save processed data to a PostgreSQL staging table.

    Args:
        pdf (pandas.DataFrame): DataFrame containing processed data.
    """
    logger.info("Connecting to PostgreSQL...")
    engine = create_engine(
        f"postgresql://{postgres_cfg['user']}:{postgres_cfg['password']}"
        f"@{postgres_cfg['host']}:{postgres_cfg['port']}/{postgres_cfg['database']}"
    )
    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {postgres_cfg['staging_schema']}"))
        current_schema = conn.execute(text("SELECT current_schema()")).scalar()
        search_path = conn.execute(text("SHOW search_path")).scalar()
        logger.info(f"Postgres current_schema() = {current_schema}")
        logger.info(f"Postgres search_path = {search_path}")
        before_cnt = _count_rows(conn, postgres_cfg["staging_schema"], table_name)
        logger.info(f"Before insert: {postgres_cfg['staging_schema']}.{table_name} has {before_cnt} rows")

    logger.info(f"Inserting {len(pdf)} records into {postgres_cfg['staging_schema']}.{table_name}'...")
    pdf.to_sql(
        name=table_name,
        con= engine,
        schema=postgres_cfg["staging_schema"],
        if_exists="append",
        index=False,
        method="multi")
    
    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {postgres_cfg['staging_schema']}"))
        after_cnt = _count_rows(conn, postgres_cfg['staging_schema'], table_name)
        logger.success(
            f"After insert: {postgres_cfg['staging_schema']}.{table_name} has {after_cnt} rows"
            f"(inserted {after_cnt - before_cnt})")

def list_subfolders(minio_client, bucket, prefix):
    """
    List immediate subfolders under a given MinIO bucket prefix.

    Args:
        minio_client (Minio): MinIO client instance.
        bucket (str): Name of the bucket.
        prefix (str): Folder path prefix.

    Returns:
        list[str]: List of subfolder names.
    """
    logger.info(f"Listing subfolders in bucket '{bucket}' with prefix '{prefix}'...")
    subfolders = set()
    objects = minio_client.list_objects(bucket, prefix=prefix, recursive=False)

    for obj in objects:
        parts = obj.object_name[len(prefix):].strip("/").split("/")
        if parts and parts[0]:
            subfolders.add(parts[0])

    folders = list(subfolders)
    logger.success(f"Found subfolders: {folders}")
    return folders

if __name__ == "__main__":
    spark = create_spark_session(memory=spark_cfg["executor_memory"])

    load_minio_config(spark.sparkContext, datalake_cfg)

    minio_client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=datalake_cfg.get("secure", False)
    )

    prefix = datalake_cfg["folder_name"] + "/"
    folders = list_subfolders(minio_client, datalake_cfg["bucket_name"], prefix)

    for folder in folders:
        logger.info(f"Processing folder: {folder}")

        parquet_path = f"s3a://{datalake_cfg['bucket_name']}/{prefix}{folder}/*.parquet"
        json_path = f"s3a://{datalake_cfg['bucket_name']}/{prefix}{folder}/*.json"

        df = spark.read.parquet(parquet_path)

        logger.info("=== Spark DataFrame (before processing) ===")
        logger.info(f"Shape (rows x cols) ≈ ({df.count()} x {len(df.columns)})")
        logger.info(f"Columns: {df.columns}")

        pdf_final = processing_dataframe(df)

        logger.info("=== Pandas DataFrame (after processing) ===")
        logger.info(f"Shape: {pdf_final.shape}")
        logger.info(f"Columns: {list(pdf_final.columns)}")
        
        load_to_staging_table(pdf_final, table_name=folder)