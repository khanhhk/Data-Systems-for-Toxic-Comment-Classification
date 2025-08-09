import sys
import traceback
import pandas as pd
from loguru import logger
from utils.config import Config
from transformers import AutoTokenizer
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from utils.helpers import load_cfg
from spark_session import create_spark_session
from minio_config import load_minio_config

CFG_FILE = "./config/datalake.yaml"
datalake_cfg = load_cfg(CFG_FILE)["datalake"]

CFG_FILE_SPARK = "./config/spark.yaml"
spark_cfg = load_cfg(CFG_FILE_SPARK)["spark_config"]

tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)

@pandas_udf("array<int>, array<int>")
def tokenize_udf(texts: pd.Series):
    encoded = tokenizer(
        texts.tolist(),
        max_length=Config.MAX_LENGTH,
        truncation=True,
        padding=False
    )
    return pd.Series(encoded["input_ids"]), pd.Series(encoded["attention_mask"])

if __name__ == "__main__":
    try:
        spark = create_spark_session(
            memory=spark_cfg["executor_memory"],
            jar_paths=[
                "jars/postgresql-42.7.3.jar",
                "jars/hadoop-aws-3.3.4.jar",
                "jars/delta-storage-2.4.0.jar"
            ],
            app_name="Batch Processing"
        )

        minio_cfg = {
            "endpoint": datalake_cfg["endpoint"],
            "bucket_name": datalake_cfg["bucket_name"],
            "folder_name": datalake_cfg["folder_name"],
            "access_key": datalake_cfg["access_key"],
            "secret_key": datalake_cfg["secret_key"]
        }
        load_minio_config(spark.sparkContext, minio_cfg)

        for table_name in cfg["tables"]:
            delta_path = f"s3a://{minio_cfg['bucket_name']}/{minio_cfg['folder_name']}/{table_name}"
            logger.info(f"📥 Reading Delta table from {delta_path}")
            df: DataFrame = spark.read.format("delta").load(delta_path)

            logger.info(f"✅ Loaded {table_name} with {df.count()} rows.")

            # Apply tokenizer to comment_text
            df_tokenized = df.withColumn("input_ids", tokenize_udf(df["comment_text"]).getItem(0)) \
                             .withColumn("attention_mask", tokenize_udf(df["comment_text"]).getItem(1))

            # Drop nulls if needed
            df_tokenized = df_tokenized.dropna()

            # Write to Postgres staging
            jdbc_url = f"jdbc:postgresql://{postgres_cfg['host']}:{postgres_cfg['port']}/{postgres_cfg['database']}"
            df_tokenized.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"staging.{table_name}") \
                .option("user", postgres_cfg["user"]) \
                .option("password", postgres_cfg["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

            logger.info(f"✅ Written {table_name} to Postgres staging.")

        spark.stop()

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logger.error(f"❌ Batch processing failed: {e}")
        sys.exit(1)
