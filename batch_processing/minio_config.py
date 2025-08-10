import sys
import traceback
import logging
from pyspark import SparkContext


def load_minio_config(spark_context: SparkContext, minio_cfg: dict):
    """
    Configure Hadoop S3A settings to connect to a MinIO storage endpoint.

    Args:
        spark_context (SparkContext): Active Spark context.
        minio_cfg (dict): Dictionary containing:
            - endpoint (str)
            - access_key (str)
            - secret_key (str)
            - optional 'secure' flag handled by MinIO client, not here.
    """
    try:
        logging.info("Applying MinIO configuration to Spark Hadoop settings...")
        hadoop_conf = spark_context._jsc.hadoopConfiguration()

        hadoop_conf.set("fs.s3a.access.key", minio_cfg["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", minio_cfg["secret_key"])
        hadoop_conf.set("fs.s3a.endpoint", minio_cfg["endpoint"])
        hadoop_conf.set("fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        logging.info("✅ MinIO configuration loaded successfully.")

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"❌ Failed to configure MinIO: {e}")
        raise
