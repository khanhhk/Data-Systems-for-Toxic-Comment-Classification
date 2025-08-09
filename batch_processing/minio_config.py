import sys
import traceback
import logging
from pyspark import SparkContext


def load_minio_config(spark_context: SparkContext, minio_cfg: dict):
    """
    Set Hadoop S3A configurations to access MinIO.
    
    Args:
        spark_context (SparkContext): Spark context object
        minio_cfg (dict): Dict with keys: endpoint, access_key, secret_key
    """
    try:
        hadoop_conf = spark_context._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", minio_cfg["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", minio_cfg["secret_key"])
        hadoop_conf.set("fs.s3a.endpoint", minio_cfg["endpoint"])
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        logging.info("✅ MinIO configuration loaded successfully.")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"❌ Failed to configure MinIO: {e}")
        raise