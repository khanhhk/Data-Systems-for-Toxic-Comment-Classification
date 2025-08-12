import sys
import traceback
import logging
from pyspark import SparkContext

def _normalize_endpoint(endpoint: str, secure: bool) -> str:
    """
    Ensure endpoint has scheme. For MinIO typically http://host:9000 (non-SSL) or https://...
    """
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    return f"{'https' if secure else 'http'}://{endpoint}"

def load_minio_config(spark_context: SparkContext, minio_cfg: dict):
    """
    Configure Hadoop S3A settings to connect to a MinIO storage endpoint.

    Expected keys in minio_cfg:
      - endpoint (str): 'host:port' or full URL
      - access_key (str)
      - secret_key (str)
      - secure (bool, optional): default False
    """
    try:
        logging.info("Applying MinIO configuration to Spark Hadoop settings...")
        hadoop_conf = spark_context._jsc.hadoopConfiguration()

        secure = bool(minio_cfg.get("secure", False))
        endpoint = _normalize_endpoint(minio_cfg["endpoint"], secure)

        hadoop_conf.set("fs.s3a.access.key", minio_cfg["access_key"])
        hadoop_conf.set("fs.s3a.secret.key", minio_cfg["secret_key"])
        hadoop_conf.set("fs.s3a.endpoint", endpoint)
        hadoop_conf.set("fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true" if secure else "false")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        logging.info("✅ MinIO configuration loaded successfully.")

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"❌ Failed to configure MinIO: {e}")
        raise