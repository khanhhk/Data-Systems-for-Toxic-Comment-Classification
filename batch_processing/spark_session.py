import sys
import traceback
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession


def create_spark_session(memory: str, jar_paths: list[str], app_name: str = "Batch Processing Application") -> SparkSession:
    """
    Create a SparkSession with configurable memory and external jars.
    
    Args:
        memory (str): Executor memory (e.g., "4g")
        jar_paths (list[str]): List of jar file paths (e.g., ["jars/postgres.jar", "jars/hadoop-aws.jar"])
        app_name (str): Spark application name

    Returns:
        SparkSession
    """
    try:
        jars_str = ",".join(jar_paths)

        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.executor.memory", memory)
            .config("spark.jars", jars_str)
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

        logging.info("✅ Spark session created successfully.")
        return spark

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"❌ Failed to create Spark session: {e}")
        raise
