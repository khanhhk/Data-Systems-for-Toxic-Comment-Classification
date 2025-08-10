import sys
import traceback
import logging
from pyspark.sql import SparkSession

def create_spark_session(
        memory: str, 
        jar_paths: list[str], 
        app_name: str = "Batch Processing Application") -> SparkSession:
    """
    Create a configured SparkSession.

    Args:
        memory (str): Executor memory allocation (e.g., "4g").
        jar_paths (list[str]): List of JAR file paths to include in Spark.
                               Should contain Postgres driver, Hadoop/S3,
                               and Delta Lake JARs if needed.
        app_name (str, optional): Name of the Spark application.
                                  Defaults to "Batch Processing Application".

    Returns:
        SparkSession: Configured Spark session instance.
    """
    try:
        logging.info("Initializing Spark session...")
        jars_str = ",".join(jar_paths) if jar_paths else ""
        if not jar_paths:
            logging.warning("No JAR paths provided. External connectors may fail.")

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