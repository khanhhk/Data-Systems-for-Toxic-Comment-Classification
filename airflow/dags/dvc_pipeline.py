from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dvc_pipeline",
    default_args=default_args,
    description="Run DVC pipeline and push data to MinIO",
    start_date=datetime(2025, 8, 15),
    schedule_interval=None,  # hoặc "0 2 * * *" để chạy hàng ngày
    catchup=False,
    tags=["ml", "dvc", "minio", "mlflow"],
) as dag:

    run_dvc_repro = BashOperator(
        task_id="run_dvc_repro",
        bash_command="cd /opt/project && dvc repro"
    )

    push_dvc_to_remote = BashOperator(
        task_id="push_dvc_to_remote",
        bash_command="cd /opt/project && dvc repro"
    )

    run_dvc_repro >> push_dvc_to_remote
