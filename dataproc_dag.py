from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.timezone import utc
from datetime import datetime

PROJECT_ID = "burnished-web-484613-t0"
REGION = "us-central1"
CLUSTER_NAME = "shahiddpcluster"

PYSPARK_URI = "gs://shahidtemp/pyspark_job.py"

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with DAG(
    dag_id="dataproc_house_pipeline",

    # start from now
    start_date=datetime.now(tz=utc),

    # Run every 10 minutes
    schedule="*/10 * * * *",

    catchup=False,

    # VERY IMPORTANT to prevent memory issues
    max_active_runs=1,
    concurrency=1,

    tags=["dataproc", "bigquery"],

) as dag:

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_dataproc_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
