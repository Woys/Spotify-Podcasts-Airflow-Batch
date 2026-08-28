from pendulum import datetime, duration
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.notification import notify_dag_failure
from include.spotify.kaggle import (
    run_kaggle_staging_workflow,
    stream_s3_object_to_zip,
    update_kaggle_dataset,
)
from airflow.sdk import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

S3_BUCKET = Variable.get("SP_S3_BUCKET")
S3_KEY = 'top-podcasts-union/'
DATASET_ID = "daniilmiheev/top-spotify-podcasts-daily-updated"
FILE_NAME = "top_podcasts.csv"
DATASET_TITLE = "Top Spotify Podcast Episodes (Daily Updated)"
LICENSE = "CC0-1.0"


@task
def update_kaggle_from_s3(s3_key: str, s3_bucket: str, file_name: str):
    logger = LoggingMixin().log
    s3 = S3Hook(aws_conn_id="aws_conn")
    files = s3.list_keys(bucket_name=s3_bucket, prefix=s3_key) or []
    csv_key = next((key for key in files if key.endswith(".csv")), None)
    if csv_key is None:
        raise FileNotFoundError(
            f"No CSV files found in S3 bucket '{s3_bucket}' with prefix '{s3_key}'"
        )

    def download_csv(destination: str) -> None:
        s3_object = s3.get_key(key=csv_key, bucket_name=s3_bucket)
        stream_s3_object_to_zip(s3_object, destination, file_name)
        logger.info("Streamed S3 CSV into temporary Kaggle ZIP")

    run_kaggle_staging_workflow(
        download_csv, update_kaggle_dataset, logger, DATASET_ID,
        DATASET_TITLE, LICENSE, file_name.replace(".csv", ".zip"),
        "spotify_kaggle_update_",
    )


@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule="30 20 * * *",
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
    on_failure_callback=notify_dag_failure,
)
def spotify_kaggle_update():
    update_kaggle_from_s3(S3_KEY, S3_BUCKET, FILE_NAME)


spotify_kaggle_update()
