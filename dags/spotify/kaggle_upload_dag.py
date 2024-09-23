import os
import shutil
import tempfile
from pendulum import datetime, duration
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from spotify.include.kaggle import create_kaggle_metadata, create_kaggle_dataset
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule

S3_BUCKET = Variable.get("SP_S3_BUCKET")
S3_KEY = 'top-podcasts-union/'
DATASET_ID = "daniilmiheev/top-spotify-podcasts-daily-"
FILE_NAME = "top_podcasts.csv"
DATASET_TITLE = "Top Spotify Podcast Episodes (Daily)"
LICENSE = "CC0-1.0"


@task
def create_temp_dir():
    tmp_dir = tempfile.mkdtemp(prefix='spotify_kaggle_upload_')
    return tmp_dir


@task
def load_s3_file(s3_key: str, s3_bucket: str, tmp_dir: str, file_name: str):
    logger = LoggingMixin().log
    logger.info(f"Loading files from S3 bucket '{
                s3_bucket}' with prefix '{s3_key}'")
    s3 = S3Hook(aws_conn_id='aws_conn')
    files = s3.list_keys(bucket_name=s3_bucket, prefix=s3_key)

    if not files:
        raise ValueError(f"No files found in S3 bucket '{
                         s3_bucket}' with prefix '{s3_key}'")

    for file in files:
        if file.endswith('.csv'):
            obj = s3.get_key(key=file, bucket_name=s3_bucket)
            csv_file_path = os.path.join(tmp_dir, file_name)
            obj.download_file(csv_file_path)
            logger.info(f"Loaded {csv_file_path}")
            break
    else:
        raise FileNotFoundError(f"No CSV files found in S3 bucket '{
                                s3_bucket}' with prefix '{s3_key}'")


@task
def create_kaggle_metadata_task(tmp_dir: str, dataset_id: str, title: str, license: str):
    logger = LoggingMixin().log
    create_kaggle_metadata(tmp_dir, logger, dataset_id, title, license)


@task
def create_kaggle_dataset_task(kaggle_folder: str):
    logger = LoggingMixin().log
    create_kaggle_dataset(kaggle_folder, logger)


@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup_temp_dir(tmp_dir: str):
    logger = LoggingMixin().log
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
        logger.info(f"Deleted temporary directory {tmp_dir}")
    else:
        logger.warning(f"Temporary directory {tmp_dir} does not exist")


@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule="10 20 * * *",
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
)
def spotify_kaggle_upload():
    tmp_dir = create_temp_dir()

    load = load_s3_file(S3_KEY, S3_BUCKET, tmp_dir, FILE_NAME)
    create_metadata = create_kaggle_metadata_task(
        tmp_dir, DATASET_ID, DATASET_TITLE, LICENSE)
    upload_dataset = create_kaggle_dataset_task(tmp_dir)
    cleanup = cleanup_temp_dir(tmp_dir)

    tmp_dir >> load >> create_metadata >> upload_dataset >> cleanup


spotify_kaggle_upload()
