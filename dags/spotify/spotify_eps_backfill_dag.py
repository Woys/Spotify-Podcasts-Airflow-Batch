import os
import shutil
import tempfile
import pandas as pd
from datetime import date
from pendulum import datetime, duration
from airflow.sdk import Param, dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.exceptions import AirflowException
from include.notification import notify_dag_failure
from include.spotify.spotify_eps import SpotifyAPI
from airflow.sdk import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.task.trigger_rule import TriggerRule

s3_bucket = Variable.get("SP_S3_BUCKET")
s3_key = 'top-charts/'
s3_key_out = 'top-podcasts/'
today = '{{ ds }}'


regions = ["ar","au","at","br","ca","cl" ,"co","fr","de","in","id","ie","it","jp","mx","nz","ph","pl","es","nl","gb","us"]
# regions = ["in", "us", "br", "jp", "gb", "ca", "de", "fr", "id", "au"]

params = {
    "start_date": Param("", type="string", format="date-time"),
    "end_date": Param("", type="string", format="date-time"),
}

@task
def create_temp_dir():
    tmp_dir = tempfile.mkdtemp(prefix='spotify_kaggle_update_')
    return tmp_dir

@task
def get_dates(**kwargs):
    params = kwargs.get('params', {})
    start_date_str = params.get('start_date')
    end_date_str = params.get('end_date')
    
    if start_date_str and end_date_str:
        start_date = pd.to_datetime(start_date_str).strftime('%Y-%m-%d')
        end_date = pd.to_datetime(end_date_str).strftime('%Y-%m-%d')
        dates = pd.date_range(start_date, end_date).strftime('%Y-%m-%d').tolist()
    else:
        dates = [kwargs['ds']]
    return dates

@task
def load_s3_files(s3_key: str, s3_bucket: str, tmp_dir: str, dates: list):
    logger = LoggingMixin().log

    s3 = S3Hook(aws_conn_id='aws_conn')
    file_paths = []

    for date in dates:
        file_name = f"top_charts_{date}.parquet"
        full_s3_key = os.path.join(s3_key, file_name)
        logger.info(f"Checking S3 path: s3://{s3_bucket}/{full_s3_key}")
        
        try:
            obj = s3.get_key(key=full_s3_key, bucket_name=s3_bucket)
            if obj:
                file_path = os.path.join(tmp_dir, file_name)
                obj.download_file(file_path)
                logger.info(f"Loaded {file_path}")
                file_paths.append((date, file_path))
            else:
                logger.warning(f"File not found: s3://{s3_bucket}/{full_s3_key}")
        except Exception as e:
            logger.warning(f"Failed to load s3://{s3_bucket}/{full_s3_key}: {e}")
            
    if not file_paths:
        raise AirflowException("No files were successfully loaded from S3.")
        
    return file_paths

@task
def spotify_api_load(tmp_dir: str, loaded_files: list):
    out_file_paths = []
    
    for date, chart_file in loaded_files:
        spotify_api = SpotifyAPI()
        file_name_out = f"top_podcasts_{date}.parquet"
        file_path_out = os.path.join(tmp_dir, file_name_out)
        
        try:
            result_df = spotify_api.get_charts_eps_file(chart_file=chart_file, regions=regions)
            result_df.to_parquet(file_path_out, index=False)
            print(f"Saved locally to {file_path_out}")
            out_file_paths.append(file_path_out)
        except Exception as e:
            print(f"Failed to process {chart_file}: {e}")
            
    if not out_file_paths:
        raise AirflowException("No files were successfully processed.")
        
    return out_file_paths

@task
def upload_to_s3(file_paths: list, s3_key_out: str, s3_bucket: str):
    s3 = S3Hook(aws_conn_id='aws_conn')
    for file_path in file_paths:
        s3_key = os.path.join(s3_key_out, os.path.basename(file_path))
        try:
            s3.load_file(filename=file_path, key=s3_key, bucket_name=s3_bucket, replace=True)
            print(f"Uploaded to s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload {file_path}: {e}")

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
    schedule=None,
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
    on_failure_callback=notify_dag_failure,
    params=params,
)
def spotify_eps_backfill():
    tmp_dir = create_temp_dir()
    dates = get_dates()

    loaded_files = load_s3_files(s3_key, s3_bucket, tmp_dir, dates)
    processed_files = spotify_api_load(tmp_dir, loaded_files)
    upload_task = upload_to_s3(processed_files, s3_key_out, s3_bucket)
    cleanup = cleanup_temp_dir(tmp_dir)

    tmp_dir >> dates >> loaded_files >> processed_files >> upload_task >> cleanup

spotify_eps_backfill()
