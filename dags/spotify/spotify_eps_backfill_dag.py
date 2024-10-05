import os
import shutil
import tempfile
import pandas as pd
from datetime import date
from pendulum import datetime, duration
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from spotify.include.spotify_eps import SpotifyAPI
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param

s3_bucket = Variable.get("SP_S3_BUCKET")
s3_key = 'top-charts/'
s3_key_out = 'top-podcasts/'
today = '{{ ds }}'
file_name = f"top_charts_{today}.parquet"
file_name_out = f"top_podcasts_{today}.parquet"
regions = ["ar","au","at","br","ca","cl" ,"co","fr","de","in","id","ie","it","jp","mx","nz","ph","pl","es","nl","gb","us"]

params = {
    "start_date": Param("",type="string",format="date-time"),
    "end_date": Param("",type="string",format="date-time"),
    "max_active_runs": 1,
    "retries": 2, 
    "retry_delay": duration(minutes=1),
    "catchup": False
}

@task
def create_temp_dir():
    tmp_dir = tempfile.mkdtemp(prefix='spotify_kaggle_update_')
    return tmp_dir

@task
def load_s3_file(s3_key: str, s3_bucket: str, tmp_dir: str, file_name: str):
    logger = LoggingMixin().log
    logger.info(f"Loading file '{file_name}' from S3 bucket '{s3_bucket}' with prefix '{s3_key}'")
    s3 = S3Hook(aws_conn_id='aws_conn')

    try:
        full_s3_key = os.path.join(s3_key, file_name)
        logger.info(f"Checking S3 path: s3://{s3_bucket}/{full_s3_key}")
        obj = s3.get_key(key=full_s3_key, bucket_name=s3_bucket)
        file_path = os.path.join(tmp_dir, file_name)
        obj.download_file(file_path)
        logger.info(f"Loaded {file_path}")
    except Exception as e:
        raise AirflowException(f"Task failed due to: {e}")
    return file_path
    
@task
def spotify_api_load(tmp_dir, chart_file, file_name):
    spotify_api = SpotifyAPI()
    result_df = spotify_api.get_charts_eps_file(chart_file=chart_file,regions=regions)
    file_path = os.path.join(tmp_dir, file_name)
    result_df.to_parquet(file_path, index=False)
    print(f"Saved locally to {file_path}")
    return file_path

@task
def upload_to_s3(file_path: str, s3_key_out: str, s3_bucket: str):
    s3 = S3Hook(aws_conn_id='aws_conn')
    s3_key = os.path.join(s3_key_out, os.path.basename(file_path))
    s3.load_file(filename=file_path, key=s3_key_out, bucket_name=s3_bucket, replace=True)
    print(f"Uploaded to s3://{s3_bucket}/{s3_key_out}")

@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup_temp_dir(tmp_dir: str):
    logger = LoggingMixin().log
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
        logger.info(f"Deleted temporary directory {tmp_dir}")
    else:
        logger.warning(f"Temporary directory {tmp_dir} does not exist")

@dag(
    schedule_interval= None,
    params = params
)
def spotify_eps_backfill():
    tmp_dir = create_temp_dir()

    load_file = load_s3_file(s3_key, s3_bucket, tmp_dir, file_name)
    file_path = spotify_api_load(tmp_dir, load_file,file_name)
    upload_task = upload_to_s3(file_path, s3_key_out, s3_bucket)
    cleanup = cleanup_temp_dir(tmp_dir)
    
    tmp_dir >> load_file >> file_path >> upload_task >> cleanup

spotify_eps_backfill()