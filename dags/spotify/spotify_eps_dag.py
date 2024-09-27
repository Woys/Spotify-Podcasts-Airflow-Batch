import os
import tempfile
import pandas as pd
from datetime import date
from pendulum import datetime, duration
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from spotify.include.spotify_eps import SpotifyAPI
from airflow.models import Variable

s3_bucket = Variable.get("SP_S3_BUCKET")
s3_key = 'top-podcasts/'
s3_union_key = 'top-podcasts-union/'
regions = ["ar","au","at","br","ca","cl" ,"co","fr","de","in","id","ie","it","jp","mx","nz","ph","pl","es","nl","gb","us"]


@task
def spotify_api_load(regions):
    tmpdirname = tempfile.mkdtemp()
    spotify_api = SpotifyAPI()
    result_df = spotify_api.get_charts_eps(regions=regions)
    file_name = f"top_podcasts_{date.today().strftime('%Y-%m-%d')}.parquet"
    file_path = os.path.join(tmpdirname, file_name)
    result_df.to_parquet(file_path, index=False)
    print(f"Saved locally to {file_path}")
    return file_path

@task
def upload_to_s3(file_path: str, s3_key: str, s3_bucket: str):
    s3 = S3Hook(aws_conn_id='aws_conn')
    s3_key = os.path.join(s3_key, os.path.basename(file_path))
    s3.load_file(filename=file_path, key=s3_key, bucket_name=s3_bucket, replace=True)
    print(f"Uploaded to s3://{s3_bucket}/{s3_key}")
    
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted local file {file_path}")

@task
def union_parquet_files(s3_key: str, s3_bucket: str, s3_union_key: str):
    s3 = S3Hook(aws_conn_id='aws_conn')
    parquet_files = s3.list_keys(bucket_name=s3_bucket, prefix=s3_key)
    df_list = []

    for file in parquet_files:
        if file.endswith('.parquet'):
            obj = s3.get_key(key=file, bucket_name=s3_bucket)
            with tempfile.NamedTemporaryFile() as tmpfile:
                obj.download_file(tmpfile.name)
                df = pd.read_parquet(tmpfile.name)
                
                df_list.append(df)

    union_df = pd.concat(df_list, ignore_index=True)
    
    csv_s3_key = os.path.join(s3_union_key, "top_podcasts.csv")
    
    with tempfile.NamedTemporaryFile(suffix=".csv") as tmpfile:
        union_df.to_csv(tmpfile.name, index=False)
        s3.load_file(filename=tmpfile.name, key=csv_s3_key, bucket_name=s3_bucket, replace=True)
    
    print(f"Union of Parquet files saved as CSV in s3://{s3_bucket}/{csv_s3_key}")

@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule="15 20 * * *",
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
)
def spotify_eps():
    file_path = spotify_api_load(regions)
    upload_task = upload_to_s3(file_path, s3_key, s3_bucket)
    union_task = union_parquet_files(s3_key, s3_bucket, s3_union_key)
    
    file_path >> upload_task >> union_task



spotify_eps()
