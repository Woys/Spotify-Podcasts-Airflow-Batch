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
    schedule="10 20 * * *",
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
)
def spotify_eps_union():
    union_task = union_parquet_files(s3_key, s3_bucket, s3_union_key)
    union_task


spotify_eps_union()
