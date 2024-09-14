import os
import tempfile
from datetime import date
from pendulum import datetime, duration
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from spotify.include.spotify_eps import SpotifyAPI
from airflow.models import Variable

s3_bucket = Variable.get("SP_S3_BUCKET")
s3_key = 'top-charts/'
regions = ["ar","au","at","br","ca","cl" ,"co","fr","de","in","id","ie","it","jp","mx","nz","ph","pl","es","nl","gb","us"]

@task
def spotify_chart_load(regions):
    tmpdirname = tempfile.mkdtemp()
    spotify_api = SpotifyAPI()
    result_df = spotify_api.get_transformed_podcastcharts(regions=regions)
    file_name = f"top_charts_{date.today().strftime('%Y-%m-%d')}.parquet"
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

@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule="05 20 * * *",
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
)
def spotify_charts():
    file_path = spotify_chart_load(regions)
    upload_task = upload_to_s3(file_path, s3_key, s3_bucket)
    
    file_path >> upload_task



spotify_charts()

