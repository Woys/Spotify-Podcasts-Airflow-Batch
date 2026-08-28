import os
import tempfile
from datetime import date
from pendulum import datetime, duration
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.notification import notify_dag_failure
from include.spotify.spotify_eps import SpotifyAPI
from airflow.sdk import Variable

s3_bucket = Variable.get("SP_S3_BUCKET")
s3_key = 'top-charts/'
regions = ["ar","au","at","br","ca","cl" ,"co","fr","de","in","id","ie","it","jp","mx","nz","ph","pl","es","nl","gb","us"]

@task
def build_and_upload_chart(regions, s3_key: str, s3_bucket: str):
    with tempfile.TemporaryDirectory(prefix="spotify_charts_") as tmpdirname:
        spotify_api = SpotifyAPI()
        result_df = spotify_api.get_transformed_podcastcharts(regions=regions)
        file_name = f"top_charts_{date.today().strftime('%Y-%m-%d')}.parquet"
        file_path = os.path.join(tmpdirname, file_name)
        result_df.to_parquet(file_path, index=False)
        s3 = S3Hook(aws_conn_id="aws_conn")
        output_key = os.path.join(s3_key, file_name)
        s3.load_file(filename=file_path, key=output_key, bucket_name=s3_bucket, replace=True)
        print(f"Uploaded to s3://{s3_bucket}/{output_key}")


@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule="10 20 * * *",
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
    on_failure_callback=notify_dag_failure,
)
def spotify_charts():
    build_and_upload_chart(regions, s3_key, s3_bucket)


spotify_charts()
