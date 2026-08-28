import os
import tempfile
import pandas as pd
from pendulum import datetime, duration
from airflow.sdk import Param, dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.exceptions import AirflowException
from include.notification import notify_dag_failure
from include.spotify.spotify_eps import SpotifyAPI
from airflow.sdk import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

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
def process_backfill(dates: list, s3_key: str, s3_bucket: str, s3_key_out: str):
    logger = LoggingMixin().log
    s3 = S3Hook(aws_conn_id="aws_conn")
    with tempfile.TemporaryDirectory(prefix="spotify_eps_backfill_") as tmp_dir:
        processed = 0
        for run_date in dates:
            input_name = f"top_charts_{run_date}.parquet"
            input_key = os.path.join(s3_key, input_name)
            input_path = os.path.join(tmp_dir, input_name)
            try:
                obj = s3.get_key(key=input_key, bucket_name=s3_bucket)
                if not obj:
                    logger.warning("File not found: s3://%s/%s", s3_bucket, input_key)
                    continue
                obj.download_file(input_path)
                result_df = SpotifyAPI().get_charts_eps_file(
                    chart_file=input_path, regions=regions
                )
                output_name = f"top_podcasts_{run_date}.parquet"
                output_path = os.path.join(tmp_dir, output_name)
                result_df.to_parquet(output_path, index=False)
                s3.load_file(
                    filename=output_path,
                    key=os.path.join(s3_key_out, output_name),
                    bucket_name=s3_bucket,
                    replace=True,
                )
                processed += 1
            except Exception as exc:
                logger.warning("Failed to process %s: %s", input_key, exc)
        if not processed:
            raise AirflowException("No files were successfully processed.")


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
    dates = get_dates()
    process_backfill(dates, s3_key, s3_bucket, s3_key_out)


spotify_eps_backfill()
