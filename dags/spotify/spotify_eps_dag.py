import os
import tempfile
import pandas as pd
from datetime import date
from pendulum import datetime, duration
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from include.notification import notify_dag_failure
from include.spotify.spotify_eps import SpotifyAPI
from airflow.sdk import Variable

s3_bucket = Variable.get("SP_S3_BUCKET")
s3_key = 'top-podcasts/'
s3_union_key = 'top-podcasts-union/'
regions = ["ar","au","at","br","ca","cl" ,"co","fr","de","in","id","ie","it","jp","mx","nz","ph","pl","es","nl","gb","us"]
# keep_cols = ['date', 'rank', 'region', 'chartRankMove', 'episodeUri', 'showUri',
#             'episodeName', 'description', 'show.name', 'show.description',
#             'show.publisher', 'duration_ms', 'explicit', 'languages', 'release_date',
#             'release_date_precision', 'show.media_type', 'show.total_episodes']
keep_cols = ['date', 'rank', 'region', 'chartRankMove', 'episodeUri', 'showUri',
            'episodeName', 'description', 'show.name', 'show.publisher', 'duration_ms', 
            'explicit', 'languages', 'release_date', 'show.media_type', 'show.total_episodes']

@task
def build_and_upload_episodes(regions, s3_key: str, s3_bucket: str):
    with tempfile.TemporaryDirectory(prefix="spotify_eps_") as tmpdirname:
        spotify_api = SpotifyAPI()
        result_df = spotify_api.get_charts_eps(regions=regions)
        file_name = f"top_podcasts_{date.today().strftime('%Y-%m-%d')}.parquet"
        file_path = os.path.join(tmpdirname, file_name)
        result_df.to_parquet(file_path, index=False)
        s3 = S3Hook(aws_conn_id="aws_conn")
        output_key = os.path.join(s3_key, file_name)
        s3.load_file(filename=file_path, key=output_key, bucket_name=s3_bucket, replace=True)
        print(f"Uploaded to s3://{s3_bucket}/{output_key}")


@task
def union_parquet_files(s3_key: str, s3_bucket: str, s3_union_key: str, keep_cols: list[str]):
    """
    Union all Parquet files under s3://{s3_bucket}/{s3_key}, but only keep the specified columns.
    If a file is missing some columns, they are added with NaN so concatenation succeeds.
    """
    s3 = S3Hook(aws_conn_id='aws_conn')
    parquet_files = s3.list_keys(bucket_name=s3_bucket, prefix=s3_key) or []
    df_list = []

    if not parquet_files:
        print(f"No objects found under s3://{s3_bucket}/{s3_key}")
        return

    for file in parquet_files:
        if not file.endswith(".parquet"):
            continue

        obj = s3.get_key(key=file, bucket_name=s3_bucket)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmpfile:
            obj.download_file(tmpfile.name)
            try:
                df = pd.read_parquet(tmpfile.name, columns=keep_cols)
            except Exception:
                df = pd.read_parquet(tmpfile.name)
                df = df.reindex(columns=keep_cols)

            df_list.append(df)

    if not df_list:
        print("No parquet files were read successfully.")
        return

    union_df = pd.concat(
        [d.reindex(columns=keep_cols) for d in df_list],
        ignore_index=True
    )

    union_df = union_df.drop_duplicates(subset=['date', 'region', 'episodeUri'], keep='last')

    csv_s3_key = os.path.join(s3_union_key, "top_podcasts.csv")
    with tempfile.NamedTemporaryFile(suffix=".csv") as tmpfile:
        union_df.to_csv(tmpfile.name, index=False)
        s3.load_file(
            filename=tmpfile.name,
            key=csv_s3_key,
            bucket_name=s3_bucket,
            replace=True
        )

    print(f"Union saved (columns={keep_cols}) to s3://{s3_bucket}/{csv_s3_key}")

@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule="15 20 * * *",
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
    on_failure_callback=notify_dag_failure,
)
def spotify_eps():
    upload_task = build_and_upload_episodes(regions, s3_key, s3_bucket)
    union_task = union_parquet_files(s3_key, s3_bucket, s3_union_key, keep_cols)
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_duck_dbt",
        trigger_dag_id="spotify_duck_dbt",
        conf={"s3_bucket": s3_bucket},
        wait_for_completion=False,
    )
    
    upload_task >> [union_task, trigger_dbt]



spotify_eps()
