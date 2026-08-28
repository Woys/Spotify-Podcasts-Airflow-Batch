from __future__ import annotations

import tempfile
from pathlib import Path

from airflow.sdk import Variable
from airflow.sdk import dag, get_current_context, task
from pendulum import datetime, duration

from include.notification import notify_dag_failure
from include.research.research_pipeline import (
    apply_openalex_key,
    apply_newsapi_key,
    bool_env,
    build_fetcher_specs,
    build_manifest,
    build_s3_keys,
    dedupe_and_quality_filter,
    ensure_non_empty_bucket,
    normalize_records,
    parse_search_terms,
    merge_daily_datasets_to_s3,
    run_text_ingest_records,
    should_fail_pipeline,
    upload_artifacts_to_s3,
    write_dataset_parquet,
)


@task
def build_ingestion_config() -> dict:
    context = get_current_context()
    logical_date = context["dag_run"].logical_date
    run_date = logical_date.date().isoformat()

    s3_bucket = Variable.get("SP_S3_BUCKET", default="")
    if not s3_bucket:
        # Backward-compatible fallback for older envs.
        s3_bucket = Variable.get("DATASET_S3_BUCKET", default="")
    ensure_non_empty_bucket(s3_bucket)

    search_terms_raw = Variable.get(
        "TI_SEARCH_TERMS",
        default='["data engineering", "machine learning", "airflow"]',
    )
    search_terms = parse_search_terms(search_terms_raw)

    max_records_per_source = int(
        Variable.get("TI_MAX_RECORDS_PER_SOURCE", default="200")
    )
    request_delay_seconds = float(
        Variable.get("TI_REQUEST_DELAY_SECONDS", default="1.0")
    )
    crossref_email = Variable.get("TI_CROSSREF_EMAIL", default="")
    openalex_api_key = Variable.get("TI_OPENALEX_API_KEY", default="")
    newsapi_api_key = Variable.get("TI_NEWSAPI_KEY", default="")
    newsapi_language = Variable.get("TI_NEWSAPI_LANGUAGE", default="en")
    hn_item_type = Variable.get("TI_HN_ITEM_TYPE", default="story")
    hn_use_date_sort = bool_env(
        Variable.get("TI_HN_USE_DATE_SORT", default="true")
    )
    min_rows_to_publish = int(
        Variable.get("TI_MIN_ROWS_TO_PUBLISH", default="1")
    )

    apply_openalex_key(openalex_api_key)
    apply_newsapi_key(newsapi_api_key)

    fetcher_specs = build_fetcher_specs(
        search_terms=search_terms,
        max_records_per_source=max_records_per_source,
        request_delay_seconds=request_delay_seconds,
        crossref_email=crossref_email,
        openalex_api_key=openalex_api_key,
        newsapi_api_key=newsapi_api_key,
        newsapi_language=newsapi_language,
        hn_item_type=hn_item_type,
        hn_use_date_sort=hn_use_date_sort,
        start_date=run_date,
        end_date=run_date,
    )

    return {
        "run_date": run_date,
        "s3_bucket": s3_bucket,
        "fetcher_specs": fetcher_specs,
        "min_rows_to_publish": min_rows_to_publish,
    }


@task
def process_and_upload(config: dict) -> dict:
    records, summary = run_text_ingest_records(config["fetcher_specs"])
    if should_fail_pipeline(records, summary):
        raise RuntimeError("All sources failed and no records were produced")

    normalized_records = normalize_records(records)
    filtered_records = dedupe_and_quality_filter(normalized_records)
    min_rows = int(config.get("min_rows_to_publish", 1))
    if len(filtered_records) < min_rows:
        raise RuntimeError(
            f"Refusing to publish low-volume dataset: {len(filtered_records)} rows < required {min_rows}"
        )

    keys = build_s3_keys(config["run_date"])
    manifest = build_manifest(
        run_date=config["run_date"], records=filtered_records,
        summary=summary, keys=keys,
    )
    with tempfile.TemporaryDirectory(prefix="research_daily_") as output_dir:
        dataset_path = str(Path(output_dir) / "dataset.parquet")
        write_dataset_parquet(filtered_records, dataset_path)
        return upload_artifacts_to_s3(
            s3_bucket=config["s3_bucket"], dataset_path=dataset_path,
            manifest=manifest, keys=keys,
        )


@task
def merge_daily_datasets(config: dict) -> dict:
    return merge_daily_datasets_to_s3(
        s3_bucket=config["s3_bucket"],
        run_date=config["run_date"],
    )


@dag(
    start_date=datetime(2024, 9, 1),
    schedule="0 2 * * *",
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": duration(minutes=2)},
    catchup=False,
    on_failure_callback=notify_dag_failure,
)
def text_daily_pipeline():
    config = build_ingestion_config()
    uploaded = process_and_upload(config)
    merged = merge_daily_datasets(config)
    uploaded >> merged


text_daily_pipeline()
