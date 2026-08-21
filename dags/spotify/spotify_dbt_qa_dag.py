from airflow.sdk import dag, task
from include.notification import (
    load_s3_report_email_context,
    notify_dag_failure,
    send_telegram,
)
from pendulum import datetime, duration

from include.spotify.dbt_workflow import DbtQaDag, DbtQaDagConfig


SYSTEM_PROMPT = (
    "You are a senior analytics engineer investigating failed dbt tests. "
    "Use only the supplied manifest metadata and query evidence. Recommend "
    "precise dbt SQL or YAML changes, and separate upstream data fixes from "
    "dbt fixes. Never invent tables, columns, counts, or file paths. Severity "
    "must reflect downstream analytical risk."
)

PROMPT = (
    "Analyze these failed dbt tests:\n{evidence}\n\n"
    "Return the requested structured report.\n{format_instructions}"
)

params = {
    "system_prompt": SYSTEM_PROMPT,
    "human_prompt": PROMPT,
}

TELEGRAM_URL_VARIABLE = "TELEGRAM_APPRISE_URL"

qa_config = DbtQaDagConfig(
    bucket_variable="SP_S3_BUCKET",
    api_key_variable="OPENROUTER_API_KEY",
    model="openrouter/free",
    failure_prefix="warehouse/dbt-test-failures",
    source_dag_id="spotify_duck_dbt",
)


@task
def send_qa_report_telegram(report_key: str) -> None:
    report_context = load_s3_report_email_context(
        report_key,
        recipient_variable=TELEGRAM_URL_VARIABLE,
        bucket_variable=qa_config.bucket_variable,
        aws_conn_id=qa_config.aws_conn_id,
    )

    message = (
        f"Source run: {report_context.source_run_id}\n"
        f"S3 report: s3://{report_context.s3_bucket}/{report_context.report_key}\n\n"
        f"{report_context.report_body}"
    )
    send_telegram(
        message,
        title=f"Spotify dbt QA report: {report_context.source_run_id}",
        url_variable=TELEGRAM_URL_VARIABLE,
    )


@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule=None,
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
    on_failure_callback=notify_dag_failure,
    params=params,
)
def spotify_dbt_qa():
    report_key = DbtQaDag(qa_config).run()
    send_qa_report_telegram(report_key)


spotify_dbt_qa()
