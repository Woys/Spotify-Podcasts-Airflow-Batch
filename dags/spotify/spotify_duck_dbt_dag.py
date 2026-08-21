from pendulum import datetime, duration
from airflow.sdk import dag
from include.notification import notify_dag_failure
from include.spotify.dbt_workflow import DuckDbtDag, DuckDbtDagConfig


dbt_config = DuckDbtDagConfig(
    dbt_project_dir="/opt/airflow/dbt/duck_dbt",
    dbt_profiles_dir="/opt/airflow/dbt",
    database_key="warehouse/spotify.duckdb",
    failure_prefix="warehouse/dbt-test-failures",
    run_command=("dbt", "run"),
    test_command=("dbt", "test"),
    qa_dag_id="spotify_dbt_qa",
)


@dag(
    start_date=datetime(2024, 9, 1),
    max_active_runs=1,
    schedule=None,
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    catchup=False,
    on_failure_callback=notify_dag_failure,
)
def spotify_duck_dbt():
    DuckDbtDag(dbt_config).run()


spotify_duck_dbt()
