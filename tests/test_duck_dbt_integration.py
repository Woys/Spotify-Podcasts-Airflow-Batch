from __future__ import annotations

import importlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"


def test_dbt_runs_in_separate_triggered_dag(monkeypatch) -> None:
    spotify_source = (
        ROOT / "dags" / "spotify" / "spotify_eps_dag.py"
    ).read_text(encoding="utf-8")
    assert "def run_dbt(" not in spotify_source
    assert 'trigger_dag_id="spotify_duck_dbt"' in spotify_source
    assert "upload_task >> [union_task, trigger_dbt]" in spotify_source

    monkeypatch.setenv("AIRFLOW_VAR_SP_S3_BUCKET", "test-bucket")
    sys.path.insert(0, str(DAGS_DIR))
    try:
        module = importlib.import_module("spotify.spotify_duck_dbt_dag")
    finally:
        sys.path.remove(str(DAGS_DIR))

    config = module.dbt_config
    dag = module.spotify_duck_dbt()
    assert config.run_command == ("dbt", "run")
    assert config.test_command == ("dbt", "test")
    assert config.database_key == "warehouse/spotify.duckdb"
    assert dag.dag_id == "spotify_duck_dbt"
    assert dag.schedule is None
    assert set(dag.task_ids) == {
        "run_dbt",
        "run_dbt_tests",
        "allow_qa_only_for_failed_tests",
        "trigger_dbt_qa",
    }
    assert dag.get_task("run_dbt_tests").upstream_task_ids == {"run_dbt"}
    assert dag.get_task("trigger_dbt_qa").trigger_dag_id == "spotify_dbt_qa"


def test_dbt_qa_saves_report_before_sending_telegram(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_SP_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("AIRFLOW_VAR_DBT_QA_EMAIL", "owner@example.com")
    sys.path.insert(0, str(DAGS_DIR))
    try:
        module = importlib.import_module("spotify.spotify_dbt_qa_dag")
    finally:
        sys.path.remove(str(DAGS_DIR))

    dag = module.spotify_dbt_qa()

    assert module.qa_config.failure_prefix == "warehouse/dbt-test-failures"
    assert module.qa_config.source_dag_id == "spotify_duck_dbt"
    assert set(dag.task_ids) == {"run_failed_test_qa", "send_qa_report_telegram"}
    assert dag.get_task("send_qa_report_telegram").upstream_task_ids == {
        "run_failed_test_qa"
    }


def test_airflow_image_pulls_independent_dbt_repository() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "DUCK_DBT_COMMIT=91e66e09deabbaaaf4807b8584e6b29b36172bf8" in dockerfile
    assert 'fetch --depth 1 origin "${DUCK_DBT_COMMIT}"' in dockerfile
    assert "github.com/Woys/Duck_dbt.git" in dockerfile
    assert "dbt deps --project-dir /opt/airflow/dbt/duck_dbt" in dockerfile
