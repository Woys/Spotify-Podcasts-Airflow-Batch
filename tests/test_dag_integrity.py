from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"


@pytest.fixture(autouse=True)
def _airflow_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_SP_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("AIRFLOW_VAR_KAGGLE_USERNAME", "test-user")
    monkeypatch.setenv("AIRFLOW_VAR_KAGGLE_KEY", "test-key")
    monkeypatch.setenv("AIRFLOW_VAR_DBT_QA_EMAIL", "owner@example.com")


def test_all_dags_import_without_errors() -> None:
    pytest.importorskip("airflow")
    try:
        from airflow.dag_processing.dagbag import DagBag
    except (ImportError, ModuleNotFoundError):
        pytest.skip("DagBag is not available in this Airflow installation")

    sys.path.insert(0, str(DAGS_DIR))
    try:
        dag_bag = DagBag(dag_folder=str(DAGS_DIR))
    finally:
        sys.path.remove(str(DAGS_DIR))
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"

    expected_dags = {
        "text_daily_pipeline",
        "spotify_dbt_qa",
        "spotify_duck_dbt",
        "spotify_charts",
        "spotify_eps",
        "spotify_eps_backfill",
        "spotify_eps_union",
        "spotify_kaggle_upload",
        "spotify_kaggle_update",
    }
    assert expected_dags.issubset(set(dag_bag.dags))

    for dag_id in expected_dags:
        callbacks = dag_bag.dags[dag_id].on_failure_callback
        if not isinstance(callbacks, list):
            callbacks = [callbacks]
        assert any(
            callback.__name__ == "notify_dag_failure" for callback in callbacks
        ), f"Telegram failure callback missing from {dag_id}"


def test_dags_do_not_use_legacy_schedule_interval() -> None:
    dag_files = DAGS_DIR.rglob("*_dag.py")
    for dag_file in dag_files:
        content = dag_file.read_text(encoding="utf-8")
        assert "schedule_interval=" not in content, (
            f"Legacy schedule_interval found in {dag_file}"
        )
