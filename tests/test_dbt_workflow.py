from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest


from include.spotify.dbt_workflow import (  # noqa: E402
    DbtQaDag,
    DbtQaDagConfig,
    DuckDbtDag,
    DuckDbtDagConfig,
)


def test_qa_request_validation_rejects_arbitrary_s3_artifacts() -> None:
    config = DbtQaDagConfig(
        failure_prefix="failures/dbt", source_dag_id="trusted_dag"
    )
    valid = {
        "artifact_prefix": "failures/dbt/run-1",
        "source_dag_id": "trusted_dag",
        "source_run_id": "run-1",
    }
    assert DbtQaDag._validate_request(valid, config) == (
        "failures/dbt/run-1",
        "trusted_dag",
        "run-1",
    )

    with pytest.raises(ValueError, match="unauthorized source DAG"):
        DbtQaDag._validate_request({**valid, "source_dag_id": "attacker"}, config)
    with pytest.raises(ValueError, match="prefix does not match"):
        DbtQaDag._validate_request(
            {**valid, "artifact_prefix": "attacker-controlled/path"}, config
        )


class FakeS3:
    def __init__(self, remote=None) -> None:
        self.remote = remote
        self.files: list[dict] = []
        self.strings: list[dict] = []

    def get_credentials(self):
        return SimpleNamespace(
            access_key="access", secret_key="secret", token="session"
        )

    def get_connection(self, conn_id: str):
        assert conn_id == "custom-aws"
        return SimpleNamespace(extra_dejson={"region_name": "us-east-2"})

    def get_key(self, **_kwargs):
        return self.remote

    def load_file(self, **kwargs) -> None:
        self.files.append(kwargs)

    def load_string(self, **kwargs) -> None:
        self.strings.append(kwargs)


def _config() -> DuckDbtDagConfig:
    return DuckDbtDagConfig(
        dbt_project_dir="/dbt/project",
        dbt_profiles_dir="/dbt/profiles",
        database_key="warehouse/example.duckdb",
        failure_prefix="failures/dbt",
    )


def test_build_dbt_argv_adds_common_arguments() -> None:
    assert DuckDbtDag._build_argv(("dbt", "run", "--full-refresh"), _config()) == [
        "dbt",
        "run",
        "--full-refresh",
        "--project-dir",
        "/dbt/project",
        "--profiles-dir",
        "/dbt/profiles",
        "--target",
        "prod",
    ]
    with pytest.raises(ValueError, match="cannot be empty"):
        DuckDbtDag._build_argv((), _config())


def test_build_dbt_environment_sets_duckdb_and_aws_values(tmp_path: Path) -> None:
    env = DuckDbtDag._build_environment(
        s3=FakeS3(),
        s3_bucket="bucket",
        database_path=str(tmp_path / "warehouse.duckdb"),
        output_dir=str(tmp_path),
        aws_conn_id="custom-aws",
    )
    assert env["SP_S3_BUCKET"] == "bucket"
    assert env["DBT_DUCKDB_PATH"].endswith("warehouse.duckdb")
    assert env["DBT_TARGET_PATH"].endswith("target")
    assert env["AWS_ACCESS_KEY_ID"] == "access"
    assert env["AWS_SESSION_TOKEN"] == "session"
    assert env["AWS_DEFAULT_REGION"] == "us-east-2"


def test_download_required_key(tmp_path: Path) -> None:
    destination = tmp_path / "artifact.json"
    remote = SimpleNamespace(download_file=lambda path: Path(path).write_text("{}"))
    DuckDbtDag._download_required_key(
        FakeS3(remote), "bucket", "artifact.json", str(destination)
    )
    assert destination.read_text() == "{}"

    with pytest.raises(FileNotFoundError, match="s3://bucket/missing.json"):
        DuckDbtDag._download_required_key(
            FakeS3(), "bucket", "missing.json", str(destination)
        )


def test_upload_failure_artifacts(tmp_path: Path) -> None:
    target = tmp_path / "target"
    logs = tmp_path / "logs"
    target.mkdir()
    logs.mkdir()
    unique_id = "test.spotify.not_null_episode_id"
    (target / "run_results.json").write_text(
        '{"results":[{"unique_id":"' + unique_id + '","status":"fail"}]}'
    )
    (target / "manifest.json").write_text(
        '{"nodes":{"' + unique_id + '":{"compiled_code":"select * from episodes"}}}'
    )
    (logs / "dbt.log").write_text("failure")
    database_path = tmp_path / "warehouse.duckdb"
    import duckdb

    connection = duckdb.connect(str(database_path))
    connection.execute("create table episodes (episode_id varchar)")
    connection.execute("insert into episodes values (null)")
    connection.close()
    s3 = FakeS3()
    completed = subprocess.CompletedProcess(
        ["dbt", "test"], 1, stdout="stdout", stderr="stderr"
    )

    DuckDbtDag._upload_failure_artifacts(
        s3=s3,
        s3_bucket="bucket",
        artifact_prefix="failures/run-1",
        completed=completed,
        output_dir=str(tmp_path),
        database_path=str(database_path),
    )

    assert {item["key"] for item in s3.files} == {
        "failures/run-1/dbt.log",
        "failures/run-1/failure_evidence.json",
    }
    assert {item["key"] for item in s3.strings} == {
        "failures/run-1/dbt_stdout.log",
        "failures/run-1/dbt_stderr.log",
        "failures/run-1/failure_summary.json",
    }
