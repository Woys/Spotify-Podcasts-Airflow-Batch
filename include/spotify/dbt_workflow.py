"""Reusable task-group builders for Spotify dbt DAGs."""

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException


@dataclass(frozen=True)
class DuckDbtDagConfig:
    """Configuration for a persisted DuckDB dbt DAG."""

    dbt_project_dir: str
    dbt_profiles_dir: str
    database_key: str
    failure_prefix: str
    run_command: tuple[str, ...] = ("dbt", "run")
    test_command: tuple[str, ...] = ("dbt", "test")
    target: str = "prod"
    bucket_variable: str = "SP_S3_BUCKET"
    aws_conn_id: str = "aws_conn"
    qa_dag_id: str = "spotify_dbt_qa"


@dataclass(frozen=True)
class DbtQaDagConfig:
    """Configuration for a failed-test QA DAG."""

    bucket_variable: str = "SP_S3_BUCKET"
    api_key_variable: str = "OPENROUTER_API_KEY"
    model: str = "openrouter/free"
    aws_conn_id: str = "aws_conn"
    failure_prefix: str = "warehouse/dbt-test-failures"
    source_dag_id: str = "spotify_duck_dbt"


class DuckDbtDag:
    """Build, test, and persist a DuckDB warehouse with dbt."""

    def __init__(self, config: DuckDbtDagConfig) -> None:
        self.config = config

    @staticmethod
    def _config_from_dict(values: dict[str, Any]) -> DuckDbtDagConfig:
        return DuckDbtDagConfig(**values)

    @staticmethod
    def _build_argv(command: tuple[str, ...], config: DuckDbtDagConfig) -> list[str]:
        if not command:
            raise ValueError("dbt command cannot be empty")
        return [
            *command,
            "--project-dir",
            config.dbt_project_dir,
            "--profiles-dir",
            config.dbt_profiles_dir,
            "--target",
            config.target,
        ]

    @staticmethod
    def _build_environment(
        *,
        s3: S3Hook,
        s3_bucket: str,
        database_path: str,
        output_dir: str,
        aws_conn_id: str,
    ) -> dict[str, str]:
        credentials = s3.get_credentials()
        env = {
            **os.environ,
            "SP_S3_BUCKET": s3_bucket,
            "DBT_DUCKDB_PATH": database_path,
            "DBT_LOG_PATH": os.path.join(output_dir, "logs"),
            "DBT_TARGET_PATH": os.path.join(output_dir, "target"),
            "AWS_ACCESS_KEY_ID": credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
        }
        if credentials.token:
            env["AWS_SESSION_TOKEN"] = credentials.token
        region_name = s3.get_connection(aws_conn_id).extra_dejson.get("region_name")
        if region_name:
            env["AWS_DEFAULT_REGION"] = region_name
        return env

    @staticmethod
    def _download_required_key(s3: S3Hook, bucket: str, key: str, destination: str) -> None:
        remote = s3.get_key(key=key, bucket_name=bucket)
        if remote is None:
            raise FileNotFoundError(f"s3://{bucket}/{key} does not exist")
        remote.download_file(destination)

    @staticmethod
    def _upload_failure_artifacts(
        *,
        s3: S3Hook,
        s3_bucket: str,
        artifact_prefix: str,
        completed: subprocess.CompletedProcess[str],
        output_dir: str,
        database_path: str,
    ) -> None:
        from include.spotify.dbt_qa import DbtQaAnalyzer

        run_results = Path(output_dir) / "target" / "run_results.json"
        manifest = Path(output_dir) / "target" / "manifest.json"
        evidence = Path(output_dir) / "failure_evidence.json"
        if run_results.is_file() and manifest.is_file():
            DbtQaAnalyzer.write_failure_evidence(
                run_results_path=run_results,
                manifest_path=manifest,
                database_path=database_path,
                output_path=evidence,
            )
        for artifact in (
            Path(output_dir) / "logs" / "dbt.log",
            evidence,
        ):
            if artifact.is_file():
                s3.load_file(
                    filename=str(artifact),
                    key=f"{artifact_prefix}/{artifact.name}",
                    bucket_name=s3_bucket,
                    replace=True,
                )

        summary = json.dumps(
            {"returncode": completed.returncode, "command": completed.args}, indent=2
        )
        for name, content in (
            ("dbt_stdout.log", completed.stdout),
            ("dbt_stderr.log", completed.stderr),
            ("failure_summary.json", summary),
        ):
            s3.load_string(
                string_data=content or "",
                key=f"{artifact_prefix}/{name}",
                bucket_name=s3_bucket,
                replace=True,
            )

    def run(self):
        config = self.config
        config_values = asdict(config)

        @task
        def run_dbt(s3_bucket: str) -> None:
            runtime_config = DuckDbtDag._config_from_dict(config_values)
            s3 = S3Hook(aws_conn_id=runtime_config.aws_conn_id)
            with tempfile.TemporaryDirectory() as tmpdirname:
                database_path = os.path.join(tmpdirname, "warehouse.duckdb")
                if s3.check_for_key(
                    key=runtime_config.database_key, bucket_name=s3_bucket
                ):
                    DuckDbtDag._download_required_key(
                        s3, s3_bucket, runtime_config.database_key, database_path
                    )
                env = DuckDbtDag._build_environment(
                    s3=s3,
                    s3_bucket=s3_bucket,
                    database_path=database_path,
                    output_dir=tmpdirname,
                    aws_conn_id=runtime_config.aws_conn_id,
                )
                subprocess.run(
                    DuckDbtDag._build_argv(
                        runtime_config.run_command, runtime_config
                    ),
                    check=True,
                    env=env,
                )
                s3.load_file(
                    filename=database_path,
                    key=runtime_config.database_key,
                    bucket_name=s3_bucket,
                    replace=True,
                )

        @task
        def run_dbt_tests(s3_bucket: str) -> None:
            runtime_config = DuckDbtDag._config_from_dict(config_values)
            s3 = S3Hook(aws_conn_id=runtime_config.aws_conn_id)
            context = get_current_context()
            artifact_prefix = f"{runtime_config.failure_prefix}/{context['run_id']}"
            with tempfile.TemporaryDirectory() as tmpdirname:
                database_path = os.path.join(tmpdirname, "warehouse.duckdb")
                DuckDbtDag._download_required_key(
                    s3, s3_bucket, runtime_config.database_key, database_path
                )
                env = DuckDbtDag._build_environment(
                    s3=s3,
                    s3_bucket=s3_bucket,
                    database_path=database_path,
                    output_dir=tmpdirname,
                    aws_conn_id=runtime_config.aws_conn_id,
                )
                completed = subprocess.run(
                    DuckDbtDag._build_argv(
                        runtime_config.test_command, runtime_config
                    ),
                    check=False,
                    capture_output=True,
                    text=True,
                    env=env,
                )
                if completed.stdout:
                    print(completed.stdout)
                if completed.stderr:
                    print(completed.stderr)
                if completed.returncode:
                    DuckDbtDag._upload_failure_artifacts(
                        s3=s3,
                        s3_bucket=s3_bucket,
                        artifact_prefix=artifact_prefix,
                        completed=completed,
                        output_dir=tmpdirname,
                        database_path=database_path,
                    )
                    context["ti"].xcom_push(key="dbt_test_failed", value=True)
                    raise subprocess.CalledProcessError(
                        completed.returncode,
                        completed.args,
                        output=completed.stdout,
                        stderr=completed.stderr,
                    )

        @task(trigger_rule="all_done", retries=0)
        def allow_qa_only_for_failed_tests() -> None:
            context = get_current_context()
            test_failed = context["ti"].xcom_pull(
                task_ids="run_dbt_tests", key="dbt_test_failed"
            )
            if test_failed is not True:
                raise AirflowSkipException(
                    "dbt QA is only triggered when dbt test reports a failure"
                )

        bucket = Variable.get(config.bucket_variable)
        build_task = run_dbt(bucket)
        test_task = run_dbt_tests(bucket)
        qa_gate = allow_qa_only_for_failed_tests()
        trigger_qa = TriggerDagRunOperator(
            task_id="trigger_dbt_qa",
            trigger_dag_id=config.qa_dag_id,
            trigger_run_id="dbt_qa__{{ run_id }}",
            conf={
                "artifact_prefix": f"{config.failure_prefix}/{{{{ run_id }}}}",
                "source_dag_id": "{{ dag.dag_id }}",
                "source_run_id": "{{ run_id }}",
            },
            wait_for_completion=False,
            skip_when_already_exists=True,
        )
        build_task >> test_task >> qa_gate >> trigger_qa


class DbtQaDag:
    """Download failed-test artifacts and produce a QA report."""

    def __init__(self, config: DbtQaDagConfig) -> None:
        self.config = config

    @staticmethod
    def _download_required_key(s3: S3Hook, bucket: str, key: str, destination: str) -> None:
        remote = s3.get_key(key=key, bucket_name=bucket)
        if remote is None:
            raise FileNotFoundError(f"s3://{bucket}/{key} does not exist")
        remote.download_file(destination)

    @staticmethod
    def _validate_request(
        conf: dict[str, Any], config: DbtQaDagConfig
    ) -> tuple[str, str, str]:
        required = {"artifact_prefix", "source_dag_id", "source_run_id"}
        missing = sorted(required.difference(conf))
        if missing:
            raise ValueError(f"Missing QA DAG configuration: {', '.join(missing)}")

        artifact_prefix = str(conf["artifact_prefix"]).rstrip("/")
        source_dag_id = str(conf["source_dag_id"])
        source_run_id = str(conf["source_run_id"])
        expected_prefix = f"{config.failure_prefix}/{source_run_id}"
        if source_dag_id != config.source_dag_id:
            raise ValueError("QA request came from an unauthorized source DAG")
        if artifact_prefix != expected_prefix:
            raise ValueError("QA artifact prefix does not match the source run")
        return artifact_prefix, source_dag_id, source_run_id

    def run(self):
        config = self.config

        @task
        def run_failed_test_qa() -> str:
            from include.spotify.dbt_qa import DbtQaAnalyzer

            context = get_current_context()
            conf = dict(context["dag_run"].conf or {})
            artifact_prefix, source_dag_id, source_run_id = DbtQaDag._validate_request(
                conf, config
            )

            s3_bucket = Variable.get(config.bucket_variable)
            api_key = Variable.get(config.api_key_variable)
            model = config.model
            params = context["params"]
            s3 = S3Hook(aws_conn_id=config.aws_conn_id)
            with tempfile.TemporaryDirectory() as tmpdirname:
                evidence_path = os.path.join(tmpdirname, "failure_evidence.json")
                DbtQaDag._download_required_key(
                    s3,
                    s3_bucket,
                    f"{artifact_prefix}/failure_evidence.json",
                    evidence_path,
                )
                report = DbtQaAnalyzer.generate_report(
                    evidence_path=evidence_path,
                    api_key=api_key,
                    model=model,
                    source_dag_id=source_dag_id,
                    source_run_id=source_run_id,
                    system_prompt=str(params["system_prompt"]),
                    human_prompt=str(params["human_prompt"]),
                )
                report_key = f"{artifact_prefix}/qa_report.json"
                s3.load_string(
                    string_data=json.dumps(report, indent=2, default=str),
                    key=report_key,
                    bucket_name=s3_bucket,
                    replace=True,
                )
                return report_key

        return run_failed_test_qa()
