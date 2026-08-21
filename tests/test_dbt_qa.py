from __future__ import annotations

import json
from pathlib import Path

import duckdb


from include.spotify.dbt_qa import DbtQaAnalyzer  # noqa: E402


def _write_artifacts(tmp_path: Path, status: str) -> tuple[Path, Path]:
    unique_id = "test.spotify.not_null_episodes_episode_id"
    run_results = {
        "results": [
            {
                "unique_id": unique_id,
                "status": status,
                "message": "1 result",
                "failures": 1,
            }
        ]
    }
    manifest = {
        "nodes": {
            unique_id: {
                "name": "not_null_episodes_episode_id",
                "original_file_path": "models/schema.yml",
                "compiled_code": "select * from episodes where episode_id is null",
                "depends_on": {"nodes": ["model.spotify.episodes"]},
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "episode_id"},
                },
            }
        }
    }
    run_results_path = tmp_path / "run_results.json"
    manifest_path = tmp_path / "manifest.json"
    run_results_path.write_text(json.dumps(run_results), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return run_results_path, manifest_path


def test_collect_failure_evidence_queries_read_only_warehouse(tmp_path: Path) -> None:
    database_path = tmp_path / "spotify.duckdb"
    connection = duckdb.connect(str(database_path))
    connection.execute(
        "create table episodes (episode_id varchar, episode_name varchar)"
    )
    connection.execute("insert into episodes values (null, 'broken'), ('ok', 'valid')")
    connection.close()
    run_results_path, manifest_path = _write_artifacts(tmp_path, "fail")

    evidence, omitted = DbtQaAnalyzer.collect_failure_evidence(
        run_results_path, manifest_path, database_path
    )

    assert omitted == 0
    assert len(evidence) == 1
    assert evidence[0]["failure_count"] == 1
    assert evidence[0]["sample_rows"] == [
        {"episode_id": None, "episode_name": "[REDACTED:str:length=6]"}
    ]
    assert evidence[0]["original_file_path"] == "models/schema.yml"
    assert "compiled_sql" not in evidence[0]
    assert "message" not in evidence[0]


def test_collect_failure_evidence_ignores_passing_tests(tmp_path: Path) -> None:
    database_path = tmp_path / "spotify.duckdb"
    connection = duckdb.connect(str(database_path))
    connection.execute("create table episodes (episode_id varchar)")
    connection.close()
    run_results_path, manifest_path = _write_artifacts(tmp_path, "pass")

    evidence, omitted = DbtQaAnalyzer.collect_failure_evidence(
        run_results_path, manifest_path, database_path
    )

    assert evidence == []
    assert omitted == 0


def test_failure_evidence_round_trip_is_bounded_and_inert(tmp_path: Path) -> None:
    database_path = tmp_path / "spotify.duckdb"
    connection = duckdb.connect(str(database_path))
    connection.execute("create table episodes (episode_id varchar, episode_name varchar)")
    connection.execute("insert into episodes values (null, 'private value')")
    connection.close()
    run_results_path, manifest_path = _write_artifacts(tmp_path, "fail")
    evidence_path = tmp_path / "failure_evidence.json"

    DbtQaAnalyzer.write_failure_evidence(
        run_results_path=run_results_path,
        manifest_path=manifest_path,
        database_path=database_path,
        output_path=evidence_path,
    )
    evidence, omitted = DbtQaAnalyzer.read_failure_evidence(evidence_path)

    assert omitted == 0
    assert len(evidence) == 1
    serialized = evidence_path.read_text(encoding="utf-8")
    assert "private value" not in serialized
    assert "select * from episodes" not in serialized


def test_read_failure_evidence_rejects_unknown_schema(tmp_path: Path) -> None:
    evidence_path = tmp_path / "failure_evidence.json"
    evidence_path.write_text(
        json.dumps({"schema_version": 999, "evidence": []}), encoding="utf-8"
    )

    try:
        DbtQaAnalyzer.read_failure_evidence(evidence_path)
    except ValueError as exc:
        assert "schema version" in str(exc)
    else:
        raise AssertionError("unknown evidence schema should be rejected")


def test_read_failure_evidence_rejects_oversized_file(tmp_path: Path) -> None:
    evidence_path = tmp_path / "failure_evidence.json"
    evidence_path.write_bytes(b"x" * (DbtQaAnalyzer.max_evidence_file_bytes + 1))

    try:
        DbtQaAnalyzer.read_failure_evidence(evidence_path)
    except ValueError as exc:
        assert "size limit" in str(exc)
    else:
        raise AssertionError("oversized evidence should be rejected")
