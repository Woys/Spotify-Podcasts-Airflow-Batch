"""Quality-analysis helpers for Spotify dbt workflows."""

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Literal

import duckdb
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openrouter import ChatOpenRouter
from pydantic import BaseModel, Field


class _TestRecommendation(BaseModel):
    test_unique_id: str
    severity: Literal["critical", "high", "medium", "low"]
    evidence: list[str]
    likely_causes: list[str]
    dbt_resolution: list[str]
    upstream_resolution: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)


class _QaAnalysis(BaseModel):
    summary: str
    recommendations: list[_TestRecommendation]


class DbtQaAnalyzer:
    """Collect dbt test evidence and request remediation advice."""

    max_failed_tests = 50
    max_sample_rows = 20
    max_evidence_file_bytes = 2_000_000
    evidence_schema_version = 1

    @staticmethod
    def _read_json(path: str | Path) -> dict[str, Any]:
        with Path(path).open(encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            raise ValueError(f"Expected a JSON object in {path}")
        return value

    @classmethod
    def _safe_value(cls, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, (date, datetime, Decimal)):
            return str(value)
        if not isinstance(value, str):
            value = repr(value)
        return f"[REDACTED:{type(value).__name__}:length={len(value)}]"

    @classmethod
    def _test_evidence(
        cls,
        connection: duckdb.DuckDBPyConnection,
        unique_id: str,
        result: dict[str, Any],
        node: dict[str, Any],
    ) -> dict[str, Any]:
        compiled_sql = (node.get("compiled_code") or "").strip().rstrip(";")
        evidence: dict[str, Any] = {
            "test_unique_id": unique_id,
            "status": result.get("status"),
            "failures": result.get("failures"),
            "test_name": node.get("name"),
            "original_file_path": node.get("original_file_path"),
            "depends_on_nodes": node.get("depends_on", {}).get("nodes", []),
            "test_metadata": node.get("test_metadata", {}),
            "failure_count": None,
            "sample_rows": [],
        }
        if not compiled_sql:
            evidence["query_error"] = "The dbt manifest did not contain compiled SQL."
            return evidence

        try:
            evidence["failure_count"] = connection.execute(
                # This SQL is generated and consumed inside the trusted dbt task.
                f"select count(*) from ({compiled_sql}) as dbt_failed_rows"  # nosec B608
            ).fetchone()[0]
            cursor = connection.execute(
                f"select * from ({compiled_sql}) as dbt_failed_rows "  # nosec B608
                f"limit {cls.max_sample_rows}"
            )
            columns = [description[0] for description in cursor.description]
            evidence["sample_rows"] = [
                {
                    column: cls._safe_value(value)
                    for column, value in zip(columns, row, strict=True)
                }
                for row in cursor.fetchall()
            ]
        except Exception as exc:
            evidence["query_error"] = type(exc).__name__
        return evidence

    @classmethod
    def collect_failure_evidence(
        cls,
        run_results_path: str | Path,
        manifest_path: str | Path,
        database_path: str | Path,
    ) -> tuple[list[dict[str, Any]], int]:
        """Collect evidence using dbt's compiled failing-test SQL."""
        run_results = cls._read_json(run_results_path)
        manifest = cls._read_json(manifest_path)
        nodes = manifest.get("nodes", {})
        failed_results = [
            result
            for result in run_results.get("results", [])
            if result.get("status") in {"fail", "error"}
            and str(result.get("unique_id", "")).startswith("test.")
        ]
        selected_results = failed_results[: cls.max_failed_tests]

        evidence = []
        connection = duckdb.connect(str(database_path), read_only=True)
        try:
            for result in selected_results:
                unique_id = result["unique_id"]
                evidence.append(
                    cls._test_evidence(
                        connection,
                        unique_id,
                        result,
                        nodes.get(unique_id, {}),
                    )
                )
        finally:
            connection.close()
        return evidence, max(0, len(failed_results) - len(selected_results))

    @classmethod
    def write_failure_evidence(
        cls,
        *,
        run_results_path: str | Path,
        manifest_path: str | Path,
        database_path: str | Path,
        output_path: str | Path,
    ) -> None:
        """Create a bounded, redacted artifact for the untrusted QA boundary."""
        evidence, omitted_test_count = cls.collect_failure_evidence(
            run_results_path, manifest_path, database_path
        )
        payload = {
            "schema_version": cls.evidence_schema_version,
            "omitted_test_count": omitted_test_count,
            "evidence": evidence,
        }
        Path(output_path).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    @classmethod
    def read_failure_evidence(
        cls, evidence_path: str | Path
    ) -> tuple[list[dict[str, Any]], int]:
        if Path(evidence_path).stat().st_size > cls.max_evidence_file_bytes:
            raise ValueError("Failure evidence exceeds the configured size limit")
        payload = cls._read_json(evidence_path)
        if payload.get("schema_version") != cls.evidence_schema_version:
            raise ValueError("Unsupported failure evidence schema version")
        evidence = payload.get("evidence")
        omitted = payload.get("omitted_test_count", 0)
        if not isinstance(evidence, list) or not all(
            isinstance(item, dict) for item in evidence
        ):
            raise ValueError("Failure evidence must contain a list of objects")
        if not isinstance(omitted, int) or omitted < 0:
            raise ValueError("omitted_test_count must be a non-negative integer")
        if len(evidence) > cls.max_failed_tests:
            raise ValueError("Failure evidence exceeds the configured test limit")
        return evidence, omitted

    @classmethod
    def generate_report(
        cls,
        *,
        evidence_path: str | Path,
        api_key: str,
        model: str,
        source_dag_id: str,
        source_run_id: str,
        system_prompt: str,
        human_prompt: str,
    ) -> dict[str, Any]:
        """Create an evidence-backed QA report with OpenRouter."""
        evidence, omitted_test_count = cls.read_failure_evidence(evidence_path)
        if not evidence:
            raise ValueError("dbt artifacts contain no failed test results to analyze")

        parser = PydanticOutputParser(pydantic_object=_QaAnalysis)
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("human", human_prompt),
            ]
        )
        llm = ChatOpenRouter(
            model=model,
            api_key=api_key,
            temperature=0,
            timeout=60,
            max_retries=2,
            default_headers={"X-Title": "spotify-dbt-qa"},
        )
        analysis = (prompt | llm | parser).invoke(
            {
                "evidence": json.dumps(evidence, ensure_ascii=False, default=str),
                "format_instructions": parser.get_format_instructions(),
            }
        )
        return {
            "source_dag_id": source_dag_id,
            "source_run_id": source_run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "provider": "openrouter",
            "model": model,
            "omitted_test_count": omitted_test_count,
            "evidence": evidence,
            "analysis": analysis.model_dump(),
        }
