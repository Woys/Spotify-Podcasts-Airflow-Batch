"""Pipeline helpers for research DAGs."""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd

try:
    from airflow.sdk.exceptions import AirflowException
except Exception:  # pragma: no cover - keep module importable without full Airflow
    class AirflowException(Exception):
        pass

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except Exception:  # pragma: no cover - tests can inject s3_hook_cls
    class S3Hook:  # type: ignore[no-redef]
        def __init__(self, *args: Any, **kwargs: Any):
            raise ImportError("Install apache-airflow-providers-amazon to use S3 uploads")
from data_ingestion.config import PipelineConfig, RuntimeOptimizationConfig
from data_ingestion.factories import build_fetchers
from data_ingestion.pipeline import DataDumperPipeline


CANONICAL_FIELDS = [
    "ingested_at",
    "source",
    "source_record_id",
    "doi",
    "title",
    "abstract",
    "authors",
    "publication_date",
    "journal",
    "topics",
    "url",
    "language",
    "license",
    "raw_payload_hash",
]


@dataclass
class MemorySink:
    records: list[Any]

    def write_many(self, batch: list[Any]) -> None:
        self.records.extend(batch)

    def close(self) -> None:
        return None


def parse_search_terms(raw_terms: str | list[str]) -> list[str]:
    if isinstance(raw_terms, list):
        terms = [str(item).strip() for item in raw_terms]
        return [term for term in terms if term]

    if not raw_terms:
        return []

    try:
        parsed = json.loads(raw_terms)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except json.JSONDecodeError:
        pass

    return [part.strip() for part in raw_terms.split(",") if part.strip()]


def _requests_per_second(request_delay_seconds: float) -> float:
    if request_delay_seconds <= 0:
        return 10.0
    return max(0.1, min(10.0, 1.0 / request_delay_seconds))


def build_fetcher_specs(
    search_terms: list[str],
    max_records_per_source: int,
    request_delay_seconds: float,
    crossref_email: str,
    openalex_api_key: str,
    newsapi_api_key: str,
    newsapi_language: str,
    hn_item_type: str,
    hn_use_date_sort: bool,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    if not search_terms:
        raise ValueError("At least one search term is required")

    rps = _requests_per_second(request_delay_seconds)
    per_page = 100
    rows = 100
    newsapi_page_size = 100
    hn_hits_per_page = 100
    federalregister_per_page = 50
    openalex_max_pages = max(1, math.ceil(max_records_per_source / per_page))
    crossref_max_pages = max(1, math.ceil(max_records_per_source / rows))
    newsapi_max_pages = max(1, math.ceil(max_records_per_source / newsapi_page_size))
    hackernews_max_pages = max(1, math.ceil(max_records_per_source / hn_hits_per_page))
    federalregister_max_pages = max(1, math.ceil(max_records_per_source / federalregister_per_page))

    specs: list[dict[str, Any]] = []
    for term in search_terms:
        openalex_config: dict[str, Any] = {
            "query": term,
            "max_pages": openalex_max_pages,
            "per_page": per_page,
            "start_date": start_date,
            "end_date": end_date,
            "http": {
                "requests_per_second": rps,
            },
        }
        if crossref_email:
            openalex_config["http"]["email"] = crossref_email
        # Kept for future compatibility; current text-ingest fetcher ignores this field.
        if openalex_api_key:
            openalex_config["api_key"] = openalex_api_key

        crossref_config: dict[str, Any] = {
            "query": term,
            "max_pages": crossref_max_pages,
            "rows": rows,
            "start_date": start_date,
            "end_date": end_date,
            "http": {
                "requests_per_second": rps,
            },
        }
        if crossref_email:
            crossref_config["http"]["email"] = crossref_email

        specs.append({"source": "openalex", "config": openalex_config})
        specs.append({"source": "crossref", "config": crossref_config})
        specs.append(
            {
                "source": "hackernews",
                "config": {
                    "query": term,
                    "max_pages": hackernews_max_pages,
                    "hits_per_page": hn_hits_per_page,
                    "hn_item_type": hn_item_type if hn_item_type in {"story", "comment", "all"} else "story",
                    "use_date_sort": hn_use_date_sort,
                    "start_date": start_date,
                    "end_date": end_date,
                    "http": {
                        "requests_per_second": rps,
                    },
                },
            }
        )
        specs.append(
            {
                "source": "federalregister",
                "config": {
                    "query": term,
                    "max_pages": federalregister_max_pages,
                    "per_page": federalregister_per_page,
                    "start_date": start_date,
                    "end_date": end_date,
                    "http": {
                        "requests_per_second": rps,
                    },
                },
            }
        )
        if newsapi_api_key:
            specs.append(
                {
                    "source": "newsapi",
                    "config": {
                        "query": term,
                        "api_key": newsapi_api_key,
                        "language": newsapi_language or "en",
                        "page_size": newsapi_page_size,
                        "max_pages": newsapi_max_pages,
                        "start_date": start_date,
                        "end_date": end_date,
                        "http": {
                            "requests_per_second": rps,
                        },
                    },
                }
            )

    return specs


def run_text_ingest_records(fetcher_specs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sink = MemorySink(records=[])
    pipeline = DataDumperPipeline(
        sink=sink,
        config=PipelineConfig(
            fail_fast=False,
            runtime=RuntimeOptimizationConfig(
                enrich_full_text=False,
                write_raw_payload=True,
                sink_write_batch_size=500,
            ),
        ),
    )
    summary = pipeline.run(build_fetchers(fetcher_specs))

    records = [record.model_dump(mode="json") for record in sink.records]
    summary_payload = summary.model_dump(mode="json")
    return records, summary_payload


def extract_doi(source: str, raw_payload: dict[str, Any]) -> str | None:
    if source == "crossref":
        return raw_payload.get("DOI")
    if source == "openalex":
        doi = raw_payload.get("doi")
        if isinstance(doi, str) and doi.startswith("https://doi.org/"):
            return doi.replace("https://doi.org/", "")
        return doi
    return None


def extract_journal(source: str, raw_payload: dict[str, Any]) -> str | None:
    if source == "crossref":
        container = raw_payload.get("container-title") or []
        if container:
            return str(container[0])
    if source == "openalex":
        host = raw_payload.get("primary_location", {}).get("source", {})
        display_name = host.get("display_name")
        if display_name:
            return str(display_name)
    return None


def extract_license(raw_payload: dict[str, Any]) -> str | None:
    licenses = raw_payload.get("license")
    if isinstance(licenses, list) and licenses:
        return str(licenses[0].get("URL") or licenses[0].get("url") or "") or None

    open_access = raw_payload.get("open_access")
    if isinstance(open_access, dict):
        oa_status = open_access.get("oa_status")
        if oa_status:
            return str(oa_status)
    return None


def normalize_record(record: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    raw_payload = record.get("raw_payload") or {}
    source = str(record.get("source") or "")
    source_record_id = record.get("external_id")
    doi = extract_doi(source, raw_payload)

    payload_bytes = json.dumps(raw_payload, sort_keys=True, default=str).encode("utf-8")
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()

    authors = record.get("authors") or []
    if isinstance(authors, list):
        authors_value = json.dumps(authors, ensure_ascii=False)
    else:
        authors_value = json.dumps([str(authors)], ensure_ascii=False)

    publication_date = record.get("published_date")
    if publication_date is not None:
        publication_date = str(publication_date)

    return {
        "ingested_at": ingested_at,
        "source": source,
        "source_record_id": source_record_id,
        "doi": doi,
        "title": record.get("title"),
        "abstract": record.get("abstract"),
        "authors": authors_value,
        "publication_date": publication_date,
        "journal": extract_journal(source, raw_payload),
        "topics": record.get("topic"),
        "url": record.get("url"),
        "language": raw_payload.get("language"),
        "license": extract_license(raw_payload),
        "raw_payload_hash": payload_hash,
    }


def normalize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    return [normalize_record(record, ingested_at) for record in records]


def dedupe_and_quality_filter(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for record in records:
        if not record.get("title"):
            continue

        doi = record.get("doi")
        source = record.get("source")
        source_record_id = record.get("source_record_id")

        if doi:
            dedupe_key = f"doi:{str(doi).lower()}"
        elif source and source_record_id:
            dedupe_key = f"source:{source}:{source_record_id}"
        else:
            continue

        if dedupe_key in seen_keys:
            continue

        seen_keys.add(dedupe_key)
        deduped.append(record)

    return deduped


def should_fail_pipeline(records: list[dict[str, Any]], summary: dict[str, Any]) -> bool:
    if records:
        return False
    failed_sources = summary.get("failed_sources") or {}
    return bool(failed_sources)


def build_s3_keys(run_date: str) -> dict[str, str]:
    base_prefix = "datasets/text_daily"
    versioned_prefix = f"{base_prefix}/dt={run_date}"

    return {
        "dataset_key": f"{versioned_prefix}/dataset.parquet",
        "manifest_key": f"{versioned_prefix}/manifest.json",
        "latest_key": f"{base_prefix}/latest.json",
        "versioned_prefix": versioned_prefix,
    }


def write_dataset_parquet(records: list[dict[str, Any]], output_path: str) -> str:
    frame = pd.DataFrame(records)
    for col in CANONICAL_FIELDS:
        if col not in frame.columns:
            frame[col] = None
    frame = frame[CANONICAL_FIELDS]
    frame.to_parquet(output_path, index=False)
    return output_path


def build_manifest(run_date: str, records: list[dict[str, Any]], summary: dict[str, Any], keys: dict[str, str]) -> dict[str, Any]:
    return {
        "dataset": "text_daily",
        "run_date": run_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(records),
        "s3_dataset_key": keys["dataset_key"],
        "s3_manifest_key": keys["manifest_key"],
        "failed_sources": summary.get("failed_sources", {}),
        "by_source": summary.get("by_source", {}),
    }


def upload_artifacts_to_s3(
    s3_bucket: str,
    dataset_path: str,
    manifest: dict[str, Any],
    keys: dict[str, str],
    aws_conn_id: str = "aws_conn",
    s3_hook_cls: type[S3Hook] = S3Hook,
) -> dict[str, str]:
    s3 = s3_hook_cls(aws_conn_id=aws_conn_id)

    s3.load_file(
        filename=dataset_path,
        key=keys["dataset_key"],
        bucket_name=s3_bucket,
        replace=True,
    )

    with NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp_manifest:
        json.dump(manifest, tmp_manifest)
        manifest_path = tmp_manifest.name

    try:
        s3.load_file(
            filename=manifest_path,
            key=keys["manifest_key"],
            bucket_name=s3_bucket,
            replace=True,
        )
    finally:
        Path(manifest_path).unlink(missing_ok=True)

    latest_payload = {
        "dataset_key": keys["dataset_key"],
        "manifest_key": keys["manifest_key"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp_latest:
        json.dump(latest_payload, tmp_latest)
        latest_path = tmp_latest.name

    try:
        s3.load_file(
            filename=latest_path,
            key=keys["latest_key"],
            bucket_name=s3_bucket,
            replace=True,
        )
    finally:
        Path(latest_path).unlink(missing_ok=True)

    return {
        "dataset_uri": f"s3://{s3_bucket}/{keys['dataset_key']}",
        "manifest_uri": f"s3://{s3_bucket}/{keys['manifest_key']}",
        "latest_uri": f"s3://{s3_bucket}/{keys['latest_key']}",
    }


def merge_daily_datasets_to_s3(
    s3_bucket: str,
    run_date: str,
    aws_conn_id: str = "aws_conn",
    s3_hook_cls: type[S3Hook] = S3Hook,
) -> dict[str, Any]:
    s3 = s3_hook_cls(aws_conn_id=aws_conn_id)

    daily_prefix = "datasets/text_daily/dt="
    merged_prefix = "datasets/text_daily/merged"
    merged_dataset_key = f"{merged_prefix}/dataset.csv"
    merged_manifest_key = f"{merged_prefix}/manifest.json"
    merged_latest_key = f"{merged_prefix}/latest.json"

    all_keys = s3.list_keys(bucket_name=s3_bucket, prefix=daily_prefix) or []
    daily_dataset_keys = sorted(
        key for key in all_keys if key.endswith("/dataset.parquet")
    )

    if not daily_dataset_keys:
        raise AirflowException("No daily datasets found to merge")

    frames: list[pd.DataFrame] = []
    for key in daily_dataset_keys:
        local_path = s3.download_file(key=key, bucket_name=s3_bucket)
        frames.append(pd.read_parquet(local_path))

    merged_frame = pd.concat(frames, ignore_index=True, sort=False)
    for col in CANONICAL_FIELDS:
        if col not in merged_frame.columns:
            merged_frame[col] = None
    merged_frame = merged_frame[CANONICAL_FIELDS]

    with NamedTemporaryFile(suffix=".csv", delete=False) as tmp_dataset:
        merged_frame.to_csv(tmp_dataset.name, index=False)
        merged_dataset_path = tmp_dataset.name

    try:
        s3.load_file(
            filename=merged_dataset_path,
            key=merged_dataset_key,
            bucket_name=s3_bucket,
            replace=True,
        )
    finally:
        Path(merged_dataset_path).unlink(missing_ok=True)

    merged_manifest = {
        "dataset": "text_daily",
        "artifact": "merged",
        "run_date": run_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(merged_frame)),
        "source_partitions": daily_dataset_keys,
        "s3_dataset_key": merged_dataset_key,
        "s3_manifest_key": merged_manifest_key,
    }

    with NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp_manifest:
        json.dump(merged_manifest, tmp_manifest)
        merged_manifest_path = tmp_manifest.name

    try:
        s3.load_file(
            filename=merged_manifest_path,
            key=merged_manifest_key,
            bucket_name=s3_bucket,
            replace=True,
        )
    finally:
        Path(merged_manifest_path).unlink(missing_ok=True)

    merged_latest = {
        "dataset_key": merged_dataset_key,
        "manifest_key": merged_manifest_key,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp_latest:
        json.dump(merged_latest, tmp_latest)
        merged_latest_path = tmp_latest.name

    try:
        s3.load_file(
            filename=merged_latest_path,
            key=merged_latest_key,
            bucket_name=s3_bucket,
            replace=True,
        )
    finally:
        Path(merged_latest_path).unlink(missing_ok=True)

    return {
        "merged_dataset_uri": f"s3://{s3_bucket}/{merged_dataset_key}",
        "merged_manifest_uri": f"s3://{s3_bucket}/{merged_manifest_key}",
        "merged_latest_uri": f"s3://{s3_bucket}/{merged_latest_key}",
        "merged_row_count": int(len(merged_frame)),
        "partition_count": len(daily_dataset_keys),
    }


def write_json_payload(path: str, payload: dict[str, Any]) -> str:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
    return path


def read_json_payload(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def ensure_non_empty_bucket(bucket: str) -> None:
    if not bucket:
        raise AirflowException(
            "SP_S3_BUCKET variable must be set (DATASET_S3_BUCKET also supported as fallback)"
        )


def bool_env(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def apply_openalex_key(openalex_api_key: str) -> None:
    if openalex_api_key:
        os.environ["OPENALEX_API_KEY"] = openalex_api_key


def apply_newsapi_key(newsapi_api_key: str) -> None:
    if newsapi_api_key:
        os.environ["NEWSAPI_KEY"] = newsapi_api_key
