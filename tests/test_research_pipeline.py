from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from include.research import research_pipeline as rp


def test_parse_search_terms_supports_json_and_csv() -> None:
    assert rp.parse_search_terms('["ml", "nlp"]') == ["ml", "nlp"]
    assert rp.parse_search_terms("ml, nlp ,  ") == ["ml", "nlp"]


def test_build_fetcher_specs_maps_limits_and_delay() -> None:
    specs = rp.build_fetcher_specs(
        search_terms=["data engineering"],
        max_records_per_source=250,
        request_delay_seconds=2.0,
        crossref_email="team@example.com",
        openalex_api_key="key-1",
        newsapi_api_key="news-key-1",
        newsapi_language="en",
        hn_item_type="story",
        hn_use_date_sort=True,
        start_date="2026-01-01",
        end_date="2026-01-01",
    )

    assert len(specs) == 5
    openalex = specs[0]
    crossref = specs[1]
    hackernews = specs[2]
    federalregister = specs[3]
    newsapi = specs[4]

    assert openalex["source"] == "openalex"
    assert openalex["config"]["max_pages"] == 3
    assert openalex["config"]["http"]["requests_per_second"] == 0.5
    assert openalex["config"]["http"]["email"] == "team@example.com"
    assert openalex["config"]["api_key"] == "key-1"

    assert crossref["source"] == "crossref"
    assert crossref["config"]["max_pages"] == 3
    assert crossref["config"]["rows"] == 100
    assert hackernews["source"] == "hackernews"
    assert hackernews["config"]["hn_item_type"] == "story"
    assert hackernews["config"]["hits_per_page"] == 100
    assert federalregister["source"] == "federalregister"
    assert federalregister["config"]["per_page"] == 50
    assert newsapi["source"] == "newsapi"
    assert newsapi["config"]["api_key"] == "news-key-1"
    assert newsapi["config"]["language"] == "en"


def test_build_fetcher_specs_skips_newsapi_when_api_key_missing() -> None:
    specs = rp.build_fetcher_specs(
        search_terms=["data engineering"],
        max_records_per_source=100,
        request_delay_seconds=1.0,
        crossref_email="",
        openalex_api_key="",
        newsapi_api_key="",
        newsapi_language="en",
        hn_item_type="story",
        hn_use_date_sort=True,
        start_date="2026-01-01",
        end_date="2026-01-01",
    )
    assert len(specs) == 4


def test_normalize_record_openalex_shape() -> None:
    record = {
        "source": "openalex",
        "external_id": "https://openalex.org/W123",
        "title": "Paper title",
        "abstract": "Abstract",
        "authors": ["A", "B"],
        "published_date": "2026-01-01",
        "url": "https://example.org/paper",
        "topic": "Data Engineering",
        "raw_payload": {
            "doi": "https://doi.org/10.1000/xyz",
            "language": "en",
            "primary_location": {"source": {"display_name": "Nature"}},
            "open_access": {"oa_status": "gold"},
        },
    }

    normalized = rp.normalize_record(record, "2026-01-01T00:00:00+00:00")

    assert normalized["doi"] == "10.1000/xyz"
    assert normalized["journal"] == "Nature"
    assert normalized["license"] == "gold"
    assert normalized["authors"] == json.dumps(["A", "B"])


def test_normalize_record_crossref_shape() -> None:
    record = {
        "source": "crossref",
        "external_id": "10.1000/abc",
        "title": "Crossref Paper",
        "abstract": "A",
        "authors": ["A"],
        "published_date": "2026-01-02",
        "url": "https://doi.org/10.1000/abc",
        "topic": "AI",
        "raw_payload": {
            "DOI": "10.1000/abc",
            "container-title": ["Journal of Data"],
            "language": "en",
            "license": [{"URL": "https://license.example/cc-by"}],
        },
    }

    normalized = rp.normalize_record(record, "2026-01-01T00:00:00+00:00")

    assert normalized["doi"] == "10.1000/abc"
    assert normalized["journal"] == "Journal of Data"
    assert normalized["license"] == "https://license.example/cc-by"


def test_dedupe_and_quality_filter_prefers_doi_then_source_id() -> None:
    records = [
        {"source": "openalex", "source_record_id": "W1", "doi": "10.1/dup", "title": "A"},
        {"source": "crossref", "source_record_id": "C1", "doi": "10.1/dup", "title": "B"},
        {"source": "openalex", "source_record_id": "W1", "doi": None, "title": "A2"},
        {"source": "openalex", "source_record_id": "W1", "doi": None, "title": "A3"},
        {"source": "openalex", "source_record_id": None, "doi": None, "title": "missing id"},
        {"source": "openalex", "source_record_id": "W2", "doi": None, "title": ""},
    ]

    filtered = rp.dedupe_and_quality_filter(records)

    assert len(filtered) == 2
    assert filtered[0]["doi"] == "10.1/dup"
    assert filtered[1]["source_record_id"] == "W1"


def test_build_manifest_contains_core_fields() -> None:
    keys = rp.build_s3_keys("2026-01-01")
    summary = {"failed_sources": {"openalex": "timeout"}, "by_source": {"crossref": 5}}
    manifest = rp.build_manifest(
        run_date="2026-01-01",
        records=[{"id": 1}, {"id": 2}],
        summary=summary,
        keys=keys,
    )

    assert manifest["dataset"] == "text_daily"
    assert manifest["row_count"] == 2
    assert manifest["failed_sources"] == {"openalex": "timeout"}
    assert manifest["s3_dataset_key"] == keys["dataset_key"]


def test_should_fail_pipeline_only_when_no_records_and_failures() -> None:
    assert rp.should_fail_pipeline([], {"failed_sources": {"openalex": "err"}}) is True
    assert rp.should_fail_pipeline([], {"failed_sources": {}}) is False
    assert rp.should_fail_pipeline([{"id": 1}], {"failed_sources": {"openalex": "err"}}) is False


def test_write_dataset_parquet_writes_canonical_columns(tmp_path: Path) -> None:
    output = tmp_path / "dataset.parquet"
    rp.write_dataset_parquet([
        {
            "source": "openalex",
            "source_record_id": "W1",
            "doi": "10.1/1",
            "title": "t",
        }
    ], str(output))

    frame = pd.read_parquet(output)
    assert list(frame.columns) == rp.CANONICAL_FIELDS
    assert frame.iloc[0]["title"] == "t"


def test_upload_artifacts_to_s3_uploads_dataset_manifest_and_latest(tmp_path: Path) -> None:
    dataset_file = tmp_path / "dataset.parquet"
    pd.DataFrame([{"a": 1}]).to_parquet(dataset_file)

    keys = rp.build_s3_keys("2026-01-01")
    manifest = {"hello": "world"}

    class FakeS3Hook:
        def __init__(self, aws_conn_id: str):
            self.aws_conn_id = aws_conn_id
            self.calls: list[tuple[str, str, str, bool]] = []

        def load_file(self, filename: str, key: str, bucket_name: str, replace: bool) -> None:
            self.calls.append((filename, key, bucket_name, replace))

    fake_hook = FakeS3Hook("aws_conn")

    class FakeS3HookCtor:
        def __new__(cls, aws_conn_id: str):
            return fake_hook

    uris = rp.upload_artifacts_to_s3(
        s3_bucket="bucket",
        dataset_path=str(dataset_file),
        manifest=manifest,
        keys=keys,
        s3_hook_cls=FakeS3HookCtor,
    )

    assert len(fake_hook.calls) == 3
    assert fake_hook.calls[0][1] == keys["dataset_key"]
    assert fake_hook.calls[1][1] == keys["manifest_key"]
    assert fake_hook.calls[2][1] == keys["latest_key"]
    assert uris["dataset_uri"].startswith("s3://bucket/")


def test_run_text_ingest_records_returns_records_and_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRecord:
        def __init__(self, payload: dict):
            self.payload = payload

        def model_dump(self, mode: str = "json") -> dict:
            return self.payload

    class FakeSummary:
        def model_dump(self, mode: str = "json") -> dict:
            return {"failed_sources": {"openalex": "timeout"}, "by_source": {"crossref": 2}}

    class FakePipeline:
        def __init__(self, sink, config):
            self.sink = sink
            self.config = config

        def run(self, fetchers):
            self.sink.records.extend(
                [
                    FakeRecord({"source": "crossref", "external_id": "10.1/abc", "raw_payload": {}}),
                    FakeRecord({"source": "crossref", "external_id": "10.1/xyz", "raw_payload": {}}),
                ]
            )
            return FakeSummary()

    monkeypatch.setattr(rp, "DataDumperPipeline", FakePipeline)
    monkeypatch.setattr(rp, "build_fetchers", lambda specs: ["fetcher"])

    records, summary = rp.run_text_ingest_records([{"source": "crossref", "config": {}}])

    assert len(records) == 2
    assert summary["failed_sources"]["openalex"] == "timeout"


def test_merge_daily_datasets_to_s3_creates_single_merged_file(tmp_path: Path) -> None:
    day1 = tmp_path / "d1.parquet"
    day2 = tmp_path / "d2.parquet"
    pd.DataFrame(
        [
            {"title": "a", "source": "openalex", "source_record_id": "1"},
            {"title": "b", "source": "crossref", "source_record_id": "2"},
        ]
    ).to_parquet(day1, index=False)
    pd.DataFrame(
        [
            {"title": "c", "source": "newsapi", "source_record_id": "3"},
        ]
    ).to_parquet(day2, index=False)

    class FakeS3Hook:
        def __init__(self, aws_conn_id: str):
            self.aws_conn_id = aws_conn_id
            self.files = {
                "datasets/text_daily/dt=2026-03-14/dataset.parquet": str(day1),
                "datasets/text_daily/dt=2026-03-15/dataset.parquet": str(day2),
            }
            self.uploads: list[tuple[str, str, str, bool]] = []

        def list_keys(self, bucket_name: str, prefix: str) -> list[str]:
            assert bucket_name == "bucket"
            assert prefix == "datasets/text_daily/dt="
            return list(self.files.keys())

        def download_file(self, key: str, bucket_name: str) -> str:
            assert bucket_name == "bucket"
            return self.files[key]

        def load_file(self, filename: str, key: str, bucket_name: str, replace: bool) -> None:
            self.uploads.append((filename, key, bucket_name, replace))

    fake_hook = FakeS3Hook("aws_conn")

    class FakeS3HookCtor:
        def __new__(cls, aws_conn_id: str):
            return fake_hook

    result = rp.merge_daily_datasets_to_s3(
        s3_bucket="bucket",
        run_date="2026-03-15",
        s3_hook_cls=FakeS3HookCtor,
    )

    assert result["merged_dataset_uri"] == "s3://bucket/datasets/text_daily/merged/dataset.csv"
    assert result["partition_count"] == 2
    assert result["merged_row_count"] == 3
    assert [upload[1] for upload in fake_hook.uploads] == [
        "datasets/text_daily/merged/dataset.csv",
        "datasets/text_daily/merged/manifest.json",
        "datasets/text_daily/merged/latest.json",
    ]
