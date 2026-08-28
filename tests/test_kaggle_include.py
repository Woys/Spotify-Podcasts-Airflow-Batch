from __future__ import annotations

import importlib
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock
from zipfile import ZipFile

import pytest


def _load_kaggle_module(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AIRFLOW_VAR_KAGGLE_USERNAME", "kaggle-user")
    monkeypatch.setenv("AIRFLOW_VAR_KAGGLE_KEY", "kaggle-key")
    module = importlib.import_module("include.spotify.kaggle")
    return importlib.reload(module)


def _logger() -> SimpleNamespace:
    return SimpleNamespace(info=Mock(), error=Mock())


def test_zip_and_delete_csv_files_zips_csvs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    csv_file = tmp_path / "data.csv"
    txt_file = tmp_path / "keep.txt"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    txt_file.write_text("keep", encoding="utf-8")

    kaggle.zip_and_delete_csv_files(str(tmp_path), logger)

    zip_file = tmp_path / "data.zip"
    assert not csv_file.exists()
    assert zip_file.exists()
    assert txt_file.exists()

    with ZipFile(zip_file, "r") as archive:
        assert archive.namelist() == ["data.csv"]


def test_zip_and_delete_csv_files_logs_when_zip_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    csv_file = tmp_path / "broken.csv"
    csv_file.write_text("x,y\n1,2\n", encoding="utf-8")

    class BrokenZip:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            raise RuntimeError("zip failed")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(kaggle, "ZipFile", BrokenZip)

    kaggle.zip_and_delete_csv_files(str(tmp_path), logger)

    assert csv_file.exists()
    assert logger.error.call_count == 1


def test_stream_s3_object_to_zip_avoids_source_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    zip_path = tmp_path / "dataset.zip"

    class Body:
        def __init__(self) -> None:
            self.data = memoryview(b"a,b\n1,2\n")

        def read(self, size: int = -1) -> bytes:
            if not self.data:
                return b""
            chunk = self.data[:size].tobytes()
            self.data = self.data[len(chunk):]
            return chunk

        def close(self) -> None:
            pass

    class S3Object:
        def get(self):
            return {"Body": Body()}

    kaggle.stream_s3_object_to_zip(S3Object(), str(zip_path), "dataset.csv")

    assert not (tmp_path / "dataset.csv").exists()
    with ZipFile(zip_path, "r") as archive:
        assert archive.namelist() == ["dataset.csv"]
        assert archive.read("dataset.csv") == b"a,b\n1,2\n"


def test_create_kaggle_metadata_creates_expected_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    kaggle.create_kaggle_metadata(
        str(tmp_path),
        logger,
        dataset_id="owner/dataset",
        title="Dataset Title",
        license="CC0-1.0",
    )

    metadata_path = tmp_path / "dataset-metadata.json"
    assert metadata_path.exists()

    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert payload == {
        "id": "owner/dataset",
        "title": "Dataset Title",
        "licenses": [{"name": "CC0-1.0"}],
    }


def test_create_kaggle_metadata_logs_and_raises_on_io_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    monkeypatch.setattr("builtins.open", Mock(side_effect=OSError("write failed")))

    with pytest.raises(OSError, match="write failed"):
        kaggle.create_kaggle_metadata(str(tmp_path), logger, "owner/dataset", "title")

    assert logger.error.call_count == 1


def test_create_kaggle_dataset_runs_expected_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    zip_mock = Mock()
    monkeypatch.setattr(kaggle, "zip_and_delete_csv_files", zip_mock)

    run_mock = Mock(
        return_value=subprocess.CompletedProcess(
            args="create", returncode=0, stdout="ok\n", stderr=""
        )
    )
    monkeypatch.setattr(kaggle.subprocess, "run", run_mock)

    kaggle.create_kaggle_dataset(str(tmp_path), logger)

    zip_mock.assert_called_once_with(str(tmp_path), logger)
    assert run_mock.call_count == 1
    assert run_mock.call_args.args[0] == [
        "kaggle", "datasets", "create", "-p", str(tmp_path)
    ]
    assert run_mock.call_args.kwargs["env"]["PYTHONWARNINGS"] == "ignore"


def test_create_kaggle_dataset_wraps_subprocess_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    monkeypatch.setattr(kaggle, "zip_and_delete_csv_files", Mock())
    run_mock = Mock(
        side_effect=subprocess.CalledProcessError(
            returncode=1, cmd="create", stderr="boom"
        )
    )
    monkeypatch.setattr(kaggle.subprocess, "run", run_mock)

    with pytest.raises(kaggle.AirflowException, match="boom"):
        kaggle.create_kaggle_dataset(str(tmp_path), logger)


def test_update_kaggle_dataset_runs_expected_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    monkeypatch.setattr(kaggle, "zip_and_delete_csv_files", Mock())
    run_mock = Mock(
        return_value=subprocess.CompletedProcess(
            args="version", returncode=0, stdout="ok\n", stderr=""
        )
    )
    monkeypatch.setattr(kaggle.subprocess, "run", run_mock)

    kaggle.update_kaggle_dataset(str(tmp_path), logger)

    assert run_mock.call_count == 1
    argv = run_mock.call_args.args[0]
    assert argv[:5] == ["kaggle", "datasets", "version", "-p", str(tmp_path)]
    assert argv[-2:] == ["-r", "zip"]


def test_update_kaggle_dataset_wraps_subprocess_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    monkeypatch.setattr(kaggle, "zip_and_delete_csv_files", Mock())
    run_mock = Mock(
        side_effect=subprocess.CalledProcessError(
            returncode=1, cmd="version", stderr="version failed"
        )
    )
    monkeypatch.setattr(kaggle.subprocess, "run", run_mock)

    with pytest.raises(kaggle.AirflowException, match="version failed"):
        kaggle.update_kaggle_dataset(str(tmp_path), logger)


def test_upload_kaggle_dataset_executes_download_command(monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    run_mock = Mock(return_value=subprocess.CompletedProcess(args="download", returncode=0, stdout="ok\n", stderr=""))
    monkeypatch.setattr(kaggle.subprocess, "run", run_mock)

    kaggle.upload_kaggle_dataset("owner/dataset", logger)

    assert run_mock.call_count == 1
    assert run_mock.call_args.args[0] == [
        "kaggle", "datasets", "download", "owner/dataset"
    ]


def test_upload_kaggle_dataset_wraps_subprocess_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()

    run_mock = Mock(side_effect=subprocess.CalledProcessError(returncode=1, cmd="download", stderr="bad"))
    monkeypatch.setattr(kaggle.subprocess, "run", run_mock)

    with pytest.raises(kaggle.AirflowException, match="bad"):
        kaggle.upload_kaggle_dataset("owner/dataset", logger)


def test_staging_workflow_removes_directory_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()
    observed = {}

    def download(destination: str) -> None:
        observed["directory"] = Path(destination).parent
        Path(destination).write_text("a,b\n1,2\n", encoding="utf-8")

    def upload(directory: str, _logger) -> None:
        assert Path(directory).exists()

    kaggle.run_kaggle_staging_workflow(
        download, upload, logger, "owner/dataset", "title"
    )
    assert not observed["directory"].exists()


def test_staging_workflow_removes_directory_after_failed_download(monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()
    observed = {}

    def download(destination: str) -> None:
        observed["directory"] = Path(destination).parent
        Path(destination).write_text("partial", encoding="utf-8")
        raise OSError("download failed")

    with pytest.raises(OSError, match="download failed"):
        kaggle.run_kaggle_staging_workflow(
            download, Mock(), logger, "owner/dataset", "title"
        )
    assert not observed["directory"].exists()


def test_staging_workflow_removes_directory_after_failed_kaggle_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    kaggle = _load_kaggle_module(monkeypatch)
    logger = _logger()
    observed = {}

    def download(destination: str) -> None:
        Path(destination).write_text("a,b\n1,2\n", encoding="utf-8")

    def upload(directory: str, _logger) -> None:
        observed["directory"] = Path(directory)
        raise kaggle.AirflowException("upload failed")

    with pytest.raises(kaggle.AirflowException, match="upload failed"):
        kaggle.run_kaggle_staging_workflow(
            download, upload, logger, "owner/dataset", "title"
        )
    assert not observed["directory"].exists()
