"""Kaggle helpers for Spotify DAGs."""

try:
    from airflow.sdk import Variable
except Exception:  # pragma: no cover - fallback for newer/partial Airflow layouts
    Variable = None  # type: ignore[assignment]

try:
    from airflow.sdk.exceptions import AirflowException
except Exception:  # pragma: no cover
    class AirflowException(Exception):
        pass
from datetime import date
import logging
import os
import json
from zipfile import ZIP_DEFLATED, ZipFile
import subprocess
import shutil
import tempfile
from collections.abc import Callable


def _resolve_var(name: str, default: str = "") -> str:
    airflow_env_name = f"AIRFLOW_VAR_{name}"
    if airflow_env_name in os.environ:
        return os.environ.get(airflow_env_name, default)

    if Variable is not None:
        try:
            return str(Variable.get(name, default=default))
        except Exception as e:
            print(f"Warning: Failed to get Airflow Variable {name}: {e}")
            pass

    return os.environ.get(name, default)


os.environ["KAGGLE_USERNAME"] = _resolve_var("KAGGLE_USERNAME")
os.environ["KAGGLE_KEY"] = _resolve_var("KAGGLE_KEY")

today = date.today()


def run_kaggle_staging_workflow(
    download_csv: Callable[[str], None],
    upload_dataset: Callable[[str, logging.Logger], None],
    logger: logging.Logger,
    dataset_id: str,
    title: str,
    license: str = "CC0-1.0",
    file_name: str = "dataset.csv",
    temp_prefix: str = "spotify_kaggle_",
) -> None:
    """Download, prepare, and upload a dataset in one self-cleaning workspace."""
    with tempfile.TemporaryDirectory(prefix=temp_prefix) as staging_dir:
        download_csv(os.path.join(staging_dir, file_name))
        create_kaggle_metadata(staging_dir, logger, dataset_id, title, license)
        upload_dataset(staging_dir, logger)


def stream_s3_object_to_zip(s3_object, zip_path: str, csv_name: str) -> None:
    """Stream an S3 object into a compressed ZIP without staging the source CSV."""
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED, allowZip64=True) as archive:
        with archive.open(csv_name, "w", force_zip64=True) as csv_entry:
            body = s3_object.get()["Body"]
            try:
                shutil.copyfileobj(body, csv_entry, length=8 * 1024 * 1024)
            finally:
                body.close()


def zip_and_delete_csv_files(directory_path: str, logger: logging.Logger) -> None:
    """Zips and deletes all CSV files in the given directory."""
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                zip_path = file_path.replace('.csv', '.zip')

                try:
                    with ZipFile(zip_path, 'w') as zipf:
                        zipf.write(file_path, os.path.basename(file_path))
                    os.remove(file_path)
                    logger.info(f"Zipped and deleted: {file_path}")
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")


def create_kaggle_metadata(kaggle_folder: str, logger: logging.Logger, dataset_id: str, title: str, license: str = "CC1-1.0") -> None:
    """Creates Kaggle metadata JSON file."""
    logger.info(f'Starting to create {kaggle_folder}/dataset-metadata.json')
    data = {
        "id": dataset_id,
        "title": title,
        "licenses": [
            {
                "name": license,
            }
        ],
    }
    metadata_file_location = os.path.join(
        kaggle_folder, 'dataset-metadata.json')

    try:
        with open(metadata_file_location, 'w', encoding='utf-8') as metadata_file:
            json.dump(data, metadata_file)
        logger.info(f'Metadata file created at {metadata_file_location}')
    except Exception as e:
        logger.error(f"Failed to create metadata file: {e}")
        raise


def create_kaggle_dataset(kaggle_folder: str, logger: logging.Logger) -> None:
    """Creates a Kaggle dataset by zipping CSVs and uploading the folder."""
    zip_and_delete_csv_files(kaggle_folder, logger)
    logger.info('Starting to upload to Kaggle')
    try:
        files = sorted(os.listdir(kaggle_folder))
        logger.info("Files: %s", ", ".join(files))

        result = subprocess.run(
            ["kaggle", "datasets", "create", "-p", kaggle_folder],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONWARNINGS": "ignore"},
        )
        logger.info(f"Command succeeded: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: stderr={e.stderr}, stdout={e.stdout}")
        raise AirflowException(f"Task failed due to: stderr={e.stderr}, stdout={e.stdout}")


def update_kaggle_dataset(kaggle_folder: str, logger: logging.Logger) -> None:
    """Updates a Kaggle dataset by zipping CSVs and uploading the folder with a new version."""
    zip_and_delete_csv_files(kaggle_folder, logger)
    logger.info('Starting to upload to Kaggle')
    try:
        files = sorted(os.listdir(kaggle_folder))
        logger.info("Files: %s", ", ".join(files))

        result = subprocess.run(
            [
                "kaggle",
                "datasets",
                "version",
                "-p",
                kaggle_folder,
                "-m",
                f"{today} Update",
                "-r",
                "zip",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONWARNINGS": "ignore"},
        )
        logger.info(f"Command succeeded: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: stderr={e.stderr}, stdout={e.stdout}")
        raise AirflowException(f"Task failed due to: stderr={e.stderr}, stdout={e.stdout}")


def upload_kaggle_dataset(dataset_id: str, logger: logging.Logger) -> None:
    """Downloads a Kaggle dataset."""
    logger.info('Starting to download dataset from Kaggle')
    try:
        result = subprocess.run(
            ["kaggle", "datasets", "download", dataset_id],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONWARNINGS": "ignore"},
        )
        logger.info(f"Command succeeded: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: stderr={e.stderr}, stdout={e.stdout}")
        raise AirflowException(f"Task failed due to: stderr={e.stderr}, stdout={e.stdout}")
