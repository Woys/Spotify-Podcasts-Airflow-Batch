from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import date
import logging
import sys
import os
import json
from zipfile import ZipFile
import subprocess

os.environ['KAGGLE_USERNAME'] = Variable.get("KAGGLE_USERNAME")
os.environ['KAGGLE_KEY'] = Variable.get("KAGGLE_KEY")

today = date.today()


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
        files = subprocess.run(
            f"ls {kaggle_folder}", shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Files: {files.stdout}")

        command = f"kaggle datasets create -p '{kaggle_folder}'"
        result = subprocess.run(command, shell=True,
                                check=True, capture_output=True, text=True)
        logger.info(f"Command succeeded: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e.stderr}")
        raise AirflowException(f"Task failed due to: {e.stderr}")


def update_kaggle_dataset(kaggle_folder: str, logger: logging.Logger) -> None:
    """Updates a Kaggle dataset by zipping CSVs and uploading the folder with a new version."""
    zip_and_delete_csv_files(kaggle_folder, logger)
    logger.info('Starting to upload to Kaggle')
    try:
        files = subprocess.run(
            f"ls {kaggle_folder}", shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Files: {files.stdout}")

        command = f"kaggle datasets version -p '{
            kaggle_folder}' -m '{today} Update' -r zip"
        result = subprocess.run(command, shell=True,
                                check=True, capture_output=True, text=True)
        logger.info(f"Command succeeded: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e.stderr}")
        raise AirflowException(f"Task failed due to: {e.stderr}")


def upload_kaggle_dataset(dataset_id: str, logger: logging.Logger) -> None:
    """Downloads a Kaggle dataset."""
    logger.info('Starting to download dataset from Kaggle')
    try:
        command = f"kaggle datasets download {dataset_id}"
        result = subprocess.run(command, shell=True,
                                check=True, capture_output=True, text=True)
        logger.info(f"Command succeeded: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e.stderr}")
        raise AirflowException(f"Task failed due to: {e.stderr}")
