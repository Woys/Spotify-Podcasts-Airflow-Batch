import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_dockerfile_uses_airflow_3_3_1() -> None:
    content = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^FROM\s+apache/airflow:3\.3\.1\s*$", content, re.MULTILINE)


def test_compose_references_airflow_3_3_1() -> None:
    content = (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
    assert "apache/airflow:3.3.1" in content


def test_compose_loads_airflow_secrets_from_env_file() -> None:
    content = (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
    assert "path: ./.env" in content
    assert "required: true" in content
    assert "format: raw" in content
    assert "PYTHONPATH: /opt/airflow" in content
    assert "airflow.secrets.local_filesystem.LocalFilesystemBackend" not in content
    assert "/opt/airflow/secrets" not in content


def test_compose_fails_closed_and_binds_admin_ports_to_loopback() -> None:
    content = (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
    assert '"127.0.0.1:8080:8080"' in content
    assert '"127.0.0.1:5555:5555"' in content
    assert "AIRFLOW__CORE__FERNET_KEY:?" in content
    assert "POSTGRES_PASSWORD:?" in content
    assert "POSTGRES_PASSWORD: airflow" not in content
    assert "/opt/airflow/dags:ro,z" in content
    assert "/opt/airflow/include:ro" in content


def test_airflow_make_targets_use_default_compose_env_file() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "AIRFLOW_COMPOSE = $(DOCKER_COMPOSE)" in makefile
    assert "--env-file" not in makefile
    assert "airflow-up: airflow-config" in makefile
    assert "$(AIRFLOW_COMPOSE) up -d" in makefile
