.PHONY: help all install test test-verbose ci-test clean \
	airflow-config airflow-init airflow-up airflow-down airflow-status

PYTHON ?= python
PIP ?= $(PYTHON) -m pip
PYTEST ?= $(PYTHON) -m pytest
DOCKER_COMPOSE ?= docker compose
AIRFLOW_COMPOSE = $(DOCKER_COMPOSE)

AIRFLOW_VERSION ?= 3.3.1

# Match env defaults used in CI.
export AIRFLOW_HOME ?= /tmp/airflow
export AIRFLOW_VAR_SP_S3_BUCKET ?= test-bucket
export AIRFLOW_VAR_KAGGLE_USERNAME ?= test-user
export AIRFLOW_VAR_KAGGLE_KEY ?= test-key
export AIRFLOW_VAR_SP_CLIENT_ID ?= test-client
export AIRFLOW_VAR_SP_CLIENT_SECRET ?= test-secret

help:
	@echo "Available targets:"
	@echo "  make all          Install deps and run tests"
	@echo "  make install      Install Python dependencies for tests"
	@echo "  make test         Run pytest (uses pytest.ini coverage settings)"
	@echo "  make test-verbose Run pytest -vv"
	@echo "  make ci-test      Install deps and run tests (same flow as CI)"
	@echo "  make clean        Remove local test artifacts"
	@echo "  make airflow-config Validate the Docker Compose configuration"
	@echo "  make airflow-init Initialize the Airflow database and admin user"
	@echo "  make airflow-up   Start Airflow in the background"
	@echo "  make airflow-down Stop the Airflow stack"
	@echo "  make airflow-status Show Airflow service status"

all: ci-test

install:
	$(PIP) install --upgrade pip
	$(PIP) uninstall -y airflow || true
	$(PIP) install --upgrade "apache-airflow==$(AIRFLOW_VERSION)" -r requirements.txt
	$(PYTHON) -c "import airflow, importlib; print('Airflow import OK:', airflow.__file__); \
mods = ('airflow.models', 'airflow.sdk'); \
ok = any(hasattr(importlib.import_module(m), 'Variable') for m in mods); \
assert ok, 'Variable not found in airflow.models or airflow.sdk'"

test:
	$(PYTEST)

test-verbose:
	$(PYTEST) -vv

ci-test: install test

airflow-config:
	$(AIRFLOW_COMPOSE) config --quiet

airflow-init: airflow-config
	$(AIRFLOW_COMPOSE) up airflow-init

airflow-up: airflow-config
	$(AIRFLOW_COMPOSE) up -d

airflow-down:
	$(AIRFLOW_COMPOSE) down

airflow-status:
	$(AIRFLOW_COMPOSE) ps

clean:
	rm -rf .pytest_cache htmlcov
	rm -f .coverage
