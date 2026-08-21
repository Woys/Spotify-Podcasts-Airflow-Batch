FROM apache/airflow:3.3.1

ARG DUCK_DBT_COMMIT=91e66e09deabbaaaf4807b8584e6b29b36172bf8

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
RUN git init /opt/airflow/dbt/duck_dbt \
    && git -C /opt/airflow/dbt/duck_dbt remote add origin https://github.com/Woys/Duck_dbt.git \
    && git -C /opt/airflow/dbt/duck_dbt fetch --depth 1 origin "${DUCK_DBT_COMMIT}" \
    && git -C /opt/airflow/dbt/duck_dbt checkout --detach FETCH_HEAD \
    && dbt deps --project-dir /opt/airflow/dbt/duck_dbt
COPY dbt/profiles.yml /opt/airflow/dbt/profiles.yml
