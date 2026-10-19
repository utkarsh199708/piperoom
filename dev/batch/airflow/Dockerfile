# Airflow 2.x (REST API v1, what internal/clients/airflow reads) plus dbt-postgres.
# dbt lives in its own venv: installing it into Airflow's environment would fight
# Airflow's pinned constraints.
FROM apache/airflow:2.10.5-python3.12

ARG DBT_POSTGRES_VERSION=1.9.0
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres==${DBT_POSTGRES_VERSION}"
USER airflow
