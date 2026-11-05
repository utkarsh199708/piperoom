"""revenue_daily: the DAG of the daily-revenue batch pipeline (dev/batch.md).

check_source makes sure the dbt source table exists, then dbt_run builds
stg_orders and fct_revenue in the dev warehouse and leaves run_results.json and
manifest.json in DBT_TARGET_PATH (the artifacts volume Piperoom reads).
"""

import os
import re

import pendulum
import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# check_source reads the source table name from this Airflow Variable, so a
# break (`devctl break dag-failed`) can point it at a missing table and fail the
# run without renaming analytics.orders, which is also the CDC sink shared by
# the rest of the stack. The value is a bare (unqualified) table name.
SOURCE_TABLE_VAR = "revenue_source_table"
SOURCE_TABLE_DEFAULT = "orders"
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def check_source():
    """Fail the run when the dbt source table is missing (dbt would fail later, less clearly)."""
    table = Variable.get(SOURCE_TABLE_VAR, default_var=SOURCE_TABLE_DEFAULT)
    if not _TABLE_RE.fullmatch(table):
        # The value is interpolated into an identifier, so refuse anything but
        # a bare name (the same regex devctl uses for identifiers).
        raise ValueError(f"{SOURCE_TABLE_VAR} must be a bare table name, got {table!r}")
    conn = psycopg2.connect(
        host=os.environ["WAREHOUSE_HOST"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
        dbname="warehouse",
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM analytics.{table}")
            print(f"analytics.{table} rows:", cur.fetchone()[0])
    finally:
        conn.close()


with DAG(
    dag_id="revenue_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,  # one run on start, not one per day since start_date
    default_args={"retries": 0},  # a break shows as a failed run at once
    tags=["piperoom-dev"],
) as dag:
    source = PythonOperator(task_id="check_source", python_callable=check_source)
    # DBT_PROFILES_DIR, DBT_TARGET_PATH and DBT_LOG_PATH come from the compose
    # service; append_env keeps them (and PATH) in the task's environment.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="/opt/dbt-venv/bin/dbt run --project-dir /opt/airflow/dbt/analytics",
        append_env=True,
    )
    source >> dbt_run
