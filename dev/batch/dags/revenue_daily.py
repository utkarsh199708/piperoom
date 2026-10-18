"""revenue_daily: the DAG of the daily-revenue batch pipeline (dev/batch.md).

check_source makes sure the dbt source table exists, then dbt_run builds
stg_orders and fct_revenue in the dev warehouse and leaves run_results.json and
manifest.json in DBT_TARGET_PATH (the artifacts volume Piperoom reads).
"""

import os

import pendulum
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def check_source():
    """Fail the run when analytics.orders is missing (dbt would fail later, less clearly)."""
    conn = psycopg2.connect(
        host=os.environ["WAREHOUSE_HOST"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
        dbname="warehouse",
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM analytics.orders")
            print("analytics.orders rows:", cur.fetchone()[0])
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
