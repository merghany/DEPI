"""
DAG: dbt_hourly_staging
========================
Runs dbt staging models every hour so the staging views
always reflect the latest data written by the Kafka consumer.

Schedule: every hour at :05
Models:   staging layer only (stg_transactions, stg_clients,
          stg_merchants, stg_fraud_alerts)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Config ────────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

DBT_ENV = (f"DBT_PROFILES_DIR={DBT_PROFILES_DIR} DBT_PROJECT_DIR={DBT_PROJECT_DIR} ")

DBT_BASE = "dbt"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"

default_args = {
    "owner":            "airflow",
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout":timedelta(minutes=15),
}

with DAG(
    dag_id="dbt_hourly_staging",
    description="Refresh dbt staging views every hour",
    schedule_interval="5 * * * *",   # every hour at :05
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "staging", "hourly"],
) as dag:

    # ── Task 1: dbt debug ─────────────────────────────────────────────────────
    t_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_ENV} {DBT_BASE} debug",
        doc_md="Verify dbt can connect to MySQL before running models.",
    )

    # ── Task 2: run staging models ────────────────────────────────────────────
    t_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_ENV} {DBT_BASE} run --select staging {DBT_FLAGS}",
        doc_md="Refresh all staging views (stg_transactions, stg_clients, stg_merchants, stg_fraud_alerts).",
    )

    # ── Task 3: test staging models ───────────────────────────────────────────
    t_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_ENV} {DBT_BASE} test --select staging",
        doc_md="Run data quality tests on staging models.",
    )

    t_debug >> t_staging >> t_test_staging
