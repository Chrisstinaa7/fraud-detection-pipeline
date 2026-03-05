"""
fraud_pipeline.py  —  Airflow DAG

Runs every 5 minutes:
  1. freshness check  — did Kafka/Spark actually deliver data recently?
  2. dbt run          — staging → intermediate → marts
  3. dbt test         — quality gates
  4. cluster tables   — Snowflake micro-partition maintenance
  5. alert routing    — Slack if HIGH risk transactions exceed threshold

SLA: complete within 3 minutes. Set execution_timeout accordingly.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# pulled from Airflow Variables / Connections, not hardcoded
SNOWFLAKE_CONN = "snowflake_fraud"
SLACK_WEBHOOK  = os.environ.get("SLACK_WEBHOOK_URL", "")
HIGH_RISK_ALERT_THRESHOLD = 100

DEFAULT_ARGS = {
    "owner":             "data-eng",
    "depends_on_past":   False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=3),
    "email_on_failure":  True,
    "email":             ["data-alerts@yourcompany.com"],
}


# ── helpers ────────────────────────────────────────────────────────────────────

def check_freshness(**ctx):
    """Warn if no data arrived from Kafka in the last 5 minutes, but don't halt."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse="FRAUD_WH",
        database=os.environ.get("SNOWFLAKE_DATABASE", "FRAUD_DB"),
        schema="RAW",
    )
    cur = conn.cursor()
    cur.execute("""
        select count(*) as n
        from transactions
        where ingested_at >= dateadd(minute, -5, current_timestamp())
    """)
    n = cur.fetchone()[0]
    cur.close(); conn.close()

    ctx["ti"].xcom_push(key="records_5min", value=n)
    if n == 0:
        print("WARNING: no transactions in last 5 min — Kafka/Spark may be down")
    else:
        print(f"OK: {n:,} transactions received in last 5 min")


def route_alerts(**ctx):
    """Branch based on how many HIGH risk transactions were scored this run."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse="FRAUD_WH",
        database=os.environ.get("SNOWFLAKE_DATABASE", "FRAUD_DB"),
        schema="FRAUD",
    )
    cur = conn.cursor()
    cur.execute("""
        select count(*) as n
        from mart_fraud_detection
        where risk_level = 'HIGH'
          and scored_at >= dateadd(minute, -5, current_timestamp())
    """)
    n = cur.fetchone()[0]
    cur.close(); conn.close()

    ctx["ti"].xcom_push(key="high_risk_count", value=n)
    print(f"HIGH risk transactions this run: {n}")

    if n > HIGH_RISK_ALERT_THRESHOLD:
        return "send_urgent_alert"
    elif n > 0:
        return "send_standard_alert"
    return "end"


def send_slack(urgent: bool, **ctx):
    import requests

    n = ctx["ti"].xcom_pull(key="high_risk_count")
    if urgent:
        text = f":rotating_light: *URGENT* — {n} HIGH risk transactions in last 5 min. Immediate review needed."
    else:
        text = f":warning: {n} HIGH risk transactions flagged in last 5 min."

    if SLACK_WEBHOOK:
        requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=5)
    print(text)


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="fraud_pipeline",
    description="Kafka → Snowflake → dbt fraud scoring, every 5 minutes",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "production"],
) as dag:

    start = EmptyOperator(task_id="start")

    freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness,
    )

    with TaskGroup("dbt", tooltip="dbt model run") as dbt_group:
        staging = SnowflakeOperator(
            task_id="staging",
            snowflake_conn_id=SNOWFLAKE_CONN,
            sql="call dbt_run_staging();",   # or use BashOperator + dbt CLI
        )
        intermediate = SnowflakeOperator(
            task_id="intermediate",
            snowflake_conn_id=SNOWFLAKE_CONN,
            sql="call dbt_run_intermediate();",
        )
        marts = SnowflakeOperator(
            task_id="marts",
            snowflake_conn_id=SNOWFLAKE_CONN,
            sql="call dbt_run_marts();",
        )
        staging >> intermediate >> marts

    dbt_test = SnowflakeOperator(
        task_id="dbt_test",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="call dbt_test();",
    )

    cluster = SnowflakeOperator(
        task_id="cluster_tables",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
            alter table fraud_db.fraud.mart_fraud_detection
                cluster by (transaction_date, risk_level);
        """,
    )

    branch = BranchPythonOperator(
        task_id="route_alerts",
        python_callable=route_alerts,
    )

    urgent_alert = PythonOperator(
        task_id="send_urgent_alert",
        python_callable=send_slack,
        op_kwargs={"urgent": True},
    )

    standard_alert = PythonOperator(
        task_id="send_standard_alert",
        python_callable=send_slack,
        op_kwargs={"urgent": False},
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    (
        start
        >> freshness
        >> dbt_group
        >> dbt_test
        >> cluster
        >> branch
    )
    branch >> [urgent_alert, standard_alert, end]
    [urgent_alert, standard_alert] >> end
