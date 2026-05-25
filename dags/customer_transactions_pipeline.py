import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from tabulate import tabulate

logger = logging.getLogger(__name__)

DAG_ID = "customer_transactions_pipeline"
CSV_PATH = "/opt/airflow/data/customer_transactions.csv"
DBT_DIR = "/opt/airflow/dbt"

EXPECTED_HEADERS = {
    "transaction_id",
    "customer_id",
    "transaction_date",
    "product_id",
    "product_name",
    "quantity",
    "price",
    "tax",
}

def load_csv_to_bronze():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    if os.path.getsize(CSV_PATH) == 0:
        raise ValueError(f"CSV file at {CSV_PATH} is empty (0 bytes)")

    with open(CSV_PATH, "r") as f:
        header = f.readline()
        line_count = sum(1 for _ in f)

    if line_count <= 1:
        raise ValueError(
            f"CSV file at {CSV_PATH} has no data rows (header only or blank lines)"
        )

    # Validate header columns
    actual_headers = set(header.strip().lower().split(","))

    missing = EXPECTED_HEADERS - actual_headers
    extra = actual_headers - EXPECTED_HEADERS

    if missing:
        raise ValueError(f"CSV is missing expected columns: {missing}")
    if extra:
        raise ValueError(f"CSV contains unexpected columns: {extra}")

    logger.info("CSV header validation passed — all expected columns present")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()

    try:
        cursor = conn.cursor()
        try:
            logger.info("Dropping and recreating bronze.customer_transactions")
            cursor.execute("""
                DROP TABLE IF EXISTS bronze.customer_transactions;
                CREATE TABLE bronze.customer_transactions (
                    transaction_id   TEXT,
                    customer_id      TEXT,
                    transaction_date TEXT,
                    product_id       TEXT,
                    product_name     TEXT,
                    quantity         TEXT,
                    price            TEXT,
                    tax              TEXT
                );
            """)

            with open(CSV_PATH, "r") as f:
                cursor.copy_expert("""
                    COPY bronze.customer_transactions
                    FROM STDIN WITH CSV HEADER
                """, f)

            conn.commit()
            logger.info(
                f"Successfully loaded {line_count - 1} data rows into bronze.customer_transactions"
            )

        except Exception:
            conn.rollback()
            logger.exception(
                "Failed to load CSV into bronze.customer_transactions; transaction rolled back"
            )
            raise
        finally:
            cursor.close()
    finally:
        conn.close()

def check_quarantine_and_notify():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()

    try:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM silver.customer_transactions_quarantine;")
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            if not rows:
                logger.info("Quarantine table is empty — no email will be sent")
                return

            logger.info(f"Found {len(rows)} quarantine record(s)")
            logger.info("First 5 quarantine rows:\n" + tabulate(rows[:5], headers=col_names, tablefmt="simple"))

        finally:
            cursor.close()
    finally:
        conn.close()


DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 25),
    schedule=None,
    catchup=False,
    tags=["case-study"],
) as dag:
    load_csv = PythonOperator(
        task_id="load_csv_to_bronze",
        python_callable=load_csv_to_bronze,
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir {DBT_DIR}",
    )
    notify_quarantine = PythonOperator(
        task_id="check_quarantine_and_notify",
        python_callable=check_quarantine_and_notify,
    )

    load_csv >> dbt_build >> notify_quarantine