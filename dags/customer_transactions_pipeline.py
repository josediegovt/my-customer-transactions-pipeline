import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

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


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
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

    load_csv >> dbt_build