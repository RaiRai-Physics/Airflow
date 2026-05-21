from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
import sqlite3

import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context


BASE_DIR = Path("/tmp/incremental_transactions_etl")
LANDING_DIR = BASE_DIR / "landing"
PROCESSED_DIR = BASE_DIR / "processed"
REJECT_DIR = BASE_DIR / "rejects"
METRICS_DIR = BASE_DIR / "metrics"
AUDIT_DIR = BASE_DIR / "audit"
METADATA_DIR = BASE_DIR / "metadata"

WAREHOUSE_DB = BASE_DIR / "warehouse.db"
WATERMARK_FILE = METADATA_DIR / "watermark.json"


default_args = {
    "owner": "rae",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="incremental_transactions_etl",
    description="Incremental transaction ETL with watermarking, validation, SQLite upsert, metrics, and audit logging",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["airflow", "etl", "incremental", "sqlite", "data-quality"],
    params={
        "process_date": Param(
            default="2026-01-03",
            type="string",
            description="Transaction batch date to process in YYYY-MM-DD format",
        )
    },
)
def incremental_transactions_etl():

    @task
    def create_sample_landing_files() -> dict:
        """
        Creates sample daily transaction files.
        In real life, this could be S3, Kafka landing files, API dumps, or database extracts.
        """

        LANDING_DIR.mkdir(parents=True, exist_ok=True)

        sample_batches = {
            "2026-01-01": [
                {"transaction_id": 1, "customer_id": 101, "amount": 120.50, "status": "completed", "transaction_date": "2026-01-01"},
                {"transaction_id": 2, "customer_id": 102, "amount": 75.00, "status": "completed", "transaction_date": "2026-01-01"},
                {"transaction_id": 3, "customer_id": 103, "amount": -15.00, "status": "completed", "transaction_date": "2026-01-01"},
            ],
            "2026-01-02": [
                {"transaction_id": 4, "customer_id": 101, "amount": 220.00, "status": "completed", "transaction_date": "2026-01-02"},
                {"transaction_id": 5, "customer_id": 104, "amount": 49.99, "status": "failed", "transaction_date": "2026-01-02"},
                {"transaction_id": 6, "customer_id": 105, "amount": 300.00, "status": "completed", "transaction_date": "2026-01-02"},
            ],
            "2026-01-03": [
                {"transaction_id": 7, "customer_id": 106, "amount": 500.00, "status": "completed", "transaction_date": "2026-01-03"},
                {"transaction_id": 8, "customer_id": 107, "amount": 0.00, "status": "completed", "transaction_date": "2026-01-03"},
                {"transaction_id": 9, "customer_id": 108, "amount": 89.99, "status": "completed", "transaction_date": "2026-01-03"},
                {"transaction_id": 9, "customer_id": 108, "amount": 89.99, "status": "completed", "transaction_date": "2026-01-03"},
            ],
        }

        created_files = {}

        for batch_date, rows in sample_batches.items():
            file_path = LANDING_DIR / f"transactions_{batch_date}.csv"
            pd.DataFrame(rows).to_csv(file_path, index=False)
            created_files[batch_date] = str(file_path)

        print("Landing files created:")
        print(json.dumps(created_files, indent=2))

        return created_files

    @task
    def get_process_date() -> str:
        """
        Gets process_date from Airflow Params.
        """

        context = get_current_context()
        process_date = context["params"]["process_date"]

        try:
            datetime.strptime(process_date, "%Y-%m-%d")
        except ValueError as exc:
            raise AirflowFailException(
                "process_date must use YYYY-MM-DD format."
            ) from exc

        print(f"Processing transaction batch date: {process_date}")
        return process_date

    @task
    def read_watermark() -> dict:
        """
        Reads the last processed batch date.
        """

        METADATA_DIR.mkdir(parents=True, exist_ok=True)

        if not WATERMARK_FILE.exists():
            watermark = {
                "last_processed_date": None,
                "last_successful_run": None,
            }

            with open(WATERMARK_FILE, "w") as file:
                json.dump(watermark, file, indent=2)

            print("Created new watermark file.")

            return watermark

        with open(WATERMARK_FILE, "r") as file:
            watermark = json.load(file)

        print("Current watermark:")
        print(json.dumps(watermark, indent=2))

        return watermark

    @task
    def extract_incremental_batch(
        landing_files: dict,
        process_date: str,
        watermark: dict,
    ) -> dict:
        """
        Extracts only the requested batch if it has not already been processed.
        """

        last_processed_date = watermark.get("last_processed_date")

        if last_processed_date and process_date <= last_processed_date:
            raise AirflowSkipException(
                f"Batch {process_date} was already processed. "
                f"Watermark is {last_processed_date}."
            )

        if process_date not in landing_files:
            raise AirflowFailException(
                f"No landing file found for process_date={process_date}"
            )

        source_file = landing_files[process_date]
        df = pd.read_csv(source_file)

        if len(df) == 0:
            raise AirflowSkipException(f"No rows found for {process_date}.")

        extract_result = {
            "process_date": process_date,
            "source_file": source_file,
            "extracted_rows": len(df),
        }

        print("Extract result:")
        print(json.dumps(extract_result, indent=2))

        return extract_result

    @task
    def clean_and_validate_transactions(extract_result: dict) -> dict:
        """
        Cleans transactions and separates good rows from rejected rows.
        """

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        REJECT_DIR.mkdir(parents=True, exist_ok=True)

        process_date = extract_result["process_date"]
        source_file = extract_result["source_file"]

        df = pd.read_csv(source_file)

        df["status"] = df["status"].fillna("").str.lower().str.strip()
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
        df["reject_reason"] = ""

        df.loc[df["transaction_id"].isna(), "reject_reason"] += "missing transaction_id; "
        df.loc[df["customer_id"].isna(), "reject_reason"] += "missing customer_id; "
        df.loc[df["amount"] <= 0, "reject_reason"] += "amount must be greater than zero; "
        df.loc[df["status"] != "completed", "reject_reason"] += "status is not completed; "
        df.loc[df["transaction_date"].isna(), "reject_reason"] += "invalid transaction_date; "
        df.loc[df["transaction_id"].duplicated(), "reject_reason"] += "duplicate transaction_id; "

        clean_df = df[df["reject_reason"] == ""].copy()
        reject_df = df[df["reject_reason"] != ""].copy()

        clean_file = PROCESSED_DIR / f"transactions_clean_{process_date}.csv"
        reject_file = REJECT_DIR / f"transactions_rejected_{process_date}.csv"

        clean_df.to_csv(clean_file, index=False)
        reject_df.to_csv(reject_file, index=False)

        validation_result = {
            "process_date": process_date,
            "clean_file": str(clean_file),
            "reject_file": str(reject_file),
            "clean_rows": len(clean_df),
            "rejected_rows": len(reject_df),
            "source_rows": len(df),
        }

        print("Validation result:")
        print(json.dumps(validation_result, indent=2))

        if len(clean_df) == 0:
            raise AirflowFailException("No valid transactions to load.")

        return validation_result

    @task
    def load_to_sqlite_warehouse(validation_result: dict) -> dict:
        """
        Loads clean transactions into SQLite with idempotent upsert behavior.
        """

        BASE_DIR.mkdir(parents=True, exist_ok=True)

        clean_df = pd.read_csv(validation_result["clean_file"])

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS fact_transactions (
                    transaction_id INTEGER PRIMARY KEY,
                    customer_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL,
                    transaction_date TEXT NOT NULL,
                    loaded_at TEXT NOT NULL
                );
                """
            )

            for _, row in clean_df.iterrows():
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO fact_transactions (
                        transaction_id,
                        customer_id,
                        amount,
                        status,
                        transaction_date,
                        loaded_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        int(row["transaction_id"]),
                        int(row["customer_id"]),
                        float(row["amount"]),
                        row["status"],
                        str(row["transaction_date"]),
                        datetime.utcnow().isoformat(),
                    ),
                )

            conn.commit()

            loaded_count = cursor.execute(
                "SELECT COUNT(*) FROM fact_transactions;"
            ).fetchone()[0]

        load_result = {
            "warehouse_db": str(WAREHOUSE_DB),
            "table_name": "fact_transactions",
            "batch_loaded_rows": validation_result["clean_rows"],
            "total_warehouse_rows": loaded_count,
        }

        print("Load result:")
        print(json.dumps(load_result, indent=2))

        return load_result

    @task
    def create_daily_metrics(
        validation_result: dict,
        load_result: dict,
    ) -> dict:
        """
        Creates daily metrics from the warehouse.
        """

        METRICS_DIR.mkdir(parents=True, exist_ok=True)

        process_date = validation_result["process_date"]

        with sqlite3.connect(load_result["warehouse_db"]) as conn:
            daily_metrics = pd.read_sql_query(
                """
                SELECT
                    DATE(transaction_date) AS transaction_date,
                    COUNT(*) AS total_transactions,
                    ROUND(SUM(amount), 2) AS total_revenue,
                    ROUND(AVG(amount), 2) AS avg_transaction_amount
                FROM fact_transactions
                WHERE DATE(transaction_date) = ?
                GROUP BY DATE(transaction_date);
                """,
                conn,
                params=(process_date,),
            )

        metrics_file = METRICS_DIR / f"daily_metrics_{process_date}.csv"
        daily_metrics.to_csv(metrics_file, index=False)

        metrics_result = {
            "process_date": process_date,
            "metrics_file": str(metrics_file),
            "metric_rows": len(daily_metrics),
        }

        print("Metrics result:")
        print(json.dumps(metrics_result, indent=2))

        return metrics_result

    @task
    def update_watermark(validation_result: dict) -> dict:
        """
        Updates watermark only after successful validation and load path.
        """

        watermark = {
            "last_processed_date": validation_result["process_date"],
            "last_successful_run": datetime.utcnow().isoformat(),
        }

        with open(WATERMARK_FILE, "w") as file:
            json.dump(watermark, file, indent=2)

        print("Updated watermark:")
        print(json.dumps(watermark, indent=2))

        return watermark

    @task
    def write_audit_summary(
        extract_result: dict,
        validation_result: dict,
        load_result: dict,
        metrics_result: dict,
        watermark: dict,
    ) -> str:
        """
        Writes a final audit summary for the run.
        """

        AUDIT_DIR.mkdir(parents=True, exist_ok=True)

        process_date = validation_result["process_date"]

        audit_summary = {
            "pipeline_name": "incremental_transactions_etl",
            "process_date": process_date,
            "extract_result": extract_result,
            "validation_result": validation_result,
            "load_result": load_result,
            "metrics_result": metrics_result,
            "watermark": watermark,
            "status": "completed",
        }

        audit_file = AUDIT_DIR / f"audit_summary_{process_date}.json"

        with open(audit_file, "w") as file:
            json.dump(audit_summary, file, indent=2)

        print(f"Audit summary written to: {audit_file}")

        return str(audit_file)

    landing_files = create_sample_landing_files()
    process_date = get_process_date()
    watermark = read_watermark()

    extract_result = extract_incremental_batch(
        landing_files=landing_files,
        process_date=process_date,
        watermark=watermark,
    )

    validation_result = clean_and_validate_transactions(extract_result)

    load_result = load_to_sqlite_warehouse(validation_result)

    metrics_result = create_daily_metrics(
        validation_result=validation_result,
        load_result=load_result,
    )

    new_watermark = update_watermark(validation_result)

    write_audit_summary(
        extract_result=extract_result,
        validation_result=validation_result,
        load_result=load_result,
        metrics_result=metrics_result,
        watermark=new_watermark,
    )


incremental_transactions_etl()
