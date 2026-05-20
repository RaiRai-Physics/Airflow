from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json

import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


BASE_DIR = Path("/tmp/retail_orders_etl")
RAW_DIR = BASE_DIR / "raw"
CLEAN_DIR = BASE_DIR / "clean"
REJECT_DIR = BASE_DIR / "rejects"
WAREHOUSE_DIR = BASE_DIR / "warehouse"
SUMMARY_DIR = BASE_DIR / "summary"


default_args = {
    "owner": "rae",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="retail_orders_quality_etl",
    description="A slightly advanced ETL DAG with validation, rejects, branching, and summary output",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "airflow", "data-quality", "retail"],
)
def retail_orders_quality_etl():

    @task
    def extract_raw_orders() -> str:
        """
        Creates raw order data.
        In a real project, this could read from S3, an API, or a database.
        """

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        data = [
            {
                "order_id": 101,
                "customer_name": "Asha",
                "product": "Laptop",
                "quantity": 1,
                "unit_price": 850,
                "order_date": "2026-01-01",
            },
            {
                "order_id": 102,
                "customer_name": "Ben",
                "product": "Mouse",
                "quantity": 2,
                "unit_price": 25,
                "order_date": "2026-01-01",
            },
            {
                "order_id": 103,
                "customer_name": "",
                "product": "Keyboard",
                "quantity": 1,
                "unit_price": 70,
                "order_date": "2026-01-02",
            },
            {
                "order_id": 104,
                "customer_name": "Mina",
                "product": "Monitor",
                "quantity": -1,
                "unit_price": 200,
                "order_date": "2026-01-02",
            },
            {
                "order_id": 102,
                "customer_name": "Ben",
                "product": "Mouse",
                "quantity": 2,
                "unit_price": 25,
                "order_date": "2026-01-01",
            },
        ]

        df = pd.DataFrame(data)

        raw_file_path = RAW_DIR / "orders_raw.csv"
        df.to_csv(raw_file_path, index=False)

        print(f"Raw orders written to: {raw_file_path}")
        return str(raw_file_path)

    @task
    def profile_raw_data(raw_file_path: str) -> dict:
        """
        Profiles the raw data before transformation.
        This gives a quick data quality snapshot.
        """

        df = pd.read_csv(raw_file_path)

        profile = {
            "total_rows": len(df),
            "duplicate_order_ids": int(df["order_id"].duplicated().sum()),
            "missing_customer_names": int(df["customer_name"].isna().sum() + (df["customer_name"] == "").sum()),
            "negative_quantity_rows": int((df["quantity"] <= 0).sum()),
            "missing_unit_price_rows": int(df["unit_price"].isna().sum()),
        }

        print("Raw data profile:")
        print(json.dumps(profile, indent=2))

        return profile

    @task
    def clean_and_validate_orders(raw_file_path: str) -> dict:
        """
        Cleans raw orders and separates valid rows from rejected rows.
        """

        CLEAN_DIR.mkdir(parents=True, exist_ok=True)
        REJECT_DIR.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(raw_file_path)

        df["customer_name"] = df["customer_name"].fillna("").str.strip()
        df["product"] = df["product"].fillna("").str.strip()
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        df["total_amount"] = df["quantity"] * df["unit_price"]

        df["reject_reason"] = ""

        df.loc[df["customer_name"] == "", "reject_reason"] += "missing customer name; "
        df.loc[df["quantity"] <= 0, "reject_reason"] += "quantity must be greater than zero; "
        df.loc[df["unit_price"] <= 0, "reject_reason"] += "unit price must be greater than zero; "
        df.loc[df["order_date"].isna(), "reject_reason"] += "invalid order date; "
        df.loc[df["order_id"].duplicated(), "reject_reason"] += "duplicate order id; "

        clean_df = df[df["reject_reason"] == ""].copy()
        reject_df = df[df["reject_reason"] != ""].copy()

        clean_file_path = CLEAN_DIR / "orders_clean.csv"
        reject_file_path = REJECT_DIR / "orders_rejected.csv"

        clean_df.to_csv(clean_file_path, index=False)
        reject_df.to_csv(reject_file_path, index=False)

        result = {
            "clean_file_path": str(clean_file_path),
            "reject_file_path": str(reject_file_path),
            "clean_rows": len(clean_df),
            "rejected_rows": len(reject_df),
            "total_rows": len(df),
        }

        print("Validation result:")
        print(json.dumps(result, indent=2))

        return result

    @task.branch
    def choose_load_path(validation_result: dict) -> str:
        """
        Branches the pipeline.
        If no clean rows exist, skip loading.
        """

        if validation_result["clean_rows"] == 0:
            return "no_clean_data"

        return "load_clean_orders"

    @task
    def load_clean_orders(validation_result: dict) -> str:
        """
        Loads clean data into a local warehouse-style CSV.
        In a real project, this could load into Snowflake, Redshift, or Postgres.
        """

        WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

        clean_file_path = validation_result["clean_file_path"]
        clean_df = pd.read_csv(clean_file_path)

        warehouse_file_path = WAREHOUSE_DIR / "orders_fact_table.csv"

        if warehouse_file_path.exists():
            existing_df = pd.read_csv(warehouse_file_path)
            final_df = pd.concat([existing_df, clean_df], ignore_index=True)
            final_df = final_df.drop_duplicates(subset=["order_id"])
        else:
            final_df = clean_df

        final_df.to_csv(warehouse_file_path, index=False)

        message = f"Loaded {len(clean_df)} clean rows into {warehouse_file_path}"
        print(message)

        return message

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def write_pipeline_summary(profile: dict, validation_result: dict) -> str:
        """
        Writes a summary JSON file for the DAG run.
        """

        SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

        summary = {
            "pipeline_name": "retail_orders_quality_etl",
            "raw_profile": profile,
            "validation_result": validation_result,
            "status": "completed",
        }

        summary_file_path = SUMMARY_DIR / "pipeline_summary.json"

        with open(summary_file_path, "w") as file:
            json.dump(summary, file, indent=2)

        print(f"Pipeline summary written to: {summary_file_path}")
        return str(summary_file_path)

    no_clean_data = EmptyOperator(task_id="no_clean_data")

    raw_file = extract_raw_orders()
    profile = profile_raw_data(raw_file)
    validation_result = clean_and_validate_orders(raw_file)

    branch = choose_load_path(validation_result)

    loaded = load_clean_orders(validation_result)

    summary = write_pipeline_summary(profile, validation_result)

    raw_file >> profile >> validation_result >> branch
    branch >> loaded >> summary
    branch >> no_clean_data >> summary


retail_orders_quality_etl()
