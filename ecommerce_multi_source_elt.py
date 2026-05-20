from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
import sqlite3

import pandas as pd

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException


BASE_DIR = Path("/tmp/ecommerce_elt")
RAW_DIR = BASE_DIR / "raw"
STAGE_DIR = BASE_DIR / "stage"
MART_DIR = BASE_DIR / "mart"
SUMMARY_DIR = BASE_DIR / "summary"
WAREHOUSE_DB = BASE_DIR / "warehouse.db"


default_args = {
    "owner": "rae",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="ecommerce_multi_source_elt",
    description="Multi-source e-commerce ELT pipeline with validation, dynamic mapping, SQLite warehouse, and metrics",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["airflow", "elt", "ecommerce", "data-quality", "sqlite"],
)
def ecommerce_multi_source_elt():

    @task
    def create_raw_sources() -> list[dict]:
        """
        Creates three raw source files:
        customers, products, and orders.
        """

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        customers = pd.DataFrame(
            [
                {"customer_id": 1, "customer_name": "Asha", "country": "USA"},
                {"customer_id": 2, "customer_name": "Ben", "country": "USA"},
                {"customer_id": 3, "customer_name": "Mina", "country": "Canada"},
                {"customer_id": 4, "customer_name": "", "country": "USA"},
            ]
        )

        products = pd.DataFrame(
            [
                {"product_id": 101, "product_name": "Laptop", "category": "Electronics", "unit_price": 850},
                {"product_id": 102, "product_name": "Mouse", "category": "Accessories", "unit_price": 25},
                {"product_id": 103, "product_name": "Keyboard", "category": "Accessories", "unit_price": 70},
                {"product_id": 104, "product_name": "Monitor", "category": "Electronics", "unit_price": 200},
            ]
        )

        orders = pd.DataFrame(
            [
                {"order_id": 1001, "customer_id": 1, "product_id": 101, "quantity": 1, "order_date": "2026-01-01"},
                {"order_id": 1002, "customer_id": 2, "product_id": 102, "quantity": 2, "order_date": "2026-01-01"},
                {"order_id": 1003, "customer_id": 3, "product_id": 104, "quantity": 1, "order_date": "2026-01-02"},
                {"order_id": 1004, "customer_id": 4, "product_id": 103, "quantity": 3, "order_date": "2026-01-02"},
                {"order_id": 1005, "customer_id": 99, "product_id": 101, "quantity": 1, "order_date": "2026-01-03"},
                {"order_id": 1006, "customer_id": 1, "product_id": 999, "quantity": 1, "order_date": "2026-01-03"},
                {"order_id": 1007, "customer_id": 2, "product_id": 103, "quantity": -2, "order_date": "2026-01-04"},
            ]
        )

        source_files = {
            "customers": RAW_DIR / "customers.csv",
            "products": RAW_DIR / "products.csv",
            "orders": RAW_DIR / "orders.csv",
        }

        customers.to_csv(source_files["customers"], index=False)
        products.to_csv(source_files["products"], index=False)
        orders.to_csv(source_files["orders"], index=False)

        manifest = [
            {
                "dataset_name": "customers",
                "file_path": str(source_files["customers"]),
                "primary_key": "customer_id",
                "required_columns": ["customer_id", "customer_name", "country"],
            },
            {
                "dataset_name": "products",
                "file_path": str(source_files["products"]),
                "primary_key": "product_id",
                "required_columns": ["product_id", "product_name", "category", "unit_price"],
            },
            {
                "dataset_name": "orders",
                "file_path": str(source_files["orders"]),
                "primary_key": "order_id",
                "required_columns": ["order_id", "customer_id", "product_id", "quantity", "order_date"],
            },
        ]

        print("Raw source manifest:")
        print(json.dumps(manifest, indent=2))

        return manifest

    @task_group(group_id="quality_and_staging")
    def quality_and_staging(source_manifest: list[dict]):

        @task
        def validate_source(dataset_config: dict) -> dict:
            """
            Validates schema, duplicate primary keys, and basic row counts.
            """

            dataset_name = dataset_config["dataset_name"]
            file_path = dataset_config["file_path"]
            primary_key = dataset_config["primary_key"]
            required_columns = dataset_config["required_columns"]

            df = pd.read_csv(file_path)

            missing_columns = [
                column for column in required_columns
                if column not in df.columns
            ]

            if missing_columns:
                raise AirflowFailException(
                    f"{dataset_name} is missing columns: {missing_columns}"
                )

            duplicate_keys = int(df[primary_key].duplicated().sum())
            total_rows = len(df)

            if total_rows == 0:
                raise AirflowFailException(f"{dataset_name} has zero rows.")

            validation_result = {
                "dataset_name": dataset_name,
                "file_path": file_path,
                "primary_key": primary_key,
                "total_rows": total_rows,
                "duplicate_keys": duplicate_keys,
                "status": "valid",
            }

            print("Validation result:")
            print(json.dumps(validation_result, indent=2))

            return validation_result

        @task
        def stage_source(validation_result: dict) -> dict:
            """
            Stages each valid source as a cleaned CSV.
            """

            STAGE_DIR.mkdir(parents=True, exist_ok=True)

            dataset_name = validation_result["dataset_name"]
            file_path = validation_result["file_path"]
            primary_key = validation_result["primary_key"]

            df = pd.read_csv(file_path)

            df = df.drop_duplicates(subset=[primary_key])

            for column in df.select_dtypes(include=["object"]).columns:
                df[column] = df[column].fillna("").str.strip()

            stage_file_path = STAGE_DIR / f"{dataset_name}_stage.csv"
            df.to_csv(stage_file_path, index=False)

            stage_result = {
                "dataset_name": dataset_name,
                "stage_file_path": str(stage_file_path),
                "staged_rows": len(df),
            }

            print("Stage result:")
            print(json.dumps(stage_result, indent=2))

            return stage_result

        validation_results = validate_source.expand(dataset_config=source_manifest)
        staged_results = stage_source.expand(validation_result=validation_results)

        return staged_results

    @task
    def build_fact_orders(staged_results) -> dict:
        """
        Joins staged customers, products, and orders.
        Applies referential integrity checks.
        """

        MART_DIR.mkdir(parents=True, exist_ok=True)

        staged_results = list(staged_results)

        stage_lookup = {
            item["dataset_name"]: item["stage_file_path"]
            for item in staged_results
        }

        customers = pd.read_csv(stage_lookup["customers"])
        products = pd.read_csv(stage_lookup["products"])
        orders = pd.read_csv(stage_lookup["orders"])

        orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")

        valid_orders = orders[
            (orders["quantity"] > 0)
            & (orders["order_date"].notna())
        ].copy()

        fact_orders = (
            valid_orders
            .merge(customers, on="customer_id", how="inner")
            .merge(products, on="product_id", how="inner")
        )

        fact_orders["sales_amount"] = fact_orders["quantity"] * fact_orders["unit_price"]

        rejected_orders = orders[
            ~orders["order_id"].isin(fact_orders["order_id"])
        ].copy()

        fact_file_path = MART_DIR / "fact_orders.csv"
        reject_file_path = MART_DIR / "rejected_orders.csv"

        fact_orders.to_csv(fact_file_path, index=False)
        rejected_orders.to_csv(reject_file_path, index=False)

        result = {
            "fact_file_path": str(fact_file_path),
            "reject_file_path": str(reject_file_path),
            "fact_rows": len(fact_orders),
            "rejected_rows": len(rejected_orders),
            "total_sales": float(fact_orders["sales_amount"].sum()),
        }

        print("Fact build result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def check_fact_quality(fact_result: dict) -> dict:
        """
        Fails the DAG if the final fact table is empty or sales are invalid.
        """

        fact_df = pd.read_csv(fact_result["fact_file_path"])

        if len(fact_df) == 0:
            raise AirflowFailException("fact_orders has zero rows.")

        if (fact_df["sales_amount"] <= 0).any():
            raise AirflowFailException("fact_orders contains invalid sales_amount values.")

        quality_result = {
            "fact_rows": len(fact_df),
            "total_sales": float(fact_df["sales_amount"].sum()),
            "quality_status": "passed",
        }

        print("Fact quality result:")
        print(json.dumps(quality_result, indent=2))

        return quality_result

    @task
    def load_to_sqlite_warehouse(fact_result: dict) -> dict:
        """
        Loads fact_orders into a local SQLite database.
        """

        BASE_DIR.mkdir(parents=True, exist_ok=True)

        fact_df = pd.read_csv(fact_result["fact_file_path"])

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            fact_df.to_sql(
                name="fact_orders",
                con=conn,
                if_exists="replace",
                index=False,
            )

        load_result = {
            "warehouse_db": str(WAREHOUSE_DB),
            "table_name": "fact_orders",
            "loaded_rows": len(fact_df),
        }

        print("Warehouse load result:")
        print(json.dumps(load_result, indent=2))

        return load_result

    @task
    def create_business_metrics(load_result: dict) -> dict:
        """
        Creates aggregate business metrics from the SQLite warehouse.
        """

        with sqlite3.connect(load_result["warehouse_db"]) as conn:
            sales_by_category = pd.read_sql_query(
                """
                SELECT
                    category,
                    ROUND(SUM(sales_amount), 2) AS total_sales,
                    COUNT(DISTINCT order_id) AS total_orders
                FROM fact_orders
                GROUP BY category
                ORDER BY total_sales DESC;
                """,
                conn,
            )

            sales_by_country = pd.read_sql_query(
                """
                SELECT
                    country,
                    ROUND(SUM(sales_amount), 2) AS total_sales,
                    COUNT(DISTINCT customer_id) AS total_customers
                FROM fact_orders
                GROUP BY country
                ORDER BY total_sales DESC;
                """,
                conn,
            )

        metrics_dir = MART_DIR / "metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)

        category_file = metrics_dir / "sales_by_category.csv"
        country_file = metrics_dir / "sales_by_country.csv"

        sales_by_category.to_csv(category_file, index=False)
        sales_by_country.to_csv(country_file, index=False)

        metrics_result = {
            "sales_by_category_file": str(category_file),
            "sales_by_country_file": str(country_file),
            "metric_tables_created": 2,
        }

        print("Business metrics result:")
        print(json.dumps(metrics_result, indent=2))

        return metrics_result

    @task
    def write_pipeline_summary(
        fact_result: dict,
        quality_result: dict,
        load_result: dict,
        metrics_result: dict,
    ) -> str:
        """
        Writes a final summary JSON file.
        """

        SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

        summary = {
            "pipeline_name": "ecommerce_multi_source_elt",
            "fact_result": fact_result,
            "quality_result": quality_result,
            "load_result": load_result,
            "metrics_result": metrics_result,
            "status": "completed",
        }

        summary_file_path = SUMMARY_DIR / "pipeline_summary.json"

        with open(summary_file_path, "w") as file:
            json.dump(summary, file, indent=2)

        print(f"Pipeline summary written to: {summary_file_path}")

        return str(summary_file_path)

    source_manifest = create_raw_sources()

    staged_results = quality_and_staging(source_manifest)

    fact_result = build_fact_orders(staged_results)
    quality_result = check_fact_quality(fact_result)
    load_result = load_to_sqlite_warehouse(fact_result)
    metrics_result = create_business_metrics(load_result)

    write_pipeline_summary(
        fact_result=fact_result,
        quality_result=quality_result,
        load_result=load_result,
        metrics_result=metrics_result,
    )


ecommerce_multi_source_elt()
