from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
import sqlite3

import pandas as pd

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException


BASE_DIR = Path("/tmp/healthcare_claims_etl")
RAW_DIR = BASE_DIR / "raw"
STAGE_DIR = BASE_DIR / "stage"
MART_DIR = BASE_DIR / "mart"
REJECT_DIR = BASE_DIR / "rejects"
METRICS_DIR = BASE_DIR / "metrics"
AUDIT_DIR = BASE_DIR / "audit"

WAREHOUSE_DB = BASE_DIR / "healthcare_claims.db"


default_args = {
    "owner": "rae",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="healthcare_claims_quality_etl",
    description="Healthcare insurance claims ETL with data quality checks, rejected records, SQLite warehouse, and metrics",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["airflow", "healthcare", "insurance", "claims", "data-quality"],
)
def healthcare_claims_quality_etl():

    @task
    def create_raw_sources() -> list[dict]:
        """
        Creates synthetic healthcare insurance source files.
        This project uses fake data only. Do not use real PHI in local demo pipelines.
        """

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        patients = pd.DataFrame(
            [
                {"patient_id": 1, "patient_name": "Asha Patel", "age": 29, "gender": "F", "city": "Dallas"},
                {"patient_id": 2, "patient_name": "Ben Carter", "age": 44, "gender": "M", "city": "Austin"},
                {"patient_id": 3, "patient_name": "Mina Lee", "age": 17, "gender": "F", "city": "Chicago"},
                {"patient_id": 4, "patient_name": "Leo Smith", "age": 63, "gender": "M", "city": "Houston"},
            ]
        )

        policies = pd.DataFrame(
            [
                {"policy_id": 1001, "policy_type": "Private", "monthly_premium": 310.00},
                {"policy_id": 1002, "policy_type": "Government", "monthly_premium": 120.00},
                {"policy_id": 1003, "policy_type": "Private", "monthly_premium": 450.00},
            ]
        )

        providers = pd.DataFrame(
            [
                {"provider_id": 501, "provider_name": "North Care Hospital", "provider_city": "Dallas"},
                {"provider_id": 502, "provider_name": "Lakeview Clinic", "provider_city": "Austin"},
                {"provider_id": 503, "provider_name": "Metro Health Center", "provider_city": "Chicago"},
            ]
        )

        claims = pd.DataFrame(
            [
                {
                    "claim_id": 9001,
                    "patient_id": 1,
                    "policy_id": 1001,
                    "provider_id": 501,
                    "diagnosis": "Diabetes",
                    "claim_amount": 850.50,
                    "claim_status": "Approved",
                    "claim_date": "2026-01-01",
                },
                {
                    "claim_id": 9002,
                    "patient_id": 2,
                    "policy_id": 1002,
                    "provider_id": 502,
                    "diagnosis": "Hypertension",
                    "claim_amount": 430.00,
                    "claim_status": "Rejected",
                    "claim_date": "2026-01-01",
                },
                {
                    "claim_id": 9003,
                    "patient_id": 3,
                    "policy_id": 1002,
                    "provider_id": 503,
                    "diagnosis": "Cancer",
                    "claim_amount": 5200.00,
                    "claim_status": "Approved",
                    "claim_date": "2026-01-02",
                },
                {
                    "claim_id": 9004,
                    "patient_id": 4,
                    "policy_id": 1003,
                    "provider_id": 501,
                    "diagnosis": "Knee Surgery",
                    "claim_amount": 7300.00,
                    "claim_status": "Pending",
                    "claim_date": "2026-01-02",
                },
                {
                    "claim_id": 9005,
                    "patient_id": 99,
                    "policy_id": 1001,
                    "provider_id": 501,
                    "diagnosis": "Flu",
                    "claim_amount": 150.00,
                    "claim_status": "Approved",
                    "claim_date": "2026-01-03",
                },
                {
                    "claim_id": 9006,
                    "patient_id": 2,
                    "policy_id": 9999,
                    "provider_id": 502,
                    "diagnosis": "Asthma",
                    "claim_amount": 600.00,
                    "claim_status": "Approved",
                    "claim_date": "2026-01-03",
                },
                {
                    "claim_id": 9007,
                    "patient_id": 1,
                    "policy_id": 1001,
                    "provider_id": 999,
                    "diagnosis": "Migraine",
                    "claim_amount": 200.00,
                    "claim_status": "Approved",
                    "claim_date": "2026-01-03",
                },
                {
                    "claim_id": 9008,
                    "patient_id": 1,
                    "policy_id": 1001,
                    "provider_id": 501,
                    "diagnosis": "",
                    "claim_amount": -50.00,
                    "claim_status": "Approved",
                    "claim_date": "2026-01-04",
                },
                {
                    "claim_id": 9003,
                    "patient_id": 3,
                    "policy_id": 1002,
                    "provider_id": 503,
                    "diagnosis": "Cancer",
                    "claim_amount": 5200.00,
                    "claim_status": "Approved",
                    "claim_date": "2026-01-02",
                },
            ]
        )

        source_files = {
            "patients": RAW_DIR / "patients.csv",
            "policies": RAW_DIR / "policies.csv",
            "providers": RAW_DIR / "providers.csv",
            "claims": RAW_DIR / "claims.csv",
        }

        patients.to_csv(source_files["patients"], index=False)
        policies.to_csv(source_files["policies"], index=False)
        providers.to_csv(source_files["providers"], index=False)
        claims.to_csv(source_files["claims"], index=False)

        manifest = [
            {
                "dataset_name": "patients",
                "file_path": str(source_files["patients"]),
                "primary_key": "patient_id",
                "required_columns": ["patient_id", "patient_name", "age", "gender", "city"],
            },
            {
                "dataset_name": "policies",
                "file_path": str(source_files["policies"]),
                "primary_key": "policy_id",
                "required_columns": ["policy_id", "policy_type", "monthly_premium"],
            },
            {
                "dataset_name": "providers",
                "file_path": str(source_files["providers"]),
                "primary_key": "provider_id",
                "required_columns": ["provider_id", "provider_name", "provider_city"],
            },
            {
                "dataset_name": "claims",
                "file_path": str(source_files["claims"]),
                "primary_key": "claim_id",
                "required_columns": [
                    "claim_id",
                    "patient_id",
                    "policy_id",
                    "provider_id",
                    "diagnosis",
                    "claim_amount",
                    "claim_status",
                    "claim_date",
                ],
            },
        ]

        print("Raw source manifest:")
        print(json.dumps(manifest, indent=2))

        return manifest

    @task_group(group_id="validate_and_stage_sources")
    def validate_and_stage_sources(source_manifest: list[dict]):

        @task
        def validate_schema(dataset_config: dict) -> dict:
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

            if len(df) == 0:
                raise AirflowFailException(f"{dataset_name} has zero rows.")

            result = {
                "dataset_name": dataset_name,
                "file_path": file_path,
                "primary_key": primary_key,
                "total_rows": len(df),
                "duplicate_keys": int(df[primary_key].duplicated().sum()),
                "status": "schema_valid",
            }

            print("Schema validation result:")
            print(json.dumps(result, indent=2))

            return result

        @task
        def stage_dataset(validation_result: dict) -> dict:
            STAGE_DIR.mkdir(parents=True, exist_ok=True)

            dataset_name = validation_result["dataset_name"]
            file_path = validation_result["file_path"]
            primary_key = validation_result["primary_key"]

            df = pd.read_csv(file_path)

            for column in df.select_dtypes(include=["object"]).columns:
                df[column] = df[column].fillna("").str.strip()

            df = df.drop_duplicates(subset=[primary_key], keep="first")

            stage_file = STAGE_DIR / f"{dataset_name}_stage.csv"
            df.to_csv(stage_file, index=False)

            result = {
                "dataset_name": dataset_name,
                "stage_file": str(stage_file),
                "staged_rows": len(df),
            }

            print("Stage result:")
            print(json.dumps(result, indent=2))

            return result

        validation_results = validate_schema.expand(dataset_config=source_manifest)
        staged_results = stage_dataset.expand(validation_result=validation_results)

        return staged_results

    @task
    def build_claims_mart(staged_results: list[dict]) -> dict:
        """
        Builds a clean fact_claims table and a rejected_claims file.
        """

        MART_DIR.mkdir(parents=True, exist_ok=True)
        REJECT_DIR.mkdir(parents=True, exist_ok=True)

        stage_lookup = {
            item["dataset_name"]: item["stage_file"]
            for item in staged_results
        }

        patients = pd.read_csv(stage_lookup["patients"])
        policies = pd.read_csv(stage_lookup["policies"])
        providers = pd.read_csv(stage_lookup["providers"])
        claims = pd.read_csv(stage_lookup["claims"])

        claims["diagnosis"] = claims["diagnosis"].fillna("").str.strip()
        claims["claim_status"] = claims["claim_status"].fillna("").str.lower().str.strip()
        claims["claim_amount"] = pd.to_numeric(claims["claim_amount"], errors="coerce")
        claims["claim_date"] = pd.to_datetime(claims["claim_date"], errors="coerce")

        valid_statuses = ["approved", "rejected", "pending"]

        claims["reject_reason"] = ""

        claims.loc[claims["claim_id"].isna(), "reject_reason"] += "missing claim_id; "
        claims.loc[claims["patient_id"].isna(), "reject_reason"] += "missing patient_id; "
        claims.loc[claims["policy_id"].isna(), "reject_reason"] += "missing policy_id; "
        claims.loc[claims["provider_id"].isna(), "reject_reason"] += "missing provider_id; "
        claims.loc[claims["diagnosis"] == "", "reject_reason"] += "missing diagnosis; "
        claims.loc[claims["claim_amount"].isna(), "reject_reason"] += "invalid claim_amount; "
        claims.loc[claims["claim_amount"] <= 0, "reject_reason"] += "claim_amount must be greater than zero; "
        claims.loc[~claims["claim_status"].isin(valid_statuses), "reject_reason"] += "invalid claim_status; "
        claims.loc[claims["claim_date"].isna(), "reject_reason"] += "invalid claim_date; "

        claims.loc[
            ~claims["patient_id"].isin(patients["patient_id"]),
            "reject_reason",
        ] += "patient_id not found; "

        claims.loc[
            ~claims["policy_id"].isin(policies["policy_id"]),
            "reject_reason",
        ] += "policy_id not found; "

        claims.loc[
            ~claims["provider_id"].isin(providers["provider_id"]),
            "reject_reason",
        ] += "provider_id not found; "

        clean_claims = claims[claims["reject_reason"] == ""].copy()
        rejected_claims = claims[claims["reject_reason"] != ""].copy()

        fact_claims = (
            clean_claims
            .merge(patients, on="patient_id", how="inner")
            .merge(policies, on="policy_id", how="inner")
            .merge(providers, on="provider_id", how="inner")
        )

        fact_claims["claim_date"] = fact_claims["claim_date"].astype(str)
        fact_claims["claim_month"] = fact_claims["claim_date"].str.slice(0, 7)
        fact_claims["claim_to_premium_ratio"] = (
            fact_claims["claim_amount"] / fact_claims["monthly_premium"]
        ).round(2)

        fact_file = MART_DIR / "fact_claims.csv"
        rejected_file = REJECT_DIR / "rejected_claims.csv"

        fact_claims.to_csv(fact_file, index=False)
        rejected_claims.to_csv(rejected_file, index=False)

        result = {
            "fact_file": str(fact_file),
            "rejected_file": str(rejected_file),
            "fact_rows": len(fact_claims),
            "rejected_rows": len(rejected_claims),
            "total_claim_amount": float(fact_claims["claim_amount"].sum()),
        }

        print("Claims mart result:")
        print(json.dumps(result, indent=2))

        if len(fact_claims) == 0:
            raise AirflowFailException("No valid claims available for mart load.")

        return result

    @task
    def check_claims_quality(mart_result: dict) -> dict:
        """
        Runs final quality checks on fact_claims.
        """

        fact_df = pd.read_csv(mart_result["fact_file"])

        if fact_df["claim_id"].duplicated().any():
            raise AirflowFailException("Duplicate claim_id found in fact_claims.")

        if fact_df[["claim_id", "patient_id", "policy_id", "provider_id"]].isna().any().any():
            raise AirflowFailException("Null business keys found in fact_claims.")

        if (fact_df["claim_amount"] <= 0).any():
            raise AirflowFailException("Invalid claim_amount found in fact_claims.")

        result = {
            "quality_status": "passed",
            "fact_rows_checked": len(fact_df),
            "total_claim_amount": float(fact_df["claim_amount"].sum()),
        }

        print("Final quality result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def load_to_sqlite_warehouse(mart_result: dict) -> dict:
        """
        Loads fact and rejected records into SQLite.
        """

        BASE_DIR.mkdir(parents=True, exist_ok=True)

        fact_df = pd.read_csv(mart_result["fact_file"])
        rejected_df = pd.read_csv(mart_result["rejected_file"])

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            fact_df.to_sql(
                name="fact_claims",
                con=conn,
                if_exists="replace",
                index=False,
            )

            rejected_df.to_sql(
                name="rejected_claims",
                con=conn,
                if_exists="replace",
                index=False,
            )

            fact_count = conn.execute(
                "SELECT COUNT(*) FROM fact_claims;"
            ).fetchone()[0]

            rejected_count = conn.execute(
                "SELECT COUNT(*) FROM rejected_claims;"
            ).fetchone()[0]

        result = {
            "warehouse_db": str(WAREHOUSE_DB),
            "fact_table": "fact_claims",
            "rejected_table": "rejected_claims",
            "fact_rows_loaded": fact_count,
            "rejected_rows_loaded": rejected_count,
        }

        print("Warehouse load result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def create_insurance_metrics(load_result: dict) -> dict:
        """
        Creates insurance analytics outputs from the SQLite warehouse.
        """

        METRICS_DIR.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(load_result["warehouse_db"]) as conn:
            claims_by_status = pd.read_sql_query(
                """
                SELECT
                    claim_status,
                    COUNT(*) AS total_claims,
                    ROUND(SUM(claim_amount), 2) AS total_claim_amount
                FROM fact_claims
                GROUP BY claim_status
                ORDER BY total_claims DESC;
                """,
                conn,
            )

            claims_by_policy_type = pd.read_sql_query(
                """
                SELECT
                    policy_type,
                    COUNT(*) AS total_claims,
                    ROUND(SUM(claim_amount), 2) AS total_claim_amount,
                    ROUND(AVG(claim_amount), 2) AS avg_claim_amount,
                    ROUND(AVG(monthly_premium), 2) AS avg_monthly_premium
                FROM fact_claims
                GROUP BY policy_type
                ORDER BY total_claim_amount DESC;
                """,
                conn,
            )

            top_diagnoses = pd.read_sql_query(
                """
                SELECT
                    diagnosis,
                    COUNT(*) AS total_claims,
                    ROUND(SUM(claim_amount), 2) AS total_claim_amount
                FROM fact_claims
                GROUP BY diagnosis
                ORDER BY total_claim_amount DESC;
                """,
                conn,
            )

            provider_claims = pd.read_sql_query(
                """
                SELECT
                    provider_name,
                    provider_city,
                    COUNT(*) AS total_claims,
                    ROUND(SUM(claim_amount), 2) AS total_claim_amount
                FROM fact_claims
                GROUP BY provider_name, provider_city
                ORDER BY total_claim_amount DESC;
                """,
                conn,
            )

            monthly_claims = pd.read_sql_query(
                """
                SELECT
                    claim_month,
                    COUNT(*) AS total_claims,
                    ROUND(SUM(claim_amount), 2) AS total_claim_amount,
                    ROUND(AVG(claim_to_premium_ratio), 2) AS avg_claim_to_premium_ratio
                FROM fact_claims
                GROUP BY claim_month
                ORDER BY claim_month;
                """,
                conn,
            )

        metric_files = {
            "claims_by_status": METRICS_DIR / "claims_by_status.csv",
            "claims_by_policy_type": METRICS_DIR / "claims_by_policy_type.csv",
            "top_diagnoses": METRICS_DIR / "top_diagnoses.csv",
            "provider_claims": METRICS_DIR / "provider_claims.csv",
            "monthly_claims": METRICS_DIR / "monthly_claims.csv",
        }

        claims_by_status.to_csv(metric_files["claims_by_status"], index=False)
        claims_by_policy_type.to_csv(metric_files["claims_by_policy_type"], index=False)
        top_diagnoses.to_csv(metric_files["top_diagnoses"], index=False)
        provider_claims.to_csv(metric_files["provider_claims"], index=False)
        monthly_claims.to_csv(metric_files["monthly_claims"], index=False)

        result = {
            "metric_tables_created": len(metric_files),
            "metric_files": {key: str(value) for key, value in metric_files.items()},
        }

        print("Insurance metrics result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def write_audit_summary(
        mart_result: dict,
        quality_result: dict,
        load_result: dict,
        metrics_result: dict,
    ) -> str:
        """
        Writes final pipeline audit summary.
        """

        AUDIT_DIR.mkdir(parents=True, exist_ok=True)

        summary = {
            "pipeline_name": "healthcare_claims_quality_etl",
            "mart_result": mart_result,
            "quality_result": quality_result,
            "load_result": load_result,
            "metrics_result": metrics_result,
            "status": "completed",
        }

        audit_file = AUDIT_DIR / "pipeline_audit_summary.json"

        with open(audit_file, "w") as file:
            json.dump(summary, file, indent=2)

        print(f"Audit summary written to {audit_file}")

        return str(audit_file)

    source_manifest = create_raw_sources()

    staged_results = validate_and_stage_sources(source_manifest)

    mart_result = build_claims_mart(staged_results)

    quality_result = check_claims_quality(mart_result)

    load_result = load_to_sqlite_warehouse(mart_result)

    metrics_result = create_insurance_metrics(load_result)

    write_audit_summary(
        mart_result=mart_result,
        quality_result=quality_result,
        load_result=load_result,
        metrics_result=metrics_result,
    )


healthcare_claims_quality_etl()
