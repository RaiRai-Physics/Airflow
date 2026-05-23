from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
import re
import sqlite3

import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


BASE_DIR = Path("/tmp/web_log_analytics")
RAW_DIR = BASE_DIR / "raw"
PARSED_DIR = BASE_DIR / "parsed"
REJECT_DIR = BASE_DIR / "rejects"
PARTITION_DIR = BASE_DIR / "partitions"
METRICS_DIR = BASE_DIR / "metrics"
SUMMARY_DIR = BASE_DIR / "summary"

WAREHOUSE_DB = BASE_DIR / "web_logs.db"


default_args = {
    "owner": "rae",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="web_log_analytics_pipeline",
    description="Web server log analytics pipeline with parsing, validation, partitioning, SQLite load, and metrics",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["airflow", "data-engineering", "logs", "analytics", "sqlite"],
)
def web_log_analytics_pipeline():

    @task
    def create_raw_logs() -> str:

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        log_lines = [
            "2026-01-01T10:01:00Z GET /home 200 120 user_101",
            "2026-01-01T10:02:15Z GET /products 200 340 user_102",
            "2026-01-01T10:03:20Z POST /cart 201 180 user_101",
            "2026-01-01T10:05:00Z GET /checkout 500 760 user_103",
            "2026-01-02T11:01:10Z GET /home 200 110 user_104",
            "2026-01-02T11:04:33Z GET /products 404 90 user_105",
            "2026-01-02T11:06:44Z POST /payment 201 640 user_104",
            "bad log line with missing fields",
            "2026-01-02T11:08:01Z GET /home abc 130 user_106",
        ]

        raw_file = RAW_DIR / "web_server_logs.txt"

        with open(raw_file, "w") as file:
            for line in log_lines:
                file.write(line + "\n")

        print(f"Raw logs written to {raw_file}")

        return str(raw_file)

    @task
    def parse_logs(raw_file: str) -> dict:
        """
        Parses raw log lines into structured records.
        """

        PARSED_DIR.mkdir(parents=True, exist_ok=True)
        REJECT_DIR.mkdir(parents=True, exist_ok=True)

        log_pattern = re.compile(
            r"^(?P<event_time>\S+) "
            r"(?P<method>GET|POST|PUT|DELETE|PATCH) "
            r"(?P<endpoint>\S+) "
            r"(?P<status_code>\d{3}) "
            r"(?P<response_time_ms>\d+) "
            r"(?P<user_id>\S+)$"
        )

        parsed_rows = []
        rejected_rows = []

        with open(raw_file, "r") as file:
            for line_number, line in enumerate(file, start=1):
                raw_line = line.strip()
                match = log_pattern.match(raw_line)

                if not match:
                    rejected_rows.append(
                        {
                            "line_number": line_number,
                            "raw_line": raw_line,
                            "reject_reason": "invalid log format",
                        }
                    )
                    continue

                parsed_rows.append(match.groupdict())

        parsed_df = pd.DataFrame(parsed_rows)
        rejected_df = pd.DataFrame(rejected_rows)

        parsed_file = PARSED_DIR / "parsed_web_logs.csv"
        reject_file = REJECT_DIR / "rejected_raw_logs.csv"

        parsed_df.to_csv(parsed_file, index=False)
        rejected_df.to_csv(reject_file, index=False)

        result = {
            "parsed_file": str(parsed_file),
            "reject_file": str(reject_file),
            "parsed_rows": len(parsed_df),
            "rejected_rows": len(rejected_df),
        }

        print("Parse result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def validate_logs(parse_result: dict) -> dict:
        """
        Validates parsed logs and separates clean events from rejected events.
        """

        parsed_file = parse_result["parsed_file"]

        df = pd.read_csv(parsed_file)

        if len(df) == 0:
            raise AirflowFailException("No parsed logs found.")

        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
        df["status_code"] = pd.to_numeric(df["status_code"], errors="coerce")
        df["response_time_ms"] = pd.to_numeric(df["response_time_ms"], errors="coerce")

        df["reject_reason"] = ""

        df.loc[df["event_time"].isna(), "reject_reason"] += "invalid event_time; "
        df.loc[df["status_code"].isna(), "reject_reason"] += "invalid status_code; "
        df.loc[df["response_time_ms"].isna(), "reject_reason"] += "invalid response_time_ms; "
        df.loc[df["response_time_ms"] <= 0, "reject_reason"] += "response_time must be positive; "
        df.loc[df["user_id"].isna(), "reject_reason"] += "missing user_id; "

        clean_df = df[df["reject_reason"] == ""].copy()
        rejected_df = df[df["reject_reason"] != ""].copy()

        clean_df["event_date"] = clean_df["event_time"].dt.date.astype(str)

        clean_file = PARSED_DIR / "clean_web_events.csv"
        rejected_file = REJECT_DIR / "rejected_parsed_logs.csv"

        clean_df.to_csv(clean_file, index=False)
        rejected_df.to_csv(rejected_file, index=False)

        result = {
            "clean_file": str(clean_file),
            "rejected_file": str(rejected_file),
            "clean_rows": len(clean_df),
            "rejected_rows": len(rejected_df),
        }

        print("Validation result:")
        print(json.dumps(result, indent=2))

        if len(clean_df) == 0:
            raise AirflowFailException("No clean web events available to load.")

        return result

    @task
    def partition_clean_events(validation_result: dict) -> dict:
        """
        Writes clean events into date-based partitions.
        Example:
        /tmp/web_log_analytics/partitions/event_date=2026-01-01/events.csv
        """

        PARTITION_DIR.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(validation_result["clean_file"])

        partition_files = []

        for event_date, partition_df in df.groupby("event_date"):
            date_partition_dir = PARTITION_DIR / f"event_date={event_date}"
            date_partition_dir.mkdir(parents=True, exist_ok=True)

            partition_file = date_partition_dir / "events.csv"
            partition_df.to_csv(partition_file, index=False)

            partition_files.append(str(partition_file))

        result = {
            "partition_count": len(partition_files),
            "partition_files": partition_files,
        }

        print("Partition result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def load_to_sqlite(validation_result: dict) -> dict:
        """
        Loads clean web events into a SQLite warehouse table.
        """

        BASE_DIR.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(validation_result["clean_file"])

        with sqlite3.connect(WAREHOUSE_DB) as conn:
            df.to_sql(
                name="fact_web_events",
                con=conn,
                if_exists="replace",
                index=False,
            )

            row_count = conn.execute(
                "SELECT COUNT(*) FROM fact_web_events;"
            ).fetchone()[0]

        result = {
            "warehouse_db": str(WAREHOUSE_DB),
            "table_name": "fact_web_events",
            "loaded_rows": row_count,
        }

        print("Load result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def create_web_metrics(load_result: dict) -> dict:
        """
        Creates analytics metrics from the web event warehouse.
        """

        METRICS_DIR.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(load_result["warehouse_db"]) as conn:
            status_metrics = pd.read_sql_query(
                """
                SELECT
                    status_code,
                    COUNT(*) AS request_count
                FROM fact_web_events
                GROUP BY status_code
                ORDER BY request_count DESC;
                """,
                conn,
            )

            endpoint_metrics = pd.read_sql_query(
                """
                SELECT
                    endpoint,
                    COUNT(*) AS request_count,
                    ROUND(AVG(response_time_ms), 2) AS avg_response_time_ms
                FROM fact_web_events
                GROUP BY endpoint
                ORDER BY request_count DESC;
                """,
                conn,
            )

            daily_metrics = pd.read_sql_query(
                """
                SELECT
                    event_date,
                    COUNT(*) AS total_requests,
                    COUNT(DISTINCT user_id) AS unique_users,
                    ROUND(AVG(response_time_ms), 2) AS avg_response_time_ms,
                    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_errors
                FROM fact_web_events
                GROUP BY event_date
                ORDER BY event_date;
                """,
                conn,
            )

        status_file = METRICS_DIR / "requests_by_status_code.csv"
        endpoint_file = METRICS_DIR / "endpoint_performance.csv"
        daily_file = METRICS_DIR / "daily_web_metrics.csv"

        status_metrics.to_csv(status_file, index=False)
        endpoint_metrics.to_csv(endpoint_file, index=False)
        daily_metrics.to_csv(daily_file, index=False)

        result = {
            "requests_by_status_code": str(status_file),
            "endpoint_performance": str(endpoint_file),
            "daily_web_metrics": str(daily_file),
            "metric_tables_created": 3,
        }

        print("Metrics result:")
        print(json.dumps(result, indent=2))

        return result

    @task
    def write_run_summary(
        parse_result: dict,
        validation_result: dict,
        partition_result: dict,
        load_result: dict,
        metrics_result: dict,
    ) -> str:
        """
        Writes a final pipeline run summary.
        """

        SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

        summary = {
            "pipeline_name": "web_log_analytics_pipeline",
            "parse_result": parse_result,
            "validation_result": validation_result,
            "partition_result": partition_result,
            "load_result": load_result,
            "metrics_result": metrics_result,
            "status": "completed",
        }

        summary_file = SUMMARY_DIR / "run_summary.json"

        with open(summary_file, "w") as file:
            json.dump(summary, file, indent=2)

        print(f"Run summary written to {summary_file}")

        return str(summary_file)

    raw_file = create_raw_logs()
    parse_result = parse_logs(raw_file)
    validation_result = validate_logs(parse_result)
    partition_result = partition_clean_events(validation_result)
    load_result = load_to_sqlite(validation_result)
    metrics_result = create_web_metrics(load_result)

    write_run_summary(
        parse_result=parse_result,
        validation_result=validation_result,
        partition_result=partition_result,
        load_result=load_result,
        metrics_result=metrics_result,
    )


web_log_analytics_pipeline()
