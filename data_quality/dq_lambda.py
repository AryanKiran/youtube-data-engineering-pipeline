"""
Lambda: Data Quality Checks
────────────────────────────
Called by Step Functions after the Silver layer is built.
Validates Silver data before allowing Gold aggregation to proceed.

Checks performed:
  1. Row count          — is there enough data?
  2. Null percentage    — are critical columns populated?
  3. Schema validation  — do expected columns exist?
  4. Value range checks — are numeric values reasonable?
  5. Freshness          — is the data recent enough?

Project-specific notes:
  - clean_statistics combines historical Kaggle and live API data.
    trending_date is not an ingestion timestamp, so its freshness check
    is explicitly skipped until the ETL adds _processed_at.
  - clean_reference_data has _ingestion_timestamp and is freshness-checked.
  - Athena performs aggregate checks across the full table; Lambda does not
    download table rows into Pandas.

Environment Variables:
    ATHENA_DATABASE          — Glue database (default: yt_pipeline_silver_dev)
    ATHENA_OUTPUT_LOCATION   — S3 URI for Athena query results (required)
    ATHENA_WORKGROUP          — Athena workgroup (default: primary)
    SNS_ALERT_TOPIC_ARN       — SNS topic ARN for failure alerts (optional)
    DQ_MIN_ROW_COUNT          — Minimum row count (default: 10)
    DQ_MAX_NULL_PERCENT       — Maximum null percentage (default: 5.0)
    DQ_MAX_VIEWS              — Maximum reasonable views (default: 50000000000)
    DQ_FRESHNESS_HOURS        — Freshness threshold (default: 48)
    ATHENA_QUERY_TIMEOUT_SECONDS — Query timeout (default: 100)
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ── Logging and AWS clients ──────────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena_client = boto3.client("athena")
glue_client = boto3.client("glue")
sns_client = boto3.client("sns")

# ── Environment variables ────────────────────────────────────────────────────

DEFAULT_DATABASE = os.environ.get("ATHENA_DATABASE", "yt_pipeline_silver_dev")
ATHENA_OUTPUT_LOCATION = os.environ["ATHENA_OUTPUT_LOCATION"]
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

# ── Thresholds ───────────────────────────────────────────────────────────────

MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = int(os.environ.get("DQ_MAX_VIEWS", "50000000000"))
FRESHNESS_HOURS = int(os.environ.get("DQ_FRESHNESS_HOURS", "48"))
QUERY_TIMEOUT_SECONDS = int(os.environ.get("ATHENA_QUERY_TIMEOUT_SECONDS", "100"))

# ── Expected Silver schemas ──────────────────────────────────────────────────
# These names match the DESCRIBE output supplied from Athena.

TABLE_CONFIG = {
    "clean_statistics": {
        "required_columns": [
            "video_id", "trending_date", "title", "channel_title",
            "category_id", "publish_time", "tags", "views", "likes",
            "dislikes", "comment_count", "thumbnail_link",
            "comments_disabled", "ratings_disabled",
            "video_error_or_removed", "description", "engagement_rate", "region"
        ],
        "critical_columns": [
            "video_id", "title", "channel_title", "views", "region"
        ],
        "non_negative_columns": ["views", "likes", "dislikes", "comment_count"],
        # Historical Kaggle dates make trending_date unsuitable for freshness.
        # Add _processed_at in the ETL before enabling this check.
        "freshness_column": None
    },
    "clean_reference_data": {
        "required_columns": [
            "kind", "etag", "id", "snippet_title", "snippet_assignable",
            "snippet_channelid", "_ingestion_timestamp", "_source_file", "region"
        ],
        "critical_columns": [
            "id", "snippet_title", "snippet_channelid", "region"
        ],
        "non_negative_columns": [],
        "freshness_column": "_ingestion_timestamp"
    }
}


# ── Athena helper functions ──────────────────────────────────────────────────

def quote_identifier(name):
    """Quote an Athena identifier."""
    return '"' + name.replace('"', '""') + '"'


def run_athena_query(sql, database):
    """
    Execute an Athena query, wait for completion and return all result rows.
    The first row (column headings) is removed.
    """
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        WorkGroup=ATHENA_WORKGROUP
    )
    query_id = response["QueryExecutionId"]
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS

    # Poll Athena until the query succeeds, fails or times out.
    while time.monotonic() < deadline:
        status = athena_client.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(
                f"Athena query {state}: "
                f"{status.get('StateChangeReason', 'No reason supplied')}"
            )
        time.sleep(2)
    else:
        athena_client.stop_query_execution(QueryExecutionId=query_id)
        raise TimeoutError(f"Athena query timed out: {query_id}")

    # Read all result pages. Athena includes a header row on the first page.
    rows = []
    first_page = True
    paginator = athena_client.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=query_id):
        page_rows = page["ResultSet"]["Rows"]
        if first_page:
            page_rows = page_rows[1:]
            first_page = False
        rows.extend(page_rows)

    return rows, query_id


def first_row_values(rows):
    """Return the first result row as a list of strings or None values."""
    if not rows:
        return []
    return [cell.get("VarCharValue") for cell in rows[0].get("Data", [])]


# ── Glue Catalog schema validation ──────────────────────────────────────────

def get_catalog_columns(database, table_name):
    """Get data columns and partition columns from Glue Catalog."""
    table = glue_client.get_table(
        DatabaseName=database,
        Name=table_name
    )["Table"]

    columns = {
        col["Name"].lower()
        for col in table.get("StorageDescriptor", {}).get("Columns", [])
    }
    columns.update(
        col["Name"].lower() for col in table.get("PartitionKeys", [])
    )
    return columns


def check_schema(database, table_name):
    """Check that every expected column exists in the Glue table."""
    expected = set(TABLE_CONFIG[table_name]["required_columns"])
    actual = get_catalog_columns(database, table_name)
    missing = sorted(expected - actual)
    passed = not missing

    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": missing,
        "passed": passed,
        "message": "All expected columns present" if passed
                   else f"Missing columns: {missing}"
    }


# ── Build aggregate SQL for full-table checks ────────────────────────────────

def build_quality_query(table_name):
    """
    Build one Athena aggregate query for row count, null percentages,
    negative values, extreme views and latest ingestion timestamp.
    """
    config = TABLE_CONFIG[table_name]
    expressions = ["COUNT(*) AS row_count"]

    # Calculate null percentages for critical columns across the full table.
    for column in config["critical_columns"]:
        col = quote_identifier(column)
        alias = quote_identifier("null_" + column)
        expressions.append(
            f"100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) "
            f"/ NULLIF(COUNT(*), 0) AS {alias}"
        )

    # Count negative values in numeric columns.
    for column in config["non_negative_columns"]:
        col = quote_identifier(column)
        alias = quote_identifier("negative_" + column)
        expressions.append(
            f"SUM(CASE WHEN {col} < 0 THEN 1 ELSE 0 END) AS {alias}"
        )

    # Add an upper-bound sanity check for video views.
    if table_name == "clean_statistics":
        expressions.append(
            f"SUM(CASE WHEN {quote_identifier('views')} > {MAX_VIEWS} "
            "THEN 1 ELSE 0 END) AS extreme_views"
        )

    # Only reference tables currently have a reliable ingestion timestamp.
    freshness_col = config["freshness_column"]
    if freshness_col:
        col = quote_identifier(freshness_col)
        expressions.append(
            "MAX(TRY(FROM_ISO8601_TIMESTAMP(CAST("
            f"{col} AS VARCHAR)))) AS latest_record"
        )

    return (
        f"SELECT {', '.join(expressions)} "
        f"FROM {quote_identifier(table_name)}"
    )


# ── Individual data quality checks ──────────────────────────────────────────

def check_row_count(count, table_name):
    """Check that a table has at least the configured minimum row count."""
    passed = count >= MIN_ROW_COUNT
    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})"
    }


def check_null_percentage(null_pct, table_name, column):
    """Check the null percentage for one critical column."""
    passed = null_pct <= MAX_NULL_PCT
    return {
        "check": "null_pct",
        "table": table_name,
        "column": column,
        "value": round(null_pct, 2),
        "threshold": MAX_NULL_PCT,
        "passed": passed,
        "message": (
            f"{column} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)"
        )
    }


def check_value_range(table_name, column, negative_count, extreme_count=0):
    """Check numeric values for negative or unusually large values."""
    passed = negative_count == 0 and extreme_count == 0

    if column == "views":
        message = (
            f"Views: {negative_count} negative, "
            f"{extreme_count} extreme (>{MAX_VIEWS})"
        )
    else:
        message = f"{column}: {negative_count} negative values"

    return {
        "check": "value_range",
        "table": table_name,
        "column": column,
        "negative_count": negative_count,
        "extreme_count": extreme_count,
        "passed": passed,
        "message": message
    }


def check_freshness(table_name, latest_record=None):
    """Check the age of the latest reliable ingestion timestamp."""
    freshness_column = TABLE_CONFIG[table_name]["freshness_column"]

    # Do not treat historical trending_date as an ingestion timestamp.
    if not freshness_column:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "skipped": True,
            "message": (
                "No reliable ingestion timestamp in clean_statistics; "
                "skipping until ETL adds _processed_at"
            )
        }

    if not latest_record:
        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": None,
            "passed": False,
            "message": "No parseable ingestion timestamp found"
        }

    try:
        latest = datetime.fromisoformat(
            str(latest_record).replace("Z", "+00:00")
        )
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        else:
            latest = latest.astimezone(timezone.utc)

        age_hours = (
            datetime.now(timezone.utc) - latest
        ).total_seconds() / 3600
        passed = age_hours <= FRESHNESS_HOURS

        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": str(latest),
            "threshold_hours": FRESHNESS_HOURS,
            "passed": passed,
            "message": (
                f"Latest: {latest}; maximum age: {FRESHNESS_HOURS} hours"
            )
        }
    except (TypeError, ValueError) as exc:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": False,
            "message": f"Could not parse timestamp: {exc}"
        }


# ── Run checks for one Silver table ──────────────────────────────────────────

def check_table(database, table_name):
    """Run schema, row count, null, range and freshness checks."""
    logger.info("Running DQ checks on %s.%s...", database, table_name)
    results = []

    # 1. Validate the Glue Catalog schema before querying the data.
    try:
        schema_result = check_schema(database, table_name)
        results.append(schema_result)
        if not schema_result["passed"]:
            logger.error(schema_result["message"])
            return results
    except Exception as exc:
        logger.exception("Could not read schema for %s", table_name)
        return [{
            "check": "schema",
            "table": table_name,
            "passed": False,
            "message": str(exc)
        }]

    # 2–5. Execute aggregate checks in Athena over the complete table.
    try:
        rows, query_id = run_athena_query(
            build_quality_query(table_name), database
        )
        values = first_row_values(rows)
        if not values:
            raise RuntimeError("Athena returned no aggregate result row")

        index = 0

        # Row count.
        count = int(values[index] or 0)
        index += 1
        results.append(check_row_count(count, table_name))

        # Null percentage for each critical column.
        for column in TABLE_CONFIG[table_name]["critical_columns"]:
            null_pct = float(values[index] or 0)
            index += 1
            results.append(
                check_null_percentage(null_pct, table_name, column)
            )

        # Numeric range checks.
        for column in TABLE_CONFIG[table_name]["non_negative_columns"]:
            negative_count = int(values[index] or 0)
            index += 1

            extreme_count = 0
            if table_name == "clean_statistics" and column == "views":
                extreme_count = int(values[index] or 0)
                index += 1

            results.append(
                check_value_range(
                    table_name, column, negative_count, extreme_count
                )
            )

        # Freshness check.
        latest_record = values[index] if index < len(values) else None
        results.append(check_freshness(table_name, latest_record))

        logger.info("Athena quality query ID: %s", query_id)

    except Exception as exc:
        logger.exception("Could not run data checks for %s", table_name)
        results.append({
            "check": "read_table",
            "table": table_name,
            "passed": False,
            "message": str(exc)
        })

    # Log every check result for CloudWatch.
    for result in results:
        logger.info(
            "  %s: %s — %s",
            result["check"],
            "PASS" if result["passed"] else "FAIL",
            result.get("message", "")
        )

    return results


# ── Lambda handler ──────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Run data quality checks on Silver layer tables.

    Expected event:
    {
        "layer": "silver",
        "database": "yt_pipeline_silver_dev",
        "tables": ["clean_statistics", "clean_reference_data"]
    }
    """
    database = event.get("database", DEFAULT_DATABASE)
    tables = event.get("tables", list(TABLE_CONFIG.keys()))

    all_results = []

    for table_name in tables:
        # Only run checks for explicitly configured tables.
        if table_name not in TABLE_CONFIG:
            all_results.append({
                "check": "configuration",
                "table": table_name,
                "passed": False,
                "message": "Table is not configured for DQ checks"
            })
            continue

        all_results.extend(check_table(database, table_name))

    # Summary.
    passed_count = sum(1 for result in all_results if result["passed"])
    total_count = len(all_results)
    overall_passed = all(
        result["passed"] for result in all_results
    )

    logger.info(
        "DQ Summary: %s/%s checks passed. Overall: %s",
        passed_count,
        total_count,
        "PASS" if overall_passed else "FAIL"
    )

    response = {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "database": database,
        "details": all_results
    }

    # Publish failed checks to SNS so the pipeline team is notified.
    if not overall_passed and SNS_TOPIC:
        failed_checks = [
            result for result in all_results if not result["passed"]
        ]
        try:
            sns_client.publish(
                TopicArn=SNS_TOPIC,
                Subject="[YT Pipeline] Data quality checks FAILED",
                Message=json.dumps(failed_checks, indent=2, default=str)
            )
            logger.info("SNS failure alert published")
        except ClientError:
            logger.exception("Could not publish SNS failure alert")

    return json.loads(json.dumps(response, default=str))
