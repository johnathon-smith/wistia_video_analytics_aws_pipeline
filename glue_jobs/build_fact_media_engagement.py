"""Build the refined fact_media_engagement Delta table from validated events.

Required AWS Glue job arguments:
    --JOB_NAME

Input resolution order:
    1. --INGESTION_RUN_ID and --VALIDATION_REPORT_URI
    2. The same properties from the current Glue workflow run

Optional arguments:
    --INGESTION_RUN_ID
    --VALIDATION_REPORT_URI
    --REFINED_PREFIX                     Default: refined/fact_media_engagement
    --FACT_MEDIA_ENGAGEMENT_TABLE_URI    Overrides the inferred Delta table URI
    --WORKFLOW_NAME                      Supplied by AWS Glue in a workflow
    --WORKFLOW_RUN_ID                    Supplied by AWS Glue in a workflow

Configure the Glue Spark job with:
    --datalake-formats delta
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
           --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import BotoCoreError, ClientError


LOGGER = logging.getLogger(__name__)


class FactMediaEngagementError(RuntimeError):
    """Raised when fact_media_engagement cannot be built safely."""


@dataclass(frozen=True)
class JobConfig:
    job_name: str
    ingestion_run_id: str | None
    validation_report_uri: str | None
    refined_prefix: str
    fact_media_engagement_table_uri: str | None
    workflow_name: str | None
    workflow_run_id: str | None


@dataclass(frozen=True)
class RunInput:
    ingestion_run_id: str
    validation_report_uri: str


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )


def parse_optional_argument(name: str, default: str | None = None) -> str | None:
    flag = f"--{name}"
    if flag not in sys.argv:
        return default
    index = sys.argv.index(flag)
    if index + 1 >= len(sys.argv) or sys.argv[index + 1].startswith("--"):
        raise FactMediaEngagementError(f"{flag} requires a value.")
    return sys.argv[index + 1]


def load_config() -> JobConfig:
    required = getResolvedOptions(sys.argv, ["JOB_NAME"])
    refined_prefix = parse_optional_argument(
        "REFINED_PREFIX", "refined/fact_media_engagement"
    )
    assert refined_prefix is not None
    return JobConfig(
        job_name=required["JOB_NAME"],
        ingestion_run_id=parse_optional_argument("INGESTION_RUN_ID"),
        validation_report_uri=parse_optional_argument("VALIDATION_REPORT_URI"),
        refined_prefix=refined_prefix.strip("/"),
        fact_media_engagement_table_uri=parse_optional_argument(
            "FACT_MEDIA_ENGAGEMENT_TABLE_URI"
        ),
        workflow_name=parse_optional_argument("WORKFLOW_NAME"),
        workflow_run_id=parse_optional_argument("WORKFLOW_RUN_ID"),
    )


def workflow_context(config: JobConfig) -> tuple[str, str] | None:
    if not config.workflow_name and not config.workflow_run_id:
        return None
    if not config.workflow_name or not config.workflow_run_id:
        raise FactMediaEngagementError(
            "WORKFLOW_NAME and WORKFLOW_RUN_ID must both be supplied."
        )
    return config.workflow_name, config.workflow_run_id


def resolve_run_input(glue_client: Any, config: JobConfig) -> RunInput:
    if config.ingestion_run_id or config.validation_report_uri:
        if not config.ingestion_run_id or not config.validation_report_uri:
            raise FactMediaEngagementError(
                "INGESTION_RUN_ID and VALIDATION_REPORT_URI must both be supplied "
                "for a manual run."
            )
        return RunInput(
            ingestion_run_id=config.ingestion_run_id,
            validation_report_uri=config.validation_report_uri,
        )

    context = workflow_context(config)
    if context is None:
        raise FactMediaEngagementError(
            "No input was supplied. Provide INGESTION_RUN_ID and "
            "VALIDATION_REPORT_URI, or run the job in a Glue workflow."
        )

    workflow_name, workflow_run_id = context
    try:
        properties = glue_client.get_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
        ).get("RunProperties", {})
    except (BotoCoreError, ClientError) as exc:
        raise FactMediaEngagementError(
            f"Unable to read properties for workflow {workflow_name!r}, "
            f"run {workflow_run_id!r}."
        ) from exc

    ingestion_run_id = properties.get("INGESTION_RUN_ID")
    validation_report_uri = properties.get("VALIDATION_REPORT_URI")
    if not ingestion_run_id or not validation_report_uri:
        raise FactMediaEngagementError(
            "The workflow run must contain INGESTION_RUN_ID and VALIDATION_REPORT_URI."
        )
    return RunInput(
        ingestion_run_id=ingestion_run_id,
        validation_report_uri=validation_report_uri,
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise FactMediaEngagementError(f"Invalid S3 URI: {uri!r}.")
    return parsed.netloc, parsed.path.lstrip("/")


def read_validation_report(s3_client: Any, uri: str) -> dict[str, Any]:
    bucket, key = parse_s3_uri(uri)
    try:
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        report = json.loads(body)
    except (BotoCoreError, ClientError, OSError, json.JSONDecodeError) as exc:
        raise FactMediaEngagementError(
            f"Unable to read validation report {uri}."
        ) from exc
    if not isinstance(report, dict):
        raise FactMediaEngagementError(f"Expected a JSON object at {uri}.")
    return report


def resolve_raw_input(
    report: dict[str, Any],
    expected_ingestion_run_id: str,
) -> str:
    report_run_id = report.get("ingestion_run_id")
    if report_run_id != expected_ingestion_run_id:
        raise FactMediaEngagementError(
            f"Validation report run ID {report_run_id!r} does not match requested "
            f"run ID {expected_ingestion_run_id!r}."
        )
    raw_uri = report.get("raw_s3_uri")
    if not isinstance(raw_uri, str):
        raise FactMediaEngagementError("Validation report is missing raw_s3_uri.")
    parse_s3_uri(raw_uri)
    return raw_uri


def valid_record_count(report: dict[str, Any]) -> int:
    count = report.get("valid_record_count")
    if type(count) is not int or count < 0:
        raise FactMediaEngagementError(
            "Validation report has an invalid valid_record_count."
        )
    return count


def resolve_table_uri(raw_input_uri: str, config: JobConfig) -> str:
    if config.fact_media_engagement_table_uri:
        parse_s3_uri(config.fact_media_engagement_table_uri)
        return config.fact_media_engagement_table_uri.rstrip("/")
    bucket, _ = parse_s3_uri(raw_input_uri)
    return f"s3://{bucket}/{config.refined_prefix}"


def build_fact_dataframe(spark: Any, raw_input_uri: str) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    events = spark.read.json(raw_input_uri)
    required_columns = {
        "event_key",
        "visitor_key",
        "media_id",
        "received_at",
        "percent_viewed",
    }
    missing_columns = sorted(required_columns.difference(events.columns))
    if missing_columns:
        raise FactMediaEngagementError(
            f"Raw input is missing required columns: {', '.join(missing_columns)}."
        )

    candidates = events.select(
        functions.col("event_key").alias("event_id"),
        functions.col("visitor_key").alias("visitor_id"),
        functions.col("media_id"),
        functions.to_timestamp("received_at").alias("_received_at"),
        functions.col("percent_viewed").cast("double").alias("watched_percent"),
    )
    latest_per_event = Window.partitionBy("event_id").orderBy(
        functions.col("_received_at").desc(),
        functions.col("visitor_id").desc(),
        functions.col("media_id").desc(),
        functions.col("watched_percent").desc(),
    )
    return (
        candidates.withColumn(
            "_row_number",
            functions.row_number().over(latest_per_event),
        )
        .filter(functions.col("_row_number") == 1)
        .select(
            "event_id",
            "visitor_id",
            "media_id",
            functions.to_date("_received_at").alias("date"),
            "watched_percent",
        )
    )


def upsert_delta_table(spark: Any, fact: Any, table_uri: str) -> int:
    from delta.tables import DeltaTable

    row_count = fact.count()
    if row_count == 0:
        LOGGER.info("No engagement records were found; leaving the Delta table unchanged.")
        return 0

    if DeltaTable.isDeltaTable(spark, table_uri):
        target = DeltaTable.forPath(spark, table_uri)
        (
            target.alias("target")
            .merge(
                fact.alias("source"),
                "target.event_id = source.event_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        fact.write.format("delta").mode("overwrite").save(table_uri)
    return row_count


def publish_workflow_properties(
    glue_client: Any,
    config: JobConfig,
    run_input: RunInput,
    table_uri: str,
    row_count: int,
) -> None:
    context = workflow_context(config)
    if context is None:
        LOGGER.info(
            "No Glue workflow context found; skipping fact_media_engagement "
            "property publication."
        )
        return

    workflow_name, workflow_run_id = context
    try:
        glue_client.put_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
            RunProperties={
                "FACT_MEDIA_ENGAGEMENT_TABLE_URI": table_uri,
                "FACT_MEDIA_ENGAGEMENT_ROW_COUNT": str(row_count),
                "FACT_MEDIA_ENGAGEMENT_INGESTION_RUN_ID": run_input.ingestion_run_id,
            },
        )
    except (BotoCoreError, ClientError) as exc:
        raise FactMediaEngagementError(
            f"Unable to publish fact_media_engagement properties for workflow "
            f"{workflow_name!r}, run {workflow_run_id!r}."
        ) from exc


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    configure_logging()
    config = load_config()
    glue_client = boto3.client("glue")
    s3_client = boto3.client("s3")
    run_input = resolve_run_input(glue_client, config)
    report = read_validation_report(s3_client, run_input.validation_report_uri)
    raw_input_uri = resolve_raw_input(report, run_input.ingestion_run_id)
    input_record_count = valid_record_count(report)
    table_uri = resolve_table_uri(raw_input_uri, config)

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(config.job_name, {})

    LOGGER.info(
        "Building fact_media_engagement ingestion_run_id=%s raw_input_uri=%s "
        "table_uri=%s",
        run_input.ingestion_run_id,
        raw_input_uri,
        table_uri,
    )
    if input_record_count == 0:
        LOGGER.info(
            "Validation report contains no valid records; leaving "
            "fact_media_engagement unchanged."
        )
        row_count = 0
    else:
        fact = build_fact_dataframe(spark, raw_input_uri)
        row_count = upsert_delta_table(spark, fact, table_uri)
    publish_workflow_properties(
        glue_client=glue_client,
        config=config,
        run_input=run_input,
        table_uri=table_uri,
        row_count=row_count,
    )
    LOGGER.info(
        "Completed fact_media_engagement upsert row_count=%s table_uri=%s "
        "ingestion_run_id=%s",
        row_count,
        table_uri,
        run_input.ingestion_run_id,
    )
    job.commit()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOGGER.exception("fact_media_engagement build failed.")
        raise
