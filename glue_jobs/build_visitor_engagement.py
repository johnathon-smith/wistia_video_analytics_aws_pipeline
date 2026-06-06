"""Build the curated visitor_engagement Delta table from the refined fact table.

Required AWS Glue job arguments:
    --JOB_NAME

Input resolution order:
    1. --INGESTION_RUN_ID and --FACT_MEDIA_ENGAGEMENT_TABLE_URI
    2. The same properties from the current Glue workflow run

Optional arguments:
    --INGESTION_RUN_ID
    --FACT_MEDIA_ENGAGEMENT_TABLE_URI
    --DATA_THROUGH_DATE               Manual run date in YYYY-MM-DD format
    --CURATED_PREFIX                  Default: curated/visitor_engagement
    --VISITOR_ENGAGEMENT_TABLE_URI    Overrides the inferred Delta table URI
    --WORKFLOW_NAME                   Supplied by AWS Glue in a workflow
    --WORKFLOW_RUN_ID                 Supplied by AWS Glue in a workflow

Configure the Glue Spark job with:
    --datalake-formats delta
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
           --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import BotoCoreError, ClientError


LOGGER = logging.getLogger(__name__)


class VisitorEngagementError(RuntimeError):
    """Raised when visitor_engagement cannot be built safely."""


@dataclass(frozen=True)
class JobConfig:
    job_name: str
    ingestion_run_id: str | None
    fact_media_engagement_table_uri: str | None
    curated_prefix: str
    visitor_engagement_table_uri: str | None
    workflow_name: str | None
    workflow_run_id: str | None
    data_through_date: str | None = None


@dataclass(frozen=True)
class RunInput:
    ingestion_run_id: str
    fact_media_engagement_table_uri: str
    data_through_date: date | None = None


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
        raise VisitorEngagementError(f"{flag} requires a value.")
    return sys.argv[index + 1]


def load_config() -> JobConfig:
    required = getResolvedOptions(sys.argv, ["JOB_NAME"])
    curated_prefix = parse_optional_argument(
        "CURATED_PREFIX", "curated/visitor_engagement"
    )
    assert curated_prefix is not None
    return JobConfig(
        job_name=required["JOB_NAME"],
        ingestion_run_id=parse_optional_argument("INGESTION_RUN_ID"),
        fact_media_engagement_table_uri=parse_optional_argument(
            "FACT_MEDIA_ENGAGEMENT_TABLE_URI"
        ),
        curated_prefix=curated_prefix.strip("/"),
        visitor_engagement_table_uri=parse_optional_argument(
            "VISITOR_ENGAGEMENT_TABLE_URI"
        ),
        workflow_name=parse_optional_argument("WORKFLOW_NAME"),
        workflow_run_id=parse_optional_argument("WORKFLOW_RUN_ID"),
        data_through_date=parse_optional_argument("DATA_THROUGH_DATE"),
    )


def workflow_context(config: JobConfig) -> tuple[str, str] | None:
    if not config.workflow_name and not config.workflow_run_id:
        return None
    if not config.workflow_name or not config.workflow_run_id:
        raise VisitorEngagementError(
            "WORKFLOW_NAME and WORKFLOW_RUN_ID must both be supplied."
        )
    return config.workflow_name, config.workflow_run_id


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise VisitorEngagementError(f"Invalid S3 URI: {uri!r}.")
    return parsed.netloc, parsed.path.lstrip("/")


def parse_data_through_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise VisitorEngagementError(
            f"DATA_THROUGH_DATE must use YYYY-MM-DD format; received {value!r}."
        ) from exc


def resolve_run_input(glue_client: Any, config: JobConfig) -> RunInput:
    if config.ingestion_run_id or config.fact_media_engagement_table_uri:
        if not config.ingestion_run_id or not config.fact_media_engagement_table_uri:
            raise VisitorEngagementError(
                "INGESTION_RUN_ID and FACT_MEDIA_ENGAGEMENT_TABLE_URI must both "
                "be supplied for a manual run."
            )
        parse_s3_uri(config.fact_media_engagement_table_uri)
        return RunInput(
            ingestion_run_id=config.ingestion_run_id,
            fact_media_engagement_table_uri=(
                config.fact_media_engagement_table_uri.rstrip("/")
            ),
            data_through_date=parse_data_through_date(config.data_through_date),
        )

    context = workflow_context(config)
    if context is None:
        raise VisitorEngagementError(
            "No input was supplied. Provide INGESTION_RUN_ID and "
            "FACT_MEDIA_ENGAGEMENT_TABLE_URI, or run the job in a Glue workflow."
        )

    workflow_name, workflow_run_id = context
    try:
        properties = glue_client.get_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
        ).get("RunProperties", {})
    except (BotoCoreError, ClientError) as exc:
        raise VisitorEngagementError(
            f"Unable to read properties for workflow {workflow_name!r}, "
            f"run {workflow_run_id!r}."
        ) from exc

    ingestion_run_id = properties.get("INGESTION_RUN_ID")
    fact_table_uri = properties.get("FACT_MEDIA_ENGAGEMENT_TABLE_URI")
    fact_run_id = properties.get("FACT_MEDIA_ENGAGEMENT_INGESTION_RUN_ID")
    data_through_date = properties.get("INGESTION_END_DATE")
    if not ingestion_run_id or not fact_table_uri or not fact_run_id:
        raise VisitorEngagementError(
            "The workflow run must contain INGESTION_RUN_ID, "
            "FACT_MEDIA_ENGAGEMENT_TABLE_URI, and "
            "FACT_MEDIA_ENGAGEMENT_INGESTION_RUN_ID."
        )
    if fact_run_id != ingestion_run_id:
        raise VisitorEngagementError(
            f"Fact table run ID {fact_run_id!r} does not match ingestion run ID "
            f"{ingestion_run_id!r}."
        )
    parse_s3_uri(fact_table_uri)
    return RunInput(
        ingestion_run_id=ingestion_run_id,
        fact_media_engagement_table_uri=fact_table_uri.rstrip("/"),
        data_through_date=parse_data_through_date(data_through_date),
    )


def resolve_table_uri(run_input: RunInput, config: JobConfig) -> str:
    if config.visitor_engagement_table_uri:
        parse_s3_uri(config.visitor_engagement_table_uri)
        return config.visitor_engagement_table_uri.rstrip("/")
    bucket, _ = parse_s3_uri(run_input.fact_media_engagement_table_uri)
    return f"s3://{bucket}/{config.curated_prefix}"


def build_aggregate_dataframe(
    spark: Any,
    run_input: RunInput,
    refreshed_at: datetime,
) -> Any:
    from pyspark.sql import functions as functions

    fact = spark.read.format("delta").load(
        run_input.fact_media_engagement_table_uri
    )
    required_columns = {
        "event_id",
        "visitor_id",
        "media_id",
        "date",
        "watched_percent",
    }
    missing_columns = sorted(required_columns.difference(fact.columns))
    if missing_columns:
        raise VisitorEngagementError(
            f"Fact table is missing required columns: {', '.join(missing_columns)}."
        )

    aggregate = (
        fact.groupBy("visitor_id", "media_id")
        .agg(
            functions.count("event_id").cast("long").alias("total_views"),
            functions.avg("watched_percent").cast("double").alias("avg_pct_viewed"),
            functions.max("watched_percent").cast("double").alias("max_pct_viewed"),
            functions.min("date").alias("first_date_watched"),
            functions.max("date").alias("last_date_watched"),
        )
        .select(
            "visitor_id",
            "media_id",
            "total_views",
            "avg_pct_viewed",
            "max_pct_viewed",
            "first_date_watched",
            "last_date_watched",
        )
    )
    return (
        aggregate.withColumn(
            "data_through_date",
            functions.lit(run_input.data_through_date).cast("date"),
        )
        .withColumn(
            "pipeline_refreshed_at",
            functions.lit(refreshed_at).cast("timestamp"),
        )
        .withColumn(
            "ingestion_run_id",
            functions.lit(run_input.ingestion_run_id),
        )
    )


def write_delta_table(aggregate: Any, table_uri: str) -> int:
    row_count = aggregate.count()
    (
        aggregate.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_uri)
    )
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
            "No Glue workflow context found; skipping visitor_engagement "
            "property publication."
        )
        return

    workflow_name, workflow_run_id = context
    try:
        glue_client.put_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
            RunProperties={
                "VISITOR_ENGAGEMENT_TABLE_URI": table_uri,
                "VISITOR_ENGAGEMENT_ROW_COUNT": str(row_count),
                "VISITOR_ENGAGEMENT_INGESTION_RUN_ID": run_input.ingestion_run_id,
            },
        )
    except (BotoCoreError, ClientError) as exc:
        raise VisitorEngagementError(
            f"Unable to publish visitor_engagement properties for workflow "
            f"{workflow_name!r}, run {workflow_run_id!r}."
        ) from exc


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    configure_logging()
    config = load_config()
    glue_client = boto3.client("glue")
    run_input = resolve_run_input(glue_client, config)
    table_uri = resolve_table_uri(run_input, config)

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(config.job_name, {})

    LOGGER.info(
        "Building visitor_engagement ingestion_run_id=%s fact_table_uri=%s "
        "table_uri=%s",
        run_input.ingestion_run_id,
        run_input.fact_media_engagement_table_uri,
        table_uri,
    )
    aggregate = build_aggregate_dataframe(
        spark,
        run_input,
        datetime.now(timezone.utc),
    )
    row_count = write_delta_table(aggregate, table_uri)
    publish_workflow_properties(
        glue_client=glue_client,
        config=config,
        run_input=run_input,
        table_uri=table_uri,
        row_count=row_count,
    )
    LOGGER.info(
        "Completed visitor_engagement rebuild row_count=%s table_uri=%s "
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
        LOGGER.exception("visitor_engagement build failed.")
        raise
