"""Validate Wistia event ingestion files and route records to raw or quarantine.

Required AWS Glue job arguments:
    --JOB_NAME

Manifest resolution order:
    1. --INPUT_MANIFEST_URI
    2. INGESTION_MANIFEST_URI from the current Glue workflow run

Optional arguments:
    --INPUT_MANIFEST_URI
    --RAW_PREFIX             Default: raw/wistia/events
    --QUARANTINE_PREFIX      Default: quarantine/wistia/events
    --REPORT_PREFIX          Default: validation_reports/wistia/events
    --WORKFLOW_NAME          Supplied by AWS Glue when run in a workflow
    --WORKFLOW_RUN_ID        Supplied by AWS Glue when run in a workflow
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, BinaryIO
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import BotoCoreError, ClientError


CONTRACT_VERSION = "2026-03-v1"
LOGGER = logging.getLogger(__name__)

REQUIRED_FIELDS: dict[str, str] = {
    "event_key": "string",
    "received_at": "timestamp",
    "visitor_key": "string",
    "media_id": "string",
    "media_name": "string",
    "media_url": "string",
    "percent_viewed": "number",
    "ip": "string",
    "country": "string",
}

OPTIONAL_FIELDS: dict[str, Any] = {
    "embed_url": "string",
    "org": "string",
    "region": "string",
    "city": "string",
    "lat": "number",
    "lon": "number",
    "email": "string",
    "iframe_heatmap_url": "string",
    "thumbnail": {
        "url": "string",
        "width": "integer",
        "height": "integer",
        "fileSize": "integer",
        "contentType": "string",
        "type": "string",
    },
    "conversion_type": ("integer", "string"),
    "conversion_data": {
        "email": "string",
        "first_name": "string",
        "is_new_lead": "boolean",
        "last_name": "string",
    },
    "user_agent_details": {
        "browser": "string",
        "browser_version": "string",
        "platform": "string",
        "mobile": "boolean",
    },
}

DOCUMENTED_FIELDS = {**REQUIRED_FIELDS, **OPTIONAL_FIELDS}


class WistiaValidationError(RuntimeError):
    """Raised when validation cannot complete safely."""


@dataclass(frozen=True)
class JobConfig:
    job_name: str
    input_manifest_uri: str | None
    raw_prefix: str
    quarantine_prefix: str
    report_prefix: str
    workflow_name: str | None
    workflow_run_id: str | None


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    field: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "field": self.field, "message": self.message}


@dataclass
class ValidationStats:
    total_records: int = 0
    valid_records: int = 0
    quarantined_records: int = 0
    malformed_records: int = 0


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
        raise WistiaValidationError(f"{flag} requires a value.")
    return sys.argv[index + 1]


def load_config() -> JobConfig:
    required = getResolvedOptions(sys.argv, ["JOB_NAME"])
    raw_prefix = parse_optional_argument("RAW_PREFIX", "raw/wistia/events")
    quarantine_prefix = parse_optional_argument(
        "QUARANTINE_PREFIX", "quarantine/wistia/events"
    )
    report_prefix = parse_optional_argument(
        "REPORT_PREFIX", "validation_reports/wistia/events"
    )
    assert raw_prefix is not None
    assert quarantine_prefix is not None
    assert report_prefix is not None

    return JobConfig(
        job_name=required["JOB_NAME"],
        input_manifest_uri=parse_optional_argument("INPUT_MANIFEST_URI"),
        raw_prefix=raw_prefix.strip("/"),
        quarantine_prefix=quarantine_prefix.strip("/"),
        report_prefix=report_prefix.strip("/"),
        workflow_name=parse_optional_argument("WORKFLOW_NAME"),
        workflow_run_id=parse_optional_argument("WORKFLOW_RUN_ID"),
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise WistiaValidationError(f"Invalid S3 URI: {uri!r}.")
    return parsed.netloc, parsed.path.lstrip("/")


def workflow_context(config: JobConfig) -> tuple[str, str] | None:
    if not config.workflow_name and not config.workflow_run_id:
        return None
    if not config.workflow_name or not config.workflow_run_id:
        raise WistiaValidationError(
            "WORKFLOW_NAME and WORKFLOW_RUN_ID must both be supplied."
        )
    return config.workflow_name, config.workflow_run_id


def resolve_manifest_uri(glue_client: Any, config: JobConfig) -> str:
    if config.input_manifest_uri:
        return config.input_manifest_uri

    context = workflow_context(config)
    if context is None:
        raise WistiaValidationError(
            "No manifest was supplied. Set INPUT_MANIFEST_URI or run the job in a "
            "Glue workflow that publishes INGESTION_MANIFEST_URI."
        )

    workflow_name, workflow_run_id = context
    try:
        properties = glue_client.get_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
        ).get("RunProperties", {})
    except (BotoCoreError, ClientError) as exc:
        raise WistiaValidationError(
            f"Unable to read properties for workflow {workflow_name!r}, "
            f"run {workflow_run_id!r}."
        ) from exc

    manifest_uri = properties.get("INGESTION_MANIFEST_URI")
    if not manifest_uri:
        raise WistiaValidationError(
            "The workflow run does not contain INGESTION_MANIFEST_URI."
        )
    return manifest_uri


def read_json_object(s3_client: Any, uri: str) -> dict[str, Any]:
    bucket, key = parse_s3_uri(uri)
    try:
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        value = json.loads(body)
    except (BotoCoreError, ClientError, OSError, json.JSONDecodeError) as exc:
        raise WistiaValidationError(f"Unable to read JSON object {uri}.") from exc
    if not isinstance(value, dict):
        raise WistiaValidationError(f"Expected a JSON object at {uri}.")
    return value


def manifest_sources(manifest: dict[str, Any]) -> list[str]:
    results = manifest.get("results")
    if not isinstance(results, list):
        raise WistiaValidationError("Ingestion manifest results must be an array.")

    sources: list[str] = []
    for index, result in enumerate(results):
        if not isinstance(result, dict) or not isinstance(result.get("s3_uri"), str):
            raise WistiaValidationError(
                f"Ingestion manifest result {index} is missing a valid s3_uri."
            )
        sources.append(result["s3_uri"])
    return sources


def is_type(value: Any, expected: str) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "timestamp":
        if not isinstance(value, str):
            return False
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        return True
    raise ValueError(f"Unsupported expected type: {expected}")


def expected_type_label(expected: Any) -> str:
    if isinstance(expected, tuple):
        return " or ".join(expected)
    if isinstance(expected, dict):
        return "object"
    return expected


def validate_value(value: Any, expected: Any, field: str) -> list[ValidationIssue]:
    if value is None:
        return []
    if isinstance(expected, tuple):
        if any(is_type(value, item) for item in expected):
            return []
    elif isinstance(expected, dict):
        if isinstance(value, dict):
            issues: list[ValidationIssue] = []
            for child_name, child_value in value.items():
                if child_name in expected:
                    issues.extend(
                        validate_value(
                            child_value,
                            expected[child_name],
                            f"{field}.{child_name}",
                        )
                    )
            return issues
    elif is_type(value, expected):
        return []

    return [
        ValidationIssue(
            code="invalid_type",
            field=field,
            message=f"Expected {expected_type_label(expected)}.",
        )
    ]


def find_unknown_fields(
    value: dict[str, Any],
    schema: dict[str, Any],
    prefix: str = "",
) -> set[str]:
    unknown: set[str] = set()
    for field, child_value in value.items():
        path = f"{prefix}.{field}" if prefix else field
        expected = schema.get(field)
        if field not in schema:
            unknown.add(path)
        elif isinstance(expected, dict) and isinstance(child_value, dict):
            unknown.update(find_unknown_fields(child_value, expected, path))
    return unknown


def validate_event(event: Any) -> tuple[list[ValidationIssue], set[str]]:
    if not isinstance(event, dict):
        return (
            [
                ValidationIssue(
                    code="invalid_record_type",
                    field="$",
                    message="Expected a JSON object.",
                )
            ],
            set(),
        )

    issues: list[ValidationIssue] = []
    for field, expected in REQUIRED_FIELDS.items():
        if field not in event:
            issues.append(
                ValidationIssue(
                    code="missing_required_field",
                    field=field,
                    message="Required field is missing.",
                )
            )
            continue
        if event[field] is None:
            issues.append(
                ValidationIssue(
                    code="null_required_field",
                    field=field,
                    message="Required field cannot be null.",
                )
            )
            continue
        issues.extend(validate_value(event[field], expected, field))

    for field, expected in OPTIONAL_FIELDS.items():
        if field in event:
            issues.extend(validate_value(event[field], expected, field))

    percent_viewed = event.get("percent_viewed")
    if is_type(percent_viewed, "number") and not 0.0 <= percent_viewed <= 1.0:
        issues.append(
            ValidationIssue(
                code="out_of_range",
                field="percent_viewed",
                message="Expected a number between 0.0 and 1.0.",
            )
        )

    return issues, find_unknown_fields(event, DOCUMENTED_FIELDS)


def output_keys(
    manifest: dict[str, Any],
    config: JobConfig,
) -> tuple[str, str, str]:
    run_id = manifest.get("run_id")
    extracted_at = manifest.get("extracted_at")
    if not isinstance(run_id, str) or not run_id:
        raise WistiaValidationError("Ingestion manifest is missing run_id.")
    if not isinstance(extracted_at, str):
        raise WistiaValidationError("Ingestion manifest is missing extracted_at.")
    try:
        extraction_date = datetime.fromisoformat(
            extracted_at.replace("Z", "+00:00")
        ).date().isoformat()
    except ValueError as exc:
        raise WistiaValidationError(
            "Ingestion manifest extracted_at is not a valid timestamp."
        ) from exc

    partition = f"extraction_date={extraction_date}/run_id={run_id}"
    return (
        f"{config.raw_prefix}/{partition}/events.jsonl.gz",
        f"{config.quarantine_prefix}/{partition}/events.jsonl.gz",
        f"{config.report_prefix}/{partition}/report.json",
    )


def open_source_lines(s3_client: Any, uri: str) -> tuple[BinaryIO, BinaryIO]:
    bucket, key = parse_s3_uri(uri)
    try:
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
    except (BotoCoreError, ClientError) as exc:
        raise WistiaValidationError(f"Unable to read ingestion object {uri}.") from exc
    return body, gzip.GzipFile(fileobj=body, mode="rb")


def write_json_line(file_object: BinaryIO, value: Any) -> None:
    file_object.write(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )
    file_object.write(b"\n")


def upload_gzip_file(
    s3_client: Any,
    local_path: str,
    bucket: str,
    key: str,
    metadata: dict[str, str],
) -> None:
    try:
        s3_client.upload_file(
            local_path,
            bucket,
            key,
            ExtraArgs={
                "ContentType": "application/x-ndjson",
                "ContentEncoding": "gzip",
                "Metadata": metadata,
            },
        )
    except (BotoCoreError, ClientError, OSError) as exc:
        raise WistiaValidationError(
            f"Unable to upload validation output to s3://{bucket}/{key}."
        ) from exc


def process_manifest(
    s3_client: Any,
    config: JobConfig,
    manifest_uri: str,
    validation_time: datetime,
) -> dict[str, Any]:
    manifest_bucket, _ = parse_s3_uri(manifest_uri)
    manifest = read_json_object(s3_client, manifest_uri)
    sources = manifest_sources(manifest)
    raw_key, quarantine_key, report_key = output_keys(manifest, config)
    run_id = manifest["run_id"]

    stats = ValidationStats()
    error_counts: Counter[str] = Counter()
    unknown_fields: Counter[str] = Counter()
    raw_path: str | None = None
    quarantine_path: str | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".jsonl.gz", delete=False
        ) as raw_temp, tempfile.NamedTemporaryFile(
            mode="wb", suffix=".jsonl.gz", delete=False
        ) as quarantine_temp:
            raw_path = raw_temp.name
            quarantine_path = quarantine_temp.name
            with gzip.GzipFile(
                fileobj=raw_temp, mode="wb", mtime=0
            ) as raw_output, gzip.GzipFile(
                fileobj=quarantine_temp, mode="wb", mtime=0
            ) as quarantine_output:
                for source_uri in sources:
                    source_body, source_lines = open_source_lines(s3_client, source_uri)
                    try:
                        for line_number, raw_line in enumerate(source_lines, start=1):
                            stats.total_records += 1
                            try:
                                event = json.loads(raw_line)
                            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                                stats.malformed_records += 1
                                stats.quarantined_records += 1
                                error_counts["malformed_json"] += 1
                                write_json_line(
                                    quarantine_output,
                                    {
                                        "contract_version": CONTRACT_VERSION,
                                        "errors": [
                                            {
                                                "code": "malformed_json",
                                                "field": "$",
                                                "message": str(exc),
                                            }
                                        ],
                                        "ingestion_run_id": run_id,
                                        "original_line": raw_line.decode(
                                            "utf-8", errors="replace"
                                        ).rstrip("\r\n"),
                                        "source_line_number": line_number,
                                        "source_s3_uri": source_uri,
                                        "validated_at": validation_time.isoformat().replace(
                                            "+00:00", "Z"
                                        ),
                                    },
                                )
                                continue

                            issues, record_unknown_fields = validate_event(event)
                            unknown_fields.update(record_unknown_fields)
                            if issues:
                                stats.quarantined_records += 1
                                error_counts.update(issue.code for issue in issues)
                                write_json_line(
                                    quarantine_output,
                                    {
                                        "contract_version": CONTRACT_VERSION,
                                        "errors": [
                                            issue.as_dict() for issue in issues
                                        ],
                                        "event": event,
                                        "ingestion_run_id": run_id,
                                        "source_line_number": line_number,
                                        "source_s3_uri": source_uri,
                                        "validated_at": validation_time.isoformat().replace(
                                            "+00:00", "Z"
                                        ),
                                    },
                                )
                            else:
                                stats.valid_records += 1
                                raw_output.write(raw_line.rstrip(b"\r\n"))
                                raw_output.write(b"\n")
                    except (OSError, EOFError) as exc:
                        raise WistiaValidationError(
                            f"Unable to decompress ingestion object {source_uri}."
                        ) from exc
                    finally:
                        source_lines.close()
                        source_body.close()

        metadata = {
            "contract-version": CONTRACT_VERSION,
            "ingestion-run-id": run_id,
            "manifest-uri": manifest_uri,
        }
        upload_gzip_file(s3_client, raw_path, manifest_bucket, raw_key, metadata)
        if stats.quarantined_records > 0:
            upload_gzip_file(
                s3_client,
                quarantine_path,
                manifest_bucket,
                quarantine_key,
                metadata,
            )
    finally:
        for path in (raw_path, quarantine_path):
            if path:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass

    raw_uri = f"s3://{manifest_bucket}/{raw_key}"
    quarantine_uri = (
        f"s3://{manifest_bucket}/{quarantine_key}"
        if stats.quarantined_records > 0
        else None
    )
    report_uri = f"s3://{manifest_bucket}/{report_key}"
    report = {
        "additive_drift_detected": bool(unknown_fields),
        "contract_version": CONTRACT_VERSION,
        "error_counts": dict(sorted(error_counts.items())),
        "ingestion_manifest_uri": manifest_uri,
        "ingestion_run_id": run_id,
        "input_s3_uris": sources,
        "job_name": config.job_name,
        "malformed_record_count": stats.malformed_records,
        "quarantine_s3_uri": quarantine_uri,
        "quarantined_record_count": stats.quarantined_records,
        "raw_s3_uri": raw_uri,
        "report_s3_uri": report_uri,
        "total_record_count": stats.total_records,
        "unknown_field_counts": dict(sorted(unknown_fields.items())),
        "valid_record_count": stats.valid_records,
        "validated_at": validation_time.isoformat().replace("+00:00", "Z"),
    }
    try:
        s3_client.put_object(
            Bucket=manifest_bucket,
            Key=report_key,
            Body=json.dumps(report, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as exc:
        raise WistiaValidationError(
            f"Unable to write validation report to {report_uri}."
        ) from exc
    return report


def publish_validation_workflow_properties(
    glue_client: Any,
    config: JobConfig,
    report: dict[str, Any],
) -> None:
    context = workflow_context(config)
    if context is None:
        LOGGER.info("No Glue workflow context found; skipping validation property publication.")
        return

    workflow_name, workflow_run_id = context
    properties = {
        "VALIDATION_REPORT_URI": str(report["report_s3_uri"]),
        "VALIDATION_TOTAL_COUNT": str(report["total_record_count"]),
        "VALIDATION_VALID_COUNT": str(report["valid_record_count"]),
        "VALIDATION_QUARANTINE_COUNT": str(report["quarantined_record_count"]),
        "VALIDATION_MALFORMED_COUNT": str(report["malformed_record_count"]),
    }
    try:
        glue_client.put_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
            RunProperties=properties,
        )
    except (BotoCoreError, ClientError) as exc:
        raise WistiaValidationError(
            f"Unable to publish validation properties for workflow "
            f"{workflow_name!r}, run {workflow_run_id!r}."
        ) from exc


def main() -> None:
    configure_logging()
    config = load_config()
    s3_client = boto3.client("s3")
    glue_client = boto3.client("glue")
    manifest_uri = resolve_manifest_uri(glue_client, config)

    LOGGER.info(
        "Starting Wistia validation manifest_uri=%s contract_version=%s",
        manifest_uri,
        CONTRACT_VERSION,
    )
    report = process_manifest(
        s3_client=s3_client,
        config=config,
        manifest_uri=manifest_uri,
        validation_time=datetime.now(timezone.utc),
    )
    publish_validation_workflow_properties(glue_client, config, report)
    LOGGER.info("Wistia validation summary=%s", json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOGGER.exception("Wistia event validation failed.")
        raise
