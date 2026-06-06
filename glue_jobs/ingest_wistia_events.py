"""Extract Wistia Stats API events for configured media into an S3 ingestion layer.

Required AWS Glue job arguments:
    --JOB_NAME
    --SECRET_ID
    --S3_BUCKET
    --MEDIA_IDS

Optional arguments:
    --S3_PREFIX          Default: ingestion/wistia/events
    --MANIFEST_PREFIX    Default: metadata/wistia/events/manifests
    --SECRET_REGION      Default: the Glue job's AWS region
    --START_DATE         Manual range start in YYYY-MM-DD format
    --END_DATE           Manual range end in YYYY-MM-DD format
    --WORKFLOW_NAME      Supplied by AWS Glue when run in a workflow
    --WORKFLOW_RUN_ID    Supplied by AWS Glue when run in a workflow

MEDIA_IDS accepts either a comma-separated string or a JSON array.
The secret may be a plain token or a JSON object containing one of:
api_token, token, wistia_api_token, or WISTIA_API_TOKEN.

START_DATE and END_DATE must be supplied together. When neither is supplied, the
job ingests yesterday in UTC, the latest fully completed day.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import random
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import boto3
import requests
from awsglue.utils import getResolvedOptions
from botocore.exceptions import BotoCoreError, ClientError
from requests.adapters import HTTPAdapter


API_URL = "https://api.wistia.com/modern/stats/events"
API_VERSION = "2026-03"
PER_PAGE = 100
REQUEST_TIMEOUT_SECONDS = (10, 60)
MAX_REQUEST_ATTEMPTS = 6
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
TOKEN_KEYS = ("api_token", "token", "wistia_api_token", "WISTIA_API_TOKEN")

LOGGER = logging.getLogger(__name__)


class WistiaIngestionError(RuntimeError):
    """Raised when the Wistia extraction cannot complete safely."""


@dataclass(frozen=True)
class JobConfig:
    job_name: str
    secret_id: str
    secret_region: str | None
    s3_bucket: str
    s3_prefix: str
    manifest_prefix: str
    media_ids: tuple[str, ...]
    workflow_name: str | None
    workflow_run_id: str | None
    start_date_override: str | None = None
    end_date_override: str | None = None


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
        raise WistiaIngestionError(f"{flag} requires a value.")
    return sys.argv[index + 1]


def parse_media_ids(value: str) -> tuple[str, ...]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = value.split(",")

    if isinstance(parsed, str):
        parsed = [parsed]
    if not isinstance(parsed, list):
        raise WistiaIngestionError("MEDIA_IDS must be a comma-separated string or a JSON array.")

    media_ids = tuple(dict.fromkeys(str(item).strip() for item in parsed if str(item).strip()))
    if not media_ids:
        raise WistiaIngestionError("At least one media ID must be supplied in MEDIA_IDS.")
    return media_ids


def load_config() -> JobConfig:
    required = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "SECRET_ID", "S3_BUCKET", "MEDIA_IDS"],
    )
    prefix = parse_optional_argument("S3_PREFIX", "ingestion/wistia/events")
    manifest_prefix = parse_optional_argument(
        "MANIFEST_PREFIX", "metadata/wistia/events/manifests"
    )
    assert prefix is not None
    assert manifest_prefix is not None

    return JobConfig(
        job_name=required["JOB_NAME"],
        secret_id=required["SECRET_ID"],
        secret_region=parse_optional_argument("SECRET_REGION"),
        s3_bucket=required["S3_BUCKET"],
        s3_prefix=prefix.strip("/"),
        manifest_prefix=manifest_prefix.strip("/"),
        media_ids=parse_media_ids(required["MEDIA_IDS"]),
        workflow_name=parse_optional_argument("WORKFLOW_NAME"),
        workflow_run_id=parse_optional_argument("WORKFLOW_RUN_ID"),
        start_date_override=parse_optional_argument("START_DATE"),
        end_date_override=parse_optional_argument("END_DATE"),
    )


def parse_date_argument(name: str, value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise WistiaIngestionError(
            f"{name} must be a valid date in YYYY-MM-DD format; received {value!r}."
        ) from exc


def resolve_date_window(
    run_date: date,
    start_date_override: str | None,
    end_date_override: str | None,
) -> tuple[date, date]:
    if not start_date_override and not end_date_override:
        latest_completed_date = run_date - timedelta(days=1)
        return latest_completed_date, latest_completed_date

    if not start_date_override or not end_date_override:
        raise WistiaIngestionError(
            "START_DATE and END_DATE must either both be supplied or both be omitted."
        )

    start_date = parse_date_argument("START_DATE", start_date_override)
    end_date = parse_date_argument("END_DATE", end_date_override)
    if start_date > end_date:
        raise WistiaIngestionError(
            f"START_DATE {start_date} cannot be later than END_DATE {end_date}."
        )
    return start_date, end_date


def get_api_token(secret_id: str, region_name: str | None) -> str:
    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except (BotoCoreError, ClientError) as exc:
        raise WistiaIngestionError(f"Unable to retrieve secret {secret_id!r}.") from exc

    secret = response.get("SecretString")
    if not secret:
        raise WistiaIngestionError(
            f"Secret {secret_id!r} must contain a non-empty SecretString."
        )

    try:
        payload = json.loads(secret)
    except json.JSONDecodeError:
        token = secret.strip()
    else:
        if not isinstance(payload, dict):
            raise WistiaIngestionError(
                f"Secret {secret_id!r} must be a token string or a JSON object."
            )
        token = next(
            (
                str(payload[key]).strip()
                for key in TOKEN_KEYS
                if payload.get(key) is not None and str(payload[key]).strip()
            ),
            "",
        )

    if not token:
        raise WistiaIngestionError(
            f"Secret {secret_id!r} does not contain a supported Wistia API token key."
        )
    return token


def build_session(api_token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
            "User-Agent": "aws-glue-wistia-events-ingestion/1.0",
            "X-Wistia-API-Version": API_VERSION,
        }
    )
    adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=0)
    session.mount("https://", adapter)
    return session


def retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
                except (TypeError, ValueError, OverflowError):
                    pass

    return min(60.0, (2 ** (attempt - 1)) + random.uniform(0.0, 1.0))


def get_event_page(
    session: requests.Session,
    media_id: str,
    page: int,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    params = {
        "media_id": media_id,
        "page": page,
        "per_page": PER_PAGE,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        response: requests.Response | None = None
        try:
            response = session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == MAX_REQUEST_ATTEMPTS:
                raise WistiaIngestionError(
                    f"Wistia request failed after {attempt} attempts for "
                    f"media_id={media_id}, page={page}."
                ) from exc
            delay = retry_delay(None, attempt)
            LOGGER.warning(
                "Transient Wistia connection error; retrying media_id=%s page=%s "
                "attempt=%s delay_seconds=%.2f",
                media_id,
                page,
                attempt,
                delay,
            )
            time.sleep(delay)
            continue
        except requests.RequestException as exc:
            raise WistiaIngestionError(
                f"Unexpected Wistia request failure for media_id={media_id}, page={page}."
            ) from exc

        if response.status_code in RETRYABLE_STATUS_CODES:
            if attempt == MAX_REQUEST_ATTEMPTS:
                raise WistiaIngestionError(
                    f"Wistia returned HTTP {response.status_code} after {attempt} attempts "
                    f"for media_id={media_id}, page={page}."
                )
            delay = retry_delay(response, attempt)
            LOGGER.warning(
                "Retryable Wistia response status=%s media_id=%s page=%s "
                "attempt=%s delay_seconds=%.2f",
                response.status_code,
                media_id,
                page,
                attempt,
                delay,
            )
            time.sleep(delay)
            continue

        if response.status_code != 200:
            detail = response.text[:500].replace("\n", " ")
            raise WistiaIngestionError(
                f"Wistia returned HTTP {response.status_code} for media_id={media_id}, "
                f"page={page}: {detail}"
            )

        try:
            payload = response.json()
        except requests.JSONDecodeError as exc:
            raise WistiaIngestionError(
                f"Wistia returned invalid JSON for media_id={media_id}, page={page}."
            ) from exc

        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise WistiaIngestionError(
                f"Wistia returned an unexpected response shape for "
                f"media_id={media_id}, page={page}."
            )
        return payload

    raise AssertionError("The request retry loop exited unexpectedly.")


def s3_key_for_events(
    prefix: str,
    extraction_time: datetime,
    media_id: str,
    run_id: str,
) -> str:
    return (
        f"{prefix}/extraction_date={extraction_time:%Y-%m-%d}/"
        f"media_id={media_id}/events_{extraction_time:%Y%m%dT%H%M%SZ}_{run_id}.jsonl.gz"
    )


def write_media_events(
    session: requests.Session,
    s3_client: Any,
    config: JobConfig,
    media_id: str,
    start_date: date,
    end_date: date,
    extraction_time: datetime,
    run_id: str,
) -> dict[str, Any]:
    page = 1
    event_count = 0
    page_count = 0
    s3_key = s3_key_for_events(config.s3_prefix, extraction_time, media_id, run_id)

    temporary_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f"wistia_{media_id}_",
            suffix=".jsonl.gz",
            delete=False,
        ) as temporary_file:
            temporary_path = temporary_file.name
            with gzip.GzipFile(fileobj=temporary_file, mode="wb", mtime=0) as compressed_file:
                while True:
                    events = get_event_page(session, media_id, page, start_date, end_date)
                    if not events:
                        break

                    for event in events:
                        compressed_file.write(
                            json.dumps(
                                event,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ).encode("utf-8")
                        )
                        compressed_file.write(b"\n")

                    event_count += len(events)
                    page_count += 1
                    LOGGER.info(
                        "Fetched Wistia events media_id=%s page=%s page_events=%s total_events=%s",
                        media_id,
                        page,
                        len(events),
                        event_count,
                    )
                    if len(events) < PER_PAGE:
                        break
                    page += 1

        s3_client.upload_file(
            temporary_path,
            config.s3_bucket,
            s3_key,
            ExtraArgs={
                "ContentType": "application/x-ndjson",
                "ContentEncoding": "gzip",
                "Metadata": {
                    "api-version": API_VERSION,
                    "end-date": end_date.isoformat(),
                    "event-count": str(event_count),
                    "media-id": media_id,
                    "run-id": run_id,
                    "start-date": start_date.isoformat(),
                },
            },
        )
    except (BotoCoreError, ClientError, OSError) as exc:
        raise WistiaIngestionError(
            f"Unable to write Wistia events for media_id={media_id} to "
            f"s3://{config.s3_bucket}/{s3_key}."
        ) from exc
    finally:
        if temporary_path:
            try:
                os.remove(temporary_path)
            except FileNotFoundError:
                pass

    return {
        "media_id": media_id,
        "event_count": event_count,
        "page_count": page_count,
        "s3_uri": f"s3://{config.s3_bucket}/{s3_key}",
        "status": "succeeded",
    }


def write_manifest(
    s3_client: Any,
    config: JobConfig,
    extraction_time: datetime,
    run_id: str,
    start_date: date,
    end_date: date,
    results: list[dict[str, Any]],
) -> str:
    manifest_key = (
        f"{config.manifest_prefix}/extraction_date={extraction_time:%Y-%m-%d}/"
        f"manifest_{extraction_time:%Y%m%dT%H%M%SZ}_{run_id}.json"
    )
    manifest = {
        "api_url": API_URL,
        "api_version": API_VERSION,
        "end_date": end_date.isoformat(),
        "extracted_at": extraction_time.isoformat().replace("+00:00", "Z"),
        "job_name": config.job_name,
        "media_count": len(results),
        "results": results,
        "run_id": run_id,
        "start_date": start_date.isoformat(),
        "total_event_count": sum(result["event_count"] for result in results),
    }

    try:
        s3_client.put_object(
            Bucket=config.s3_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as exc:
        raise WistiaIngestionError(
            f"Unable to write ingestion manifest to "
            f"s3://{config.s3_bucket}/{manifest_key}."
        ) from exc
    return f"s3://{config.s3_bucket}/{manifest_key}"


def publish_ingestion_workflow_properties(
    glue_client: Any,
    config: JobConfig,
    manifest_uri: str,
    run_id: str,
) -> None:
    if not config.workflow_name and not config.workflow_run_id:
        LOGGER.info("No Glue workflow context found; skipping workflow property publication.")
        return
    if not config.workflow_name or not config.workflow_run_id:
        raise WistiaIngestionError(
            "WORKFLOW_NAME and WORKFLOW_RUN_ID must both be supplied for workflow publication."
        )

    try:
        glue_client.put_workflow_run_properties(
            Name=config.workflow_name,
            RunId=config.workflow_run_id,
            RunProperties={
                "INGESTION_MANIFEST_URI": manifest_uri,
                "INGESTION_RUN_ID": run_id,
            },
        )
    except (BotoCoreError, ClientError) as exc:
        raise WistiaIngestionError(
            f"Unable to publish ingestion properties for workflow "
            f"{config.workflow_name!r}, run {config.workflow_run_id!r}."
        ) from exc


def main() -> None:
    configure_logging()
    config = load_config()
    extraction_time = datetime.now(timezone.utc)
    start_date, end_date = resolve_date_window(
        extraction_time.date(),
        config.start_date_override,
        config.end_date_override,
    )
    run_id = uuid.uuid4().hex

    LOGGER.info(
        "Starting Wistia event ingestion media_count=%s start_date=%s end_date=%s "
        "api_version=%s run_id=%s",
        len(config.media_ids),
        start_date,
        end_date,
        API_VERSION,
        run_id,
    )

    api_token = get_api_token(config.secret_id, config.secret_region)
    session = build_session(api_token)
    s3_client = boto3.client("s3")
    glue_client = boto3.client("glue")
    results: list[dict[str, Any]] = []

    try:
        for media_id in config.media_ids:
            results.append(
                write_media_events(
                    session=session,
                    s3_client=s3_client,
                    config=config,
                    media_id=media_id,
                    start_date=start_date,
                    end_date=end_date,
                    extraction_time=extraction_time,
                    run_id=run_id,
                )
            )

        manifest_uri = write_manifest(
            s3_client=s3_client,
            config=config,
            extraction_time=extraction_time,
            run_id=run_id,
            start_date=start_date,
            end_date=end_date,
            results=results,
        )
        publish_ingestion_workflow_properties(
            glue_client=glue_client,
            config=config,
            manifest_uri=manifest_uri,
            run_id=run_id,
        )
    finally:
        session.close()

    LOGGER.info(
        "Completed Wistia event ingestion total_events=%s manifest_uri=%s run_id=%s",
        sum(result["event_count"] for result in results),
        manifest_uri,
        run_id,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOGGER.exception("Wistia event ingestion failed.")
        raise
