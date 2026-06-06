"""Delta Lake data access and enrichment for the Streamlit dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

import pandas as pd
import streamlit as st
from deltalake import DeltaTable


@dataclass(frozen=True)
class DashboardConfig:
    """Validated AWS credentials and Delta table locations from Streamlit secrets."""

    curated_table_uri: str
    media_table_uri: str | None
    visitors_table_uri: str | None
    aws_region: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str | None = None

    @classmethod
    def from_secrets(cls, secrets: Mapping[str, Any]) -> "DashboardConfig":
        """Build dashboard configuration from Streamlit's TOML secrets mapping."""

        aws = secrets["aws"]
        tables = secrets["tables"]
        curated_uri = str(tables["visitor_engagement_uri"]).rstrip("/")
        if not curated_uri.startswith("s3://"):
            raise ValueError("tables.visitor_engagement_uri must be an s3:// URI.")
        return cls(
            curated_table_uri=curated_uri,
            media_table_uri=_optional_s3_uri(tables.get("dim_media_uri")),
            visitors_table_uri=_optional_s3_uri(tables.get("dim_visitors_uri")),
            aws_region=str(aws["region"]),
            aws_access_key_id=str(aws["access_key_id"]),
            aws_secret_access_key=str(aws["secret_access_key"]),
            aws_session_token=_optional_string(aws.get("session_token")),
        )

    def storage_options(self) -> dict[str, str]:
        """Translate secrets into the option names expected by delta-rs."""

        options = {
            "AWS_REGION": self.aws_region,
            "AWS_ACCESS_KEY_ID": self.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.aws_secret_access_key,
        }
        if self.aws_session_token:
            options["AWS_SESSION_TOKEN"] = self.aws_session_token
        return options


def _optional_string(value: Any) -> str | None:
    """Return a stripped optional value, treating blanks as missing."""

    if value is None or not str(value).strip():
        return None
    return str(value).strip()


def _optional_s3_uri(value: Any) -> str | None:
    """Normalize an optional S3 location and reject non-S3 paths."""

    parsed = _optional_string(value)
    if parsed is None:
        return None
    if not parsed.startswith("s3://"):
        raise ValueError("Optional table locations must use s3:// URIs.")
    return parsed.rstrip("/")


def validate_columns(
    frame: pd.DataFrame,
    required: set[str],
    table_name: str,
) -> None:
    """Fail early when a Delta table is missing columns the dashboard needs."""

    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(
            f"{table_name} is missing required columns: {', '.join(missing)}."
        )


def normalize_engagement(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate the curated model and convert its watch dates to timestamps."""

    required = {
        "visitor_id",
        "media_id",
        "total_views",
        "avg_pct_viewed",
        "max_pct_viewed",
        "first_date_watched",
        "last_date_watched",
    }
    validate_columns(frame, required, "visitor_engagement")
    normalized = frame.copy()
    normalized["first_date_watched"] = pd.to_datetime(
        normalized["first_date_watched"], errors="coerce"
    )
    normalized["last_date_watched"] = pd.to_datetime(
        normalized["last_date_watched"], errors="coerce"
    )
    if normalized[["first_date_watched", "last_date_watched"]].isna().any().any():
        raise ValueError("visitor_engagement contains invalid watch dates.")
    return normalized


def pipeline_metadata(frame: pd.DataFrame) -> dict[str, Any]:
    """Read freshness and lineage values stored on the curated table rows."""

    metadata: dict[str, Any] = {}
    if "data_through_date" in frame.columns:
        values = pd.to_datetime(frame["data_through_date"], errors="coerce").dropna()
        if not values.empty:
            metadata["data_through_date"] = values.max().date()
    if "pipeline_refreshed_at" in frame.columns:
        values = pd.to_datetime(
            frame["pipeline_refreshed_at"], errors="coerce", utc=True
        ).dropna()
        if not values.empty:
            metadata["pipeline_refreshed_at"] = values.max().to_pydatetime()
    if "ingestion_run_id" in frame.columns:
        values = frame["ingestion_run_id"].dropna()
        if not values.empty:
            metadata["ingestion_run_id"] = str(values.iloc[-1])
    return metadata


def enrich_dashboard_data(
    engagement: pd.DataFrame,
    media: pd.DataFrame | None,
    visitors: pd.DataFrame | None,
) -> pd.DataFrame:
    """Join optional dimensions onto the curated engagement aggregates."""

    enriched = normalize_engagement(engagement)

    if media is not None:
        validate_columns(media, {"media_id", "title", "channel"}, "dim_media")
        media_columns = ["media_id", "title", "channel"]
        if "url" in media.columns:
            media_columns.append("url")
        enriched = enriched.merge(
            media[media_columns].drop_duplicates("media_id"),
            on="media_id",
            how="left",
            validate="many_to_one",
        )
    else:
        enriched["title"] = enriched["media_id"]
        enriched["channel"] = "Unknown"

    if visitors is not None:
        validate_columns(
            visitors,
            {"visitor_id", "ip_address", "country"},
            "dim_visitors",
        )
        enriched = enriched.merge(
            visitors[["visitor_id", "ip_address", "country"]].drop_duplicates(
                "visitor_id"
            ),
            on="visitor_id",
            how="left",
            validate="many_to_one",
        )
    else:
        enriched["ip_address"] = "Unknown"
        enriched["country"] = "Unknown"

    for column in ("title", "channel", "country", "ip_address"):
        enriched[column] = enriched[column].fillna("Unknown")
    return enriched


def _read_delta(
    uri: str,
    storage_options: dict[str, str],
) -> tuple[pd.DataFrame, int]:
    """Read one Delta table from S3 and return its rows and Delta version."""

    table = DeltaTable(uri, storage_options=storage_options)
    return table.to_pandas(), table.version()


@st.cache_data(ttl=900, show_spinner="Loading Delta tables from S3...")
def load_dashboard_data(
    config: DashboardConfig,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load, enrich, and cache every dataset needed by the dashboard."""

    options = config.storage_options()
    engagement, curated_version = _read_delta(config.curated_table_uri, options)
    freshness = pipeline_metadata(engagement)

    media = None
    if config.media_table_uri:
        media, _ = _read_delta(config.media_table_uri, options)

    visitors = None
    if config.visitors_table_uri:
        visitors, _ = _read_delta(config.visitors_table_uri, options)

    enriched = enrich_dashboard_data(engagement, media, visitors)
    media_labels = (
        enriched[["media_id", "title"]]
        .drop_duplicates("media_id")
        .set_index("media_id")["title"]
        .to_dict()
    )
    metadata = {
        "curated_version": curated_version,
        "loaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "media_labels": media_labels,
        **freshness,
    }
    return enriched, metadata
