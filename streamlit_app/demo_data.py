"""Synthetic dashboard data used only when local demo mode is enabled."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd


def load_demo_data() -> tuple[pd.DataFrame, dict[str, object]]:
    rows: list[dict[str, object]] = []
    media = [
        (
            "gskhw4w4lm",
            "Chris Face VSL The Gap Method Youtube Paid Ads",
            "Youtube",
        ),
        (
            "v08dlrgr7v",
            "Chris Face VSL The Gap Method Facebook Paid Ads",
            "Facebook",
        ),
    ]
    countries = ["US", "CA", "GB", "AU", "DE", "NL"]

    for index in range(1, 121):
        visitor_id = f"visitor-{index:03d}"
        country = countries[index % len(countries)]
        for media_index, (media_id, title, channel) in enumerate(media):
            if (index + media_index) % 4 == 0:
                continue
            total_views = 1 + ((index * (media_index + 2)) % 8)
            avg_pct = min(0.98, 0.18 + ((index * 7 + media_index * 13) % 77) / 100)
            max_pct = min(1.0, avg_pct + 0.08 + (index % 14) / 100)
            first_date = pd.Timestamp("2025-01-01") + pd.Timedelta(
                days=(index * 5 + media_index * 17) % 410
            )
            last_date = first_date + pd.Timedelta(days=(index * 3) % 90)
            rows.append(
                {
                    "visitor_id": visitor_id,
                    "media_id": media_id,
                    "title": title,
                    "channel": channel,
                    "country": country,
                    "ip_address": f"192.0.2.{(index % 250) + 1}",
                    "total_views": total_views,
                    "avg_pct_viewed": avg_pct,
                    "max_pct_viewed": max_pct,
                    "first_date_watched": first_date,
                    "last_date_watched": last_date,
                }
            )

    frame = pd.DataFrame(rows)
    metadata = {
        "curated_version": "demo",
        "loaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_through_date": frame["last_date_watched"].max().date(),
        "pipeline_refreshed_at": datetime.now(timezone.utc),
        "ingestion_run_id": "demo",
        "media_labels": {
            media_id: title for media_id, title, _channel in media
        },
    }
    return frame, metadata
