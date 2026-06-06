import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock

import pandas as pd


def load_module():
    fake_streamlit = types.ModuleType("streamlit")
    fake_streamlit.cache_data = lambda **kwargs: lambda function: function
    sys.modules.setdefault("streamlit", fake_streamlit)

    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.DeltaTable = Mock()
    sys.modules.setdefault("deltalake", fake_deltalake)

    path = Path(__file__).parents[1] / "streamlit_app" / "data_access.py"
    spec = importlib.util.spec_from_file_location("streamlit_data_access", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


data_access = load_module()


class ConfigTests(unittest.TestCase):
    def test_config_reads_required_and_optional_tables(self):
        config = data_access.DashboardConfig.from_secrets(
            {
                "aws": {
                    "region": "us-east-1",
                    "access_key_id": "key",
                    "secret_access_key": "secret",
                },
                "tables": {
                    "visitor_engagement_uri": "s3://lake/curated/visitor_engagement/",
                    "dim_media_uri": "s3://lake/refined/dim_media",
                },
            }
        )
        self.assertEqual(
            "s3://lake/curated/visitor_engagement", config.curated_table_uri
        )
        self.assertIsNone(config.visitors_table_uri)
        self.assertNotIn("AWS_SESSION_TOKEN", config.storage_options())

    def test_non_s3_table_uri_fails(self):
        with self.assertRaises(ValueError):
            data_access.DashboardConfig.from_secrets(
                {
                    "aws": {
                        "region": "us-east-1",
                        "access_key_id": "key",
                        "secret_access_key": "secret",
                    },
                    "tables": {"visitor_engagement_uri": "/local/table"},
                }
            )


class ValidationTests(unittest.TestCase):
    def test_pipeline_metadata_reads_curated_audit_columns(self):
        frame = pd.DataFrame(
            {
                "data_through_date": ["2026-06-05"],
                "pipeline_refreshed_at": ["2026-06-06T08:15:00Z"],
                "ingestion_run_id": ["run-123"],
            }
        )

        metadata = data_access.pipeline_metadata(frame)

        self.assertEqual(pd.Timestamp("2026-06-05").date(), metadata["data_through_date"])
        self.assertEqual("run-123", metadata["ingestion_run_id"])
        self.assertEqual(
            pd.Timestamp("2026-06-06T08:15:00Z").to_pydatetime(),
            metadata["pipeline_refreshed_at"],
        )

    def test_missing_curated_column_fails(self):
        frame = Mock()
        frame.columns = ["visitor_id"]
        with self.assertRaises(ValueError):
            data_access.validate_columns(
                frame,
                {"visitor_id", "media_id"},
                "visitor_engagement",
            )

    def test_enrichment_joins_dimensions_and_normalizes_dates(self):
        engagement = pd.DataFrame(
            {
                "visitor_id": ["visitor-1"],
                "media_id": ["media-1"],
                "total_views": [3],
                "avg_pct_viewed": [0.5],
                "max_pct_viewed": [0.9],
                "first_date_watched": ["2026-01-01"],
                "last_date_watched": ["2026-01-05"],
            }
        )
        media = pd.DataFrame(
            {
                "media_id": ["media-1"],
                "title": ["Youtube Paid Ads"],
                "url": ["https://example.com"],
                "channel": ["Youtube"],
            }
        )
        visitors = pd.DataFrame(
            {
                "visitor_id": ["visitor-1"],
                "ip_address": ["192.0.2.1"],
                "country": ["US"],
            }
        )

        result = data_access.enrich_dashboard_data(
            engagement,
            media,
            visitors,
        )

        self.assertEqual("Youtube Paid Ads", result.loc[0, "title"])
        self.assertEqual("US", result.loc[0, "country"])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["last_date_watched"]))

    def test_dimensions_are_optional(self):
        engagement = pd.DataFrame(
            {
                "visitor_id": ["visitor-1"],
                "media_id": ["media-1"],
                "total_views": [1],
                "avg_pct_viewed": [0.25],
                "max_pct_viewed": [0.25],
                "first_date_watched": ["2026-01-01"],
                "last_date_watched": ["2026-01-01"],
            }
        )

        result = data_access.enrich_dashboard_data(engagement, None, None)

        self.assertEqual("media-1", result.loc[0, "title"])
        self.assertEqual("Unknown", result.loc[0, "country"])


if __name__ == "__main__":
    unittest.main()
