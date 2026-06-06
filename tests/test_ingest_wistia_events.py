import importlib.util
import json
import sys
import types
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock


def load_module():
    awsglue = types.ModuleType("awsglue")
    awsglue_utils = types.ModuleType("awsglue.utils")
    awsglue_utils.getResolvedOptions = Mock()
    sys.modules.setdefault("awsglue", awsglue)
    sys.modules.setdefault("awsglue.utils", awsglue_utils)

    path = Path(__file__).parents[1] / "glue_jobs" / "ingest_wistia_events.py"
    spec = importlib.util.spec_from_file_location("ingest_wistia_events", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ingestion = load_module()


class DateWindowTests(unittest.TestCase):
    def test_defaults_to_latest_completed_utc_day(self):
        self.assertEqual(
            (date(2026, 6, 5), date(2026, 6, 5)),
            ingestion.resolve_date_window(date(2026, 6, 6), None, None),
        )

    def test_accepts_manual_date_range(self):
        self.assertEqual(
            (date(2026, 5, 1), date(2026, 5, 31)),
            ingestion.resolve_date_window(
                date(2026, 6, 6),
                "2026-05-01",
                "2026-05-31",
            ),
        )

    def test_requires_both_manual_dates(self):
        with self.assertRaisesRegex(
            ingestion.WistiaIngestionError,
            "must either both be supplied",
        ):
            ingestion.resolve_date_window(date(2026, 6, 6), "2026-05-01", None)

    def test_rejects_invalid_date(self):
        with self.assertRaisesRegex(
            ingestion.WistiaIngestionError,
            "YYYY-MM-DD",
        ):
            ingestion.resolve_date_window(
                date(2026, 6, 6),
                "2026-02-30",
                "2026-03-01",
            )

    def test_rejects_reversed_range(self):
        with self.assertRaisesRegex(
            ingestion.WistiaIngestionError,
            "cannot be later",
        ):
            ingestion.resolve_date_window(
                date(2026, 6, 6),
                "2026-05-31",
                "2026-05-01",
            )


class WorkflowPublicationTests(unittest.TestCase):
    def config(self, workflow_name=None, workflow_run_id=None):
        return ingestion.JobConfig(
            job_name="ingestion",
            secret_id="secret",
            secret_region=None,
            s3_bucket="bucket",
            s3_prefix="ingestion/wistia/events",
            manifest_prefix="metadata/wistia/events/manifests",
            media_ids=("media-1", "media-2"),
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
        )

    def test_publishes_exact_manifest_and_run_id(self):
        glue_client = Mock()
        ingestion.publish_ingestion_workflow_properties(
            glue_client,
            self.config("workflow", "workflow-run"),
            "s3://bucket/manifest.json",
            "ingestion-run",
            date(2026, 6, 5),
            date(2026, 6, 5),
        )
        glue_client.put_workflow_run_properties.assert_called_once_with(
            Name="workflow",
            RunId="workflow-run",
            RunProperties={
                "INGESTION_MANIFEST_URI": "s3://bucket/manifest.json",
                "INGESTION_RUN_ID": "ingestion-run",
                "INGESTION_START_DATE": "2026-06-05",
                "INGESTION_END_DATE": "2026-06-05",
            },
        )

    def test_manual_run_skips_publication(self):
        glue_client = Mock()
        ingestion.publish_ingestion_workflow_properties(
            glue_client,
            self.config(),
            "s3://bucket/manifest.json",
            "ingestion-run",
            date(2026, 6, 5),
            date(2026, 6, 5),
        )
        glue_client.put_workflow_run_properties.assert_not_called()

    def test_partial_workflow_context_fails(self):
        with self.assertRaises(ingestion.WistiaIngestionError):
            ingestion.publish_ingestion_workflow_properties(
                Mock(),
                self.config(workflow_name="workflow"),
                "s3://bucket/manifest.json",
                "ingestion-run",
                date(2026, 6, 5),
                date(2026, 6, 5),
            )


class ManifestTests(unittest.TestCase):
    def test_manifest_uses_metadata_prefix(self):
        config = ingestion.JobConfig(
            job_name="ingestion",
            secret_id="secret",
            secret_region=None,
            s3_bucket="bucket",
            s3_prefix="ingestion/wistia/events",
            manifest_prefix="metadata/wistia/events/manifests",
            media_ids=("media-1",),
            workflow_name=None,
            workflow_run_id=None,
        )
        s3_client = Mock()
        manifest_uri = ingestion.write_manifest(
            s3_client=s3_client,
            config=config,
            extraction_time=datetime(2026, 6, 5, 17, 30, tzinfo=timezone.utc),
            run_id="run-123",
            start_date=date(2024, 6, 5),
            end_date=date(2026, 6, 5),
            results=[
                {
                    "event_count": 1,
                    "media_id": "media-1",
                    "page_count": 1,
                    "s3_uri": "s3://bucket/ingestion/events.jsonl.gz",
                    "status": "succeeded",
                }
            ],
        )

        self.assertEqual(
            "s3://bucket/metadata/wistia/events/manifests/"
            "extraction_date=2026-06-05/"
            "manifest_20260605T173000Z_run-123.json",
            manifest_uri,
        )
        put_call = s3_client.put_object.call_args.kwargs
        self.assertEqual("bucket", put_call["Bucket"])
        self.assertEqual(
            "metadata/wistia/events/manifests/extraction_date=2026-06-05/"
            "manifest_20260605T173000Z_run-123.json",
            put_call["Key"],
        )
        self.assertEqual("run-123", json.loads(put_call["Body"])["run_id"])


if __name__ == "__main__":
    unittest.main()
