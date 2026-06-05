import importlib.util
import gzip
import io
import json
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock


def load_module():
    awsglue = types.ModuleType("awsglue")
    awsglue_utils = types.ModuleType("awsglue.utils")
    awsglue_utils.getResolvedOptions = Mock()
    sys.modules.setdefault("awsglue", awsglue)
    sys.modules.setdefault("awsglue.utils", awsglue_utils)

    path = Path(__file__).parents[1] / "glue_jobs" / "validate_wistia_events.py"
    spec = importlib.util.spec_from_file_location("validate_wistia_events", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


validator = load_module()


def valid_event():
    return {
        "event_key": "event-1",
        "received_at": "2026-06-05T12:00:00Z",
        "visitor_key": "visitor-1",
        "media_id": "media-1",
        "media_name": "Demo",
        "media_url": "https://example.wistia.com/medias/media-1",
        "percent_viewed": 0.75,
        "ip": "192.0.2.1",
        "country": "US",
    }


class ValidateEventTests(unittest.TestCase):
    def test_valid_event(self):
        issues, unknown = validator.validate_event(valid_event())
        self.assertEqual([], issues)
        self.assertEqual(set(), unknown)

    def test_missing_required_field(self):
        event = valid_event()
        del event["event_key"]
        issues, _ = validator.validate_event(event)
        self.assertIn(
            ("missing_required_field", "event_key"),
            {(issue.code, issue.field) for issue in issues},
        )

    def test_null_required_field(self):
        event = valid_event()
        event["country"] = None
        issues, _ = validator.validate_event(event)
        self.assertIn(
            ("null_required_field", "country"),
            {(issue.code, issue.field) for issue in issues},
        )

    def test_wrong_optional_type(self):
        event = valid_event()
        event["lat"] = "not-a-number"
        issues, _ = validator.validate_event(event)
        self.assertIn(
            ("invalid_type", "lat"),
            {(issue.code, issue.field) for issue in issues},
        )

    def test_invalid_timestamp_and_percentage(self):
        event = valid_event()
        event["received_at"] = "not-a-timestamp"
        event["percent_viewed"] = 1.5
        issues, _ = validator.validate_event(event)
        issue_pairs = {(issue.code, issue.field) for issue in issues}
        self.assertIn(("invalid_type", "received_at"), issue_pairs)
        self.assertIn(("out_of_range", "percent_viewed"), issue_pairs)

    def test_unknown_fields_are_additive_drift(self):
        event = valid_event()
        event["new_field"] = "new"
        event["thumbnail"] = {"url": "https://example.com/image.jpg", "new_size": 42}
        issues, unknown = validator.validate_event(event)
        self.assertEqual([], issues)
        self.assertEqual({"new_field", "thumbnail.new_size"}, unknown)


class ManifestResolutionTests(unittest.TestCase):
    def config(self, input_uri=None, workflow_name=None, workflow_run_id=None):
        return validator.JobConfig(
            job_name="validator",
            input_manifest_uri=input_uri,
            raw_prefix="raw/wistia/events",
            quarantine_prefix="quarantine/wistia/events",
            report_prefix="validation_reports/wistia/events",
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
        )

    def test_explicit_manifest_overrides_workflow(self):
        glue_client = Mock()
        uri = validator.resolve_manifest_uri(
            glue_client,
            self.config(
                input_uri="s3://bucket/manual.json",
                workflow_name="workflow",
                workflow_run_id="run",
            ),
        )
        self.assertEqual("s3://bucket/manual.json", uri)
        glue_client.get_workflow_run_properties.assert_not_called()

    def test_manifest_resolves_from_workflow(self):
        glue_client = Mock()
        glue_client.get_workflow_run_properties.return_value = {
            "RunProperties": {
                "INGESTION_MANIFEST_URI": "s3://bucket/workflow.json"
            }
        }
        uri = validator.resolve_manifest_uri(
            glue_client,
            self.config(workflow_name="workflow", workflow_run_id="run"),
        )
        self.assertEqual("s3://bucket/workflow.json", uri)

    def test_missing_manifest_fails(self):
        with self.assertRaises(validator.WistiaValidationError):
            validator.resolve_manifest_uri(Mock(), self.config())


class StreamingValidationTests(unittest.TestCase):
    class FakeS3Client:
        def __init__(self, objects):
            self.objects = dict(objects)

        def get_object(self, Bucket, Key):
            return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

        def upload_file(self, local_path, bucket, key, ExtraArgs=None):
            self.objects[(bucket, key)] = Path(local_path).read_bytes()

        def put_object(self, Bucket, Key, Body, ContentType):
            self.objects[(Bucket, Key)] = Body

    def test_process_manifest_routes_and_reports_records(self):
        manifest_uri = "s3://lake/ingestion/manifest.json"
        source_uri = "s3://lake/ingestion/events.jsonl.gz"
        valid = valid_event()
        additive = valid_event()
        additive["event_key"] = "event-2"
        additive["new_field"] = "new"
        invalid = valid_event()
        invalid["event_key"] = "event-3"
        invalid["country"] = None
        source_lines = [
            json.dumps(valid, separators=(",", ":")),
            json.dumps(additive, separators=(",", ":")),
            json.dumps(invalid, separators=(",", ":")),
            "{malformed",
        ]
        manifest = {
            "extracted_at": "2026-06-05T12:00:00Z",
            "results": [{"s3_uri": source_uri}],
            "run_id": "run-123",
            "total_event_count": 4,
        }
        s3_client = self.FakeS3Client(
            {
                ("lake", "ingestion/manifest.json"): json.dumps(manifest).encode(),
                ("lake", "ingestion/events.jsonl.gz"): gzip.compress(
                    ("\n".join(source_lines) + "\n").encode(), mtime=0
                ),
            }
        )
        config = validator.JobConfig(
            job_name="validator",
            input_manifest_uri=manifest_uri,
            raw_prefix="raw/wistia/events",
            quarantine_prefix="quarantine/wistia/events",
            report_prefix="validation_reports/wistia/events",
            workflow_name=None,
            workflow_run_id=None,
        )

        report = validator.process_manifest(
            s3_client,
            config,
            manifest_uri,
            datetime(2026, 6, 5, 13, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(4, report["total_record_count"])
        self.assertEqual(2, report["valid_record_count"])
        self.assertEqual(2, report["quarantined_record_count"])
        self.assertEqual(1, report["malformed_record_count"])
        self.assertEqual({"new_field": 1}, report["unknown_field_counts"])

        raw_bucket, raw_key = validator.parse_s3_uri(report["raw_s3_uri"])
        raw_lines = gzip.decompress(s3_client.objects[(raw_bucket, raw_key)]).decode().splitlines()
        self.assertEqual(source_lines[:2], raw_lines)

        quarantine_bucket, quarantine_key = validator.parse_s3_uri(
            report["quarantine_s3_uri"]
        )
        quarantine_lines = gzip.decompress(
            s3_client.objects[(quarantine_bucket, quarantine_key)]
        ).decode().splitlines()
        self.assertEqual(2, len(quarantine_lines))


if __name__ == "__main__":
    unittest.main()
