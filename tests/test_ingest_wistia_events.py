import importlib.util
import sys
import types
import unittest
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


class WorkflowPublicationTests(unittest.TestCase):
    def config(self, workflow_name=None, workflow_run_id=None):
        return ingestion.JobConfig(
            job_name="ingestion",
            secret_id="secret",
            secret_region=None,
            s3_bucket="bucket",
            s3_prefix="ingestion/wistia/events",
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
        )
        glue_client.put_workflow_run_properties.assert_called_once_with(
            Name="workflow",
            RunId="workflow-run",
            RunProperties={
                "INGESTION_MANIFEST_URI": "s3://bucket/manifest.json",
                "INGESTION_RUN_ID": "ingestion-run",
            },
        )

    def test_manual_run_skips_publication(self):
        glue_client = Mock()
        ingestion.publish_ingestion_workflow_properties(
            glue_client,
            self.config(),
            "s3://bucket/manifest.json",
            "ingestion-run",
        )
        glue_client.put_workflow_run_properties.assert_not_called()

    def test_partial_workflow_context_fails(self):
        with self.assertRaises(ingestion.WistiaIngestionError):
            ingestion.publish_ingestion_workflow_properties(
                Mock(),
                self.config(workflow_name="workflow"),
                "s3://bucket/manifest.json",
                "ingestion-run",
            )


if __name__ == "__main__":
    unittest.main()
