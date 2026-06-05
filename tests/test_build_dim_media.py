import importlib.util
import json
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

    path = Path(__file__).parents[1] / "glue_jobs" / "build_dim_media.py"
    spec = importlib.util.spec_from_file_location("build_dim_media", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


dim_media = load_module()


class ChannelTests(unittest.TestCase):
    def test_youtube_title(self):
        self.assertEqual(
            "Youtube",
            dim_media.channel_from_title(
                "Chris Face VSL The Gap Method Youtube  Paid Ads"
            ),
        )

    def test_facebook_title(self):
        self.assertEqual(
            "Facebook",
            dim_media.channel_from_title(
                "Chris Face VSL The Gap Method Facebook Paid Ads"
            ),
        )

    def test_channel_matching_is_case_insensitive(self):
        self.assertEqual("Youtube", dim_media.channel_from_title("YOUTUBE campaign"))

    def test_missing_or_ambiguous_channel_returns_none(self):
        self.assertIsNone(dim_media.channel_from_title("Unknown campaign"))
        self.assertIsNone(dim_media.channel_from_title("Youtube Facebook campaign"))


class InputResolutionTests(unittest.TestCase):
    def config(
        self,
        ingestion_run_id=None,
        validation_report_uri=None,
        workflow_name=None,
        workflow_run_id=None,
    ):
        return dim_media.JobConfig(
            job_name="build-dim-media",
            ingestion_run_id=ingestion_run_id,
            validation_report_uri=validation_report_uri,
            refined_prefix="refined/dim_media",
            dim_media_table_uri=None,
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
        )

    def test_manual_parameters_override_workflow(self):
        glue_client = Mock()
        run_input = dim_media.resolve_run_input(
            glue_client,
            self.config(
                ingestion_run_id="manual-run",
                validation_report_uri="s3://lake/manual-report.json",
                workflow_name="workflow",
                workflow_run_id="workflow-run",
            ),
        )
        self.assertEqual("manual-run", run_input.ingestion_run_id)
        self.assertEqual(
            "s3://lake/manual-report.json", run_input.validation_report_uri
        )
        glue_client.get_workflow_run_properties.assert_not_called()

    def test_workflow_properties_are_used(self):
        glue_client = Mock()
        glue_client.get_workflow_run_properties.return_value = {
            "RunProperties": {
                "INGESTION_RUN_ID": "ingestion-run",
                "VALIDATION_REPORT_URI": "s3://lake/report.json",
            }
        }
        run_input = dim_media.resolve_run_input(
            glue_client,
            self.config(workflow_name="workflow", workflow_run_id="workflow-run"),
        )
        self.assertEqual("ingestion-run", run_input.ingestion_run_id)
        self.assertEqual("s3://lake/report.json", run_input.validation_report_uri)

    def test_partial_manual_input_fails(self):
        with self.assertRaises(dim_media.DimMediaError):
            dim_media.resolve_run_input(
                Mock(),
                self.config(ingestion_run_id="ingestion-run"),
            )


class ReportTests(unittest.TestCase):
    def test_report_run_id_must_match(self):
        with self.assertRaises(dim_media.DimMediaError):
            dim_media.resolve_raw_input(
                {
                    "ingestion_run_id": "different-run",
                    "raw_s3_uri": "s3://lake/raw/events.jsonl.gz",
                },
                "requested-run",
            )

    def test_table_uri_is_in_raw_bucket(self):
        config = InputResolutionTests().config()
        self.assertEqual(
            "s3://lake/refined/dim_media",
            dim_media.resolve_table_uri(
                "s3://lake/raw/wistia/events/events.jsonl.gz",
                config,
            ),
        )

    def test_valid_record_count_accepts_zero(self):
        self.assertEqual(0, dim_media.valid_record_count({"valid_record_count": 0}))

    def test_invalid_valid_record_count_fails(self):
        with self.assertRaises(dim_media.DimMediaError):
            dim_media.valid_record_count({"valid_record_count": "2"})


class WorkflowPublicationTests(unittest.TestCase):
    def test_publishes_table_details(self):
        config = InputResolutionTests().config(
            workflow_name="workflow",
            workflow_run_id="workflow-run",
        )
        glue_client = Mock()
        dim_media.publish_workflow_properties(
            glue_client,
            config,
            dim_media.RunInput(
                ingestion_run_id="ingestion-run",
                validation_report_uri="s3://lake/report.json",
            ),
            "s3://lake/refined/dim_media",
            2,
        )
        glue_client.put_workflow_run_properties.assert_called_once_with(
            Name="workflow",
            RunId="workflow-run",
            RunProperties={
                "DIM_MEDIA_TABLE_URI": "s3://lake/refined/dim_media",
                "DIM_MEDIA_ROW_COUNT": "2",
                "DIM_MEDIA_INGESTION_RUN_ID": "ingestion-run",
            },
        )


if __name__ == "__main__":
    unittest.main()
