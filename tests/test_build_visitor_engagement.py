import importlib.util
import sys
import types
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock


def load_module():
    awsglue = types.ModuleType("awsglue")
    awsglue_utils = types.ModuleType("awsglue.utils")
    awsglue_utils.getResolvedOptions = Mock()
    sys.modules.setdefault("awsglue", awsglue)
    sys.modules.setdefault("awsglue.utils", awsglue_utils)

    path = (
        Path(__file__).parents[1]
        / "glue_jobs"
        / "build_visitor_engagement.py"
    )
    spec = importlib.util.spec_from_file_location("build_visitor_engagement", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


visitor_engagement = load_module()


class InputResolutionTests(unittest.TestCase):
    def config(
        self,
        ingestion_run_id=None,
        fact_table_uri=None,
        workflow_name=None,
        workflow_run_id=None,
        data_through_date=None,
    ):
        return visitor_engagement.JobConfig(
            job_name="build-visitor-engagement",
            ingestion_run_id=ingestion_run_id,
            fact_media_engagement_table_uri=fact_table_uri,
            curated_prefix="curated/visitor_engagement",
            visitor_engagement_table_uri=None,
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            data_through_date=data_through_date,
        )

    def test_manual_parameters_override_workflow(self):
        glue_client = Mock()
        run_input = visitor_engagement.resolve_run_input(
            glue_client,
            self.config(
                ingestion_run_id="manual-run",
                fact_table_uri="s3://lake/refined/fact_media_engagement",
                workflow_name="workflow",
                workflow_run_id="workflow-run",
            ),
        )
        self.assertEqual("manual-run", run_input.ingestion_run_id)
        self.assertEqual(
            "s3://lake/refined/fact_media_engagement",
            run_input.fact_media_engagement_table_uri,
        )
        glue_client.get_workflow_run_properties.assert_not_called()

    def test_workflow_properties_are_used_and_run_ids_match(self):
        glue_client = Mock()
        glue_client.get_workflow_run_properties.return_value = {
            "RunProperties": {
                "INGESTION_RUN_ID": "ingestion-run",
                "FACT_MEDIA_ENGAGEMENT_TABLE_URI": (
                    "s3://lake/refined/fact_media_engagement"
                ),
                "FACT_MEDIA_ENGAGEMENT_INGESTION_RUN_ID": "ingestion-run",
                "INGESTION_END_DATE": "2026-06-05",
            }
        }
        run_input = visitor_engagement.resolve_run_input(
            glue_client,
            self.config(workflow_name="workflow", workflow_run_id="workflow-run"),
        )
        self.assertEqual("ingestion-run", run_input.ingestion_run_id)
        self.assertEqual(date(2026, 6, 5), run_input.data_through_date)

    def test_manual_data_through_date_is_parsed(self):
        run_input = visitor_engagement.resolve_run_input(
            Mock(),
            self.config(
                ingestion_run_id="manual-run",
                fact_table_uri="s3://lake/refined/fact_media_engagement",
                data_through_date="2026-06-05",
            ),
        )
        self.assertEqual(date(2026, 6, 5), run_input.data_through_date)

    def test_workflow_fact_run_id_mismatch_fails(self):
        glue_client = Mock()
        glue_client.get_workflow_run_properties.return_value = {
            "RunProperties": {
                "INGESTION_RUN_ID": "ingestion-run",
                "FACT_MEDIA_ENGAGEMENT_TABLE_URI": (
                    "s3://lake/refined/fact_media_engagement"
                ),
                "FACT_MEDIA_ENGAGEMENT_INGESTION_RUN_ID": "different-run",
            }
        }
        with self.assertRaises(visitor_engagement.VisitorEngagementError):
            visitor_engagement.resolve_run_input(
                glue_client,
                self.config(
                    workflow_name="workflow",
                    workflow_run_id="workflow-run",
                ),
            )

    def test_partial_manual_input_fails(self):
        with self.assertRaises(visitor_engagement.VisitorEngagementError):
            visitor_engagement.resolve_run_input(
                Mock(),
                self.config(ingestion_run_id="ingestion-run"),
            )


class TableResolutionTests(unittest.TestCase):
    def test_table_uri_is_in_fact_bucket(self):
        config = InputResolutionTests().config()
        run_input = visitor_engagement.RunInput(
            ingestion_run_id="ingestion-run",
            fact_media_engagement_table_uri=(
                "s3://lake/refined/fact_media_engagement"
            ),
        )
        self.assertEqual(
            "s3://lake/curated/visitor_engagement",
            visitor_engagement.resolve_table_uri(run_input, config),
        )


class WorkflowPublicationTests(unittest.TestCase):
    def test_publishes_table_details(self):
        config = InputResolutionTests().config(
            workflow_name="workflow",
            workflow_run_id="workflow-run",
        )
        run_input = visitor_engagement.RunInput(
            ingestion_run_id="ingestion-run",
            fact_media_engagement_table_uri=(
                "s3://lake/refined/fact_media_engagement"
            ),
        )
        glue_client = Mock()
        visitor_engagement.publish_workflow_properties(
            glue_client,
            config,
            run_input,
            "s3://lake/curated/visitor_engagement",
            250,
        )
        glue_client.put_workflow_run_properties.assert_called_once_with(
            Name="workflow",
            RunId="workflow-run",
            RunProperties={
                "VISITOR_ENGAGEMENT_TABLE_URI": (
                    "s3://lake/curated/visitor_engagement"
                ),
                "VISITOR_ENGAGEMENT_ROW_COUNT": "250",
                "VISITOR_ENGAGEMENT_INGESTION_RUN_ID": "ingestion-run",
            },
        )


if __name__ == "__main__":
    unittest.main()
