# Wistia Video Analytics AWS Pipeline

## Glue Workflow

Create one AWS Glue Workflow with:

1. A scheduled trigger that starts the Wistia ingestion job daily.
2. A conditional trigger that starts the validation job only when the ingestion job
   finishes with `SUCCEEDED`.

AWS Glue supplies `--WORKFLOW_NAME` and `--WORKFLOW_RUN_ID` to jobs launched by the
workflow. The ingestion job publishes `INGESTION_MANIFEST_URI` for its run. The
validation job reads that property, so it processes the exact manifest produced by
the preceding ingestion job.

The ingestion job stores manifests separately from temporary ingestion data. Its
default manifest prefix is:

```text
metadata/wistia/events/manifests
```

Override it with the ingestion job parameter `--MANIFEST_PREFIX`.

For a manual validation run outside the workflow, provide:

```text
--INPUT_MANIFEST_URI s3://bucket/path/to/manifest.json
```

### Validation job parameters

The validation job requires only the Glue-provided `--JOB_NAME`. These optional
parameters override its default output prefixes:

```text
--RAW_PREFIX raw/wistia/events
--QUARANTINE_PREFIX quarantine/wistia/events
--REPORT_PREFIX validation_reports/wistia/events
```

The validation job creates a quarantine S3 object only when one or more records
fail validation. For a clean run, the report contains a quarantine count of `0`
and `quarantine_s3_uri` is `null`, allowing S3 object-created notifications under
the quarantine prefix to represent actual data-quality problems.

### IAM permissions

The shared Glue execution role needs access to the data-lake objects and these Glue
workflow actions:

```text
glue:GetWorkflowRunProperties
glue:PutWorkflowRunProperties
s3:GetObject
s3:PutObject
```

The ingestion job also needs `secretsmanager:GetSecretValue` and any applicable
KMS permissions.
