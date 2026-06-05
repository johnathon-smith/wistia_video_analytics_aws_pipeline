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
