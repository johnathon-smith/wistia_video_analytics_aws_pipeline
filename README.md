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

### Ingestion date parameters

Scheduled ingestion runs default to the latest fully completed UTC day. For
example, a run on June 6 processes June 5:

```text
start_date=2026-06-05
end_date=2026-06-05
```

No date parameters are needed on the Glue workflow job for this daily behavior.
For a manual backfill or rerun, supply both optional parameters:

```text
--START_DATE 2026-05-01
--END_DATE 2026-05-31
```

Dates use `YYYY-MM-DD`. Both parameters must be supplied together, and
`START_DATE` cannot be later than `END_DATE`. The selected dates are recorded in
the S3 object metadata and ingestion manifest.

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

## Refined dim_media

`build_dim_media.py` reads the validated raw object for one ingestion run and
upserts a Delta table at:

```text
s3://<data-lake-bucket>/refined/dim_media
```

The table contains `media_id`, `title`, `url`, and `channel`. Channel is derived
case-insensitively from `Youtube` or `Facebook` in the Wistia media title.

Workflow runs read `INGESTION_RUN_ID` and `VALIDATION_REPORT_URI` from workflow
properties. For a manual run, supply both:

```text
--INGESTION_RUN_ID <ingestion-run-id>
--VALIDATION_REPORT_URI s3://<bucket>/validation_reports/wistia/events/.../report.json
```

Configure this as a Spark Glue job with Delta enabled:

```text
--datalake-formats delta
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

The job does not register the Delta table in the Glue Data Catalog.

The execution role needs `s3:GetObject` for the validation report and raw object,
plus `s3:ListBucket` and `s3:PutObject` for the Delta table location. Include
`s3:DeleteObject` if later maintenance or vacuum operations will remove Delta files.

## Refined dim_visitors

`build_dim_visitors.py` follows the same workflow/manual input contract and Delta
configuration as `build_dim_media.py`. It upserts on `visitor_id` at:

```text
s3://<data-lake-bucket>/refined/dim_visitors
```

The table contains:

```text
visitor_id   <- visitor_key
ip_address   <- ip
country
```

When a visitor appears multiple times, the event with the latest `received_at`
supplies the current IP address and country. Manual runs require
`--INGESTION_RUN_ID` and `--VALIDATION_REPORT_URI`.

## Refined fact_media_engagement

`build_fact_media_engagement.py` creates an unpartitioned Delta table at:

```text
s3://<data-lake-bucket>/refined/fact_media_engagement
```

The table contains:

```text
event_id          <- event_key
visitor_id        <- visitor_key
media_id
date              <- UTC date from received_at
watched_percent   <- percent_viewed
```

The job keeps one row per `event_id` and performs a Delta upsert on that key.
`watched_percent` remains Wistia's decimal value, such as `0.75`. The table is
currently unpartitioned to avoid tiny partitions at the present data volume.

Workflow and manual parameters, Delta configuration, and IAM requirements match
the other refined jobs. To partition later, rewrite the existing Delta table to a
new location with derived `event_year` and `event_month` columns and validate it
before switching consumers to the new path.

## Curated visitor_engagement

`build_visitor_engagement.py` reads the complete refined
`fact_media_engagement` Delta table and rebuilds an unpartitioned curated Delta
table at:

```text
s3://<data-lake-bucket>/curated/visitor_engagement
```

It produces one row per `visitor_id` and `media_id` with:

```text
visitor_id
media_id
total_views
avg_pct_viewed
max_pct_viewed
first_date_watched
last_date_watched
```

The aggregate is fully recomputed and atomically overwritten each run so
corrections to the refined fact table cannot leave stale groups behind. Workflow
runs consume `FACT_MEDIA_ENGAGEMENT_TABLE_URI` and verify it was produced for the
current ingestion run.

The curated table also stores `data_through_date`, `pipeline_refreshed_at`, and
`ingestion_run_id` on each row. The Streamlit dashboard uses these audit fields
to display the latest successfully processed Wistia date. This is more reliable
than using `last_date_watched`, because a successfully processed day may contain
no viewing activity.

For a manual run, supply:

```text
--INGESTION_RUN_ID <ingestion-run-id>
--FACT_MEDIA_ENGAGEMENT_TABLE_URI s3://<bucket>/refined/fact_media_engagement
--DATA_THROUGH_DATE 2026-06-05
```

`--DATA_THROUGH_DATE` is optional for manual runs but recommended so the
dashboard can report freshness. Workflow runs receive the date automatically
from the ingestion job.

Use the same Delta Spark configuration and IAM permissions as the refined jobs.

## Streamlit dashboard

The dashboard entrypoint is `streamlit_app/app.py`. It reads the curated Delta
table directly from S3 with `delta-rs` and optionally enriches it with `dim_media`
and `dim_visitors`.

For local development:

```text
pip install -r streamlit_app/requirements.txt
streamlit run streamlit_app/app.py
```

Create `streamlit_app/.streamlit/secrets.toml` from
`secrets.toml.example`. Never commit the real secrets file. In Streamlit
Community Cloud, use `streamlit_app/app.py` as the entrypoint and paste the same
TOML into the app's Secrets settings.

The AWS identity only needs read access:

```text
s3:ListBucket
s3:GetObject
```

Scope those permissions to the three Delta table prefixes. The dashboard caches
data for 15 minutes and includes a manual refresh button.

For a local UI preview using synthetic records instead of AWS data:

```text
WISTIA_DASHBOARD_DEMO_MODE=true streamlit run streamlit_app/app.py
```

Do not set that environment variable in the production Streamlit app.
