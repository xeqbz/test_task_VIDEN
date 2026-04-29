# Part 1 - Mini Marketing Pipeline

This project implements a small Python ETL pipeline for a mocked Facebook Ads
Insights API response.

The pipeline:

1. Reads the local `fb_ads_mock.json` file.
2. Extracts records from the top-level `data` array.
3. Normalizes nested Facebook Ads metric arrays into flat columns.
4. Produces one output row per `(ad_id, date_start, age, gender)`.
5. Writes the normalized result to JSONL.
6. Includes a BigQuery schema and an optional BigQuery loader stub.

## Project Structure

```text
part1/
  config/
    settings.yaml
  input/
    fb_ads_mock.json
  output/
    fb_ads_normalized.jsonl
  src/
    config.py
    extract.py
    load.py
    logging_config.py
    main.py
    schemas.py
    transform.py
  requirements.txt
  README.md
```

## How To Run

From the repository root:

```bash
cd part1
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.main
```

The pipeline writes the normalized file to:

```text
part1/output/fb_ads_normalized.jsonl
```

The input and output paths are configured in:

```text
part1/config/settings.yaml
```

## Output

The output is newline-delimited JSON. Each row contains:

- campaign, ad set, and ad identifiers;
- reporting grain fields: `date_start`, `date_stop`, `age`, `gender`;
- flat metrics such as `impressions`, `clicks`, `reach`, `spend`, `cpm`,
  `cpc`, `ctr`, and `cpp`;
- selected funnel events from `actions`, such as `view_content`,
  `add_to_cart`, `initiate_checkout`, `add_payment_info`, and `purchases`;
- selected values from `action_values`, including `purchase_revenue`;
- selected cost-per-action fields;
- video metrics when present;
- a derived `roas` field.

The BigQuery-compatible output schema is defined in:

```text
part1/src/schemas.py
```

## Transformation Logic

Facebook Ads returns nested arrays such as `actions`, `action_values`,
`cost_per_action_type`, `website_ctr`, `video_play_actions`, and
`video_avg_time_watched_actions`.

Each array is converted into configured flat output columns using mappings in
`config/settings.yaml`. For example:

- `actions.action_type = purchase` becomes `purchases`;
- `action_values.action_type = purchase` becomes `purchase_revenue`;
- `cost_per_action_type.action_type = purchase` becomes `cost_per_purchase`.

The source contains duplicate event namespaces, for example `purchase`,
`omni_purchase`, and `offsite_conversion.fb_pixel_purchase`. This pipeline uses
`purchase` as the canonical purchase event and does not sum duplicate
namespaces.

`roas` is calculated as:

```text
purchase_revenue / spend
```

If spend is missing, null, or less than or equal to zero, `roas` is set to
`null`.

## BigQuery Loading

No real BigQuery access is required for this assignment.

`src/load.py` contains:

- `write_jsonl`, which writes the local assignment output;
- `load_to_bigquery_stub`, which shows how the normalized rows could be loaded
  into BigQuery using the schema from `src/schemas.py`.

The BigQuery loader is not executed by default because it requires real GCP
credentials, a target project, and the `google-cloud-bigquery` package.

## Assumptions

- The input JSON contains a top-level `data` array.
- The source data already has the required reporting grain:
  `(ad_id, date_start, age, gender)`.
- Numeric source metrics may arrive as strings.
- `purchase` is the canonical purchase event.
- Duplicate namespaces such as `omni_purchase` and
  `offsite_conversion.fb_pixel_purchase` are not summed.
- Missing arrays or fields should not stop the pipeline.
- Local JSONL output is sufficient for this assignment.
- Missing action counts and action values default to `0`.
- Missing cost-per-action and rate-like nested metrics default to `null`.
- Missing flat numeric metrics default to `0`, which keeps the assignment output
  simple but should be reviewed for production reporting semantics.

## Tradeoffs

- The pipeline reads the whole JSON file into memory. This is acceptable for a
  small mock file, but production API responses should be processed
  incrementally or staged in cloud storage.
- The action mapping is explicit and configuration-driven, but still static. In
  production, unknown action types should be monitored and reviewed.
- The BigQuery loader is included as optional code, but it is not executed by
  default to avoid requiring real credentials.
- The pipeline trusts that the source grain is already unique. In production, I
  would add validation or aggregation for duplicate `(ad_id, date_start, age,
  gender)` rows.

## Production Considerations

For production usage, I would add:

- API extraction with pagination and retries;
- raw response storage in GCS or a raw BigQuery table;
- a partitioned BigQuery target table, likely by `date_start`;
- monitoring and alerting;
- schema change detection;
- data quality checks;
- orchestration with Cloud Composer, Cloud Run jobs, Workflows, or another
  scheduler.
