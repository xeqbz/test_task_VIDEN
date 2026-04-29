# Part 2 - GA4 Raw Export Transformation Design

This document proposes a lightweight transformation layer on top of the raw GA4
BigQuery export schema in `schema/raw_ga4_schema.json`.

The design is intentionally concise: it focuses on the table grains, core
transformation logic, assumptions, and tradeoffs rather than trying to document
every possible GA4 field.

## Proposed Model Structure

```text
part2/
  schema/
    raw_ga4_schema.json
  sql/
    01_stg_ga4_events.sql
    02_int_ga4_sessions.sql
    03_mart_ga4_daily_acquisition.sql
    04_fct_ga4_items.sql
```

In a dbt or Dataform project, I would organize the same logic as:

```text
models/
  staging/
    stg_ga4_events.sql
  intermediate/
    int_ga4_sessions.sql
  marts/
    mart_ga4_daily_acquisition.sql
    fct_ga4_items.sql
```

## Layer Overview

| Layer | Grain | Purpose |
| --- | --- | --- |
| `stg_ga4_events` | one row per GA4 event | Type casting, key event parameters, device, geo, traffic source, ecommerce fields |
| `int_ga4_sessions` | one row per reconstructed session | Session start/end, engagement, landing page, source/medium/campaign |
| `mart_ga4_daily_acquisition` | one row per date/source/medium/campaign/device/country | Looker Studio friendly reporting table |
| `fct_ga4_items` | one row per event item | Ecommerce item-level reporting |

## Output Table Structure

### `stg_ga4_events`

This is the main flattened event table. It keeps event-level grain and extracts
only commonly used parameters from `event_params`.

Important fields:

- `event_key`
- `event_date`
- `event_timestamp`
- `event_name`
- `user_id`
- `user_pseudo_id`
- `ga_session_id`
- `session_key`
- `ga_session_number`
- `page_location`
- `page_referrer`
- `page_title`
- `engagement_time_msec`
- `session_engaged`
- `source`
- `medium`
- `campaign`
- `device_category`
- `operating_system`
- `browser`
- `country`
- `city`
- `transaction_id`
- `purchase_revenue_in_usd`
- `item_count`

### `int_ga4_sessions`

This table reconstructs sessions using `user_pseudo_id` and `ga_session_id`.
It is the preferred source for session-based reporting.

Important fields:

- `session_key`
- `user_pseudo_id`
- `ga_session_id`
- `session_date`
- `session_start_timestamp`
- `session_end_timestamp`
- `session_duration_seconds`
- `landing_page`
- `session_source`
- `session_medium`
- `session_campaign`
- `device_category`
- `country`
- `event_count`
- `page_views`
- `is_engaged_session`
- `engagement_time_msec`
- `purchase_count`
- `purchase_revenue_in_usd`

### `mart_ga4_daily_acquisition`

This is a wide, reporting-friendly aggregate for Looker Studio.

Suggested grain:

```text
report_date, source, medium, campaign, device_category, country
```

Suggested metrics:

- `sessions`
- `users`
- `engaged_sessions`
- `engagement_rate`
- `page_views`
- `purchases`
- `purchase_revenue_in_usd`
- `average_order_value`

### `fct_ga4_items`

This table unnests the repeated `items` array for ecommerce analysis.

Suggested grain:

```text
event_key, item_index
```

Suggested fields:

- event identifiers and session fields;
- item identifiers and item categories;
- price, quantity, item revenue, refund;
- transaction id when available.

## Key Design Choices

### Nested and Repeated Fields

GA4 stores many useful values in repeated key-value arrays. The staging layer
extracts a small, explicit set of stable parameters from `event_params`, such
as `ga_session_id`, `page_location`, `page_referrer`, and
`engagement_time_msec`.

I would not dynamically pivot every event parameter into columns. That creates
wide unstable tables and makes schema changes harder to control. Unknown or
new parameters should remain available in the raw export and be added to the
staging model only when they become useful for reporting.

The `items` array is handled separately in `fct_ga4_items`, because item-level
analysis has a different grain from event and session reporting.

### Session Reconstruction

GA4 sessions can be reconstructed with:

```text
session_key = user_pseudo_id || '.' || ga_session_id
```

`ga_session_id` is extracted from `event_params`. Session start and end are
calculated from the minimum and maximum event timestamps in the session.

The landing page is the first non-null `page_location` in the session. Session
source, medium, and campaign use the first non-null event-level traffic values
inside the session.

If `session_traffic_source_last_click` is available, it should be preferred for
session attribution. If it is not populated, the fallback is
`collected_traffic_source`, then `traffic_source`.

### Reporting Grain

The main reporting mart uses daily acquisition grain:

```text
report_date, source, medium, campaign, device_category, country
```

This keeps Looker Studio queries simple and fast for common dashboards while
preserving event-level and session-level tables for drilldown.

### Wide Table vs Multiple Tables

I would not build one single wide table for all use cases. Instead:

- use `stg_ga4_events` for event-level debugging and flexible analysis;
- use `int_ga4_sessions` for session metrics;
- use `mart_ga4_daily_acquisition` for dashboards;
- use `fct_ga4_items` for ecommerce item reporting.

This avoids mixing event, session, user, and item grains in one table.

## Assumptions

- The raw source is the standard GA4 BigQuery export table pattern
  `events_*`.
- `event_date` is a `YYYYMMDD` string and can be parsed into a BigQuery `DATE`.
- `event_timestamp` is stored in microseconds and can be converted with
  `TIMESTAMP_MICROS`.
- `ga_session_id` and `ga_session_number` are present in `event_params`.
- `user_pseudo_id` is the default anonymous user identifier.
- A session is uniquely identified by `(user_pseudo_id, ga_session_id)`.
- Reporting should be based on event data after basic type casting and selected
  parameter extraction.
- The model should support Looker Studio dashboards first, not every possible
  raw GA4 exploration.
- Item-level ecommerce reporting needs a separate table because `items` is a
  repeated field.

## Tradeoffs

- Extracting only selected event parameters keeps the staging model stable, but
  analysts may need engineering changes when a new parameter becomes important.
- A daily aggregate mart is fast and easy for dashboards, but it loses
  event-level detail. The event and session tables remain available for
  drilldown.
- Session reconstruction from `user_pseudo_id` and `ga_session_id` is practical,
  but it can fail when consent mode, missing identifiers, or implementation
  issues prevent those values from being populated.
- Using `session_traffic_source_last_click` improves attribution when present,
  but older exports or some properties may not have it populated consistently.
- Keeping item reporting in a separate table avoids grain confusion, but Looker
  Studio users may need blended data or separate charts for item-level views.

## Production Considerations

For production usage, I would add:

- incremental models partitioned by `event_date`;
- clustering by `event_name`, `user_pseudo_id`, and `session_key` where useful;
- data quality checks for duplicate event keys, null session keys, and revenue
  consistency;
- source freshness checks;
- documentation for each reporting field;
- monitoring for new `event_params` keys;
- CI checks for SQL formatting and model compilation;
- explicit tests for session counts, purchase counts, and revenue totals;
- separate dev/prod datasets.

## SQL Files

The SQL files in `part2/sql` are representative BigQuery SQL.
