-- Intermediate model: one row per reconstructed GA4 session.

WITH events AS (
  SELECT
    *
  FROM `project_id.analytics_reporting.stg_ga4_events`
  WHERE session_key IS NOT NULL
),

sessionized AS (
  SELECT
    session_key,
    ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
    ANY_VALUE(user_id) AS user_id,
    ANY_VALUE(ga_session_id) AS ga_session_id,
    MIN(event_timestamp) AS session_start_timestamp,
    MAX(event_timestamp) AS session_end_timestamp,
    DATE(MIN(event_timestamp)) AS session_date,
    TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND)
      AS session_duration_seconds,

    ARRAY_AGG(page_location IGNORE NULLS ORDER BY event_timestamp LIMIT 1)
      [SAFE_OFFSET(0)] AS landing_page,

    ARRAY_AGG(source IGNORE NULLS ORDER BY event_timestamp LIMIT 1)
      [SAFE_OFFSET(0)] AS session_source,

    ARRAY_AGG(medium IGNORE NULLS ORDER BY event_timestamp LIMIT 1)
      [SAFE_OFFSET(0)] AS session_medium,

    ARRAY_AGG(campaign IGNORE NULLS ORDER BY event_timestamp LIMIT 1)
      [SAFE_OFFSET(0)] AS session_campaign,

    ARRAY_AGG(device_category IGNORE NULLS ORDER BY event_timestamp LIMIT 1)
      [SAFE_OFFSET(0)] AS device_category,

    ARRAY_AGG(country IGNORE NULLS ORDER BY event_timestamp LIMIT 1)
      [SAFE_OFFSET(0)] AS country,

    COUNT(*) AS event_count,
    COUNTIF(event_name = 'page_view') AS page_views,
    LOGICAL_OR(
      COALESCE(session_engaged IN ('1', 'true'), FALSE)
      OR event_name = 'user_engagement'
    ) AS is_engaged_session,
    SUM(COALESCE(engagement_time_msec, 0)) AS engagement_time_msec,
    COUNTIF(event_name = 'purchase') AS purchase_count,
    SUM(
      CASE
        WHEN event_name = 'purchase'
          THEN COALESCE(purchase_revenue_in_usd, event_value_in_usd, 0)
        ELSE 0
      END
    ) AS purchase_revenue_in_usd
  FROM events
  GROUP BY session_key
)

SELECT
  *
FROM sessionized;
