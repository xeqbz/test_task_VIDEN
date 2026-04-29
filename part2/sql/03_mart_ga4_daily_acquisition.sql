-- Reporting mart: daily acquisition table for Looker Studio.

WITH sessions AS (
  SELECT
    *
  FROM `project_id.analytics_reporting.int_ga4_sessions`
),

aggregated AS (
  SELECT
    session_date AS report_date,
    COALESCE(session_source, '(direct)') AS source,
    COALESCE(session_medium, '(none)') AS medium,
    COALESCE(session_campaign, '(not set)') AS campaign,
    COALESCE(device_category, '(not set)') AS device_category,
    COALESCE(country, '(not set)') AS country,

    COUNT(*) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNTIF(is_engaged_session) AS engaged_sessions,
    SUM(page_views) AS page_views,
    SUM(purchase_count) AS purchases,
    SUM(purchase_revenue_in_usd) AS purchase_revenue_in_usd
  FROM sessions
  GROUP BY
    report_date,
    source,
    medium,
    campaign,
    device_category,
    country
)

SELECT
  *,
  SAFE_DIVIDE(engaged_sessions, sessions) AS engagement_rate,
  SAFE_DIVIDE(purchase_revenue_in_usd, purchases) AS average_order_value
FROM aggregated;
