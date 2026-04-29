-- Staging model: one row per GA4 event.

WITH source AS (
  SELECT
    *
  FROM `project_id.analytics_123456789.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20240101' AND '20240131'
),

extracted AS (
  SELECT
    TO_HEX(SHA256(CONCAT(
      COALESCE(user_pseudo_id, ''),
      '|',
      COALESCE(CAST(event_timestamp AS STRING), ''),
      '|',
      COALESCE(event_name, ''),
      '|',
      COALESCE(CAST(event_bundle_sequence_id AS STRING), ''),
      '|',
      COALESCE(CAST(batch_event_index AS STRING), '')
    ))) AS event_key,

    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    event_bundle_sequence_id,
    batch_event_index,

    user_id,
    user_pseudo_id,

    (
      SELECT COALESCE(
        ep.value.int_value,
        SAFE_CAST(ep.value.string_value AS INT64),
        CAST(ep.value.float_value AS INT64),
        CAST(ep.value.double_value AS INT64)
      )
      FROM UNNEST(event_params) AS ep
      WHERE ep.key = 'ga_session_id'
      LIMIT 1
    ) AS ga_session_id,

    (
      SELECT COALESCE(
        ep.value.int_value,
        SAFE_CAST(ep.value.string_value AS INT64),
        CAST(ep.value.float_value AS INT64),
        CAST(ep.value.double_value AS INT64)
      )
      FROM UNNEST(event_params) AS ep
      WHERE ep.key = 'ga_session_number'
      LIMIT 1
    ) AS ga_session_number,

    (
      SELECT COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.float_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      )
      FROM UNNEST(event_params) AS ep
      WHERE ep.key = 'page_location'
      LIMIT 1
    ) AS page_location,

    (
      SELECT COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.float_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      )
      FROM UNNEST(event_params) AS ep
      WHERE ep.key = 'page_referrer'
      LIMIT 1
    ) AS page_referrer,

    (
      SELECT COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.float_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      )
      FROM UNNEST(event_params) AS ep
      WHERE ep.key = 'page_title'
      LIMIT 1
    ) AS page_title,

    (
      SELECT COALESCE(
        ep.value.int_value,
        SAFE_CAST(ep.value.string_value AS INT64),
        CAST(ep.value.float_value AS INT64),
        CAST(ep.value.double_value AS INT64)
      )
      FROM UNNEST(event_params) AS ep
      WHERE ep.key = 'engagement_time_msec'
      LIMIT 1
    ) AS engagement_time_msec,

    (
      SELECT COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.float_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      )
      FROM UNNEST(event_params) AS ep
      WHERE ep.key = 'session_engaged'
      LIMIT 1
    ) AS session_engaged,

    COALESCE(
      session_traffic_source_last_click.cross_channel_campaign.source,
      session_traffic_source_last_click.manual_campaign.source,
      collected_traffic_source.manual_source,
      traffic_source.source
    ) AS source,

    COALESCE(
      session_traffic_source_last_click.cross_channel_campaign.medium,
      session_traffic_source_last_click.manual_campaign.medium,
      collected_traffic_source.manual_medium,
      traffic_source.medium
    ) AS medium,

    COALESCE(
      session_traffic_source_last_click.cross_channel_campaign.campaign_name,
      session_traffic_source_last_click.manual_campaign.campaign_name,
      collected_traffic_source.manual_campaign_name,
      traffic_source.name
    ) AS campaign,

    collected_traffic_source.gclid,

    device.category AS device_category,
    device.operating_system,
    COALESCE(device.web_info.browser, device.browser) AS browser,
    geo.country,
    geo.region,
    geo.city,
    platform,
    stream_id,

    ecommerce.transaction_id,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.purchase_revenue,
    ecommerce.total_item_quantity AS item_count,
    event_value_in_usd
  FROM source
)

SELECT
  *,
  CASE
    WHEN user_pseudo_id IS NOT NULL AND ga_session_id IS NOT NULL
      THEN CONCAT(user_pseudo_id, '.', CAST(ga_session_id AS STRING))
  END AS session_key
FROM extracted;
