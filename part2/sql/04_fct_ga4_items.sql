-- Item-level fact table for ecommerce reporting.

WITH source AS (
  SELECT
    *
  FROM `project_id.analytics_123456789.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20240101' AND '20240131'
),

events AS (
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

    ecommerce.transaction_id,
    items
  FROM source
  WHERE ARRAY_LENGTH(items) > 0
)

SELECT
  event_key,
  event_date,
  event_timestamp,
  event_name,
  user_id,
  user_pseudo_id,
  CASE
    WHEN user_pseudo_id IS NOT NULL AND ga_session_id IS NOT NULL
      THEN CONCAT(user_pseudo_id, '.', CAST(ga_session_id AS STRING))
  END AS session_key,
  transaction_id,
  item_index,

  item.item_id,
  item.item_name,
  item.item_brand,
  item.item_variant,
  item.item_category,
  item.item_category2,
  item.item_category3,
  item.item_category4,
  item.item_category5,
  item.price_in_usd,
  item.price,
  item.quantity,
  item.item_revenue_in_usd,
  item.item_revenue,
  item.item_refund_in_usd,
  item.item_refund,
  item.coupon,
  item.affiliation,
  item.item_list_id,
  item.item_list_name,
  item.promotion_id,
  item.promotion_name,
  item.creative_name,
  item.creative_slot
FROM events
CROSS JOIN UNNEST(items) AS item WITH OFFSET AS item_index;
