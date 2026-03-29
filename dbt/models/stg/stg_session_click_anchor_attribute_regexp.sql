{{ config(materialized='view') }}

SELECT
    event_date,
    event_ts,
    event_name,
    page_location,
    SPLIT(page_location, '?')[OFFSET(0)] AS splited_page_location,
    Click_Classes,
    LOWER(
      REGEXP_EXTRACT(page_location, r'utm_source=([^&]+)')
      ) AS utm_source,
    device_category,
    CONCAT(upid, '-', ga_session_id) AS user_unique_session_id,
    LAST_VALUE(
      IF(
       REGEXP_CONTAINS(page_location, r'official-events/[0-9]+'),
       SPLIT(page_location, '?')[OFFSET(0)],
      NULL
      )
      IGNORE NULLS
     )
      OVER (
        PARTITION BY upid,ga_session_id
        ORDER BY event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as anchor_event_page
  FROM {{ref('stg_session_click_anchor_attribute')}}
