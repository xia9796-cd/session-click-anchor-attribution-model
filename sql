# これは？
実際に使用したSQLのモデル。
  
# 前提となる環境
  
# SQL内容
## STEP1 基盤となるCTE（base,date_base,entrance_source_base,device_category_base）
### base
```
WITH normalized AS (
  SELECT
    upid,
    event_date,
    event_ts,
    event_name,
    ANY_VALUE(device_category) AS device_category,
    MAX(IF(key = 'ga_session_id', int_value, NULL)) AS ga_session_id,
    MAX(IF(key = 'page_location', string_value, NULL)) AS page_location,
    MAX(IF(key = 'Click_Classes', string_value, NULL)) AS Click_Classes,
    MAX(IF(key = 'traffic_source_source', string_value, NULL)) AS traffic_source
  FROM `jobq-159704.analytics_279364443.events_flat`
  WHERE event_date BETWEEN start_date AND end_date
  GROUP BY
    upid,
    event_date,
    event_ts,
    event_name
)
  
  ```

session_base AS (
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
  FROM normalized
),

date_base AS (
  SELECT DISTINCT event_date
  FROM session_base
),

entrance_source_master as(
  SELECT 'X' AS entrance_source
  UNION ALL
  SELECT 'Organic'
  UNION ALL
  SELECT 'instagram'
),

device_category_base as(
  SELECT 'mobile' AS device_category
  UNION ALL
  SELECT 'desktop'
  UNION ALL
  SELECT 'tablet'
  UNION ALL
  SELECT 'smart.tv'
)
