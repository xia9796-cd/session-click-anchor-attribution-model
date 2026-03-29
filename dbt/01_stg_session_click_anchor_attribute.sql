{{ config(materialized='view')}}

SELECT
    upid,
    event_date,
    event_ts,
    event_name,
    ANY_VALUE(device_category) AS device_category,
    MAX(IF(params_key = 'ga_session_id', int_value, NULL)) AS ga_session_id,
    MAX(IF(params_key = 'page_location', string_value, NULL)) AS page_location,
    MAX(IF(params_key = 'Click_Classes', string_value, NULL)) AS Click_Classes,
    MAX(IF(params_key = 'traffic_source_source', string_value, NULL)) AS traffic_source
  FROM {{ source('raw', 'test01')}}
  GROUP BY
    upid,
    event_date,
    event_ts,
    event_name
