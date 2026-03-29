{{ config(materialized='view')}}

SELECT DISTINCT event_date
  FROM {{ref('stg_session_click_anchor_attribute_regexp')}}
