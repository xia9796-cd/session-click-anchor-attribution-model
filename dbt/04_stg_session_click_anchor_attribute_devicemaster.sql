{{ config(materialized='view')}}

  SELECT 'mobile' AS device_category
  UNION ALL
  SELECT 'desktop'
  UNION ALL
  SELECT 'tablet'
  UNION ALL
  SELECT 'smart.tv'
