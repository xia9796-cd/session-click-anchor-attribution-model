{{ config(materialized='view')}}

  SELECT 'X' AS entrance_source
  UNION ALL
  SELECT 'Organic'
  UNION ALL
  SELECT 'instagram'
