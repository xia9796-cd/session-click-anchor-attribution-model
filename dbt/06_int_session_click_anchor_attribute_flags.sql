{{ config(materialized='view')}}
       
 SELECT
    user_unique_session_id,
    ANY_VALUE(device_category) as device_category,
          
        
    IF(
      MIN(IF(page_location LIKE '%/users/add%', event_ts, NULL)) >=
      MIN(IF(page_location LIKE '%official-events%',event_ts, NULL)),1,0)
      as is_logged_in_at_official_events,
      
      
    IF(   
      MIN(IF(page_location LIKE '%/users/add%' 
      OR page_location LIKE '%new_users/which%', event_ts, NULL)) >=
      MIN(IF(page_location LIKE '%official-events%',event_ts, NULL)),1,0)
      as is_registered_at_official_events,
      
      
      
    CASE WHEN
      MAX(
       CASE WHEN
       page_location LIKE '%official-events%'
       AND (
          page_location LIKE '%utm_source=twitter%'
          OR LOWER(utm_source) IN ('t.co','x')
         )
        THEN 1 ELSE 0 END
      ) = 1
      THEN 'X'
      WHEN
      MAX
      (CASE WHEN
      page_location LIKE '%official-events%'
      AND(
        page_location LIKE '%utm_source=insta%'
        OR utm_source = 'insta')
      THEN 1 ELSE 0 END)
      = 1
      THEN 'instagram'
      ELSE 'Organic'
      END as entrance_source

FROM {{ref('stg_session_click_anchor_attribute_regexp')}}
  GROUP BY
    user_unique_session_id
