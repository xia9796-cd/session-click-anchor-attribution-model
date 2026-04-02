{{ config(materialized='view') }}

SELECT
  b.event_date,
  sf.device_category,
  sf.entrance_source,
  b.anchor_event_page AS event_page_location,

  /* ★ ALL */
  COALESCE(SUM(CASE WHEN
    b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%participateBtn%'
  THEN 1 ELSE 0 END), 0) AS Click_All_event_participation,

  COALESCE(SUM(CASE WHEN
    b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%squareBtnM%'
  THEN 1 ELSE 0 END), 0) AS Click_All_questionnaire_complete_event_participation,

  COALESCE(SUM(CASE WHEN
    b.event_name = 'AfterQuestionnaireGoogleFormClick'
  THEN 1 ELSE 0 END), 0) AS Click_All_googleform_event_participation,


  /* ★ login */
  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 0
    AND b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%participateBtn%'
  THEN 1 ELSE 0 END), 0) AS Click_login_event_participation,

  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 0
    AND b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%squareBtnM%'
  THEN 1 ELSE 0 END), 0) AS Click_login_questionnaire_complete_event_participation,

  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 0
    AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
  THEN 1 ELSE 0 END), 0) AS Click_login_googleform_event_participation,


  /* ★ UnloginAll */
  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 1
    AND b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%participateBtn%'
  THEN 1 ELSE 0 END), 0) AS Click_UnloginAll_event_participation,

  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 1
    AND b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%squareBtnM%'
  THEN 1 ELSE 0 END), 0) AS Click_UnloginAll_questionnaire_complete_event_participation,

  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 1
    AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
  THEN 1 ELSE 0 END), 0) AS Click_UnloginAll_googleform_event_participation,


  /* ★ UnloginNew */
  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 1
    AND sf.is_registered_at_official_events = 1
    AND b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%participateBtn%'
  THEN 1 ELSE 0 END), 0) AS Click_UnloginNew_event_participation,

  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 1
    AND sf.is_registered_at_official_events = 1
    AND b.event_name = 'OfficialEventsClick'
    AND b.Click_Classes LIKE '%squareBtnM%'
  THEN 1 ELSE 0 END), 0) AS Click_UnloginNew_questionnaire_complete_event_participation,

  COALESCE(SUM(CASE WHEN
    sf.is_logged_in_at_official_events = 1
    AND sf.is_registered_at_official_events = 1
    AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
  THEN 1 ELSE 0 END), 0) AS Click_UnloginNew_googleform_event_participation

FROM {{ ref('stg_session_click_anchor_attribute_regexp') }} b
LEFT JOIN {{ ref('int_session_click_anchor_attribute_flags') }} sf
  ON sf.user_unique_session_id = b.user_unique_session_id

WHERE b.anchor_event_page IS NOT NULL

GROUP BY 
  b.event_date,
  sf.device_category,
  sf.entrance_source,
  b.anchor_event_page
