{{ config(materialized='view')}}

SELECT
b.event_date,
sf.device_category,
sf.entrance_source,
b.anchor_event_page as event_page_location,


/* ★ クリック：全部の属性総計 */

SUM(CASE WHEN
  b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_All_event_participation,

SUM(CASE WHEN
  b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_All_questionnaire_complete_event_participation,

SUM(CASE WHEN
  b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_All_googleform_event_participation,


/* ★ クリック：ログインユーザー */

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 0
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_login_event_participation,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 0
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_login_questionnaire_complete_event_participation,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 0
  AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_login_googleform_event_participation,

/* ★ クリック：未ログインユーザー全て */

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_UnloginAll_event_participation,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_UnloginAll_questionnaire_complete_event_participation,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_UnloginAll_googleform_event_participation,



/* ★ クリック：未ログインユーザーで新規登録会員 */

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events = 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_UnloginNew_event_participation,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events= 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_UnloginNew_questionnaire_complete_event_participation,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events = 1
  AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_UnloginNew_googleform_event_participation,



from {{ref('stg_session_click_anchor_attribute_regexp')}} b
LEFT JOIN {{ref('int_session_click_anchor_attribute_flags')}} sf
ON sf.user_unique_session_id = b.user_unique_session_id
WHERE b.anchor_event_page IS NOT NULL

group by 
b.event_date,
sf.device_category,
sf.entrance_source,
b.anchor_event_page
