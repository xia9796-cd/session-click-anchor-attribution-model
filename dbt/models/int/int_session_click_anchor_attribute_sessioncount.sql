{{config(materialized='view')}}

SELECT
b.event_date,
sf.device_category,
sf.entrance_source,
b.anchor_event_page as event_page_location,


/* ★ セッション：全部の属性総計 */
COUNT(DISTINCT IF(
  b.splited_page_location = b.anchor_event_page,
  b.user_unique_session_id,
  NULL
)) AS All_official_events_session,

COUNT(DISTINCT IF(
  b.page_location LIKE '%surveys%'
  AND b.page_location LIKE '%responses/new%',
  b.user_unique_session_id,
  NULL
)) AS All_questionnaire_entrance_session,

COUNT(DISTINCT IF(
  b.page_location LIKE '%surveys%'
  AND REGEXP_CONTAINS(b.page_location, r'responses/[0-9]+'),
  b.user_unique_session_id,
  NULL
)) AS All_questionnaire_complete_session,

COUNT(DISTINCT IF(
  b.page_location LIKE '%surveys%'
  AND page_location LIKE '%content%',
  b.user_unique_session_id,
  NULL
)) AS All_LP_after_questionnaire_complete_session,



/* ★ セッション：ログインユーザー */
COUNT(DISTINCT IF(
   sf.is_logged_in_at_official_events = 0
  AND b.splited_page_location = b.anchor_event_page,
  b.user_unique_session_id,
  NULL
)) AS login_official_events_session,

COUNT(DISTINCT IF(
 sf.is_logged_in_at_official_events = 0
  AND b.page_location LIKE '%surveys%'
  AND b.page_location LIKE '%responses/new%',
  b.user_unique_session_id,
  NULL
)) AS login_questionnaire_entrance_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 0
  AND b.page_location LIKE '%surveys%'
  AND REGEXP_CONTAINS(b.page_location, r'responses/[0-9]+'),
  b.user_unique_session_id,
  NULL
)) AS login_questionnaire_complete_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 0
  AND b.page_location LIKE '%surveys%'
  AND page_location LIKE '%content%',
  b.user_unique_session_id,
  NULL
)) AS login_LP_after_questionnaire_complete_session,





/* ★ セッション：未ログインユーザー全て */
COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND b.splited_page_location = b.anchor_event_page,
  b.user_unique_session_id,
  NULL
)) AS UnloginAll_official_events_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND b.page_location LIKE '%surveys%'
  AND b.page_location LIKE '%responses/new%',
  b.user_unique_session_id,
  NULL
)) AS UnloginAll_questionnaire_entrance_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND b.page_location LIKE '%surveys%'
  AND REGEXP_CONTAINS(b.page_location, r'responses/[0-9]+'),
  b.user_unique_session_id,
  NULL
)) AS UnloginAll_questionnaire_complete_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND b.page_location LIKE '%surveys%'
  AND b.page_location LIKE '%content%',
  b.user_unique_session_id,
  NULL
)) AS UnloginAll_LP_after_questionnaire_complete_session,


/* ★ セッション：未ログインユーザーで新規登録会員 */
COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events = 1
  AND b.splited_page_location = b.anchor_event_page,
  b.user_unique_session_id,
  NULL
)) AS UnloginNew_official_events_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events = 1
  AND b.page_location LIKE '%surveys%'
  AND b.page_location LIKE '%responses/new%',
  b.user_unique_session_id,
  NULL
)) AS UnloginNew_questionnaire_entrance_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events = 1
  AND b.page_location LIKE '%surveys%'
  AND REGEXP_CONTAINS(b.page_location, r'responses/[0-9]+'),
  b.user_unique_session_id,
  NULL
)) AS UnloginNew_questionnaire_complete_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events= 1
  AND b.page_location LIKE '%surveys%'
  AND b.page_location LIKE '%content%',
  b.user_unique_session_id,
  NULL
)) AS UnloginNew_LP_after_questionnaire_complete_session


FROM (
  SELECT DISTINCT
    event_date,
    device_category,
    user_unique_session_id,
    page_location,
    anchor_event_page,
    splited_page_location
  FROM {{ref('stg_session_click_anchor_attribute_regexp')}}
) b
JOIN {{ref('int_session_click_anchor_attribute_flags')}} sf
ON b.user_unique_session_id = sf.user_unique_session_id
WHERE b.anchor_event_page IS NOT NULL

group by 
b.event_date,
sf.device_category,
sf.entrance_source,
b.anchor_event_page
