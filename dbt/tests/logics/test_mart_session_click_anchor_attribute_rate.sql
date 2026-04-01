-- =====================================
-- official_events_participate_rate
-- =====================================

SELECT
  'official_events_participate_rate' AS metric,
  'invalid_rate' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE All_events_participate_rate > 1
   OR All_events_participate_rate < 0

UNION ALL

SELECT
  'login_events_participate_rate',
  'invalid_rate',
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE login_events_participate_rate > 1
   OR login_events_participate_rate < 0

UNION ALL

SELECT
  'unlogin_events_participate_rate',
  'invalid_rate',
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE UnloginAll_events_participate_rate > 1
   OR UnloginAll_events_participate_rate < 0

UNION ALL

SELECT
  'unlogin_new_events_participate_rate',
  'invalid_rate',
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE UnloginNew_events_participate_rate > 1
   OR UnloginNew_events_participate_rate < 0


-- =====================================
-- questionnaire_complete_rate
-- =====================================

UNION ALL

SELECT
  'questionnaire_complete_rate',
  'invalid_rate',
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE All_questionnaire_complete_rate > 1
   OR All_questionnaire_complete_rate < 0

UNION ALL

SELECT
  'login_questionnaire_complete_rate',
  'invalid_rate',
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE login_questionnaire_complete_rate > 1
   OR login_questionnaire_complete_rate < 0

UNION ALL

SELECT
  'unlogin_questionnaire_complete_rate',
  'invalid_rate',
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE UnloginAll_questionnaire_complete_rate > 1
   OR UnloginAll_questionnaire_complete_rate < 0

UNION ALL

SELECT
  'unlogin_new_questionnaire_complete_rate',
  'invalid_rate',
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE UnloginNew_questionnaire_complete_rate > 1
   OR UnloginNew_questionnaire_complete_rate < 0
