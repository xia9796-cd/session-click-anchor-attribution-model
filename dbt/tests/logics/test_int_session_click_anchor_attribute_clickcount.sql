-- =====================================
-- metric: event_participation (click)
-- =====================================

SELECT
  'event_participation_click' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref('int_session_click_anchor_attribute_clickcount') }}
WHERE COALESCE(Click_All_event_participation,0)
  != COALESCE(Click_login_event_participation,0)
   + COALESCE(Click_UnloginAll_event_participation,0)

UNION ALL

SELECT
  'event_participation_click' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref('int_session_click_anchor_attribute_clickcount') }}
WHERE COALESCE(Click_All_event_participation,0) < COALESCE(Click_login_event_participation,0)
   OR COALESCE(Click_All_event_participation,0) < COALESCE(Click_UnloginAll_event_participation,0)
   OR COALESCE(Click_UnloginAll_event_participation,0) < COALESCE(Click_UnloginNew_event_participation,0)

-- =====================================
-- metric: questionnaire_complete (click)
-- =====================================

UNION ALL

SELECT
  'questionnaire_complete_click' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref('int_session_click_anchor_attribute_clickcount') }}
WHERE COALESCE(Click_All_questionnaire_complete_event_participation,0)
  != COALESCE(Click_login_questionnaire_complete_event_participation,0)
   + COALESCE(Click_UnloginAll_questionnaire_complete_event_participation,0)

UNION ALL

SELECT
  'questionnaire_complete_click' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref('int_session_click_anchor_attribute_clickcount') }}
WHERE COALESCE(Click_All_questionnaire_complete_event_participation,0) < COALESCE(Click_login_questionnaire_complete_event_participation,0)
   OR COALESCE(Click_All_questionnaire_complete_event_participation,0) < COALESCE(Click_UnloginAll_questionnaire_complete_event_participation,0)
OR COALESCE(Click_UnloginAll_questionnaire_complete_event_participation,0) < COALESCE(Click_UnloginNew_questionnaire_complete_event_participation,0)

-- =====================================
-- metric: googleform (click)
-- =====================================

UNION ALL

SELECT
  'googleform_click' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref('int_session_click_anchor_attribute_clickcount') }}
WHERE COALESCE(Click_All_googleform_event_participation,0)
  != COALESCE(Click_login_googleform_event_participation,0)
   + COALESCE(Click_UnloginAll_googleform_event_participation,0)

UNION ALL

SELECT
  'googleform_click' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref('int_session_click_anchor_attribute_clickcount') }}
WHERE COALESCE(Click_All_googleform_event_participation,0) < COALESCE(Click_login_googleform_event_participation,0)
   OR COALESCE(Click_All_googleform_event_participation,0) < COALESCE(Click_UnloginAll_googleform_event_participation,0)
OR COALESCE(Click_UnloginAll_googleform_event_participation,0) < COALESCE(Click_UnloginNew_googleform_event_participation,0)
