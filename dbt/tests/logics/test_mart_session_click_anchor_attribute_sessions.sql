-- =========================
-- official_events
-- =========================

SELECT
  'official_events' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_official_events_session,0)
  != COALESCE(login_official_events_session,0)
   + COALESCE(UnloginAll_official_events_session,0)

UNION ALL

SELECT
  'official_events' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_official_events_session,0) < COALESCE(login_official_events_session,0)
   OR COALESCE(All_official_events_session,0) < COALESCE(UnloginAll_official_events_session,0)
   OR COALESCE(UnloginAll_official_events_session,0) < COALESCE(UnloginNew_official_events_session,0)


-- =========================
-- questionnaire_entrance
-- =========================

UNION ALL

SELECT
  'questionnaire_entrance' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_questionnaire_entrance_session,0)
  != COALESCE(login_questionnaire_entrance_session,0)
   + COALESCE(UnloginAll_questionnaire_entrance_session,0)

UNION ALL

SELECT
  'questionnaire_entrance' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_questionnaire_entrance_session,0) < COALESCE(login_questionnaire_entrance_session,0)
   OR COALESCE(All_questionnaire_entrance_session,0) < COALESCE(UnloginAll_questionnaire_entrance_session,0)
   OR COALESCE(UnloginAll_questionnaire_entrance_session,0) < COALESCE(UnloginNew_questionnaire_entrance_session,0)

-- =========================
-- questionnaire_complete
-- =========================

UNION ALL

SELECT
  'questionnaire_complete' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_questionnaire_complete_session,0)
  != COALESCE(login_questionnaire_complete_session,0)
   + COALESCE(UnloginAll_questionnaire_complete_session,0)

UNION ALL

SELECT
  'questionnaire_complete' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_questionnaire_complete_session,0) < COALESCE(login_questionnaire_complete_session,0)
   OR COALESCE(All_questionnaire_complete_session,0) < COALESCE(UnloginAll_questionnaire_complete_session,0)
   OR COALESCE(UnloginAll_questionnaire_complete_session,0) < COALESCE(UnloginNew_questionnaire_complete_session,0)

-- =========================
-- LP_after_complete
-- =========================

UNION ALL

SELECT
  'lp_after_complete' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_LP_after_questionnaire_complete_session,0)
  != COALESCE(login_LP_after_questionnaire_complete_session,0)
   + COALESCE(UnloginAll_LP_after_questionnaire_complete_session,0)

UNION ALL

SELECT
  'lp_after_complete' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref('mart_session_click_anchor_attribute') }}
WHERE COALESCE(All_LP_after_questionnaire_complete_session,0) < COALESCE(login_LP_after_questionnaire_complete_session,0)
   OR COALESCE(All_LP_after_questionnaire_complete_session,0) < COALESCE(UnloginAll_LP_after_questionnaire_complete_session,0)
   OR COALESCE(UnloginAll_LP_after_questionnaire_complete_session,0) < COALESCE(UnloginNew_LP_after_questionnaire_complete_session,0)
