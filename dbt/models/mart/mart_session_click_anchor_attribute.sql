{{ config(
materialized='incremental',
incremental_strategy='insert_overwrite',
unique_key=['event_date','event_page_location','device_category'],
partition_by={
"field": "event_date",
"data_type": "date"
},
cluster_by=["event_page_location", "device_category"]
)}}

SELECT
  CAST(db.event_date AS DATE) AS event_date,
  CAST(esm.entrance_source AS STRING) AS entrance_source,
  CAST(sc.event_page_location AS STRING) AS event_page_location,
  CAST(dcb.device_category AS STRING) AS device_category,

  /* ===== セッション（INT64固定） ===== */
  CAST(COALESCE(sc.All_official_events_session, 0) AS INT64) AS All_official_events_session,
  CAST(COALESCE(sc.All_questionnaire_entrance_session, 0) AS INT64) AS All_questionnaire_entrance_session,
  CAST(COALESCE(sc.All_questionnaire_complete_session, 0) AS INT64) AS All_questionnaire_complete_session,
  CAST(COALESCE(sc.All_LP_after_questionnaire_complete_session, 0) AS INT64) AS All_LP_after_questionnaire_complete_session,

  /* ===== クリック（INT64固定） ===== */
  CAST(COALESCE(cc.Click_All_event_participattion, 0) AS INT64) AS Click_All_event_participattion,
  CAST(COALESCE(cc.Click_All_questionnaire_complete_event_participattion, 0) AS INT64) AS Click_All_questionnaire_complete_event_participattion,
  CAST(COALESCE(cc.Click_All_googleform_event_participattion, 0) AS INT64) AS Click_All_googleform_event_participattion,

  /* login */
  CAST(COALESCE(sc.login_official_events_session, 0) AS INT64) AS login_official_events_session,
  CAST(COALESCE(sc.login_questionnaire_entrance_session, 0) AS INT64) AS login_questionnaire_entrance_session,
  CAST(COALESCE(sc.login_questionnaire_complete_session, 0) AS INT64) AS login_questionnaire_complete_session,
  CAST(COALESCE(sc.login_LP_after_questionnaire_complete_session, 0) AS INT64) AS login_LP_after_questionnaire_complete_session,

  CAST(COALESCE(cc.Click_login_event_participattion, 0) AS INT64) AS Click_login_event_participattion,
  CAST(COALESCE(cc.Click_login_questionnaire_complete_event_participattion, 0) AS INT64) AS Click_login_questionnaire_complete_event_participattion,
  CAST(COALESCE(cc.Click_login_googleform_event_participattion, 0) AS INT64) AS Click_login_googleform_event_participattion,

  /* UnloginAll */
  CAST(COALESCE(sc.UnloginAll_official_events_session, 0) AS INT64) AS UnloginAll_official_events_session,
  CAST(COALESCE(sc.UnloginAll_questionnaire_entrance_session, 0) AS INT64) AS UnloginAll_questionnaire_entrance_session,
  CAST(COALESCE(sc.UnloginAll_questionnaire_complete_session, 0) AS INT64) AS UnloginAll_questionnaire_complete_session,
  CAST(COALESCE(sc.UnloginAll_LP_after_questionnaire_complete_session, 0) AS INT64) AS UnloginAll_LP_after_questionnaire_complete_session,

  CAST(COALESCE(cc.Click_UnloginAll_event_participattion, 0) AS INT64) AS Click_UnloginAll_event_participattion,
  CAST(COALESCE(cc.Click_UnloginAll_questionnaire_complete_event_participattion, 0) AS INT64) AS Click_UnloginAll_questionnaire_complete_event_participattion,
  CAST(COALESCE(cc.Click_UnloginAll_googleform_event_participattion, 0) AS INT64) AS Click_UnloginAll_googleform_event_participattion,

  /* UnloginNew */
  CAST(COALESCE(sc.UnloginNew_official_events_session, 0) AS INT64) AS UnloginNew_official_events_session,
  CAST(COALESCE(sc.UnloginNew_questionnaire_entrance_session, 0) AS INT64) AS UnloginNew_questionnaire_entrance_session,
  CAST(COALESCE(sc.UnloginNew_questionnaire_complete_session, 0) AS INT64) AS UnloginNew_questionnaire_complete_session,
  CAST(COALESCE(sc.UnloginNew_LP_after_questionnaire_complete_session, 0) AS INT64) AS UnloginNew_LP_after_questionnaire_complete_session,

  CAST(COALESCE(cc.Click_UnloginNew_event_participattion, 0) AS INT64) AS Click_UnloginNew_event_participattion,
  CAST(COALESCE(cc.Click_UnloginNew_questionnaire_complete_event_participattion, 0) AS INT64) AS Click_UnloginNew_questionnaire_complete_event_participattion,
  CAST(COALESCE(cc.Click_UnloginNew_googleform_event_participattion, 0) AS INT64) AS Click_UnloginNew_googleform_event_participattion,

  /* ===== rate（FLOAT64固定） ===== */
  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.All_questionnaire_entrance_session, sc.All_official_events_session), 4), 0) AS FLOAT64) AS All_events_participate_rate,
  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.login_questionnaire_entrance_session, sc.login_official_events_session), 4), 0) AS FLOAT64) AS login_events_participate_rate,
  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.UnloginAll_questionnaire_entrance_session, sc.UnloginAll_official_events_session), 4), 0) AS FLOAT64) AS UnloginAll_events_participate_rate,
  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.UnloginNew_questionnaire_entrance_session, sc.UnloginNew_official_events_session), 4), 0) AS FLOAT64) AS UnloginNew_events_participate_rate,

  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.All_questionnaire_complete_session, sc.All_questionnaire_entrance_session), 4), 0) AS FLOAT64) AS All_questionnaire_complete_rate,
  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.login_questionnaire_complete_session, sc.login_questionnaire_entrance_session), 4), 0) AS FLOAT64) AS login_questionnaire_complete_rate,
  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.UnloginAll_questionnaire_complete_session, sc.UnloginAll_questionnaire_entrance_session), 4), 0) AS FLOAT64) AS UnloginAll_questionnaire_complete_rate,
  CAST(IFNULL(ROUND(SAFE_DIVIDE(sc.UnloginNew_questionnaire_complete_session, sc.UnloginNew_questionnaire_entrance_session), 4), 0) AS FLOAT64) AS UnloginNew_questionnaire_complete_rate


FROM {{ref('stg_session_click_anchor_attribute_datebase')}} db 
CROSS JOIN {{ref('stg_session_click_anchor_attribute_sourcemaster')}} esm 
CROSS JOIN {{ref('stg_session_click_anchor_attribute_devicemaster')}} dcb 
LEFT JOIN {{ref('int_session_click_anchor_attribute_sessioncount')}} sc 
 ON sc.event_date = db.event_date 
 AND sc.device_category = dcb.device_category 
 AND sc.entrance_source = esm.entrance_source
LEFT JOIN {{ref('int_session_click_anchor_attribute_clickcount')}} cc 
 ON cc.event_date = db.event_date 
 AND cc.device_category = dcb.device_category 
 AND cc.entrance_source = esm.entrance_source
 AND cc.event_page_location = sc.event_page_location



{% if is_incremental()%}
where sc.event_date BETWEEN
    DATE_SUB(CURRENT_DATE(),INTERVAL 7 DAY)
AND DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY)
{% endif %}
