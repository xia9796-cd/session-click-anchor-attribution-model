-- ==================================================
-- Session Click Anchor Attribution Model
-- ==================================================
-- Purpose:
-- セッション内のクリックアンカーを元に
-- セッション属性を決定するモデル
-- Execution Environment
-- 
-- 
-- Key Techniques:
-- - WINDOW関数
-- - LAST_VALUE IGNORE NULLS
-- - セッション粒度設計
-- ==================================================



-- ==========================================
-- STEP1 基盤となるCTE
-- normalized：flatから必要なevent_paramsを展開したもの
-- session_base：イベント粒度で必要用に正規化したもの
-- 
-- 
-- ==========================================


WITH normalized AS (
  SELECT
    upid,
    event_date,
    event_ts,
    event_name,
    ANY_VALUE(device_category) AS device_category,
    MAX(IF(params_key = 'ga_session_id', int_value, NULL)) AS ga_session_id,
    MAX(IF(params_key = 'page_location', string_value, NULL)) AS page_location,
    MAX(IF(params_key = 'Click_Classes', string_value, NULL)) AS Click_Classes,
    MAX(IF(params_key = 'traffic_source_source', string_value, NULL)) AS traffic_source
  FROM `jobq-159704.analytics_279364443.events_flat`
  WHERE event_date BETWEEN start_date AND end_date
  GROUP BY
    upid,
    event_date,
    event_ts,
    event_name
),

base AS (
  SELECT
    event_date,
    event_ts,
    event_name,
    page_location,
    SPLIT(page_location, '?')[OFFSET(0)] AS splited_page_location,
    Click_Classes,
    LOWER(
      REGEXP_EXTRACT(page_location, r'utm_source=([^&]+)')
      ) AS utm_source,
    device_category,
    CONCAT(upid, '-', ga_session_id) AS user_unique_session_id,
    LAST_VALUE(
      IF(
       REGEXP_CONTAINS(page_location, r'official-events/[0-9]+'),
       SPLIT(page_location, '?')[OFFSET(0)],
      NULL
      )
      IGNORE NULLS
     )
      OVER (
        PARTITION BY upid,ga_session_id
        ORDER BY event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as anchor_event_page
  FROM normalized
),

date_base AS (
  SELECT DISTINCT event_date
  FROM base
),

entrance_source_master as(
  SELECT 'X' AS entrance_source
  UNION ALL
  SELECT 'Organic'
  UNION ALL
  SELECT 'instagram'
),

device_category_base as(
  SELECT 'mobile' AS device_category
  UNION ALL
  SELECT 'desktop'
  UNION ALL
  SELECT 'tablet'
  UNION ALL
  SELECT 'smart.tv'
)

-- ==========================================
-- STEP2 　セッションごとのフラグを立てるCTE
-- ANY_VALUE(device_category)：GA4想定なのでセッション内でデバイスが揺れないことを前提。
-- alias "is_logged_in_at_official_events"：`official-events`ページ以降にログインしたユーザーをログインユーザーとして識別。
-- alias "is_registered_at_official_events"：`official-events`ページ以降に会員登録したユーザーを会員登録ユーザーとして識別。
-- alias "entrance_source"：`official-events`を見た時の流入元を特定。（UTMパラメータにsourceの記載はあるが、traffic_sourceでも判別できるようにしている。）
-- ==========================================
*/
session_flags AS (
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
      MAX
      (CASE WHEN 
      page_location LIKE '%official-events%'
      AND page_location LIKE '%utm_source=twitter%' 
      OR LOWER(utm_source) IN ('t.co','x') THEN 1 ELSE 0 END IGNORE NULLS)
      = 1 
      THEN 'X'
      WHEN
      MAX
      (CASE WHEN 
      page_location LIKE '%official-events%'
      AND page_location LIKE '%utm_source=insta%' 
      OR utm_source = 'insta' THEN 1 ELSE 0 END)
      = 1
      THEN 'instagram'
      ELSE 'Organic'
      END as entrance_source


  FROM base
  GROUP BY
    user_unique_session_id

-- ==========================================
-- STEP3 セッション集計
-- 以下のセグメントごとに集計
-- - 全ユーザー
-- - `official-events`ページ前にログインあり
-- - `official-events`ページ後にログイン
-- - `official-events`ページ後に新規会員登録 
-- ========================================== 

  
/*セッションとクリック：ここから集計ロジック*/
session_count as (
SELECT
sb.event_date,
sf.device_category,
sf.entrance_source,
sb.anchor_event_page as event_page_location,


/* ★ セッション：全部の属性総計 */
COUNT(DISTINCT IF(
  sb.splited_page_location = sb.anchor_event_page,
  sb.user_unique_session_id,
  NULL
)) AS All_official_events_session,

COUNT(DISTINCT IF(
  sb.page_location LIKE '%surveys%'
  AND sb.page_location LIKE '%responses/new%',
  sb.user_unique_session_id,
  NULL
)) AS All_questionnaire_entrance_session,

COUNT(DISTINCT IF(
  sb.page_location LIKE '%surveys%'
  AND REGEXP_CONTAINS(sb.page_location, r'responses/[0-9]+'),
  sb.user_unique_session_id,
  NULL
)) AS All_questionnaire_complete_session,

COUNT(DISTINCT IF(
  sb.page_location LIKE '%surveys%'
  AND page_location LIKE '%content%',
  sb.user_unique_session_id,
  NULL
)) AS All_LP_after_questionnaire_complete_session,



/* ★ セッション：ログインユーザー */
COUNT(DISTINCT IF(
   sf.is_logged_in_at_official_events = 0
  AND sb.splited_page_location = sb.anchor_event_page,
  sb.user_unique_session_id,
  NULL
)) AS login_official_events_session,

COUNT(DISTINCT IF(
 sf.is_logged_in_at_official_events = 0
  AND sb.page_location LIKE '%surveys%'
  AND sb.page_location LIKE '%responses/new%',
  sb.user_unique_session_id,
  NULL
)) AS login_questionnaire_entrance_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 0
  AND sb.page_location LIKE '%surveys%'
  AND REGEXP_CONTAINS(sb.page_location, r'responses/[0-9]+'),
  sb.user_unique_session_id,
  NULL
)) AS login_questionnaire_complete_session,

COUNT(DISTINCT IF(
  sf.is_logged_in_at_official_events = 0
  AND sb.page_location LIKE '%surveys%'
  AND page_location LIKE '%content%',
  sb.user_unique_session_id,
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
)) AS UnloginNew_LP_after_questionnaire_complete_session,


FROM (
  SELECT DISTINCT
    event_date,
    device_category,
    user_unique_session_id,
    page_location,
    anchor_event_page,
    splited_page_location
  FROM base
) b
JOIN session_flags sf
ON b.user_unique_session_id = sf.user_unique_session_id
WHERE b.anchor_event_page IS NOT NULL

group by 
b.event_date,
sf.device_category,
sf.entrance_source,
b.anchor_event_page
),

-- ==========================================
-- STEP4 クリック集計
-- 以下のセグメントごとに集計
-- - 全ユーザー
-- - `official-events`ページ前にログインあり
-- - `official-events`ページ後にログイン
-- - `official-events`ページ後に新規会員登録 
-- ========================================== 

  
click_count as (
SELECT
sb.event_date,
sf.device_category,
sf.entrance_source,
b.anchor_event_page as event_page_location,


/* ★ クリック：全部の属性総計 */

SUM(CASE WHEN
  b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_All_event_participattion,

SUM(CASE WHEN
  b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_All_questionnaire_complete_event_participattion,

SUM(CASE WHEN
  b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_All_googleform_event_participattion,


/* ★ クリック：ログインユーザー */

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 0
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_login_event_participattion,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 0
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_login_questionnaire_complete_event_participattion,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 0
  AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_login_googleform_event_participattion,

/* ★ クリック：未ログインユーザー全て */

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_UnloginAll_event_participattion,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_UnloginAll_questionnaire_complete_event_participattion,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_UnloginAll_googleform_event_participattion,



/* ★ クリック：未ログインユーザーで新規登録会員 */

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events = 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%participateBtn%'
THEN 1 ELSE 0 END) AS Click_UnloginNew_event_participattion,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events= 1
  AND b.event_name = 'OfficialEventsClick'
  AND b.Click_Classes LIKE '%squareBtnM%'
THEN 1 ELSE 0 END) AS Click_UnloginNew_questionnaire_complete_event_participattion,

SUM(CASE WHEN
  sf.is_logged_in_at_official_events = 1
  AND sf.is_registered_at_official_events = 1
  AND b.event_name = 'AfterQuestionnaireGoogleFormClick'
THEN 1 ELSE 0 END) AS Click_UnloginNew_googleform_event_participattion,



from base b
LEFT JOIN session_flags sf
ON sf.user_unique_session_id = b.user_unique_session_id
WHERE b.anchor_event_page IS NOT NULL

group by 
sb.event_date,
sf.device_category,
sf.entrance_source,
b.anchor_event_page
)

-- ==========================================
-- STEP5 最終集計集計
-- ========================================== 
SELECT
  db.event_date,
  esm.entrance_source,
  sc.event_page_location,
  dcb.device_category,



 -- ★ 全部の属性総計
COALESCE(sc.All_official_events_session, 0) AS All_official_events_session,
COALESCE(sc.All_questionnaire_entrance_session, 0) AS All_questionnaire_entrance_session,
COALESCE(sc.All_questionnaire_complete_session, 0) AS All_questionnaire_complete_session,
COALESCE(sc.All_LP_after_questionnaire_complete_session, 0) AS All_LP_after_questionnaire_complete_session,
COALESCE(cc.Click_All_event_participattion, 0) AS Click_All_event_participattion,
COALESCE(cc.Click_All_questionnaire_complete_event_participattion, 0) AS Click_All_questionnaire_complete_event_participattion,
COALESCE(cc.Click_All_googleform_event_participattion, 0) AS Click_All_googleform_event_participattion,

-- ★ ログインユーザー
COALESCE(sc.login_official_events_session, 0) AS login_official_events_session,
COALESCE(sc.login_questionnaire_entrance_session, 0) AS login_questionnaire_entrance_session,
COALESCE(sc.login_questionnaire_complete_session, 0) AS login_questionnaire_complete_session,
COALESCE(sc.login_LP_after_questionnaire_complete_session, 0) AS login_LP_after_questionnaire_complete_session,
COALESCE(cc.Click_login_event_participattion, 0) AS Click_login_event_participattion,
COALESCE(cc.Click_login_questionnaire_complete_event_participattion, 0) AS Click_login_questionnaire_complete_event_participattion,
COALESCE(cc.Click_login_googleform_event_participattion, 0) AS Click_login_googleform_event_participattion,

-- ★ 未ログインユーザー全て
COALESCE(sc.UnloginAll_official_events_session, 0) AS UnloginAll_official_events_session,
COALESCE(sc.UnloginAll_questionnaire_entrance_session, 0) AS UnloginAll_questionnaire_entrance_session,
COALESCE(sc.UnloginAll_questionnaire_complete_session, 0) AS UnloginAll_questionnaire_complete_session,
COALESCE(sc.UnloginAll_LP_after_questionnaire_complete_session, 0) AS UnloginAll_LP_after_questionnaire_complete_session,
COALESCE(cc.Click_UnloginAll_event_participattion, 0) AS Click_UnloginAll_event_participattion,
COALESCE(cc.Click_UnloginAll_questionnaire_complete_event_participattion, 0) AS Click_UnloginAll_questionnaire_complete_event_participattion,
COALESCE(cc.Click_UnloginAll_googleform_event_participattion, 0) AS Click_UnloginAll_googleform_event_participattion,

-- ★ 未ログインユーザーで新規登録会員
COALESCE(sc.UnloginNew_official_events_session, 0) AS UnloginNew_official_events_session,
COALESCE(sc.UnloginNew_questionnaire_entrance_session, 0) AS UnloginNew_questionnaire_entrance_session,
COALESCE(sc.UnloginNew_questionnaire_complete_session, 0) AS UnloginNew_questionnaire_complete_session,
COALESCE(sc.UnloginNew_LP_after_questionnaire_complete_session, 0) AS UnloginNew_LP_after_questionnaire_complete_session,
COALESCE(cc.Click_UnloginNew_event_participattion, 0) AS Click_UnloginNew_event_participattion,
COALESCE(cc.Click_UnloginNew_questionnaire_complete_event_participattion, 0) AS Click_UnloginNew_questionnaire_complete_event_participattion,
COALESCE(cc.Click_UnloginNew_googleform_event_participattion, 0) AS Click_UnloginNew_googleform_event_participattion

FROM date_base db 
CROSS JOIN entrance_source_master esm 
CROSS JOIN device_category_base dcb 
LEFT JOIN session_count sc 
 ON sc.event_date = db.event_date 
 AND sc.device_category = dcb.device_category 
 AND sc.entrance_source = esm.entrance_source
LEFT JOIN click_count cc 
 ON cc.event_date = db.event_date 
 AND cc.device_category = dcb.device_category 
 AND cc.entrance_source = esm.entrance_source
 AND cc.event_page_location = sc.event_page_location


ORDER BY
  db.event_date;
