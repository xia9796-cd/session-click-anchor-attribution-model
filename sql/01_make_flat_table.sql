-- ==================================================
-- flatテーブル（GA4のrawデータをunnestしたもの）を作成するSQL
-- ==================================================
-- Purpose:
-- - GA4のrawデータをunnestして展開し、集計時のベースとして使いやすくしたもの。（SQL_session_clicl_attribution_modelのCTE"normalized"の元になるテーブル）
-- - DELETE ,INSERTでデータを更新してくモデル。
-- Execution Environment
-- - GA4のrawデータを`UNNEST(event_params)`したもの。
-- - DELETE,INSERTで7日前〜2日前までのデータを更新する。（GA4のrawデータがDHWに落ちてくるまでの遅延を想定しての期間設定。）
-- - SUFFIXでは安定しない（pruneきく条件が不安定）ため、SET,EXECUTE IMMEDIATEとUNIONALLで実行している。
-- ==================================================


DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE("Asia/Tokyo"), INTERVAL 7 DAY);
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE("Asia/Tokyo"), INTERVAL 2 DAY);

DECLARE sql STRING;

BEGIN

-- ===============================
-- STEP 1: DELETE（読む範囲を消して入れ直す。）
-- ===============================
DELETE FROM `project199709.analytics_00000008.unnest_event_flat`
WHERE event_date BETWEEN start_date AND end_date;

-- ===============================
-- STEP 2: 動的 SQL（1ステートメント）を生成する
-- ===============================
SET sql = (
  SELECT """
    INSERT INTO `project199709.analytics_00000008.unnest_event_flat` (
      event_date, upid, event_name, device_category, event_ts,
      key, string_value, int_value, float_value, double_value,traffic_source_name,traffic_source_medium,traffic_source_source
    )
    """ || STRING_AGG(
      FORMAT("""
        SELECT
          PARSE_DATE('%%Y%%m%%d', event_date),
          user_pseudo_id,
          event_name,
          device.category,
          TIMESTAMP_MICROS(event_timestamp),
          ep.key,
          ep.value.string_value,
          ep.value.int_value,
          ep.value.float_value,
          ep.value.double_value,
          traffic_source.name,
          traffic_source.medium,
          traffic_source.source
        FROM `project199709.analytics_00000008.events_%s`,
        UNNEST(event_params) AS ep
      """, FORMAT_DATE('%Y%m%d', d))
    , " UNION ALL ")
  FROM UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS d
);

-- Debug
-- SELECT sql;

-- ===============================
-- STEP 3: EXECUTE IMMEDIATE（1ステートメント）
-- ===============================
EXECUTE IMMEDIATE sql;

END;

