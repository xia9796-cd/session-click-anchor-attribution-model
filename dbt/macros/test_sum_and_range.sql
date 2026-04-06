{% macro test_sum_and_range(model, metric, all_col, login_col, unlogin_col, unloginnew_col) %}

-- sumチェック
SELECT
  '{{ metric }}' AS metric,
  'sum_mismatch' AS error_type,
  *
FROM {{ ref(model) }}
WHERE COALESCE({{ all_col }},0)
  != COALESCE({{ login_col }},0)
   + COALESCE({{ unlogin_col }},0)

UNION ALL

-- 範囲チェック
SELECT
  '{{ metric }}' AS metric,
  'invalid_range' AS error_type,
  *
FROM {{ ref(model) }}
WHERE COALESCE({{ all_col }},0) < COALESCE({{ login_col }},0)
   OR COALESCE({{ all_col }},0) < COALESCE({{ unlogin_col }},0)
   OR COALESCE({{ unlogin_col }},0) < COALESCE({{ unloginnew_col }},0)

{% endmacro %}
