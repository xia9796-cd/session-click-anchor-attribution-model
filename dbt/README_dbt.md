# dbt modelsについて
※'README.md'にて大まかな概要は記載しているので、ここではdbtモデルにのみ言及する。
## stg/int/martのそれぞれの役割について
### stg
- stg_session_click_anchor_attribute.sql
- stg_session_click_anchor_attribute_regexp.sql
- stg_session_click_anchor_attribute_datebase.sql
- stg_session_click_anchor_attribute_devicemaster.sql
- stg_session_click_anchor_attribute_sourcemaster.sql
### int
- int_session_click_anchor_attribute_flags.sql
- int_session_click_anchor_attribute_sessioncount.sql
- int_session_click_anchor_attribute_clickcount.sql
### mart
- mart_session_click_anchor_attribute.sql
## refの依存関係
## 設計思想
### incrementalの期間について
### uniue_keyについて

