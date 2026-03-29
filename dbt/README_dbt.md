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
[stg]
stg_session_click_anchor_attribute
  ↓
stg_session_click_anchor_attribute_regexp
  ├─stg_session_click_anchor_attribute_datebase.sql
  ├─stg_session_click_anchor_attribute_devicemaster.sql
  └─ stg_session_click_anchor_attribute_sourcemaster.sql

[int]
stg_session_click_anchor_attribute_regexp
  ↓
int_session_click_anchor_attribute_flags
  ├─ int_session_click_anchor_attribute_clickcount
  └─ int_session_click_anchor_attribute_sessioncount

[mart]
stg_session_click_anchor_attribute_datebase.sql
stg_session_click_anchor_attribute_devicemaster.sql
stg_session_click_anchor_attribute_sourcemaster.sql
int_session_click_anchor_attribute_clickcount
int_session_click_anchor_attribute_sessioncount
  ↓
mart_session_click_anchor_attribute
## 設計思想
### incrementalの期間について
### uniue_keyについて

