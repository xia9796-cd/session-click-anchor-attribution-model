# dbt modelsについて
※'[README.md](https://github.com/xia9796-cd/session-click-anchor-attribution-model/blob/main/README.md)'にて大まかな概要は記載しているので、ここではdbtモデルにのみ言及する。
## dbtファイル内の構成（目次）
```
dbt
  ├─ macros：macroフォルダ 
  ├─ models：SQLモデルフォルダ
  |   ├─ int：intのSQLファイルが格納してあるフォルダ
  |   ├─ mart：martのSQLファイルが格納してあるフォルダ
  |   └─ stg：stgのSQLファイルが格納してあるフォルダ
  ├─ tests
  |   ├─ logics：テストのロジックのSQLファイルが格納してある（macro使用なし）フォルダ
  |   ├─ tests_output_CLI：macro使用したテスト、使用してないテストの、両者のテスト結果
  |   └─ with_macros：macroを使用したSQLファイルが格納してあるフォルダ
  └─ demo_project.yml：プロジェクト設定ファイル
     
```

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
```
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

stg_session_click_anchor_attribute_regexp
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
```
## 設計思想
### incrementalの期間について
- CURRENT_DATE INTERVAL 7 DAY ~ CURRENT_DATE INTERVAL 2 DAY の期間をDELETE&INSERT
- 毎日JST09:00に実行 →GA4のデータが落ちるまでの遅延を考慮して、期間の開始日をINTERVAL7日前からにしている。 →またGA4ではリアルタイムでデータテーブルが作られるわけではないので、確実にデータが存在する2日前までを期間の終了日にする

## テスト
### sessionの依存・相関関係チェック
#### テスト対象のカラム
- 全ユーザー
  - All_official_events_session
  - All_questionnaire_entrance_session
  - All_questionnaire_complete_session
  - All_LP_after_questionnaire_complete_session
- ログインユーザー
  - login_official_events_session
  - login_questionnaire_entrance_session
  - login_questionnaire_complete_session
  - login_LP_after_questionnaire_complete_session
- 未ログインユーザー
  - UnloginAll_official_events_session
  - UnloginAll_questionnaire_entrance_session
  - UnloginAll_questionnaire_complete_session
  - UnloginAll_LP_after_questionnaire_complete_session
- 未ログインユーザーのうち新規会員
  - UnloginNew_official_events_session
  - UnloginNew_questionnaire_entrance_session
  - UnloginNew_questionnaire_complete_session
  - UnloginNew_LP_after_questionnaire_complete_session
  
#### テスト内容
テスト内容は以下。
- ログインユーザー(login_*)と未ログインユーザー(UnloginAll_*)のsession数の合計が、全ユーザー（ALl_*）のsession数と等しくなるか。
- ログインユーザー(login_*)と未ログインユーザー(UnloginAll_*)それぞれのsession数が、全ユーザー（ALl_*）のsession数よりも大きくならないか。
- 未ログインユーザー(UnloginAll_*)のsession数が、未ログインユーザーのうち新規会員（UnlogiNew_*）のsession数よりも大きくならないか。
※intとmartの両方でテストを実行。
errorが発生した際に、原因がJOIN事故なのか算出ロジックなのか判別するため。
### clickの依存・相関関係チェック
#### テスト対象のカラム
- 全ユーザー
  - Click_All_event_participation
  - Click_All_questionnaire_complete_event_participation
  - Click_All_googleform_event_participation
- ログインユーザー
  - Click_login_event_participation
  - Click_login_questionnaire_complete_event_participation
  - Click_login_googleform_event_participation
- 未ログインユーザー
  - Click_UnloginAll_event_participation
  - Click_UnloginAll_questionnaire_complete_event_participation
  - Click_UnloginAll_googleform_event_participation
- 未ログインユーザーのうち新規会員
  - Click_UnloginNew_event_participation
  - Click_UnloginNew_questionnaire_complete_event_participation
  - Click_UnloginNew_googleform_event_participation
#### テスト内容
- ログインユーザー(login_*)と未ログインユーザー(UnloginAll_*)のclick数の合計が、全ユーザー（ALl_*）のclick数と等しくなるか。
- ログインユーザー(login_*)と未ログインユーザー(UnloginAll_*)それぞれのclick数が、全ユーザー（ALl_*）のclick数よりも大きくならないか。
- 未ログインユーザー(UnloginAll_*)のclick数が、未ログインユーザーのうち新規会員（UnlogiNew_*）のclick数よりも大きくならないか。
※intとmartの両方でテストを実行。
errorが発生した際に、原因がJOIN事故なのか算出ロジックなのか判別するため。
### rateの状態チェック
#### テスト対象
`mart_session_click_anchor_attribute.sql`で作成した以下4つの
- イベント参加率
 - All_events_participate_rate
 - login_events_participate_rate
 - UnloginAll_events_participate_rate
 - UnloginNew_events_participate_rate

- イベント完了率
 - All_questionnaire_complete_rate
 - login_questionnaire_complete_rate
 - UnloginAll_questionnaire_complete_rate
 - UnloginNew_questionnaire_complete_rate
#### テスト内容
rateが0よりも大きく１よりも小さいかどうか。

