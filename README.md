# session-click-anchor-attribution-model
interval-based attribution model using session anchors and window function inBigQuery.

README

## 概要
このプロジェクトはGA4において、日跨ぎセッションを考慮した上で、 複数あるアンカーページのうち、通過したページの種類ごとにセッション属性を確定して、
BI用に、属性ごとにイベント・セッションを集計したテーブルを作成する実装例です。  

インターバルの確定にはJOINではなくWINDOW関数を用い、粒度崩壊並び多対多爆発を防ぐ設計としています。

## 詳細
 特定のページ（/official-events/を含む全てのページ）、そのセッションの入り口source、デバイスを基準として、 このページを通ったのちの、以下のページセッションとボタンクリックを全てofficial-eventsページに載せる。 

## テーブルイメージとテーブルスキーマ
### テーブルイメージ
event_date	entrance_source	event_page_location	device_category	All_official_events_session	All_questionnaire_entrance_session	All_questionnaire_complete_session	All_LP_after_questionnaire_complete_session	Click_All_event_participattion	Click_All_questionnaire_complete_event_participattion	Click_All_googleform_event_participattion	login_official_events_session	login_questionnaire_entrance_session	login_questionnaire_complete_session	login_LP_after_questionnaire_complete_session	Click_login_event_participattion	Click_login_questionnaire_complete_event_participattion	Click_login_googleform_event_participattion	UnloginAll_official_events_session	UnloginAll_questionnaire_entrance_session	UnloginAll_questionnaire_complete_session	UnloginAll_LP_after_questionnaire_complete_session	Click_UnloginAll_event_participattion	Click_UnloginAll_questionnaire_complete_event_participattion	Click_UnloginAll_googleform_event_participattion	UnloginNew_official_events_session	UnloginNew_questionnaire_entrance_session	UnloginNew_questionnaire_complete_session	UnloginNew_LP_after_questionnaire_complete_session	Click_UnloginNew_event_participattion	Click_UnloginNew_questionnaire_complete_event_participattion	Click_UnloginNew_googleform_event_participattion
2026-02-17	Organic	https://job-q.me/official-events/12/	mobile	2	0	0	0	0	0	0	2	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0
2026-02-17	Organic		tablet	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0
2026-02-17	Organic		smart.tv	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0
2026-02-17	Organic		desktop	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0
2026-02-17	X		mobile	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0



### テーブルスキーマ   
- event_date,DATE
- entrance_source,STRING
- event_page_location,STRING
- device_category,STRING
 以下全てINT64
    -- ★ 全部の属性総計
- All_official_events_session,
- All_questionnaire_entrance_session,
- All_questionnaire_complete_session,
- All_LP_after_questionnaire_complete_session,
- Click_All_event_participattion,
- Click_All_questionnaire_complete_event_participattion,
- Click_All_googleform_event_participattion,


  -- ★ ログインユーザー
- login_official_events_session,
- login_questionnaire_entrance_session,
- login_questionnaire_complete_session,
- login_LP_after_questionnaire_complete_session,
- Click_login_event_participattion,
- Click_login_questionnaire_complete_event_participattion,
- Click_login_googleform_event_participattion,

  -- ★ 未ログインユーザー全て
- UnloginAll_official_events_session,
- UnloginAll_questionnaire_entrance_session,
- UnloginAll_questionnaire_complete_session,
- UnloginAll_LP_after_questionnaire_complete_session,
- Click_UnloginAll_event_participattion,
- Click_UnloginAll_questionnaire_complete_event_participattion,
- Click_UnloginAll_googleform_event_participattion,

  -- ★ 未ログインユーザーで新規登録会員
- UnloginNew_official_events_session,
- UnloginNew_questionnaire_entrance_session,
- UnloginNew_questionnaire_complete_session,
- UnloginNew_LP_after_questionnaire_complete_session,
- Click_UnloginNew_event_participattion,
- Click_UnloginNew_questionnaire_complete_event_participattion,
- Click_UnloginNew_googleform_event_participattion,



 
## 計測環境
### サイトのファネル
/official-events/　ページ
↓ 
"Go"ボタンクリック 
↓ 
''と’’を含むページに遷移。 ボタンを押すことでユーザーごとに発行される。　（このページ以外に、これらをURLにもつページはない）
↓
"Submit"ボタンクリック 
↓
''と’’を含むページに遷移。 ボタンを押すことでユーザーごとに発行される。　（このページ以外に、これらをURLにもつページはない）
↓
'back_to_home'ボタンクリック
↓
''と’’を含むページに遷移　（このページ以外に、これらをURLにもつページはない）
↓
google form 
### GTMのイベント設定
該当のボタンクリックは以下イベントで取れている。
* OfficialEventsClick（"GO"ボタン、"Submit"ボタン、'back_to_home'ボタン）
    * classが付与されているので、Click_class名でクリック箇所を判別できるよう、 event_paramsにClick_Classesを取得。
* AfterQuestionnaireGoogleFormClick
    * google formはclass付与がないため、CSSセレクタで選択。
### GA4→BQのパイプライン
* GA4 analyticsのUIでBQと連携。（Google側に依存。） 

## 要件

* ログインユーザー
    * これらを通らずに以下。
        * offical-events/  より後に、users/add を通らなかったもの （offial-events/ より前に未ログインだった→ログインしたユーザーもこちらに含まれる。）
* 未ログインユーザー
    * official-events/  より後に users/add を通ったもの全て
* 未登録ユーザ・同時登録ユーザー
    * 未ログインユーザーのうちnew_users/which を通ったものとする
    * 未ログインユーザーのうち/new_users/adult_basic/agreement を通ったものとする
　
## 定義
* BIでグラフが欠損しない。（日付・デバイス・入り口source単位で）
* official-eventsページにカウントしたセッション・クリック数が帰属するようにする。
* セッションの日付は発生時（開始日ではない）
* アンカーページ通過時のフラグのgrainはセッション単位(CONCAT(user_pseudo_id,'-',ga_session_id))


## DAG（データフロー）
GTM  →  GA4         → Bigquery GA4_raw　→ Bigquery raw_to_flat　
                                          ↓
　　　　　　　　　　　　　　　　　　　　stg_official_event
                                 　　　　  ↓
　　　　　　　　　　　　　　　　　　　　int_session_base
　　　　　　　　　　　　　　　　　　　　　　　    ↓
　　　　　　　　　　　　　　　　　　　　int_session_flag
         　　　　　　　　　　　　　　　　　　　　↓
　　　　　　　　　　　　　　　int_click_count　　int_session_count　　
         　　　　　　　　　　　　　　　　　　　　↓
　　　　　　　　　　　　　　　　　　　mart_official_events
         　　　　　　　　　　　　　　　　　　　　
　　　　　　　　　　　　　　　　　　　


### flat内の粒度
rawをunnestして、event_paramsを縦持ちにしたもの。


### agg/martのSQL内の粒度（CTE）
* base   
→より後に users/add を通ったもの全て

* date_base 
→日付欠損防止のための日付ベースCTE

* entrance_source_base 
→入り口のsource欠損のためのベースCTE

* device_category_base 
→デバイス欠損防止のためのデバイスベースCTE

* int_session_base 
→イベント粒度で、baseを正規化(utmを抜き出す)、直前の/official-events/をwindowで持つ。
（セッションカウントやクリックカウントを正確にofficial-events/[0-9+]ページに帰属させるため 直近のofficial-events/[0-9+]ページをwindow関数で保持し、ページの帰属元と帰属範囲を確定。）

* int_session_flag 
→セッション粒度で、フラグ、デバイスの確定を行う。

* int_session_count 
→日付、/official-events/ページ、デバイス、入り口sourceごと、ユーザー属性ごとにセッションカウント

* int_click_count 
→日付、/official-events/ページ、デバイス、入り口sourceごと、ユーザー属性ごとにクリックカウント

* mart_official_events 
→日付、/official-events/ページ、デバイス、入り口sourceごとに集計。 



### バックフィル
* CURRENT_DATE INTERVAL 7 DAY ~ CURRENT_DATE INTERVAL 2 DAY の期間をDELETE&INSERT
* 毎日JST09:00に実行

### データ品質管理
* CTE”base  ”のもとになるflatの品質を、rawのイベントパラメータ数をSQLで監視することで担保。 
→rawのパラメータが綺麗にflatに落ちているかを監視。数値の不一致が発生した場合はflatを作成しているバックフィルSQLの見直しを実施する。



  



