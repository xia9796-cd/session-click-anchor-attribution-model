{{ test_sum_and_range(
    model='int_session_click_anchor_attribute_sessioncount',
    metric='official_events',
    all_col='All_official_events_session',
    login_col='login_official_events_session',
    unlogin_col='UnloginAll_official_events_session',
    unloginnew_col='UnloginNew_official_events_session'
) }}

UNION ALL

{{ test_sum_and_range(
    model='int_session_click_anchor_attribute_sessioncount',
    metric='questionnaire_entrance',
    all_col='All_questionnaire_entrance_session',
    login_col='login_questionnaire_entrance_session',
    unlogin_col='UnloginAll_questionnaire_entrance_session',
    unloginnew_col='UnloginNew_questionnaire_entrance_session'
) }}

UNION ALL

{{ test_sum_and_range(
    model='int_session_click_anchor_attribute_sessioncount',
    metric='questionnaire_complete',
    all_col='All_questionnaire_complete_session',
    login_col='login_questionnaire_complete_session',
    unlogin_col='UnloginAll_questionnaire_complete_session',
    unloginnew_col='UnloginNew_questionnaire_complete_session'
) }}

UNION ALL

{{ test_sum_and_range(
    model='int_session_click_anchor_attribute_sessioncount',
    metric='lp_after_complete',
    all_col='All_LP_after_questionnaire_complete_session',
    login_col='login_LP_after_questionnaire_complete_session',
    unlogin_col='UnloginAll_LP_after_questionnaire_complete_session',
    unloginnew_col='UnloginNew_LP_after_questionnaire_complete_session'
) }}
