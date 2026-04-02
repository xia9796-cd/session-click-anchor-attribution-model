{{ test_sum_and_range(
    model='mart_session_click_anchor_attribute',
    metric='event_participation_click',
    all_col='Click_All_event_participation',
    login_col='Click_login_event_participation',
    unlogin_col='Click_UnloginAll_event_participation',
    unloginnew_col='Click_UnloginNew_event_participation'
) }}

UNION ALL

{{ test_sum_and_range(
    model='mart_session_click_anchor_attribute',
    metric='questionnaire_complete_click',
    all_col='Click_All_questionnaire_complete_event_participation',
    login_col='Click_login_questionnaire_complete_event_participation',
    unlogin_col='Click_UnloginAll_questionnaire_complete_event_participation',
    unloginnew_col='Click_UnloginNew_questionnaire_complete_event_participation'
) }}

UNION ALL

{{ test_sum_and_range(
    model='mart_session_click_anchor_attribute',
    metric='googleform_click',
    all_col='Click_All_googleform_event_participation',
    login_col='Click_login_googleform_event_participation',
    unlogin_col='Click_UnloginAll_googleform_event_participation',
    unloginnew_col='Click_UnloginNew_googleform_event_participation'
) }}

