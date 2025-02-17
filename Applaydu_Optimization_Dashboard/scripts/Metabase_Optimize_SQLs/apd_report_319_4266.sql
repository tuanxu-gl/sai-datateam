insert into APPLAYDU_NOT_CERTIFIED.apd_report_319 (dashboard_id, query_id, start_date, end_date, load_time, kpi_name, value1
    ,value2
    ,value3
    ,value4
    , value5_str)

select 319 as dashboard_id
    ,4266 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Overall KPIs Eduland LS for Scan Users' as kpi_name
    ,"Number of scan user launch Let's Story" as value1
    ,"Number of Let's Story sessions" as value2
    ,"AVG Number of Let's Story sessions per user" as value3
    ,avg_time_spent as value4
    ,"Average time spent per user in Let's Story" as value5_str
    
from
(
    with tbl_ls_scan_users as 
    (
        select distinct user_id
        from
        (
            select user_id
            from ELEPHANT_DB.APPLAYDU.CUSTOM_INSTALL_REFERRAL
            join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 ) using (user_id)
            where utm_campaign like '%KCLTS%'
                and version >= '5.0.0'
                and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
                
            union all 
            select user_id
            from ELEPHANT_DB.APPLAYDU.SCAN_MODE_FINISHED
            join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 ) using (user_id)
            where ((REFERENCE like '%KCLTS%' and not (game_id != 81335 and scan_type = 'Deep Link')) or scan_type = 'Scan_QR_LS')
                and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
                
                and scan_result in ('New_Toy','Old_Toy')
        )
    ), 
    tbl_ls_scan_users_launch_lets_story as 
    (
        select count(distinct user_id) as users
            , count(0) as lets_story_sessions
        from ELEPHANT_DB.APPLAYDU.VISIT_SCREEN
        join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 ) using (user_id)
        where (screen_from in ('World Map', 'Mini Game Screen') or screen_from like 'Eduland%Minigame Menu') and screen_to = 'Eduland Lets Story'
            and version >= '5.0.0'
            and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
            and client_time < dateadd(day, 1, 'iend_date')
            and user_id in (select distinct user_id from tbl_ls_scan_users)
    ),  
    tbl_users_launch_lets_story_time_spent as (
        select sum(time_spent) / count (distinct user_id) as time_spent_per_user
        from ELEPHANT_DB.APPLAYDU.VISIT_SCREEN
        join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 ) using (user_id)
        where screen_from like 'Eduland Lets Story%'
            and version >= '5.0.0'
            and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
            and client_time < dateadd(day, 1, 'iend_date')
            and time_spent > 0 and time_spent < 36000
            and user_id in (select distinct user_id from tbl_ls_scan_users)
    )

    select (select count(distinct user_id) from tbl_ls_scan_users) as "Number of scan user launch Let's Story"
        ,(select lets_story_sessions from tbl_ls_scan_users_launch_lets_story) as "Number of Let's Story sessions"
        ,"Number of Let's Story sessions"/"Number of scan user launch Let's Story" as "AVG Number of Let's Story sessions per user"
        , (select time_spent_per_user from tbl_users_launch_lets_story_time_spent) as avg_time_spent
        , hour (avg_time_spent::int::string::time) || ' hour '|| minute(avg_time_spent::int::string::time) || ' min '|| second(avg_time_spent::int::string::time) || ' sec ' as "Average time spent per user in Let's Story"
)