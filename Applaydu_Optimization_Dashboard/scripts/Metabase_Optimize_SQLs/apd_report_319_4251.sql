insert into APPLAYDU_NOT_CERTIFIED.apd_report_319 (dashboard_id, query_id, start_date, end_date, load_time, kpi_name, value1, value2, value3, value4, value5_str)

with 
scan_kdr_users as (
    select distinct user_id 
    from ELEPHANT_DB.APPLAYDU.SCAN_MODE_FINISHED
    where server_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
          and client_time >= 'istart_date' 
      and client_time < dateadd(day, 1, 'iend_date')
      and scan_result in ('New_Toy','Old_Toy')
      and ((scan_type ='Deep Link' and upper(reference) like '%KINDERINI%')
            or (scan_type in ('Scan_QR_Biscuit','Scan_Toy_Biscuit') )) 
),
tbl_scan_kdr_users as (
    select count(distinct user_id) as users, count(0) as kdr_sessions
    from applaydu.visit_screen
    where server_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
          and client_time >= 'istart_date' 
      and client_time < dateadd(day, 1, 'iend_date')
      and screen_to = 'Eduland Kinderini'
      and (screen_from in ('World Map', 'Mini Game Screen','Scan Mode') or screen_from like 'Eduland%Minigame Menu') 
      and user_id in (select distinct user_id from scan_kdr_users)
),
tbl_kdr_users_time_spent as (
    select sum(time_spent)/count(distinct user_id) as time_spent_per_user
    from applaydu.visit_screen
    where server_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
          and client_time >= 'istart_date' 
      and client_time < dateadd(day, 1, 'iend_date')
      and screen_from like 'Eduland Kinderini%' 
      and user_id in (select distinct user_id from scan_kdr_users)
)

select 319 as dashboard_id
    ,4251 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Overall KPIs Eduland Kinderini for scan user' as kpi_name
    ,"Number of scan user Kinderini" as value1
,"Number of Kinderini sessions" as value2
   ,"AVG Number of Kinderini sessions per user" as value3
   ,avg_time_spent as value4
   ,"Average time spent per user in Kinderini" as value5_str
from
(
 select (select users from tbl_scan_kdr_users) as "Number of scan user Kinderini"
    ,(select kdr_sessions from tbl_scan_kdr_users) as "Number of Kinderini sessions"
    ,"Number of Kinderini sessions"/"Number of scan user Kinderini" as "AVG Number of Kinderini sessions per user"
    , (select time_spent_per_user from tbl_kdr_users_time_spent) as avg_time_spent
    , hour (avg_time_spent::int::string::time) || ' hour '|| minute(avg_time_spent::int::string::time) || ' min '|| second(avg_time_spent::int::string::time) || ' sec ' as "Average time spent per user in Kinderini"
)