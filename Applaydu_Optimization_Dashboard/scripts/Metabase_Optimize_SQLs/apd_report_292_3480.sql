
insert into APPLAYDU_NOT_CERTIFIED.apd_report_292 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4,value5,value6_str)
with tbl_apd_users as 
(
   


select count(distinct user_id) as users
    from ELEPHANT_DB.APPLAYDU.LAUNCH_RESUME
    where 1=1
      and version >= '5.0.0'
      and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date') 
      and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
      
      
      
      
)
  
, tbl_users_launch_lets_story as 
(
    select count(distinct user_id) as users
        , count(0) as lets_story_sessions
    from ELEPHANT_DB.APPLAYDU.VISIT_SCREEN
    where (screen_from in ('World Map', 'Mini Game Screen') or screen_from like 'Eduland%Minigame Menu') and screen_to = 'Eduland Lets Story'
      and version >= '5.0.0'
      and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date') and 
      client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
      
      
  )
,  tbl_users_launch_lets_story_time_spent as 
(
    select sum(time_spent) / count (distinct user_id) as time_spent_per_user
    from ELEPHANT_DB.APPLAYDU.VISIT_SCREEN
    where screen_from like 'Eduland Lets Story%'
      and version >= '5.0.0'
      and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date') 
        and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
      
      
      and time_spent > 0 and time_spent < 36000
  )

select 292 as dashboard_id
    ,3480 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Funnel for FTUE enter Lets Story Eduland' as kpi_name
    ,"Number of user launch Let's Story" as  value1
    ,"Number of Let's Story sessions" as value2
    ,"% APD users access Let's Story eduland" as value3
    ,"AVG Number of Let's Story sessions per user" as value4
    ,avg_time_spent as value5
    ,"Average time spent per user in Let's Story" as value6_str
    
from
(
 select (select users from tbl_users_launch_lets_story) as "Number of user launch Let's Story"
    ,(select lets_story_sessions from tbl_users_launch_lets_story) as "Number of Let's Story sessions"
    ,"Number of user launch Let's Story" /(select users from tbl_apd_users) as "% APD users access Let's Story eduland"
    ,"Number of Let's Story sessions"/"Number of user launch Let's Story" as "AVG Number of Let's Story sessions per user"
    , (select time_spent_per_user from tbl_users_launch_lets_story_time_spent) as avg_time_spent
    , hour (avg_time_spent::int::string::time) || ' hour '|| minute(avg_time_spent::int::string::time) || ' min '|| second(avg_time_spent::int::string::time) || ' sec ' as "Average time spent per user in Let's Story"

)