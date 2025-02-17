insert into APPLAYDU_NOT_CERTIFIED.apd_report_294 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4,value5,value6,value7_str)
with tbl_apd_users as 
(
    select count (distinct user_id) as users
    from applaydu.launch_resume
    where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
),
tbl_kdr_users as 
(
    select count(distinct user_id) as users, count(0) as kdr_sessions
    from applaydu.visit_screen
        where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
          and screen_to = 'Eduland Kinderini'
          and (screen_from in ('World Map', 'Mini Game Screen','Scan Mode') or screen_from like 'Eduland%Minigame Menu') 
),
tbl_kdr_users_time_spent as 
(
    select sum(time_spent)/count(distinct user_id) as time_spent_per_user
    from applaydu.visit_screen
        where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
          and screen_from like 'Eduland Kinderini%' 
)
--main query

select 294 as dashboard_id
		,3754 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Overall KPIs Eduland Kinderini' as kpi_name
		,"Number of user launch Kinderini" as value1,"APD users" as value2,"Number of Kinderini sessions" as value3,"% APD users launch Kinderini" as value4,avg_time_spent as value5,"AVG Number of Kinderini sessions per user" as value6,"Average time spent per user in Kinderini" as value7_str
	from
	(
	
 select (select users from tbl_kdr_users) as "Number of user launch Kinderini"
    ,(select users from tbl_apd_users) as "APD users"
    ,(select kdr_sessions from tbl_kdr_users) as "Number of Kinderini sessions"
    ,"Number of user launch Kinderini" /(select users from tbl_apd_users) as "% APD users launch Kinderini"
    ,"Number of Kinderini sessions"/"Number of user launch Kinderini" as "AVG Number of Kinderini sessions per user"
    , (select time_spent_per_user from tbl_kdr_users_time_spent) as avg_time_spent
    , hour (avg_time_spent::int::string::time) || ' hour '|| minute(avg_time_spent::int::string::time) || ' min '|| second(avg_time_spent::int::string::time) || ' sec ' as "Average time spent per user in Kinderini"
)