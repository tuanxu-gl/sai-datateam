with r3754 as(
SELECT value1 as "Number of user launch Kinderini",value2 as "APD users",value3 as "Number of Kinderini sessions",value4 as "% APD users launch Kinderini",value5 as avg_time_spent,value6 as "AVG Number of Kinderini sessions per user",value7_str as "Average time spent per user in Kinderini"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_294
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=294 and query_id = 3754 
)
,tbl_check_preprocess_report as
(
SELECT CASE 
    WHEN (
        SELECT COUNT(0) 
        FROM APPLAYDU_NOT_CERTIFIED.apd_report_294
        WHERE 1=1
        AND start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
		 and dashboard_id=294 and query_id = 3754
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, tbl_apd_users as 
(
    select count (distinct user_id) as users
    from applaydu.launch_resume
    where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
      and client_time < current_date()
      and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] )
      and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] ))
      and country in (select country from tbl_country_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{icountry}}]])
      and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
),
tbl_kdr_users as 
(
    select count(distinct user_id) as users, count(0) as kdr_sessions
    from applaydu.visit_screen
        where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
          and client_time < current_date()
          and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] )
          and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] ))
          and country in (select country from tbl_country_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{icountry}}]])
          and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
          and screen_to = 'Eduland Kinderini'
          and (screen_from in ('World Map', 'Mini Game Screen','Scan Mode') or screen_from like 'Eduland%Minigame Menu') 
),
tbl_kdr_users_time_spent as 
(
    select sum(time_spent)/count(distinct user_id) as time_spent_per_user
    from applaydu.visit_screen
        where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')
          and client_time < current_date()
          and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] )
          and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] ))
          and country in (select country from tbl_country_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{icountry}}]])
          and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
          and screen_from like 'Eduland Kinderini%' 
)

select * from r3754
union
select * from
(

 select (select users from tbl_kdr_users) as "Number of user launch Kinderini"
    ,(select users from tbl_apd_users) as "APD users"
    ,(select kdr_sessions from tbl_kdr_users) as "Number of Kinderini sessions"
    ,"Number of user launch Kinderini" /(select users from tbl_apd_users) as "% APD users launch Kinderini"
    ,"Number of Kinderini sessions"/"Number of user launch Kinderini" as "AVG Number of Kinderini sessions per user"
    , (select time_spent_per_user from tbl_kdr_users_time_spent) as avg_time_spent
    , hour (avg_time_spent::int::string::time) || ' hour '|| minute(avg_time_spent::int::string::time) || ' min '|| second(avg_time_spent::int::string::time) || ' sec ' as "Average time spent per user in Kinderini"
)
where "Number of user launch Kinderini" > 0
