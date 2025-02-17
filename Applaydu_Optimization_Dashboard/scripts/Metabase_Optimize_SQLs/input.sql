with tbl_MAU as
(
    select year(client_time) as year
        ,month(client_time) as month
        ,concat(year(client_time),' ',MONTHNAME(client_time)) as year_month
        ,count(distinct USER_ID) as users
        ,sum(time_spent::int) as total_time_spent
        ,sum(case when (session_id=1 or time_between_sessions::int>=30) then 1 else 0 end) AS total_sessions
        ,total_time_spent/total_sessions as time_result
        ,minute(time_result::int::string::time) || ' min '|| second(time_result::int::string::time) || ' sec ' as "Average Time per Sessions"
    from APPLAYDU.LAUNCH_RESUME t
    join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[AND {{iINSTALL_SOURCE}}]]) using (user_id)
       
    WHERE 1=1
        and not(t.game_id = 82471 and client_time <'2020-12-14')
        and client_time >= dateadd(year, -2,date_trunc (month , CURRENT_DATE()))
        and client_time < date_trunc (month , CURRENT_DATE())
    	and time_spent::float >= 0
    	and time_spent::float < 86400
    	and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] )
        and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] ))
        and t.country in (select country from tbl_country_filter where 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])    
    	and version in (select version from tbl_version_filter where 1=1  [[AND {{iversion}}]])
    group by all
)
,t_users as
(
    select user_id
        ,year(server_date) as year
        ,month(server_date) as month
        ,concat(year(server_date),' ',MONTHNAME(server_date)) as year_month
        ,sum (total_time_spent) as total_time_spent
        ,sum (toy_unlocked_by_scan_count) as toy_unlocked_by_scan_count
        ,sum (scan_mode_finished_count) as scan_mode_finished_count
        
        
    from APPLAYDU_NOT_CERTIFIED.tbl_users t
    join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[AND {{iINSTALL_SOURCE}}]]) using (user_id)
    where server_date >= dateadd(year, -2,date_trunc (month , CURRENT_DATE()))
        and server_date < date_trunc (month , CURRENT_DATE())
        and server_date >= (SELECT min(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] )
        and server_date < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 [[AND {{idate}}]] ))
        and t.COUNTRY_NAME in (select COUNTRY_NAME from tbl_country_filter where 1=1  [[AND {{icountry}}]] [[AND {{iregion}}]])   
    	and version in (select version from tbl_version_filter where 1=1  [[AND {{iversion}}]])   
    group by all
  )
,t_scan_users as (
    select year
        ,month
        ,year_month
        ,count(distinct user_id) as users
        ,sum(total_time_spent) as sum_total_time_spent
        ,sum_total_time_spent / users as time_result
        ,hour(time_result::int::string::time) || ' hour '|| minute(time_result::int::string::time) || ' min '|| second(time_result::int::string::time) || ' sec ' AS time_spent
    from t_users
    where toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0 
         
    group by all
)
,t_not_scan_users as (
   select year
        ,month
        ,year_month
        ,count(distinct user_id) as users
        ,sum(total_time_spent) as sum_total_time_spent
        ,sum_total_time_spent / users as time_result
        ,hour(time_result::int::string::time) || ' hour '|| minute(time_result::int::string::time) || ' min '|| second(time_result::int::string::time) || ' sec ' AS time_spent
    from t_users
    where toy_unlocked_by_scan_count = 0 and  scan_mode_finished_count = 0 
        
    group by all
)
   
select year,month,year_month as "Time"
    ,tbl_MAU.users as "Users"
    ,t_scan_users.users as "Scanned Users"
    ,t_scan_users.users / tbl_MAU.users as "Scan users ratio"
    ,"Average Time per Sessions"
    ,t_scan_users.time_spent as "Average Time per scanned user"
    ,t_not_scan_users.time_spent as "Average Time per NOT scanned user"
from tbl_MAU
    join t_scan_users using (Year,month,year_month)
    join t_not_scan_users using (Year,month,year_month)
order by 1 desc, 2 desc