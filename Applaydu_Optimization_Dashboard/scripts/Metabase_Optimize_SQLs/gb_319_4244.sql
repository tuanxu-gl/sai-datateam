with tbl_MAU as
(
 select year(client_time) as year
,month(client_time) as month
,concat(year(client_time),' ',MONTHNAME(client_time)) as year_month
,count(distinct user_id) as users
,sum(time_spent) as total_time_spent
,sum(case when (session_id=1 or time_between_sessions>=30) then 1 else 0 end) AS total_sessions
,total_time_spent/total_sessions as time_result
,minute(time_result) || ' min '|| second(time_result) || ' sec ' as `Average Time per Sessions`
 from gcp-bi-elephant-db-gold.applaydu.LAUNCH_RESUME t
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
 WHERE 1=1
 and not(t.game_id=82471 and date(client_time)<'2020-12-14')
 and date(client_time)>=dateadd(year,-2,date_trunc (month,CURRENT_DATE()))
 and date(client_time)<date_trunc (month,CURRENT_DATE())
 	and time_spent>=0
 	and time_spent<86400
 	and date(client_time)>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]])
 and date(client_time)<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and t.country in (select country from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]) 
 	and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]])
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
 from gcp-gfb-sai-tracking-gold.applaydu.tbl_users t
 join (select distinct user_id from gcp-bi-elephant-db-gold.applaydu.USER_ACTIVITY where 1=1 [[AND {{iinstall_source}}]]) using (user_id)
 where server_date>=dateadd(year,-2,date_trunc (month,CURRENT_DATE()))
 and server_date<date_trunc (month,CURRENT_DATE())
 and server_date>=(SELECT min(server_date) from `applaydu.tbl_date_filter` where 1=1 [[AND {{idate}}]])
 and server_date<DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
 and t.country_NAME in (select country_NAME from `applaydu.tbl_country_filter` where 1=1 [[AND {{icountry}}]] [[AND {{iregion}}]]) 
 	and version in (select version from `applaydu.tbl_version_filter` where 1=1 [[AND {{iversion}}]]) 
 group by all
 )
,t_scan_users as (
 select year
,month
,year_month
,count(distinct user_id) as users
,sum(total_time_spent) as sum_total_time_spent
,sum_total_time_spent / users as time_result
,hour(time_result) || ' hour '|| minute(time_result) || ' min '|| second(time_result) || ' sec ' AS time_spent
 from t_users
 where toy_unlocked_by_scan_count>0 or scan_mode_finished_count>0 
 group by all
)
,t_not_scan_users as (
 select year
,month
,year_month
,count(distinct user_id) as users
,sum(total_time_spent) as sum_total_time_spent
,sum_total_time_spent / users as time_result
,hour(time_result) || ' hour '|| minute(time_result) || ' min '|| second(time_result) || ' sec ' AS time_spent
 from t_users
 where toy_unlocked_by_scan_count=0 and scan_mode_finished_count=0 
 group by all
)
select year,month,year_month as `Time`
,tbl_MAU.users as `Users`
,t_scan_users.users as `Scanned Users`
,t_scan_users.users / tbl_MAU.users as `Scan users ratio`
,`Average Time per Sessions`
,t_scan_users.time_spent as `Average Time per scanned user`
,t_not_scan_users.time_spent as `Average Time per NOT scanned user`
from tbl_MAU
 join t_scan_users using (Year,month,year_month)
 join t_not_scan_users using (Year,month,year_month)
order by 1 desc,2 desc