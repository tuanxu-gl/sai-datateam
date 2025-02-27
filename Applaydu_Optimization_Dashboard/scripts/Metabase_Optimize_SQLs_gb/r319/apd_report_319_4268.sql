insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3_str,value4,value5,value6,value7_str,value8_str,value9_str)
with gb4268 as (select 0)
,tbl_mau as
(
  select EXTRACT(YEAR from client_time) as year
    ,EXTRACT(MONTH from client_time) as month
    ,concat(EXTRACT(YEAR from client_time),' ',FORMAT_TIMESTAMP('%B', client_time)) as year_month
    ,count(distinct user_id) as users
    ,sum(cast(time_spent as int)) as total_time_spent
    ,sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as total_sessions
    ,sum(cast(time_spent as int)) / sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as time_result
    ,concat(EXTRACT(MINUTE from TIMESTAMP_SECONDS(cast(sum(cast(time_spent as int)) / sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as int))), ' min ', EXTRACT(SECOND from TIMESTAMP_SECONDS(cast(sum(cast(time_spent as int)) / sum(case when (session_id=1 or cast(time_between_sessions as int)>=30) then 1 else 0 end) as int))), ' sec') as `Average Time per Sessions`
  from `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
  join (
    select distinct user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    and install_source in (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
    and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
  ) using (user_id)
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
    and not(t.game_id=82471 and client_time<'2020-12-14')
    and date(client_time)>=date_sub(date_trunc(current_date(), month), interval 2 year)
    and date(client_time)<date_trunc(current_date(), month)
    and cast(time_spent as FLOAT64)>=0
    and cast(time_spent as FLOAT64)<86400
    and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
    and t.country in (select country from `applaydu.tbl_country_filter` where 2=2  )  
    and version in (select version from `applaydu.tbl_version_filter` where 2=2 )
  group by year, month, year_month
)
,t_users as
(
  select user_id
    ,EXTRACT(YEAR from server_date) as year
    ,EXTRACT(MONTH from server_date) as month
    ,concat(EXTRACT(YEAR from server_date),' ',FORMAT_TIMESTAMP('%B', server_date)) as year_month
    ,sum(total_time_spent) as total_time_spent
    ,sum(toy_unlocked_by_scan_count) as toy_unlocked_by_scan_count
    ,sum(scan_mode_finished_count) as scan_mode_finished_count
  from `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
  join (
    select distinct user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    and install_source in (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
    and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
  ) using (user_id)
  where date(server_date)>=date_sub(date_trunc(current_date(), month), interval 2 year)
    and date(server_date)<date_trunc(current_date(), month)
    and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), interval 1 day)
    and t.country_name in (select country_name from `applaydu.tbl_country_filter` where 2=2  )  
    and version in (select version from `applaydu.tbl_version_filter` where 2=2 )  
  group by user_id, year, month, year_month
)
,t_scan_users as (
  select year
    ,month
    ,year_month
    ,count(distinct user_id) as users
    ,sum(total_time_spent) as sum_total_time_spent
    ,sum(total_time_spent) / count(distinct user_id) as time_result
    ,concat(EXTRACT(HOUR from TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' hour ', EXTRACT(MINUTE from TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' min ', EXTRACT(SECOND from TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' sec') as time_spent
  from t_users
  where toy_unlocked_by_scan_count>0 or scan_mode_finished_count>0 
  group by year, month, year_month
)
,t_not_scan_users as (
  select year
    ,month
    ,year_month
    ,count(distinct user_id) as users
    ,sum(total_time_spent) as sum_total_time_spent
    ,sum(total_time_spent) / count(distinct user_id) as time_result
    ,concat(EXTRACT(HOUR from TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' hour ', EXTRACT(MINUTE from TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' min ', EXTRACT(SECOND from TIMESTAMP_SECONDS(cast(sum(total_time_spent) / count(distinct user_id) as int))), ' sec') as time_spent
  from t_users
  where toy_unlocked_by_scan_count=0 and scan_mode_finished_count=0 
  group by year, month, year_month
)
--main query

select 319 as dashboard_id
		,4268 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Number of mau, scans & ratio' as kpi_name
		,`year` as value1,`month` as value2,`Time` as value3_str,`Users` as value4,`Scanned Users` as value5,`Scan users ratio` as value6,`Average Time per Sessions` as value7_str,`Average Time per scanned user` as value8_str,`Average Time per NOT scanned user` as value9_str
	from
	(
	
select year, month, year_month as `Time`
  ,tbl_mau.users as `Users`
  ,t_scan_users.users as `Scanned Users`
  ,t_scan_users.users / tbl_mau.users as `Scan users ratio`
  ,`Average Time per Sessions`
  ,t_scan_users.time_spent as `Average Time per scanned user`
  ,t_not_scan_users.time_spent as `Average Time per NOT scanned user`
from tbl_mau
  join t_scan_users using (year, month, year_month)
  join t_not_scan_users using (year, month, year_month)
order by year asc, month asc
)