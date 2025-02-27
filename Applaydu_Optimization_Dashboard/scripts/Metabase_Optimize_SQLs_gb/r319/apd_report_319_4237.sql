insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4_str)
with gb4237 as (select 0)
--main query

select 319 as dashboard_id
		,4237 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Average Time per Session' as kpi_name
		,`Total time spent` as value1,`Total Session` as value2,time_result as value3,`Average Time per Users` as value4_str
	from
	(
	
select 
  SUM(CAST(time_spent AS INT64)) AS `Total time spent`,
  SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT64)>=30) THEN 1 ELSE 0 END) AS `Total Session`,
  SUM(CAST(time_spent AS INT64)) / SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT64)>=30) THEN 1 ELSE 0 END) AS time_result,
  FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(CAST(time_spent AS INT64)) / SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT64)>=30) THEN 1 ELSE 0 END) AS INT64))) AS `Average Time per Users`
from 
  `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
join 
  (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` 
    where 2=2 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
      and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
      and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
      and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  ) USING (user_id)
where 
  CAST(time_spent AS INT64)>=0
  and CAST(time_spent AS INT64)<86400
  and version>=(select min(version) from `applaydu.tbl_version_filter` where 2=2 ) 
  and version<=(select max(version) from `applaydu.tbl_version_filter` where 2=2 )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and t.country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and (date(client_time)>='2020-08-10' )
)