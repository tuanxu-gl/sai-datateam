insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3)
with gb4248 as (select 0)
--main query

select 319 as dashboard_id
		,4248 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Number of Sessions' as kpi_name
		,`Number of Sessions` as value1,Total_Users as value2,`Average Session per User` as value3
	from
	(
	
select 
  SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT)>=30) THEN 1 ELSE 0 END) AS `Number of Sessions`,
  COUNT(DISTINCT user_id) AS Total_Users,
  SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT)>=30) THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) AS `Average Session per User`
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
  CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and (date(client_time)>='2020-08-10' )
  and version>=(select min(version) from `applaydu.tbl_version_filter` where 2=2 ) 
  and version<=(select max(version) from `applaydu.tbl_version_filter` where 2=2 )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and t.country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
)