insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with gb4261 as (select 0)
--main query

select 319 as dashboard_id
		,4261 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Weekly Active Users' as kpi_name
		,`Start of Week` as value1_str,`Active Users` as value2
	from
	(
	
select cast(DATE_TRUNC(date(client_time), WEEK) as string) AS `Start of Week`,
    COUNT(DISTINCT user_id) AS `Active Users` 
from `gcp-bi-elephant-db-gold.applaydu.launch_resume` a
join (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
) USING (user_id)
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and NOT (game_id=82471 and client_time<'2020-12-14')
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
GROUP BY `Start of Week`
ORDER BY `Start of Week`
)