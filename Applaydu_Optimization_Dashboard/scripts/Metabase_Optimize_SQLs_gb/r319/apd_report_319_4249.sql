insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3_str,value4)
with gb4249 as (select 0)
--main query

select 319 as dashboard_id
		,4249 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Monthly Active Users (MAU)' as kpi_name
		,`Month` as value1,`Year` as value2,`Time` as value3_str,`Monthly Active Users` as value4
	from
	(
	
select 
  EXTRACT(MONTH from client_time) AS `Month`,
  EXTRACT(YEAR from client_time) AS `Year`,
  CONCAT(EXTRACT(YEAR from client_time), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS `Time`,
  COUNT(DISTINCT user_id) AS `Monthly Active Users`
from `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
join (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
) USING (user_id)
join `applaydu.tbl_shop_filter` using (game_id ,country) 
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and NOT (t.game_id=82471 and client_time<'2020-12-14')
  and (client_time>='2020-08-10' and client_time<TIMESTAMP(date_sub(current_date(), INTERVAL 3 DAY)))
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and client_time>=TIMESTAMP((select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ))
  and client_time<TIMESTAMP(date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY))
  and t.country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
GROUP BY all
ORDER BY `Year` ASC, `Month` ASC
)