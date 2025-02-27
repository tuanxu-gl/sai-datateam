insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with gb4232 as (select 0)
--main query

select 319 as dashboard_id
		,4232 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Number of Users - by Shop' as kpi_name
		,Shop as value1_str,Total_Users as value2
	from
	(
	
select 
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cast(game_id as string), 
    '81335', 'App Store')
    ,'81337', 'Google Play')
    , '82471','AppInChina')
    , '84155','Google Play')
    , '84515','Samsung')
    , '84137','AppInChina') AS Shop,
  COUNT(DISTINCT user_id) AS Total_Users
from 
  `gcp-bi-elephant-db-gold.applaydu.user_activity`
where 
  1=1 
  and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
  and NOT (game_id=82471 and active_date<'2020-12-14')
  and (date(active_date)>='2020-08-10' and date(active_date)<date_sub(current_date(), INTERVAL 3 DAY))
  and CAST(time_spent AS FLOAT64)>=0 
  and CAST(time_spent AS FLOAT64)<86400
  and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )  
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and version>=(select min(version) from `applaydu.tbl_version_filter` where 2=2 ) 
  and version<=(select max(version) from `applaydu.tbl_version_filter` where 2=2 )
GROUP BY 
  Shop
ORDER BY 
  2 DESC
)