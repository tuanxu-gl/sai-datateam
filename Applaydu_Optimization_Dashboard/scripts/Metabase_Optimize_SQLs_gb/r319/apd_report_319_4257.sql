insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)
with gb4257 as (select 0)
--main query

select 319 as dashboard_id
		,4257 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'[Lets Story] Scan Users that created the story' as kpi_name
		,`Users` as value1
	from
	(
	
select 
  EXTRACT(MONTH from client_time) AS Month,
  EXTRACT(YEAR from client_time) AS Year,
  CONCAT(EXTRACT(YEAR from client_time), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS Time,
  COUNT(DISTINCT user_id) AS users
from `gcp-bi-elephant-db-gold.applaydu.custom_install_referral`
join (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
) USING (user_id)
where utm_campaign LIKE '%CLTS%'
  and version>='5.0.0'
  and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
  and date(client_time)<current_date()
  and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
GROUP BY Month, Year, Time
ORDER BY Month, Year ASC
)