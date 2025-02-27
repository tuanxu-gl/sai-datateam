insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)
WITH gb4246 as (select 0)
,tbl_utm AS (
  select 
    user_id,
    game_id,
    client_time,
    utm_campaign AS pack,
    'Deep Link' AS scan_type
  from `gcp-bi-elephant-db-gold.applaydu.custom_install_referral`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where utm_campaign LIKE '%CLTS%'
    and game_id<>81335
    and version>='5.0.0'
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
),
tbl_scan_mode AS (
  select 
    user_id,
    game_id,
    client_time,
    RIGHT(reference, 7) AS pack,
    scan_type
  from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where reference LIKE '%CLTS%'
    and NOT (game_id !=81335 and scan_type='Deep Link')
    and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v5_lets_story_start_date')
    and date(client_time)<current_date()
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and scan_result IN ('New_Toy', 'Old_Toy')
)
--main query

select 319 as dashboard_id
		,4246 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Total Scan Users' as kpi_name
		,`Total Scan Users` as value1
	from
	(
	
select COUNT(DISTINCT user_id) AS `Total Scan Users`
from (
  select * from tbl_utm 
  UNION ALL 
  select * from tbl_scan_mode
)
)