insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5,value6)
--main query

select 319 as dashboard_id
		,4229 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'ENGAGE | FOCUS - Users & Scan per season' as kpi_name
		,`Season` as value1_str,`Users who have scanned surprises` as value2,sum_toy_unlocked_count as value3,sum_scan_mode_finished_count as value4,`Total Scans` as value5,`Average Toys Scanned per User` as value6
	from
	(
	
select 
  CONCAT('Season ', LEFT(version, 1)) AS `Season`,
  COUNT(DISTINCT user_id) AS `Users who have scanned surprises`,
  SUM(toy_unlocked_by_scan_count) AS sum_toy_unlocked_count,
  SUM(scan_mode_finished_count) AS sum_scan_mode_finished_count,
  SUM(toy_unlocked_by_scan_count) + SUM(scan_mode_finished_count) AS `Total Scans`,
  (SUM(toy_unlocked_by_scan_count) + SUM(scan_mode_finished_count)) / COUNT(DISTINCT user_id) AS `Average Toys Scanned per User`
from 
  `gcp-gfb-sai-tracking-gold.applaydu.tbl_users`
join 
  (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
      and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
      and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
      and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  ) USING (user_id)
where 
  date(server_date)<date_sub(current_date(), INTERVAL 3 DAY)
  and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country_name IN (select country_name from `applaydu.tbl_country_filter` where 2=2  )  
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )  	
  and (toy_unlocked_by_scan_count>0 OR scan_mode_finished_count>0)
  and version LIKE ANY ('5.%','4.%','3.%')
GROUP BY 
  1
ORDER BY 
  1
)