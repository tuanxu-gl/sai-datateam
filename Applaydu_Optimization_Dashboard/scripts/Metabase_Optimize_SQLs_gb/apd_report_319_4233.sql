insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str)
WITH gb4233 as (select 0)
,t_users AS (
  select 
    user_id,
    SUM(total_time_spent) AS total_time_spent,
    SUM(toy_unlocked_by_scan_count) AS toy_unlocked_by_scan_count,
    SUM(scan_mode_finished_count) AS scan_mode_finished_count
  from 
    `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
  join 
    (
      select DISTINCT user_id 
      from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
        and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
        and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
        and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    ) USING (user_id)
  join 
    `applaydu.tbl_shop_filter` using (game_id ,country_name)
  where 
    date(server_date)<date_sub(current_date(), INTERVAL 3 DAY)
    and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and t.country_name IN (select country_name from `applaydu.tbl_country_filter` where 2=2  )  
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )  	
  GROUP BY 
    user_id
)
--main query

select 319 as dashboard_id
		,4233 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Physical toys brought to life - Time spent by users who have scanned surprises' as kpi_name
		,`Time spent` as value1_str
	from
	(
	
select `Time spent`
from (
  select 
    COUNT(DISTINCT user_id) AS users,
    SUM(total_time_spent) AS sum_total_time_spent,
    SUM(total_time_spent) / COUNT(DISTINCT user_id) AS time_result,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Time spent`
  from 
    t_users
  where 
    toy_unlocked_by_scan_count>0 
    OR scan_mode_finished_count>0 
)
)