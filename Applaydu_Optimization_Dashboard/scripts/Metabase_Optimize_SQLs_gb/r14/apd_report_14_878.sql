insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str)
WITH t_users AS (
  select 
    user_id,
    SUM(sessions_count) AS sessions_count,
    SUM(total_time_spent) AS total_time_spent,
    SUM(toy_unlocked_by_scan_count) AS toy_unlocked_by_scan_count,
    SUM(scan_mode_finished_count) AS scan_mode_finished_count
  from 
    `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
  join 
    `applaydu.tbl_shop_filter` sf ON sf.game_id=t.game_id and sf.country_name=t.country_name
  where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY)
    and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY) )
    and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY) ), INTERVAL 1 DAY)
  GROUP BY 
    user_id
)
--main query

select 14 as dashboard_id
            ,878 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Time spent per session by users who have scanned surprises' as kpi_name
            ,CAST(`Time spent` as STRING) as value1_str
        from
        (
        
select `Time spent`
from (
  select 
    SUM(sessions_count) AS sum_sessions_count,
    SUM(total_time_spent) AS sum_total_time_spent,
    SUM(total_time_spent) / SUM(sessions_count) AS time_result,
    FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / SUM(sessions_count) AS INT64))) AS `Time spent`
  from 
    t_users
  where 
    toy_unlocked_by_scan_count>0 OR scan_mode_finished_count>0 
)
where `Time spent` IS NOT NULL
)