insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4_str)
--main query

select 14 as dashboard_id
            ,256 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Average Time Spent per User' as kpi_name
            ,`Total time spent` as value1,`Total Users` as value2,time_result as value3,CAST(`Average Time per Users` as STRING) as value4_str
        from
        (
        
select 
  SUM(CAST(time_spent AS INT64)) AS `Total time spent`,
  COUNT(DISTINCT user_id) AS `Total Users`,
  SUM(CAST(time_spent AS INT64)) / COUNT(DISTINCT user_id) AS time_result,
  FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Average Time per Users`
from 
  `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
join 
  `applaydu.tbl_shop_filter` sf ON sf.game_id=t.game_id and sf.country=t.country
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and CAST(time_spent AS INT64)>=0
  and CAST(time_spent AS INT64)<86400
)