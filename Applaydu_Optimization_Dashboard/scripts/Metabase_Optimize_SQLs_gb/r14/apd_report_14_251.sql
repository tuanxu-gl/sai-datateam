insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
            ,251 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Daily Active Users' as kpi_name
            ,CAST(`Client time` as STRING) as value1_str,`DAU` as value2
        from
        (
        
select 
  date(client_time) AS `Client time`,
  COUNT(DISTINCT user_id) AS `DAU`
from 
  `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and NOT (t.game_id=82471 and client_time<'2020-12-14')
GROUP BY 
  date(client_time)
ORDER BY 
  date(client_time) ASC
)