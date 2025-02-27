insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3_str,value4)
--main query

select 14 as dashboard_id
            ,252 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Monthly Active Users' as kpi_name
            ,`Month` as value1,`Year` as value2,CAST(`Time` as STRING) as value3_str,`Monthly Active Users` as value4
        from
        (
        
select 
  EXTRACT(MONTH from client_time) AS `Month`,
  EXTRACT(YEAR from client_time) AS `Year`,
  CONCAT(CAST(EXTRACT(YEAR from client_time) AS STRING), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS `Time`,
  COUNT(DISTINCT user_id) AS `Monthly Active Users`
from 
  `gcp-bi-elephant-db-gold.applaydu.launch_resume`
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and NOT (game_id=82471 and client_time<'2020-12-14')
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and country in (select country from `applaydu.tbl_country_filter` where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )  
GROUP BY all
)