insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
            ,916 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Number of Users' as kpi_name
            ,CAST(`Shop` as STRING) as value1_str,`Total Users` as value2
        from
        (
        
select 
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cast(game_id as STRING), 
    '81335', 'App Store')
    ,'81337', 'Google Play')
    , '82471','AppInChina')
    , '84155','Google Play')
    , '84515','Samsung')
    , '84137','AppInChina') 
    , '85837','Amazon') AS `Shop`,
  COUNT(DISTINCT user_id) AS `Total Users`
from 
  `gcp-bi-elephant-db-gold.applaydu.launch_resume`
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and NOT (game_id=82471 and client_time<'2020-12-14')
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
GROUP BY 
  `Shop`
)