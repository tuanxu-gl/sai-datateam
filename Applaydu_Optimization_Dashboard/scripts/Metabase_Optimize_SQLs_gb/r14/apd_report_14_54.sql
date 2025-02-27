insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
            ,54 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Downloads per Country' as kpi_name
            ,CAST(`Country name` as STRING) as value1_str,`Users` as value2
        from
        (
        
select 
  d_country AS `Country name`,
  SUM(event_count) AS `Users`
from 
  `applaydu.store_stats`
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and country_name in (select country_name from `applaydu.tbl_country_filter` where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )  
  and country_name in (select country_name from `applaydu.tbl_shop_filter` sf where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and game_id in (select game_id from `applaydu.tbl_shop_filter` sf where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and event_id=393584 
  and kpi_name in ('App Units', 'Install Events', 'Install events', 'New Downloads')
GROUP BY 
  d_country
)