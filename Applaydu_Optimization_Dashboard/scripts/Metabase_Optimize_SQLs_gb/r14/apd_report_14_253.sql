insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)
--main query

select 14 as dashboard_id
            ,253 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Number of Sessions' as kpi_name
            ,`Number of Sessions` as value1
        from
        (
        
select 
  COUNT(*) AS `Number of Sessions`
from 
  `gcp-bi-elephant-db-gold.applaydu.launch_resume`
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY)
  and CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and (session_id=1 OR CAST(time_between_sessions AS INT64)>=30)
  and country in (select country from `applaydu.tbl_country_filter` where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )  
)