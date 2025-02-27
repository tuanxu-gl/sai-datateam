insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
            ,450 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Daily Downloads' as kpi_name
            ,CAST(`Date` as STRING) as value1_str,`Daily Downloads` as value2
        from
        (
        
select 
  date(client_time) AS `Date`,
  SUM(event_count) AS `Daily Downloads`
from 
  `gcp-gfb-sai-tracking-gold.applaydu.store_stats` t
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) and
  event_id=393584 
  and kpi_name in ('App Units', 'Install Events', 'Install events', 'New Downloads')
  and version in ('1.0.0')
GROUP BY 
  date(client_time)
)