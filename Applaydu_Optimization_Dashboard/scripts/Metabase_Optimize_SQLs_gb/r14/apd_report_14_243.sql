insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
            ,243 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Top 30 countries' as kpi_name
            ,CAST(`Country name` as STRING) as value1_str,`Downloads` as value2
        from
        (
        
select 
  REPLACE(country_name, 'Undefined', '(no country code)') AS `Country name`,
  SUM(event_count) AS `Downloads`
from 
  `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
where 
  1=1 
  and country_name in (select country_name from `applaydu.tbl_country_filter` where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) )
  and event_id=393584 
  and kpi_name in ('App Units', 'Install Events', 'Install events', 'New Downloads')
  and version in ('1.0.0')
GROUP BY 
  country_name
LIMIT 30
)