insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)
--main query

select 14 as dashboard_id
            ,247 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Ratings Google Play' as kpi_name
            ,`rating` as value1
        from
        (
        
select 
 (CAST(custom_tracking AS FLOAT64)) AS rating -- Max Rating in a week
from 
 `applaydu.store_stats`
where 
 event_id=393584 
 and kpi_name IN ('Total Average Rating')
 and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
 and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
 and version IN ('1.0.0')
 and country_name IN (select country_name from `applaydu.tbl_country_filter` where 2=2 )
ORDER BY 
 client_time DESC
LIMIT 1
)