insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_999` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 999 as dashboard_id
            ,249 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Total Downloads' as kpi_name
            ,Shop as value1_str,`Total Installations` as value2
        from
        (
        
select 
  REPLACE(country_name, 'Undefined', '(no country code)' ) as "Country name",
  sum(event_count) AS "Downloads"
from APPLAYDU_NOT_CERTIFIED.store_stats
where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_sub('iend_date', INTERVAL 3 DAY) and (select available from tbl_check_preprocess_report)='N/A' 
  and client_time>=(select min(server_date) from tbl_date_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_sub('iend_date', INTERVAL 3 DAY) ) and client_time<dateadd(day, 1,(select max(server_date) from tbl_date_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_sub('iend_date', INTERVAL 3 DAY) ))
  and country_name in (select country_name from tbl_country_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_sub('iend_date', INTERVAL 3 DAY) ) 
  and version in (select version from tbl_version_filter where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_sub('iend_date', INTERVAL 3 DAY)  ) 
  and event_id=393584 
  and kpi_name in ('App Units','Install Events','Install events','New Downloads')
  and client_time>='2020-08-10' and client_time<dateadd(day, -3, current_date())
  and VERSION IN ('1.0.0')
group by country_name
limit 30
)