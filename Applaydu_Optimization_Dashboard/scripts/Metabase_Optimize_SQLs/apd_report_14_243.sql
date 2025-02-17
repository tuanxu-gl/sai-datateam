insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q243 as (select 0)
--main query

select 14 as dashboard_id
		,243 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Top 30 countries' as kpi_name
		,"Country name" as value1_str,"Downloads" as value2
	from
	(
	
SELECT 
    REPLACE(COUNTRY_NAME, 'Undefined', '(no country code)' ) as "Country name",
    sum(event_count) AS "Downloads"
from APPLAYDU_NOT_CERTIFIED.store_stats
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') 
    and event_id = 393584 
    and kpi_name in ('App Units','Install Events','Install events','New Downloads')
    and VERSION IN ('1.0.0')
group by COUNTRY_NAME
order by "Downloads" desc
limit 30
)