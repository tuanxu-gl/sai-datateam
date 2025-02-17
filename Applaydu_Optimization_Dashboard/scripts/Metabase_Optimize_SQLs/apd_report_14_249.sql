insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
		,249 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Total Downloads' as kpi_name
		,Shop as value1_str,"Total Installations" as value2
	from
	(
	
SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(t.GAME_ID, '81335', 'App Store'),'81337', 'Google Play'), '82471','AppInChina'), '85247','AppInChina'), '84515','Samsung'), '85837','Amazon') as Shop, sum(event_count) as "Total Installations"
from APPLAYDU_NOT_CERTIFIED.STORE_STATS t
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') 	
    and event_id = 393584 
    and kpi_name in ('App Units','Install Events','Install events','New Downloads') 
    and VERSION IN ('1.0.0') 
    group by Shop
)