insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q54 as (select 0)
--main query

select 14 as dashboard_id
		,54 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Downloads per Country' as kpi_name
		,"Country name" as value1_str,"Users" as value2
	from
	(
	
SELECT D_COUNTRY as "Country name", sum(event_count) AS "Users"
FROM APPLAYDU_NOT_CERTIFIED.store_stats
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date') 	
    and event_id = 393584 
    and kpi_name in ('App Units','Install Events','Install events','New Downloads')
    and VERSION IN ('1.0.0')
group by D_COUNTRY
)