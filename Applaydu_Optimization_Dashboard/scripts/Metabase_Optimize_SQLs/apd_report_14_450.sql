insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q450 as (select 0)
--main query

select 14 as dashboard_id
		,450 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Daily Downloads' as kpi_name
		,"Date" as value1_str,"Daily Downloads" as value2
	from
	(
	
 select to_date(CLIENT_TIME) as "Date", sum(event_count) as "Daily Downloads"
    from APPLAYDU_NOT_CERTIFIED.STORE_STATS t
 where event_id = 393584 
    and kpi_name in ('App Units','Install Events','Install events','New Downloads')
    and VERSION IN ('1.0.0')
group by   to_date(CLIENT_TIME)
order by to_date(CLIENT_TIME) asc
)