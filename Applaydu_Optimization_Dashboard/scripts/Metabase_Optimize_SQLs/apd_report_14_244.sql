insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)
with q244 as (select 0)
--main query

select 14 as dashboard_id
		,244 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Physical toys brought to life - Users who have scanned surprises' as kpi_name
		,"Users who have scanned surprises" as value1
	from
	(
	
select count (distinct user_id) as "Users who have scanned surprises"
from APPLAYDU_NOT_CERTIFIED.tbl_users
where 1=1 and server_date >= 'istart_date' and server_date < dateadd(day, 1, 'iend_date') 
    and (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0 )
)