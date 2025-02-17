insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4,value5)
with q258 as (select 0)
--main query

select 14 as dashboard_id
		,258 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Average Surprises Scanned per User' as kpi_name
		,total_users as value1,sum_toy_unlocked_count as value2,sum_scan_mode_finished_count as value3,total_scans as value4,"Average Toys Scanned per User" as value5
	from
	(
	
select count (distinct user_id) as total_users
    ,sum (toy_unlocked_by_scan_count) as sum_toy_unlocked_count
    ,sum (scan_mode_finished_count) as sum_scan_mode_finished_count
    ,sum_toy_unlocked_count + sum_scan_mode_finished_count as total_scans 
    , total_scans/total_users as "Average Toys Scanned per User"
from APPLAYDU_NOT_CERTIFIED.tbl_users
where 1=1 and server_date >= 'istart_date' and server_date < dateadd(day, 1, 'iend_date') 
    and (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0 )
)