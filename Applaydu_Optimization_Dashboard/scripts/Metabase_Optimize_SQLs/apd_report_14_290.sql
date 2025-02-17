insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4)
with q290 as (select 0)
--main query

select 14 as dashboard_id
		,290 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Total toy unlocked by Scan Toy and QR/Leaflet' as kpi_name
		,Week as value1_str,"Users who have scanned surprises" as value2,total_users as value3,"Rate Users scanned the toys/total" as value4
	from
	(
	
select date_trunc('week', to_date(server_date)) as Week
  , count(DISTINCT case when (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0) then USER_ID end) as "Users who have scanned surprises"
  , count(DISTINCT USER_ID) as Total_Users
  , (count(DISTINCT case when (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0) then USER_ID end) / count(DISTINCT USER_ID)) as "Rate Users scanned the toys/total"
from APPLAYDU_NOT_CERTIFIED.tbl_users t
where 1=1 and server_date >= 'istart_date' and server_date < dateadd(day, 1, 'iend_date') and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
group by Week
order by Week asc
)