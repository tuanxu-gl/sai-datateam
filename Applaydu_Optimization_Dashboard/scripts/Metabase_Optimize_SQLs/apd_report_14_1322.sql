insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q1332 as (select 0)
--main query

select 14 as dashboard_id
		,1322 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Number of scans v3 - daily' as kpi_name
		,"Server date" as value1_str,"Total Scans" as value2
	from
	(
	
select to_date(server_date) as "Server date"
  ,sum(total_scan) as "Total Scans"
from APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock
where scan_type in ('Alternative_Vignette','Scan_Vignette','Alternative Vignette','Vignette','Scan_QR','QR Code','Toy Scan','Scan_Toy','Deep_Link')
  and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
group by "Server date"
order by "Server date" asc
)