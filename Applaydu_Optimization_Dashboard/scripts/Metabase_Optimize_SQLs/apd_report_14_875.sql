insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q875 as (select 0)
--main query

select 14 as dashboard_id
		,875 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Number of scans v3 - by leftover toys' as kpi_name
		,"Leftover type" as value1_str,total_scans as value2
	from
	(
	
select leftover_type as "Leftover type"
  ,sum(total_scan) as total_scans
from APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock
where scan_type in ('Alternative_Vignette','Scan_Vignette','Alternative Vignette','Vignette','Scan_QR','QR Code','Toy Scan','Scan_Toy','Deep_Link')
  and server_date < dateadd(day, -3, CURRENT_DATE())
group by leftover_type
)