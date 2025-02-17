insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q874 as (select 0)
--main query

select 14 as dashboard_id
		,874 as query_id
		,'2020-08-10' as start_date
		,'2025-02-08' as end_date
		,current_timestamp() as load_time
		,'Total Scans (QR + Vignettes) splitted between Mainstream vs. Licensing toys v3' as kpi_name
		,"Category" as value1_str,"Total Scans" as value2
	from
	(
	
select coalesce(Category,'Other') as "Category"
  -- ,t.toy_name
  ,sum(total_scan) as "Total Scans" 
from APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock t
where 1=1 and server_date >= '2020-08-10' and server_date < dateadd(day, 1, '2025-02-08') and scan_type in ('Alternative_Vignette','Scan_Vignette','Alternative Vignette','Vignette','Scan_QR','QR Code','Toy Scan','Scan_Toy','Deep_Link')
  and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
group by "Category"--,t.toy_name
Order by "Total Scans" desc
)