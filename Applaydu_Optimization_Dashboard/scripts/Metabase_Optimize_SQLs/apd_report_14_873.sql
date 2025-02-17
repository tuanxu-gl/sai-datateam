insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q54 as (select 0)
--main query

select 14 as dashboard_id
		,873 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Number of scans v3' as kpi_name
		,"Scan type" as value1_str,"Scans" as value2
	from
	(
	
select case when scan_type in ('Alternative_Vignette','Scan_Vignette','Alternative Vignette','Vignette') then 'Vignette' 
        else case when (scan_type in ('Scan_QR','QR Code','Deep_Link'))then 'QR Code' 
        else case when scan_type in('Toy Scan','Scan_Toy') then 'Scan toy' 
        else 'Others' 
        end end end as "Scan type"
    ,sum(total_scan) as "Scans"
from APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock t
where scan_type in ('Alternative_Vignette','Scan_Vignette','Alternative Vignette','Vignette','Scan_QR','QR Code','Toy Scan','Scan_Toy','Deep_Link')
group by "Scan type"
order by "Scan type" asc
)