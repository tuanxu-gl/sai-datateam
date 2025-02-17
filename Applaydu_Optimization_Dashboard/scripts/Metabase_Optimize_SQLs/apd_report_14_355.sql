insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q355 as (select 0)
--main query

select 14 as dashboard_id
		,355 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Total Scans (QR + Vignettes) Licensing toys by surprise family' as kpi_name
		,"Surprise family" as value1_str,"Total Scans" as value2
	from
	(
	
select coalesce(l.GAME_ELEMENT_INTERNAL_SUBCATEGORY, 'Other') as "Surprise family" 
  -- ,t.toy_name
  ,sum(total_scan) as "Total Scans" 
from APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock t
  left join APPLAYDU_NOT_CERTIFIED.GDD_TOY_LIST l on t.toy_name = l.OLD_ODD_NAME
where scan_type in ('Alternative_Vignette','Scan_Vignette','Alternative Vignette','Vignette','Scan_QR','QR Code','Toy Scan','Scan_Toy','Deep_Link')
  and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
  and toy_name <> 'UNDEFINED_TOY' -- undefine toys
group by "Surprise family" --,t.toy_name
order by "Total Scans" desc
)