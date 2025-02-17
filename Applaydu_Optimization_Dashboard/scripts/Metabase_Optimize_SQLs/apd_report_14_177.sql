insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4)
with q177 as (select 0)
--main query

select 14 as dashboard_id
		,177 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Total toy unlocked by Scan Toy and QR/Leaflet' as kpi_name
		,"Toy name" as value1_str,"Scan Toy" as value2,"Scan QR/Leaflet" as value3,"Total scan" as value4
	from
	(
	
select toy_name as "Toy name", coalesce("'Scan Toy'", 0) as "Scan Toy", coalesce("'QR/Leaflet'", 0) as "Scan QR/Leaflet", coalesce("'Scan Toy'", 0) + coalesce("'QR/Leaflet'", 0) as "Total scan"
from (
  select *
  from (
    select 
      toy_name,
      (case when SCAN_TYPE = 'Scan_Toy' then 'Scan Toy' else 'QR/Leaflet' end) as scan_type,
      sum(total_scan) as toy_count
    FROM  APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock
    where 1=1 and server_date >= 'istart_date' and server_date < dateadd(day, 1, 'iend_date') and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
      and toy_name <> 'ZEBRA_VV114'
      and SCAN_TYPE not in ('EXPERIENCE','Experience')
    group by toy_name, scan_type
  ) as source
  pivot (
    sum(toy_count)
    for scan_type in ('Scan Toy', 'QR/Leaflet')
  ) as pvt
)
order by "Total scan" desc
limit 10
)