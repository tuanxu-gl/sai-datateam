insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5)
--main query

select 319 as dashboard_id
		,4241 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Total Scans split by Scan Deeplink, Leaflet and Toy- by country' as kpi_name
		,country_name as value1_str,`Scan Toy` as value2,`Scan Leaflet` as value3,`Scan Deep Link` as value4,`Total scans` as value5
	from
	(
	
select country_name,
  SUM(CASE WHEN scan_type IN ('Toy Scan', 'Scan_Toy') THEN total_scan ELSE 0 END) AS `Scan Toy`,
  SUM(CASE WHEN scan_type IN ('Scan_QR', 'QR Code', 'Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette') THEN total_scan ELSE 0 END) AS `Scan Leaflet`,
  SUM(CASE WHEN scan_type IN ('Deep_Link') THEN total_scan ELSE 0 END) AS `Scan Deep Link`,
  SUM(total_scan) AS `Total scans`
from `gcp-gfb-sai-tracking-gold.applaydu.tbl_sum_scan_unlock` t
join `applaydu.tbl_shop_filter` a ON a.game_id=t.game_id and a.country=t.country 
where scan_type IN ('Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette', 'Scan_QR', 'QR Code', 'Toy Scan', 'Scan_Toy', 'Deep_Link')
  and date(server_date)>='2020-08-10' 
  and date(server_date)<date_sub(current_date(), INTERVAL 3 DAY)
  and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and t.country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
GROUP BY country_name
ORDER BY `Total scans` DESC
)