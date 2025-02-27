insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)
--main query

select 319 as dashboard_id
		,4272 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Total users scan biscuit' as kpi_name
		,`Total Scans Users` as value1
	from
	(
	
select COUNT(DISTINCT user_id) AS `Total Scans Users`
from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
join (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
) USING (user_id)
where date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_kinderini_start_date')
  and date(client_time)<current_date()
  and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  and scan_result IN ('New_Toy', 'Old_Toy')
  and (
    (scan_type='Deep Link' and UPPER(reference) LIKE '%KINDERINI%')
    OR scan_type IN ('Scan_QR_Biscuit', 'Scan_Toy_Biscuit')
  )
)