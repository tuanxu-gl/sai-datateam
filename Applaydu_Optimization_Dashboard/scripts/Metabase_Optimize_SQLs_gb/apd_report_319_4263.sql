insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4)
WITH gb4263 AS (select 0),
regis_regis AS (
  select 
    DATE_TRUNC(client_time, MONTH) AS month,
    COUNT(DISTINCT regis.user_id) AS `Successfully Registered Email`
  from `gcp-bi-elephant-db-gold.applaydu.account_operation` AS regis
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and client_time>='2024-01-01'
    and account_operation='Email registration'
    and result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
  GROUP BY month
), 
regis_veri AS (
  select 
    month,
    COUNT(DISTINCT `Verified`) AS `Verified`
  from (
    select 
      DATE_TRUNC(client_time, MONTH) AS month,
      CAST(user_id AS STRING) AS `Verified`
    from `gcp-bi-elephant-db-gold.applaydu.account_operation`
    where account_operation='Email registration confirmation'
      and result='Success'
      and 1=1
      and date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_v4_start_date')
      and date(client_time)<(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_be_parent_registration_start_date')
      and date(client_time)<current_date()
      and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
      and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
      and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
      and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
      and client_time>='2024-01-01'
    UNION ALL    
    select 
      DATE_TRUNC(client_time, MONTH) AS month,
      anon_id AS `Verified`
    from `gcp-gfb-sai-tracking-gold.applaydu.store_stats_subscriptions`
    where date(client_time)>=(select date(ivalue) from `applaydu.tbl_variables` where ikey='apd_be_parent_registration_start_date')
      and date(client_time)<current_date()
      and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
      and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
      and country_name IN (select country_name from `applaydu.tbl_country_filter` where 2=2  )
  ) 
  GROUP BY month
)
--main query

select 319 as dashboard_id
		,4263 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'[PARENTAL] Email registration funnel' as kpi_name
		,cast(month as string)as value1_str,`Successfully Registered Email` as value2,`Verified email after registration` as value3,ratio as value4
	from
	(
	
select 
  month,
  `Successfully Registered Email`,
  `Verified` AS `Verified email after registration`,
  `Verified` / `Successfully Registered Email` AS ratio
from regis_regis
LEFT join regis_veri USING (month)
ORDER BY month DESC
)