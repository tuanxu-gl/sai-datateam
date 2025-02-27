insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2_str,value3,value4,value5)
--main query

select 319 as dashboard_id
		,4236 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'[PARENTAL] Email registration funnel - by country' as kpi_name
		,`year_month` as value1_str,`Country` as value2_str,`Successfully Registered Email` as value3,`Verified email after registration` as value4,ratio as value5
	from
	(
	
select 
    CONCAT(EXTRACT(YEAR from client_time), '-', LPAD(CAST(EXTRACT(MONTH from client_time) AS STRING), 2, '0')) AS year_month,
    `gcp-bi-elephant-db-gold.dimensions.country`.name AS `Country`,
    COUNT(DISTINCT CASE 
      WHEN account_operation='Email registration' 
        and result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
      THEN user_id 
      ELSE NULL 
    END) AS `Successfully Registered Email`,
    COUNT(DISTINCT CASE 
      WHEN account_operation='Email registration confirmation' 
        and result='Success' 
      THEN user_id 
      ELSE NULL 
    END) AS `Verified email after registration`,
    COUNT(DISTINCT CASE 
      WHEN account_operation='Email registration confirmation' 
        and result='Success' 
      THEN user_id 
      ELSE NULL 
    END) / COUNT(DISTINCT CASE 
      WHEN account_operation='Email registration' 
        and result IN ('Good Email: Wrong Age then Correct Age and Success', 'Good Email: Correct Age and Success', 'Success', 'Bad Email then Good Email: Wrong Age then Correct Age and Success', 'Bad Email then Good Email: Correct Age and Success')
      THEN user_id 
      ELSE NULL 
    END) AS ratio
  from `gcp-bi-elephant-db-gold.applaydu.account_operation`
  LEFT join `gcp-bi-elephant-db-gold.dimensions.country` ON `gcp-bi-elephant-db-gold.applaydu.account_operation`.country=`gcp-bi-elephant-db-gold.dimensions.country`.code
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  ) USING (user_id)
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
    and client_time>=TIMESTAMP(date_sub(DATE_TRUNC(current_date(), MONTH), INTERVAL 2 YEAR))
    and client_time<TIMESTAMP(DATE_TRUNC(current_date(), MONTH))
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and date(client_time)>=(select min(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(date(server_date)) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and `gcp-bi-elephant-db-gold.dimensions.country`.name IN ('Brazil', 'United States', 'Russian Federation', 'Italy', 'United Kingdom', 'Germany', 'France', 'Mexico', 'Argentina', 'Canada')
  GROUP BY year_month, `Country`
  ORDER BY `Country` ASC
)