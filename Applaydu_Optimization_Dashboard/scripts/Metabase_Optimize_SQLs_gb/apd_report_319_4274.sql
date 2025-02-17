insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4)
with q4274 as (select 0)
--main query

select 319 as dashboard_id
		,4274 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'ENGAGE | EVOLUTION D1/7/28' as kpi_name
		,cast(`Month` as string) as value1_str,`Retention D1` as value2,`Retention D7` as value3,`Retention D28` as value4
	from
	(
	
select 
  Month,
  SUM(Day_1) / SUM(Day_0) AS `Retention D1`,
  SUM(Day_7) / SUM(Day_0) AS `Retention D7`,
  SUM(Day_28) / SUM(Day_0) AS `Retention D28`
from (
  select DATE_TRUNC(with_Day_number.`First Day`, MONTH) AS Month,
      FORMAT_TIMESTAMP('%A', with_Day_number.`First Day`) AS `Weekday`,
      `First Day`,
      SUM(CASE WHEN Day_number=0 THEN 1 ELSE 0 END) AS `No. of New user Acquired`,
      SUM(CASE WHEN Day_number=0 THEN 1 ELSE 0 END) AS Day_0,
      SUM(CASE WHEN Day_number=1 THEN 1 ELSE 0 END) AS Day_1,
      SUM(CASE WHEN Day_number=7 THEN 1 ELSE 0 END) AS Day_7,
      SUM(CASE WHEN Day_number=28 THEN 1 ELSE 0 END) AS Day_28
  from (
    select
      a.user_id,
      a.login_Day,
      b.first_day AS `First Day`,
      b.first_version AS first_version,
      b.first_country AS first_country,
      DATE_DIFF(a.login_Day, b.first_day, DAY) AS Day_number
    from (
      select
        user_id,
        DATE_TRUNC(date(client_time), DAY) AS login_Day
      from `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
      join (
        select DISTINCT user_id 
        from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
        and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
      ) USING (user_id)
      join `applaydu.tbl_shop_filter` using (game_id,country )
      GROUP BY user_id, DATE_TRUNC(date(client_time), DAY)
    ) a,
    (
      select
        user_id,
        min(DATE_TRUNC(date(client_time), DAY)) AS first_day,
        min(version) AS first_version,
        min(t.country) AS first_country
      from `gcp-bi-elephant-db-gold.applaydu.launch_resume` t
      join (
        select DISTINCT user_id 
        from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
        and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
      ) USING (user_id)
      join `applaydu.tbl_shop_filter` using (game_id,country )
      GROUP BY user_id
    ) b
    where a.user_id=b.user_id
  ) AS with_Day_number
  where FORMAT_TIMESTAMP('%A', with_Day_number.`First Day`) IN ('Friday', 'Saturday')
  and date(with_Day_number.`First Day`)>=(date_sub(DATE_TRUNC(current_date(), MONTH), INTERVAL 2 YEAR))
  and date(with_Day_number.`First Day`)<(DATE_TRUNC(current_date(), MONTH))
  and date(with_Day_number.`First Day`)>=((select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ))
  and date(with_Day_number.`First Day`)<(date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY))
  and first_country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  and first_version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
  GROUP BY `First Day`
  ORDER BY `First Day`
)
GROUP BY Month
ORDER BY Month
)