insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2_str,value3_str,value4_str,value5_str,value6_str,value7_str,value8_str)
WITH gb4254 as (select 0)
,launch_raw AS (
  select user_id, 
      date(client_time) AS login_Day, 
      min(date(client_time)) OVER (PARTITION BY user_id) AS `First Day`, 
      version
  from `gcp-bi-elephant-db-gold.applaydu.launch_resume`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
  ) USING (user_id)
  where country IN ('IN', 'BR', 'RU', 'US', 'IT', 'MX')
  and game_id IN (81337, 81335)
  and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
  GROUP BY user_id, login_Day, version,client_time
),
launch_with_version AS (
  select user_id, login_Day, `First Day`, DATE_DIFF(login_Day, `First Day`, DAY) AS Day_number
  from launch_raw
  where `First Day`>='2022-06-01' 
  and `First Day`<date_sub(current_date(), INTERVAL 10 DAY) -- full D30 (-10 days)
  and `First Day`>=date((select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ))
  and `First Day`<date(date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY))
  and `First Day`>=date(date_sub(DATE_TRUNC(current_date(), MONTH), INTERVAL 30 MONTH))
  ORDER BY user_id, `First Day`
)
,
retention AS (
  select DATE_TRUNC(`First Day`, MONTH) AS Month,
      FORMAT_TIMESTAMP('%A', `First Day`) AS `Weekday`,
      `First Day`,
      SUM(CASE WHEN Day_number=0 THEN 1 ELSE 0 END) AS `No. of New user Acquired`,
      SUM(CASE WHEN Day_number=0 THEN 1 ELSE 0 END) AS Day_0,
      SUM(CASE WHEN Day_number=1 THEN 1 ELSE 0 END) AS Day_1,
      SUM(CASE WHEN Day_number=3 THEN 1 ELSE 0 END) AS Day_3,
      SUM(CASE WHEN Day_number=7 THEN 1 ELSE 0 END) AS Day_7,
      SUM(CASE WHEN Day_number=14 THEN 1 ELSE 0 END) AS Day_14,
      SUM(CASE WHEN Day_number=28 THEN 1 ELSE 0 END) AS Day_28,
      SUM(CASE WHEN Day_number=30 THEN 1 ELSE 0 END) AS Day_30
  from launch_with_version
  GROUP BY `First Day`
  HAVING `Weekday` IN ('Friday', 'Saturday')
  ORDER BY `First Day`
)
--main query

select 319 as dashboard_id
		,4254 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'ENGAGE | EVOLUTION D1/7/28 Keys Countries' as kpi_name
		,cast(`Month` as string) as value1_str,`Retention D1` as value2_str,`Retention D3` as value3_str,`Retention D7` as value4_str,`Retention D14` as value5_str,`Retention D28` as value6_str,`Retention D30` as value7_str,`D7 per D1` as value8_str
	from
	(
	
select 'All' AS Platform,
    Month,
    case when SUM(Day_0)=0 then '0' else CONCAT(ROUND(SUM(Day_1) / SUM(Day_0) * 100, 2), '%') end AS `Retention D1`,
    case when SUM(Day_0)=0 then '0' else CONCAT(ROUND(SUM(Day_3) / SUM(Day_0) * 100, 2), '%') end AS `Retention D3`,
    case when SUM(Day_0)=0 then '0' else CONCAT(ROUND(SUM(Day_7) / SUM(Day_0) * 100, 2), '%') end AS `Retention D7`,
    case when SUM(Day_0)=0 then '0' else CONCAT(ROUND(SUM(Day_14) / SUM(Day_0) * 100, 2), '%') end AS `Retention D14`,
    case when SUM(Day_0)=0 then '0' else CONCAT(ROUND(SUM(Day_28) / SUM(Day_0) * 100, 2), '%') end AS `Retention D28`,
    case when SUM(Day_0)=0 then '0' else CONCAT(ROUND(SUM(Day_30) / SUM(Day_0) * 100, 2), '%') end AS `Retention D30`,
    case when SUM(Day_1)=0 then '0' else CONCAT(ROUND(SUM(Day_7) / SUM(Day_1) * 100, 2), '%') end AS `D7 per D1`
from retention
GROUP BY Month
ORDER BY Month ASC
)