insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)
WITH gb4228 as (select 0)
,user_type AS (
  select 
    user_id, 
    COUNT(*) AS number_of_sessions, 
    'One and Done Users' AS UserType
  from 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume` t 
  join 
    (
      select DISTINCT user_id 
      from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
        and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
        and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
        and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    ) USING (user_id)
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
  GROUP BY 
    1
  HAVING 
    number_of_sessions=1
),
main AS (
  select 
    lr.*, 
    ut.UserType
  from 
    `gcp-bi-elephant-db-gold.applaydu.launch_resume` lr
  join 
    (
      select DISTINCT user_id 
      from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
        and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
        and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
        and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    ) USING (user_id)
  LEFT join 
    user_type ut USING (user_id)
  where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 
  ORDER BY 
    lr.user_id
)
--main query

select 319 as dashboard_id
		,4228 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'One and Done Ratio' as kpi_name
		,`One and Done` as value1
	from
	(
	
select 
  SUM(CASE WHEN UserType='One and Done Users' THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) AS `One and Done`
from 
  main t
where 
  CAST(time_spent AS FLOAT64)>=0
  and CAST(time_spent AS FLOAT64)<86400
  and (session_id=1 OR CAST(time_between_sessions AS INT64)>=30)
  and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
  and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  and t.country IN (select country from `applaydu.tbl_country_filter` where 2=2  ) 
  and t.country IN (select country from `applaydu.tbl_country_filter` where 2=2 ) 
  and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
)