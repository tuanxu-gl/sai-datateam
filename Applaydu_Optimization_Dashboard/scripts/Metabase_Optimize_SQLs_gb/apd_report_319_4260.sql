insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3_str,value4_str,value5)
WITH gb4260 as (select 0)
,unlock AS (
  select DISTINCT user_id, COUNT(*) AS `Number of Toys Unlocked`
  from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
  where (unlock_cause='QR Code'
    OR unlock_cause='Toy Scan' 
    OR unlock_cause='Deep_Link')
    and isnewtoy=1
    and client_time>=CAST((select ivalue from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey='persona_starting_date') AS TIMESTAMP)
    and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
  GROUP BY user_id
),
Persona AS (
  select user_id, 
      CASE WHEN `Number of Toys Unlocked` IN (1,2,3) THEN 'Persona #2'
        ELSE 'Persona #3' END AS `Persona Type`
  from unlock
)
--main query

select 319 as dashboard_id
		,4260 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'ENGAGE | PERSONA REPARTITION' as kpi_name
		,year as value1,month as value2,`Time` as value3_str,`Persona_Type` as value4_str,`Total Users` as value5
	from
	(
	
select EXTRACT(YEAR from client_time) AS year,
    EXTRACT(MONTH from client_time) AS month,
    CONCAT(EXTRACT(YEAR from client_time), ' ', FORMAT_TIMESTAMP('%B', client_time)) AS `Time`,
    CASE WHEN p.`Persona Type` IS NULL THEN 'Persona #1' ELSE p.`Persona Type` END AS `Persona_Type`,
    COUNT(DISTINCT l.user_id) AS `Total Users`
from `gcp-bi-elephant-db-gold.applaydu.launch_resume` l
join (
  select DISTINCT user_id 
  from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
  and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
) USING (user_id)
LEFT join Persona p ON l.user_id=p.user_id
where l.user_id IS NOT NULL
and client_time>=CAST((select ivalue from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey='persona_starting_date') AS TIMESTAMP)
and client_time>=TIMESTAMP((select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ))
and client_time<TIMESTAMP(date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY))
and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
and CASE WHEN p.`Persona Type` IS NULL THEN 'Persona #1' ELSE p.`Persona Type` END IN (select persona from `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` where 2=2 )
and country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
and country IN (select country from `applaydu.tbl_country_filter` where 2=2 )
and client_time>=TIMESTAMP(date_sub(DATE_TRUNC(current_date(), month), INTERVAL 2 YEAR))
and client_time<TIMESTAMP(DATE_TRUNC(current_date(), month))
GROUP BY year, month, `Time`, `Persona_Type`
ORDER BY year ASC, month ASC, `Persona_Type` ASC
)