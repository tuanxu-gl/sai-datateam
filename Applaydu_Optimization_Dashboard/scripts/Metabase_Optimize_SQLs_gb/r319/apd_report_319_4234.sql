insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2,value3,value4,value5,value6,value7,value8)
WITH gb4234 as (select 0)
,unlock AS (
  select DISTINCT user_id, COUNT(*) AS `Number of Toys Unlocked`
  from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
  where (unlock_cause='QR Code'
    OR unlock_cause='Toy Scan' 
    OR unlock_cause='Deep_Link')
    and isnewtoy=1
    and client_time>=CAST((select ivalue from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey='persona_starting_date') AS TIMESTAMP)
  GROUP BY user_id
),
Persona AS (
  select user_id, 
      CASE WHEN `Number of Toys Unlocked` IN (1,2,3) THEN 'Persona #2'
        ELSE 'Persona #3' END AS `Persona Type`
  from unlock
),
tbl_launch_resume AS (
  select user_id, COALESCE(c.name, 'Unknown') AS country_name, client_time
  from `gcp-bi-elephant-db-gold.applaydu.launch_resume` l
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= 'istart_date' and date(active_date) < date_add('iend_date', INTERVAL 1 DAY) 
    and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
    and date(active_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(active_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  ) USING (user_id)
  LEFT join `gcp-bi-elephant-db-gold.dimensions.country` c ON l.country=c.code
)
--main query

select 319 as dashboard_id
		,4234 as query_id
		,timestamp('istart_date') as start_date
		,timestamp('iend_date') as end_date
		,current_timestamp() as load_time
		,'Personas distribution by Country' as kpi_name
		,`country_name` as value1_str,`Persona #1` as value2,`Persona #2` as value3,`Persona #3` as value4,`Active Users` as value5,`% Persona 1` as value6,`% Persona 2` as value7,`% Persona 3` as value8
	from
	(
	
select country_name, 
    COALESCE(`Persona #1`, 0) AS `Persona #1`, 
    COALESCE(`Persona #2`, 0) AS `Persona #2`, 
    COALESCE(`Persona #3`, 0) AS `Persona #3`,
    (`Persona #1` + `Persona #2` + `Persona #3`) AS `Active Users`,
    (`Persona #1` / (`Persona #1` + `Persona #2` + `Persona #3`)) * 100 AS `% Persona 1`,
    (`Persona #2` / (`Persona #1` + `Persona #2` + `Persona #3`)) * 100 AS `% Persona 2`, 
    (`Persona #3` / (`Persona #1` + `Persona #2` + `Persona #3`)) * 100 AS `% Persona 3`
from (
  select *
  from (
    select country_name, 
        CASE WHEN p.`Persona Type` IS NULL THEN 'Persona #1' ELSE p.`Persona Type` END AS `Persona_Type`, 
        COUNT(DISTINCT l.user_id) AS `No. of Users`
    from tbl_launch_resume l
    LEFT join Persona p ON l.user_id=p.user_id
    where l.user_id IS NOT NULL
    and client_time>=CAST((select ivalue from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey='persona_starting_date') AS TIMESTAMP)
    and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    --and `Persona_Type` IN (select persona from `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` where 2=2 )
    and country_name IN (select country_name from `applaydu.tbl_country_filter` where 2=2  )
    GROUP BY country_name, `Persona_Type`
    having `Persona_Type` IN (select persona from `gcp-gfb-sai-tracking-gold.applaydu.tbl_persona_filter` where 2=2 )
  )
  PIVOT(SUM(`No. of Users`) FOR `Persona_Type` IN ('Persona #1', 'Persona #2', 'Persona #3')) AS pivottable
)
ORDER BY `Active Users` DESC, `Persona #3` DESC 
LIMIT 20
)