insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
with q3144 as (select 0),
result as (
select case when environment_id like 'Natoons v4%' then 'Natoons Experience'
      when environment_id like '%Travel%' then 'Travel Experience'
      when environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') then 'Fantasy Experience'
      when environment_id like '%Space%' and environment_id <> 'Space' then 'Space Experience'
      when environment_id = 'Experience - Dino Museum' then 'Dino Experience - since v4.7.0'
      when environment_id = 'Eduland Lets Story' then 'Lets Story'
      when environment_id = 'Kinderini' then 'Kinderini'
      end as Environment
  , USER_ID
from APPLAYDU.STORY_MODE_TRIGGERED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
  and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' and client_time < CURRENT_DATE()
union 
select case when environment_id like 'Natoons v4%' then 'Natoons Experience'
      when environment_id like '%Travel%' then 'Travel Experience'
      when environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') then 'Fantasy Experience'
      when environment_id like '%Space%' and environment_id <> 'Space' then 'Space Experience'
      when environment_id = 'Experience - Dino Museum' then 'Dino Experience - since v4.7.0'
      when environment_id = 'Eduland Lets Story' then 'Lets Story'
      when environment_id = 'Kinderini' then 'Kinderini'
      end as Environment
  , USER_ID
from APPLAYDU.STORY_MODE_FINISHED
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
  and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' and client_time < CURRENT_DATE()
)
,
active_user as (
select COUNT(DISTINCT USER_ID)AS Total_Users
from APPLAYDU.LAUNCH_RESUME t
where 1=1 and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
  and time_spent::float >= 0	and time_spent::float < 86400
  and (version >= '4.0.0' and client_time >= '2023-08-22') and version < '9.0.0' and client_time < CURRENT_DATE()
  and not(t.game_id = 82471 and client_time <'2020-12-14')
) 
--main query

select 14 as dashboard_id
		,3144 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Percentage of Applaydu users played each Dedicated Experience' as kpi_name
		,"Environment" as value1_str,"Percentage" as value2
	from
	(
	
select Environment as "Environment", count (distinct USER_ID) /(select Total_Users from active_user) as "Percentage"
from result
where Environment is not null
group by 1
order by 2 desc
)