insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)

with q2899 as (select 0),
user_type as
(
SELECT user_id, count(*) as number_of_sessions, 'One and Done Users' as UserType
FROM "ELEPHANT_DB"."APPLAYDU"."LAUNCH_RESUME"
group by 1
having number_of_sessions = 1
)
,
main as
(
select LR.*, ut.UserType
from "ELEPHANT_DB"."APPLAYDU"."LAUNCH_RESUME" LR
left join user_type ut using (user_id)
order by LR.user_id
)
select 14 as dashboard_id
    ,2899 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'One and Done Ratio' as kpi_name
    ,"One and Done" as value1
from
(
    select sum(case when UserType = 'One and Done Users' then 1 else 0 end)/count(distinct user_id) as "One and Done"
from main t
where  time_spent::float >= 0
    and time_spent::float < 86400
    and (session_id=1 or time_between_sessions::int>=30)
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
)
