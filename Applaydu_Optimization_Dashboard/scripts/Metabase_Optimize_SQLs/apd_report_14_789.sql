insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id, query_id, start_date, end_date, load_time, kpi_name, dimension1, value1)

with p789 as (select 0),
new_user as (
select date_trunc(month, to_date(client_time)) as "Month"
    ,count(distinct USER_ID) as "Monthly New Users"
from applaydu.LAUNCH_RESUME t
 WHERE 1=1
    and SESSION_ID = 1
    and not(t.game_id = 82471 and client_time <'2020-12-14')
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and time_spent::float >= 0
    and time_spent::float < 86400
   
   
group by 1
order by 1
),
total_user as (
select date_trunc(month, to_date(client_time)) as "Month"
    ,count(distinct USER_ID) as "Monthly Active Users"
from APPLAYDU.LAUNCH_RESUME t
WHERE 1=1
    and not(t.game_id = 82471 and client_time <'2020-12-14')
    and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
    and time_spent::float >= 0
    and time_spent::float < 86400
    
group by 1 
order by 1
)
select 14 as dashboard_id
    ,789 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Monthly New Users' as kpi_name
    ,"Month" as dimension1
    ,"% New Users" as value1
from
(
select "Month", "Monthly New Users" / "Monthly Active Users" as "% New Users"
from total_user inner join new_user using ("Month")
order by "Month"
)
