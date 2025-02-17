insert into ELEPHANT_DB.APPLAYDU_NOT_CERTIFIED.APD_REPORT_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,dimension1,value1,value2,value3)
select 
    14 as dashboard_id,
    3326 as query_id,
    'istart_date' as start_date,
    'iend_date' as end_date,
    current_timestamp() as load_time,
    'APD Micro Session using App Survival' as kpi_name,
    "Time Spent per Session" as dimension1,
    Percentage as value1,
    "Number of Sessions" as value2,
    "Total Sessions" as value3
from (
with q3326 as (select 0),
cleaned as ( 
select user_id, da_session_id ,max(app_time_spent) as app_time_spent
from applaydu.app_survival
where 1=1 
    and game_id in (81337,81335)  
    and version >= '4.3.0' and version < '9.0.0'
    and client_time >= 'istart_date' and client_time < 'iend_date'
group by 1,2 
),
renamed as (
select case when app_time_spent = 1 then '1-2 seconds Average Duration'
            when app_time_spent = 3 then '3-9 seconds Average Duration'
            when app_time_spent = 10 then '10-29 seconds Average Duration'
            when app_time_spent = 30 then '30-59 seconds Average Duration'
            when app_time_spent = 60 then '1-2 minutes Average Duration'
            when app_time_spent = 120 then '2-3 minutes Average Duration'
            when app_time_spent = 180 then '3-5 minutes Average Duration'
            when app_time_spent = 300 then '5-10 minutes Average Duration'
            when app_time_spent = 600 then '10-30 minutes Average Duration'
            when app_time_spent = 1800 then '30-60 minutes Average Duration'
            when app_time_spent = 3600 then '1+ hours Average Duration'
        end as "Time Spent per Session", 
        app_time_spent, count(*) as Number_of_sessions
from cleaned
group by 1,2
)
select "Time Spent per Session", ratio as Percentage, "Number of Sessions", "Total Sessions"
from (
    select "Time Spent per Session"
        ,Number_of_sessions  as  "Number of Sessions" 
        ,sum(Number_of_sessions) over() as "Total Sessions"
        ,"Number of Sessions"/"Total Sessions" as ratio
    from renamed
    group by "Time Spent per Session",Number_of_sessions,app_time_spent
)
order by case when "Time Spent per Session" = '1-2 seconds Average Duration' then 1
                when "Time Spent per Session" = '3-9 seconds Average Duration' then 2
                when "Time Spent per Session" = '10-29 seconds Average Duration' then 3
                when "Time Spent per Session" = '30-59 seconds Average Duration' then 4
                when "Time Spent per Session" = '1-2 minutes Average Duration' then 5
                when "Time Spent per Session" = '2-3 minutes Average Duration' then 6
                when "Time Spent per Session" = '3-5 minutes Average Duration' then 7
                when "Time Spent per Session" = '5-10 minutes Average Duration' then 8
                when "Time Spent per Session" = '10-30 minutes Average Duration' then 9
                when "Time Spent per Session" = '30-60 minutes Average Duration' then 10
                when "Time Spent per Session" = '1+ hours Average Duration' then 11
                end
)