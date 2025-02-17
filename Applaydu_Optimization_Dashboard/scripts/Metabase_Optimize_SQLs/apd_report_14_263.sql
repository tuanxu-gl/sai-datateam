insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3)

select 14 as dashboard_id
    ,263 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Average Session per User' as kpi_name
    ,"Number of Sessions" as value1
    ,Total_Users as value2
    ,"Average Session per User" as value3
from
(
    SELECT 
        sum(case when (session_id = 1 or time_between_sessions::int >= 30) then 1 else 0 end) AS "Number of Sessions",
        COUNT(DISTINCT USER_ID) AS Total_Users,
        "Number of Sessions" / Total_Users as "Average Session per User"
    FROM APPLAYDU.LAUNCH_RESUME t
    WHERE time_spent::float >= 0
        and time_spent::float < 86400
        and client_time >= 'istart_date' and client_time < dateadd(day, 1, 'iend_date')
        
)
