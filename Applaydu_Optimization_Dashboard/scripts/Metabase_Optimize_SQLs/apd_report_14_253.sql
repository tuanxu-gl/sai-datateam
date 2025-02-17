
insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1)

select 14 as dashboard_id
    ,253 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Number of Sessions' as kpi_name
    ,"Number of Sessions" as value1
from
(
    SELECT 
        COUNT(*) AS "Number of Sessions"
    FROM APPLAYDU.LAUNCH_RESUME t
    WHERE time_spent::float >= 0
        and time_spent::float < 86400
        and (session_id = 1 or time_between_sessions::int >= 30)
        and client_time >= 'istart_date'
        and client_time < dateadd(day, 1, 'iend_date')
        and (client_time < dateadd(day, 1, 'iend_date'))
       
)

