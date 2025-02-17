
insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,dimension1,value1)

select 14 as dashboard_id
    ,251 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'DAU' as kpi_name
    ,"Client time" as dimension1
    ,dau as value1
from
(
    select TO_DATE(client_time) "Client time", count(distinct USER_ID) as dau
    from APPLAYDU.LAUNCH_RESUME 
    WHERE 1=1 
        and time_spent::float >= 0
        and time_spent::float < 86400
        and client_time >= 'istart_date'
        and client_time < dateadd(day, 1, 'iend_date')
        and not(game_id = 82471 and client_time <'2020-12-14')
        and (client_time < dateadd(day, 1, 'iend_date'))
        
    group by TO_DATE(client_time) 
    order by TO_DATE(client_time) asc
)

