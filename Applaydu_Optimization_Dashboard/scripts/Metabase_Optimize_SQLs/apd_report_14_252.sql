
insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,dimension1,dimension2,dimension3,value1)

select 14 as dashboard_id
    ,252 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Monthly Active Users' as kpi_name
    ,"Month" as dimension1
     ,"Year" as dimension2
     ,"Time" as dimension3
    ,"Monthly Active Users" as value1
from
(
    select month(client_time) as "Month"
    ,year(client_time) as "Year"
    ,concat(year(client_time),' ',MONTHNAME(client_time)) as "Time"
    ,count(distinct USER_ID) as "Monthly Active Users"
    from APPLAYDU.LAUNCH_RESUME 
    WHERE not(game_id = 82471 and client_time <'2020-12-14')
    and time_spent::float >= 0
	and time_spent::float < 86400
	and client_time >= 'istart_date'
    and client_time < dateadd(day, 1, 'iend_date')

        
    group by MONTHNAME(client_time) ,month(client_time),"Year"
    
)

