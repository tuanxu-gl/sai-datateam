
insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,dimension1,value1)

select 14 as dashboard_id
    ,916 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Number of Users - by Shop' as kpi_name
    ,Shop as dimension1
    ,Total_Users as value1
from
(
    select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(game_id, 
        '81335', 'App Store')
        ,'81337', 'Google Play')
        , '82471','AppInChina')
        , '84155','Google Play')
        , '84515','Samsung')
        , '84137','AppInChina') 
        , '85837','Amazon') as Shop
    ,COUNT(DISTINCT USER_ID)AS Total_Users
    from APPLAYDU.LAUNCH_RESUME 
    WHERE 1=1
    and not(game_id = 82471 and client_time <'2020-12-14')
    and time_spent::float >= 0	and time_spent::float < 86400
	

	and client_time >= 'istart_date'
    and client_time < dateadd(day, 1, 'iend_date')

        
    group by Shop
    
)

