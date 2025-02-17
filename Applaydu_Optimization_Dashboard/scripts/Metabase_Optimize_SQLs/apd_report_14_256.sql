
insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4_str)

select 14 as dashboard_id
    ,256 as query_id
    ,'istart_date' as start_date
    ,'iend_date' as end_date
    ,current_timestamp() as load_time
    ,'Average Time Spent per User' as kpi_name
   
    ,"Total time spent"  as value1
    , "Total Users"  as value2
    ,time_result  as value3
    ,"Average Time per Users"  as value4_str
from
(
    SELECT sum(time_spent::int) as "Total time spent" 
        ,count(DISTINCT user_id) AS "Total Users"
        ,"Total time spent" /"Total Users" as time_result
        ,hour(time_result::int::string::time) || ' hour '||  minute(time_result::int::string::time) || ' min '|| second(time_result::int::string::time) || ' sec ' AS "Average Time per Users"
    FROM   APPLAYDU.LAUNCH_RESUME t
     WHERE  time_spent::int >= 0
        AND time_spent::int < 86400
        AND client_time >= 'istart_date' 
        AND client_time < dateadd(day, 1, 'iend_date')
      

)
