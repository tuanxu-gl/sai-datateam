insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4_str)
with q255 as (select 0)
--main query

select 14 as dashboard_id
		,255 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Average Time per Session' as kpi_name
		,"Total time spent" as value1,"Total Session" as value2,time_result as value3,"Average Time per Users" as value4_str
	from
	(
	
SELECT sum(time_spent::int) as "Total time spent"
    ,sum(case when (session_id=1 or time_between_sessions::int>=30) then 1 else 0 end) AS "Total Session"
    ,"Total time spent"/"Total Session" as time_result
    ,minute(time_result::int::string::time) || ' min '|| second(time_result::int::string::time) || ' sec ' as "Average Time per Users"
FROM   APPLAYDU.LAUNCH_RESUME t
WHERE 1=1 and time_spent::int >= 0
    AND time_spent::int < 86400
    and (client_time >= '2020-08-10' and client_time < dateadd(day, -3, CURRENT_DATE()))
)