insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str)
with q54 as (select 0)
,t_users as
(
select user_id
    ,sum (total_time_spent) as total_time_spent
    ,sum (toy_unlocked_by_scan_count) as toy_unlocked_by_scan_count
    ,sum (scan_mode_finished_count) as scan_mode_finished_count
   from APPLAYDU_NOT_CERTIFIED.tbl_users t
   where 1=1 and server_date >= 'istart_date' and server_date < dateadd(day, 1, 'iend_date') 
   group by user_id
  )
--main query

select 14 as dashboard_id
		,245 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'Physical toys brought to life - Time spent by users who have scanned surprises' as kpi_name
		,"Time spent" as value1_str
	from
	(
	
select "Time spent"
from
(
select count(distinct user_id) as users
    ,sum(total_time_spent) as sum_total_time_spent
    ,sum_total_time_spent / users as time_result
    ,hour(time_result::int::string::time) || ' hour '|| minute(time_result::int::string::time) || ' min '|| second(time_result::int::string::time) || ' sec ' AS "Time spent"
    from t_users
    where toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0 
   )
)