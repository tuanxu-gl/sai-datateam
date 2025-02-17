with r245 as(
SELECT value1_str as "Time spent"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 245 
)
,tbl_check_preprocess_report as
(
SELECT CASE 
    WHEN (
        SELECT COUNT(0) 
        FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
        WHERE 1=1
        AND start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
        AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
		 and dashboard_id=14 and query_id = 245
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q54 as (select 0)
,t_users as
(
select user_id
    ,sum (total_time_spent) as total_time_spent
    ,sum (toy_unlocked_by_scan_count) as toy_unlocked_by_scan_count
    ,sum (scan_mode_finished_count) as scan_mode_finished_count
   from APPLAYDU_NOT_CERTIFIED.tbl_users t
    join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.COUNTRY_NAME = t.COUNTRY_NAME 
   where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and server_date < dateadd(day, -3, CURRENT_DATE())
   and server_date >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] )
    and server_date < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and t.COUNTRY_NAME in (select COUNTRY_NAME from tbl_country_filter where 2=2  [[AND {{icountry}}]])   
	and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])    	
    [[AND {{ishopfilter}}]]
   group by user_id
  )

select * from r245
union
select * from
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
where "Time spent" > 0
