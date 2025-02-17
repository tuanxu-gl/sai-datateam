with r258 as(
SELECT value1 as total_users,value2 as sum_toy_unlocked_count,value3 as sum_scan_mode_finished_count,value4 as total_scans,value5 as "Average Toys Scanned per User"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 258 
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
		 and dashboard_id=14 and query_id = 258
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q258 as (select 0)

select * from r258
union
select * from
(

select count (distinct user_id) as total_users
    ,sum (toy_unlocked_by_scan_count) as sum_toy_unlocked_count
    ,sum (scan_mode_finished_count) as sum_scan_mode_finished_count
    ,sum_toy_unlocked_count + sum_scan_mode_finished_count as total_scans 
    , total_scans/total_users as "Average Toys Scanned per User"
from APPLAYDU_NOT_CERTIFIED.tbl_users
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and server_date < dateadd(day, -3, CURRENT_DATE())
    and server_date >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] )
    and server_date < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and COUNTRY_NAME in (select COUNTRY_NAME from tbl_country_filter where 2=2  [[AND {{icountry}}]])   
	and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])    	
    and (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0 )
)
where total_users > 0
