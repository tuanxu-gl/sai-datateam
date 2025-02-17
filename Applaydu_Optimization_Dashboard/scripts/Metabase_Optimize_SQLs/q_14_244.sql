with r244 as(
SELECT value1 as "Users who have scanned surprises"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 244 
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
		 and dashboard_id=14 and query_id = 244
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q244 as (select 0)

select * from r244
union
select * from
(

select count (distinct user_id) as "Users who have scanned surprises"
from APPLAYDU_NOT_CERTIFIED.tbl_users
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and server_date < dateadd(day, -3, CURRENT_DATE())
    and server_date >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] )
    and server_date < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and COUNTRY_NAME in (select COUNTRY_NAME from tbl_country_filter where 2=2  [[AND {{icountry}}]])   
	and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])    	
    and (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0 )
)
where "Users who have scanned surprises" > 0
