with r290 as(
SELECT value1_str as Week,value2 as "Users who have scanned surprises",value3 as total_users,value4 as "Rate Users scanned the toys/total"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 290 
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
		 and dashboard_id=14 and query_id = 290
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q290 as (select 0)

select * from r290
union
select * from
(

select date_trunc('week', to_date(server_date)) as Week
    , count(DISTINCT case when (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0) then USER_ID end) as "Users who have scanned surprises"
    , count(DISTINCT USER_ID) as Total_Users
    , (count(DISTINCT case when (toy_unlocked_by_scan_count > 0 or scan_mode_finished_count > 0) then USER_ID end) / count(DISTINCT USER_ID)) as "Rate Users scanned the toys/total"
from APPLAYDU_NOT_CERTIFIED.tbl_users t
    join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.COUNTRY_NAME = t.COUNTRY_NAME 
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
    and server_date < dateadd(day, 1, (SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]]))
    and t.COUNTRY_NAME in (select COUNTRY_NAME from tbl_country_filter where 2=2 [[AND {{icountry}}]])   
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{iversion}}]])    	
    [[AND {{ishop_filter}}]]
group by Week
order by Week asc
)
where Week > 0
