with r255 as(
SELECT value1 as "Total time spent",value2 as "Total Session",value3 as time_result,value4_str as "Average Time per Users"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 255 
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
		 and dashboard_id=14 and query_id = 255
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q255 as (select 0)

select * from r255
union
select * from
(

SELECT sum(time_spent::int) as "Total time spent"
    ,sum(case when (session_id=1 or time_between_sessions::int>=30) then 1 else 0 end) AS "Total Session"
    ,"Total time spent"/"Total Session" as time_result
    ,minute(time_result::int::string::time) || ' min '|| second(time_result::int::string::time) || ' sec ' as "Average Time per Users"
FROM   APPLAYDU.LAUNCH_RESUME t
    join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country = t.country 
WHERE 1=1 and time_spent::int >= 0
    AND time_spent::int < 86400
    and version >= (select min(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{from_version}}]]) and version <= (select max(version) from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{to_version}}]])
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ) and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and t.country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]] [[AND {{iregion}}]])
	[[AND {{ishopfilter}}]]
    and (client_time >= '2020-08-10' and client_time < dateadd(day, -3, CURRENT_DATE()))
)
where "Total Session" > 0
