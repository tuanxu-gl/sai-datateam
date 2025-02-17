with r243 as(
SELECT value1_str as "Country name",value2 as "Downloads"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 243 
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
		 and dashboard_id=14 and query_id = 243
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q243 as (select 0)

select * from r243
union
select * from
(

SELECT 
    REPLACE(COUNTRY_NAME, 'Undefined', '(no country code)' ) as "Country name",
    sum(event_count) AS "Downloads"
from APPLAYDU_NOT_CERTIFIED.store_stats
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' 
    and DATE(client_time) >= (
        CASE
            WHEN (SELECT MIN(server_date) FROM tbl_date_filter WHERE 1=1 [[AND {{idate}}]]) > '2020-08-10'
            THEN (SELECT MIN(server_date) FROM tbl_date_filter WHERE 1=1 [[AND {{idate}}]])
            ELSE CAST((SELECT ivalue FROM tbl_variables WHERE ikey = 'db_start_date') AS DATE)
        END
    )
    and client_time < dateadd(day, 1, (SELECT max(SERVER_DATE) from tbl_date_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] ))
    and COUNTRY_NAME in (select COUNTRY_NAME from tbl_country_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{icountry}}]]) 
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]]) 
    and event_id = 393584 
    and kpi_name in ('App Units','Install Events','Install events','New Downloads')
    and client_time >= '2020-08-10' and client_time < dateadd(day, -3, CURRENT_DATE())
    and VERSION IN ('1.0.0')
group by COUNTRY_NAME
order by "Downloads" desc
limit 30
)
where "Country name" > 0
