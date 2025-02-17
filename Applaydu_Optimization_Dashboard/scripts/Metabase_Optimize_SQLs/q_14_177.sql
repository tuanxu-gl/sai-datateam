with r177 as(
SELECT value1_str as "Toy name",value2 as "Scan Toy",value3 as "Scan QR/Leaflet",value4 as "Total scan"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 177 
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
		 and dashboard_id=14 and query_id = 177
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q177 as (select 0)

select * from r177
union
select * from
(

select toy_name as "Toy name", coalesce("'Scan Toy'", 0) as "Scan Toy", coalesce("'QR/Leaflet'", 0) as "Scan QR/Leaflet", coalesce("'Scan Toy'", 0) + coalesce("'QR/Leaflet'", 0) as "Total scan"
from (
    select *
    from (
        SELECT 
            toy_name,
            (case when SCAN_TYPE = 'Scan_Toy' then 'Scan Toy' else 'QR/Leaflet' end) as scan_type,
            sum(total_scan) as toy_count
        FROM   APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock
        where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
            and toy_name <> 'ZEBRA_VV114'
            and SCAN_TYPE not in ('EXPERIENCE','Experience')
            and server_date < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
            and country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]])    
            and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
            and GAME_ID in (select GAME_ID from tbl_shop_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{ishopfilter}}]])
        group by toy_name, scan_type
    ) as source
    pivot (
        sum(toy_count)
        for scan_type in ('Scan Toy', 'QR/Leaflet')
    ) as pvt
)
order by "Total scan" desc
limit 10
)
where "Toy name" > 0
