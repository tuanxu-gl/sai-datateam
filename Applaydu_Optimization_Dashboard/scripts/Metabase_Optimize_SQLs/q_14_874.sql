with r874 as(
SELECT value1_str as "Category",value2 as "Total Scans"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 874 
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
		 and dashboard_id=14 and query_id = 874
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q874 as (select 0)

select * from r874
union
select * from
(

select coalesce(Category,'Other') as "Category"
   -- ,t.toy_name
    ,sum(total_scan) as "Total Scans" 
from APPLAYDU_NOT_CERTIFIED.tbl_sum_scan_unlock  t
        join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country = t.country
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' and scan_type in ('Alternative_Vignette','Scan_Vignette','Alternative Vignette','Vignette','Scan_QR','QR Code','Toy Scan','Scan_Toy','Deep_Link')
    and server_date >= '2020-08-10' and server_date < dateadd(day, -3, CURRENT_DATE())
    and server_date < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and t.country in (select country from tbl_country_filter where 2=2  [[AND {{icountry}}]])    
    and version in (select version from tbl_version_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{iversion}}]])
    [[AND {{ishopfilter}}]]
group by "Category"--,t.toy_name
Order by "Total Scans" desc
)
where "Category" > 0
