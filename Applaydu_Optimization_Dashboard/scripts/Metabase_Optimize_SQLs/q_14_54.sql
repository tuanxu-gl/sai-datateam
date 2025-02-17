with r54 as(
SELECT value1_str as "Country name",value2 as "Users"
FROM APPLAYDU_NOT_CERTIFIED.apd_report_14
where start_date =  GREATEST((SELECT MIN(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), '2020-08-10')
    AND end_date = LEAST((SELECT MAX(SERVER_DATE) FROM applaydu_not_certified.tbl_date_filter WHERE 1=1 [[AND {{idate}}]]), DATEADD(day, -4, CURRENT_DATE()))
	and dashboard_id=14 and query_id = 54 
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
		 and dashboard_id=14 and query_id = 54
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, q54 as (select 0)

select * from r54
union
select * from
(

SELECT D_COUNTRY as "Country name",sum(event_count)AS "Users"
FROM store_stats
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' 	
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] )
    and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and country_name in (select country_name from tbl_country_filter where 2=2  [[AND {{icountry}}]])    
	and country_name in (select country_name from tbl_shop_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{ishopfilter}}]])
    and GAME_ID in (select GAME_ID from tbl_shop_filter where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{ishopfilter}}]])
    and event_id = 393584 
    and kpi_name in ('App Units','Install Events','Install events','New Downloads')
    and CLIENT_TIME >= '2020-08-10' and CLIENT_TIME < dateadd(day, -3, CURRENT_DATE())
    and VERSION IN ('1.0.0')
group by D_COUNTRY
)
where "Country name" > 0
