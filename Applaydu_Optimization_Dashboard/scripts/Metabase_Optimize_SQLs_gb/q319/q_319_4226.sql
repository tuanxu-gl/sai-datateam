with r4226 as(
SELECT value1_str as Shop,value2 as `Total Installations`
FROM `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319`
where date(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]),'2020-08-10')
    AND date(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]),DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
    and ((select count(0) from `applaydu.tbl_country_filter` ) = (select count(0) from `applaydu.tbl_country_filter` where 1=1  [[AND {{icountry}}]]))
	and dashboard_id=319 and query_id = 4226 
)
,tbl_check_preprocess_report as
(
SELECT CASE 
    WHEN (
        SELECT COUNT(0) 
        FROM `applaydu.apd_report_319`
        WHERE 1=1 and date(start_date) = GREATEST((SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]),'2020-08-10')
            AND date(end_date) = LEAST((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]),DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
            and ((select count(0) from `applaydu.tbl_country_filter` ) = (select count(0) from `applaydu.tbl_country_filter` where 1=1  [[AND {{icountry}}]]))
		 and dashboard_id=319 and query_id = 4226
    ) > 0 
    THEN 'Available data from preprocess report'
    ELSE 'N/A'
END as available  
)
, bq4226 as (select 0)

select * from r4226
union all
select * from
(

SELECT 
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cast(t.game_id as string), '81335', 'App Store'),'81337', 'Google Play'), '82471','AppInChina'), '85247','AppInChina'), '84515','Samsung'),'85837','Amazon') as Shop, 
    SUM(event_count) as `Total Installations`
FROM 
    `gcp-gfb-sai-tracking-gold.applaydu.store_stats` t
JOIN 
    `applaydu.tbl_shop_filter` using (game_id ,country_name)
where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' 	
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]] )
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A' [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` where 1=1 and (select available from tbl_check_preprocess_report) = 'N/A'  [[AND {{icountry}}]])    
    [[AND {{ishopfilter}}]]
    AND event_id = 393584 
    AND kpi_name IN ('App Units','Install Events','Install events','New Downloads') 
    AND DATE(client_time) >= '2020-08-10' 
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND version IN ('1.0.0') 
GROUP BY 
    Shop
)
where `Total Installations` > 0
