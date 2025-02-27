--main query
SELECT 
    REPLACE(country_name, 'Undefined', '(no country code)') AS `Country name`,
    SUM(event_count) AS `Downloads`
FROM 
    `gcp-gfb-sai-tracking-gold.applaydu.store_stats`
WHERE 
    1=1  
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])
    AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])
    AND event_id = 393584 
    AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads')
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND version IN ('1.0.0')
GROUP BY 
    country_name
LIMIT 30