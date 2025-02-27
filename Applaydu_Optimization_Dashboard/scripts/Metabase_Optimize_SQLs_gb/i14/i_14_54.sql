--main query
SELECT 
    d_country AS `Country name`,
    SUM(event_count) AS `Users`
FROM 
    `applaydu.store_stats`
WHERE 1=1
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])    
    AND country_name IN (SELECT country_name FROM `applaydu.tbl_shop_filter` sf WHERE 1=1 [[AND {{ishopfilter}}]])
    AND game_id IN (SELECT game_id FROM `applaydu.tbl_shop_filter` sf WHERE 1=1 [[AND {{ishopfilter}}]])
    AND event_id = 393584 
    AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads')
GROUP BY 
    d_country