--main query
SELECT 
    DATE_TRUNC(DATE(client_time), WEEK) AS `Week`,
    SUM(event_count) AS `New Installations`
FROM 
    `gcp-gfb-sai-tracking-gold.applaydu.store_stats` t
JOIN 
    `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country_name = t.country_name
WHERE 1=1 and
    event_id = 393584 
    AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads')
    AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND version IN ('1.0.0')
    AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]])
    AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 2=2 [[AND {{idate}}]]), INTERVAL 1 DAY)
    AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])    
    [[AND {{ishopfilter}}]]
GROUP BY 
    `Week`