--main query
SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(t.game_id AS STRING), '81335', 'App Store'), '81337', 'Google Play'), '82471', 'AppInChina'), '85247', 'AppInChina'), '84515', 'Samsung'), '85837', 'Amazon') AS `Shop`, 
        SUM(event_count) AS `Total Installations`
      FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.store_stats` t
      JOIN 
        `applaydu.tbl_shop_filter` USING (game_id, country_name)
      WHERE 1=1 	
        AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
        AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
        AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])    
        AND event_id = 393584 
        AND kpi_name IN ('App Units', 'Install Events', 'Install events', 'New Downloads') 
        AND DATE(client_time) >= '2020-08-10' 
        AND DATE(client_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND version IN ('1.0.0') 
		[[AND {{ishopfilter}}]]
      GROUP BY `Shop`