--main query
SELECT (CAST(custom_tracking AS FLOAT64)) AS rating -- Max Rating in a week
FROM 
  `applaydu.store_stats`
WHERE 
  event_id = 393584 
  AND kpi_name IN ('Total Average Rating')
  AND DATE(client_time) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
  AND DATE(client_time) < DATE_ADD((SELECT MAX(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)
  AND version IN ('1.0.0')
  AND country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 1=1 [[AND {{icountry}}]])
ORDER BY 
  client_time DESC
LIMIT 1
