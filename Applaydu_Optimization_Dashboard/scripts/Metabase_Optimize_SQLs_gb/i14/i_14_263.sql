
WITH t_users AS (
    SELECT 
        user_id,
        SUM(sessions_count) AS sessions_count,
        SUM(total_time_spent) AS total_time_spent,
        SUM(toy_unlocked_by_scan_count)+SUM(scan_mode_finished_count) AS total_scans
        
        

    FROM 
        `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
    JOIN 
        `applaydu.tbl_shop_filter` sf ON sf.game_id = t.game_id AND sf.country_name = t.country_name
    WHERE 1=1
            and date(server_date) >= (SELECT MIN(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]])
            and date(server_date) < DATE_ADD((SELECT max(server_date) FROM `applaydu.tbl_date_filter` WHERE 1=1 [[AND {{idate}}]]), INTERVAL 1 DAY)

        AND t.country_name IN (SELECT country_name FROM `applaydu.tbl_country_filter` WHERE 2=2 [[AND {{icountry}}]])   
        AND version IN (SELECT version FROM `applaydu.tbl_version_filter` WHERE 1=1 [[AND {{iversion}}]])    	
        [[AND {{ishopfilter}}]]
    GROUP BY 
        user_id
)
--main query
SELECT 
    --generic users
        COUNT(DISTINCT user_id) AS users,
        SUM(sessions_count) AS sessions,
        SUM(sessions_count)/COUNT(DISTINCT user_id) AS avg_sessions_per_user,
        SUM(total_time_spent) AS sum_total_time_spent,
        cast(SUM(total_time_spent) / COUNT(DISTINCT user_id) as INT64) AS time_spent_per_user,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Time spent per user`,

    --scan users    
        COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS scan_users,
        SUM(total_scans) AS sum_scans,
        SUM(total_scans)/COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS avg_scans,
        SUM(case when total_scans > 0 then sessions_count else 0 end ) AS scan_sessions,
        SUM(case when total_scans > 0 then sessions_count else 0 end )/COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS scan_avg_sessions_per_user,
        
        SUM(case when total_scans > 0 then total_time_spent else 0 end ) AS scan_total_time_spent,
        SUM(case when total_scans > 0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS scan_avg_time_spent_per_user,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans > 0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans > 0 then user_id else 0 end) AS INT64))) AS `Time spent per scan user`,
        
        SUM(case when total_scans > 0 then total_time_spent else 0 end ) / SUM(case when total_scans > 0 then sessions_count else 0 end ) AS scan_time_spent_per_session,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans > 0 then total_time_spent else 0 end ) / SUM(case when total_scans > 0 then sessions_count else 0 end ) AS INT64))) AS `Time spent per session of scan user`,
        cast(SUM(total_time_spent) / SUM(sessions_count) as INT64) AS time_spent_per_session,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(cast(SUM(total_time_spent) / SUM(sessions_count) as INT64))) AS `Time spent per session`,
	--no scan users
		FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans = 0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans = 0 then user_id else 0 end) AS INT64))) AS `Time spent per no scan user`,
        FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans = 0 then total_time_spent else 0 end ) / SUM(case when total_scans = 0 then sessions_count else 0 end ) AS INT64))) AS `Time spent per session of no scan user`
    FROM 
        t_users
    
