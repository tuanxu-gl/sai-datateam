insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1,value2,value3,value4,value5,value6_str,value7,value8,value9,value10,value11,value12,value13,value14_str,value15,value16_str,value17,value18_str,value19_str,value20_str)
WITH t_users AS (
  select 
    user_id,
    SUM(sessions_count) AS sessions_count,
    SUM(total_time_spent) AS total_time_spent,
    SUM(toy_unlocked_by_scan_count)+SUM(scan_mode_finished_count) AS total_scans
  from 
    `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` t
  join 
    `applaydu.tbl_shop_filter` sf ON sf.game_id=t.game_id and sf.country_name=t.country_name
  where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY)
      and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY) )
      and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY) ), INTERVAL 1 DAY)
  GROUP BY 
    user_id
)
--main query

select 14 as dashboard_id
            ,263 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Generic KPIs' as kpi_name
            ,users as value1,sessions as value2,avg_sessions_per_user as value3,sum_total_time_spent as value4,time_spent_per_user as value5,CAST(`Time spent per user` as STRING) as value6_str,scan_users as value7,sum_scans as value8,avg_scans as value9,scan_sessions as value10,scan_avg_sessions_per_user as value11,scan_total_time_spent as value12,scan_avg_time_spent_per_user as value13,CAST(`Time spent per scan user` as STRING) as value14_str,scan_time_spent_per_session as value15,CAST(`Time spent per session of scan user` as STRING) as value16_str,`time_spent_per_session` as value17,CAST(`Time spent per session` as STRING) as value18_str,CAST(`Time spent per no scan user` as STRING) as value19_str,CAST(`Time spent per session of no scan user` as STRING) as value20_str
        from
        (
        
select 
  --generic users
    COUNT(DISTINCT user_id) AS users,
    SUM(sessions_count) AS sessions,
    SUM(sessions_count)/COUNT(DISTINCT user_id) AS avg_sessions_per_user,
    SUM(total_time_spent) AS sum_total_time_spent,
    cast(SUM(total_time_spent) / COUNT(DISTINCT user_id) as INT64) AS time_spent_per_user,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(total_time_spent) / COUNT(DISTINCT user_id) AS INT64))) AS `Time spent per user`,
  --scan users  
    COUNT(DISTINCT case when total_scans>0 then user_id else 0 end) AS scan_users,
    SUM(total_scans) AS sum_scans,
    SUM(total_scans)/COUNT(DISTINCT case when total_scans>0 then user_id else 0 end) AS avg_scans,
    SUM(case when total_scans>0 then sessions_count else 0 end ) AS scan_sessions,
    SUM(case when total_scans>0 then sessions_count else 0 end )/COUNT(DISTINCT case when total_scans>0 then user_id else 0 end) AS scan_avg_sessions_per_user,
    SUM(case when total_scans>0 then total_time_spent else 0 end ) AS scan_total_time_spent,
    SUM(case when total_scans>0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans>0 then user_id else 0 end) AS scan_avg_time_spent_per_user,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans>0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans>0 then user_id else 0 end) AS INT64))) AS `Time spent per scan user`,
    SUM(case when total_scans>0 then total_time_spent else 0 end ) / SUM(case when total_scans>0 then sessions_count else 0 end ) AS scan_time_spent_per_session,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans>0 then total_time_spent else 0 end ) / SUM(case when total_scans>0 then sessions_count else 0 end ) AS INT64))) AS `Time spent per session of scan user`,
    cast(SUM(total_time_spent) / SUM(sessions_count) as INT64) AS time_spent_per_session,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(cast(SUM(total_time_spent) / SUM(sessions_count) as INT64))) AS `Time spent per session`,
	--no scan users
		FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans=0 then total_time_spent else 0 end ) / COUNT(DISTINCT case when total_scans=0 then user_id else 0 end) AS INT64))) AS `Time spent per no scan user`,
    FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(SUM(case when total_scans=0 then total_time_spent else 0 end ) / SUM(case when total_scans=0 then sessions_count else 0 end ) AS INT64))) AS `Time spent per session of no scan user`
  from 
    t_users
)