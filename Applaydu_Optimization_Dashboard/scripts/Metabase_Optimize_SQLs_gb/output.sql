insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_319` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2_str)
with gb4250 as (select 0)
,tbl_install as (
  select user_id, date(min(install_date)) as install_date
  from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= '2025-02-01' and date(active_date) < date_sub('2025-02-15', INTERVAL 3 DAY) 
    and install_country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and install_source IN (select install_source from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= '2025-02-01' and date(active_date) < date_sub('2025-02-15', INTERVAL 3 DAY) )
    and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
  GROUP BY 1 
),
t_users as (
  select user_id,
      SUM(sessions_count) as sessions_count,
      SUM(total_time_spent) as total_time_spent,
      SUM(toy_unlocked_by_scan_count) + SUM(scan_mode_finished_count) as scans
  from `gcp-gfb-sai-tracking-gold.applaydu.tbl_users`
  join (
    select DISTINCT user_id 
    from `gcp-bi-elephant-db-gold.applaydu.user_activity` where 10=10 and date(active_date) >= '2025-02-01' and date(active_date) < date_sub('2025-02-15', INTERVAL 3 DAY) 
      and install_source IN (select ua_filter from `applaydu.tbl_ua_filter` where 2=2 )
      and date(ACTIVE_DATE)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
      and date(ACTIVE_DATE)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
  ) USING (user_id)
  where date(server_date)<date_sub(current_date(), INTERVAL 3 DAY)
    and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and country_name IN (select country_name from `applaydu.tbl_country_filter` where 2=2  )
    and version>=(select min(version) from `applaydu.tbl_version_filter` where 2=2 )
    and version<=(select max(version) from `applaydu.tbl_version_filter` where 2=2 )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
  GROUP BY user_id
),
tbl_launch_resume_src as (
  select 'All' as period,
      SUM(time_spent) as `Total time spent`,
      COUNT(DISTINCT user_id) AS `Total Users`,
      SUM(time_spent) / COUNT(DISTINCT user_id) as time_result,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result)) AS `Average Time per Users`,
      SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT)>=30) THEN 1 ELSE 0 END) AS `Total Sessions`,
      SUM(time_spent) / SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT)>=30) THEN 1 ELSE 0 END) as time_result_sessions,
      --FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(time_result_sessions)) AS `Average Time per Session`,
      SUM(CASE WHEN (session_id=1 OR CAST(time_between_sessions AS INT)>=30) THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id) as `Average Session per User`,
      SUM(CASE WHEN install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN CAST(time_spent AS INT) ELSE 0 END) as `Total Time Spent New users`,
      COUNT(DISTINCT CASE WHEN install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
                THEN user_id ELSE 0 END) AS `Total New Users`,
      SUM(CASE WHEN install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN CAST(time_spent AS INT) ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
                THEN user_id ELSE 0 END) as time_result_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_new_users)) AS `Average Time Spent Per New Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT)>=30) 
             and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
             and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)) 
          THEN 1 ELSE 0 END) AS `Total Sessions New Users`,
      SUM(CASE WHEN install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN CAST(time_spent AS INT) ELSE 0 END) / SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT)>=30) 
             and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
             and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)) 
          THEN 1 ELSE 0 END) as time_result_sessions_new_users,
      --FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(time_result_sessions_new_users)) AS `Average Time per Session New Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT)>=30) 
             and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
             and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)) 
          THEN 1 ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
                THEN user_id ELSE 0 END) as `Average Session per New User`,
      SUM(CASE WHEN install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN CAST(time_spent AS INT) ELSE 0 END) as `Total Time Spent Old users`,
      COUNT(DISTINCT CASE WHEN install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                THEN user_id ELSE 0 END) AS `Total Old Users`,
      SUM(CASE WHEN install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN CAST(time_spent AS INT) ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                THEN user_id ELSE 0 END) as time_result_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_old_users)) AS `Average Time Per Old Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT)>=30) 
             and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )) 
          THEN 1 ELSE 0 END) AS `Total Sessions Old Users`,
      SUM(CASE WHEN install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN CAST(time_spent AS INT) ELSE 0 END) / SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT)>=30) 
             and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )) 
          THEN 1 ELSE 0 END) as time_result_sessions_old_users,
      --FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(time_result_sessions_old_users)) AS `Average Time per Session Old Users`,
      SUM(CASE WHEN ((session_id=1 OR CAST(time_between_sessions AS INT)>=30) 
             and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )) 
          THEN 1 ELSE 0 END) / COUNT(DISTINCT CASE WHEN install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                THEN user_id ELSE 0 END) as `Average Session per Old User`
  from `gcp-bi-elephant-db-gold.applaydu.launch_resume`
  LEFT join tbl_install USING (user_id)
  where CAST(time_spent AS INT)>=0
    and CAST(time_spent AS INT)<86400
    and version>=(select min(version) from `applaydu.tbl_version_filter` where 2=2 )
    and version<=(select max(version) from `applaydu.tbl_version_filter` where 2=2 )
    and version IN (select version from `applaydu.tbl_version_filter` where 2=2 )
    and date(client_time)>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 )
    and date(client_time)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY)
    and `gcp-bi-elephant-db-gold.applaydu.launch_resume`.country IN (select country from `applaydu.tbl_country_filter` where 2=2  )
    and game_id IN (select game_id from `applaydu.tbl_shop_filter` where 2=2 )
    and (date(client_time)>='2020-08-10' )
),
tbl_users_src as (
  select 'All' as period,
      COUNT(DISTINCT CASE WHEN scans>0 THEN user_id ELSE 0 END) as scan_users,
      SUM(CASE WHEN scans>0 THEN total_time_spent ELSE 0 END) as sum_total_time_spent_scan_users,
      SUM(CASE WHEN scans>0 THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans>0 THEN user_id ELSE 0 END) as time_result_scan_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_scan_users)) AS `Time Spent Per Scan User`,
      SUM(CASE WHEN scans>0 THEN sessions_count ELSE 0 END) as scan_sessions_count,
      SUM(CASE WHEN scans>0 THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans>0 THEN sessions_count ELSE 0 END) as time_result_session_scan_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_scan_users)) AS `Time Spent Per Session of Scan Users`,
      COUNT(DISTINCT CASE WHEN scans>0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
                THEN user_id ELSE 0 END) as scan_new_users,
      SUM(CASE WHEN scans>0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_scan_new_users,
      SUM(CASE WHEN scans>0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans>0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
                THEN user_id ELSE 0 END) as time_result_scan_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_scan_new_users)) AS `Time Spent Per Scan New User`,
      SUM(CASE WHEN scans>0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN sessions_count ELSE 0 END) as scan_sessions_new_users_count,
      SUM(CASE WHEN scans>0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans>0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN sessions_count ELSE 0 END) as time_result_session_scan_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_scan_new_users)) AS `Time Spent Per Session of Scan New Users`,
      COUNT(DISTINCT CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                THEN user_id ELSE 0 END) as scan_old_users,
      SUM(CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_scan_old_users,
      SUM(CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                THEN user_id ELSE 0 END) as time_result_scan_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_scan_old_users)) AS `Time Spent Per Scan Old User`,
      SUM(CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN sessions_count ELSE 0 END) as scan_sessions_old_users_count,
      CASE WHEN SUM(CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN sessions_count ELSE 0 END)=0 THEN 0 ELSE SUM(CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans>0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN sessions_count ELSE 0 END) END as time_result_session_scan_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_scan_old_users)) AS `Time Spent Per Session of Scan Old Users`,
      COUNT(DISTINCT CASE WHEN scans=0 THEN user_id ELSE 0 END) as free_users,
      SUM(CASE WHEN scans=0 THEN total_time_spent ELSE 0 END) as sum_total_time_spent_free_users,
      SUM(CASE WHEN scans=0 THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans=0 THEN user_id ELSE 0 END) as time_result_free_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_free_users)) AS `Time Spent Per Free User`,
      SUM(CASE WHEN scans=0 THEN sessions_count ELSE 0 END) as scan_sessions_free_users_count,
      CASE WHEN SUM(CASE WHEN scans=0 THEN sessions_count ELSE 0 END)=0 THEN 0 ELSE SUM(CASE WHEN scans=0 THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans=0 THEN sessions_count ELSE 0 END) END as time_result_session_free_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_free_users)) AS `Time Spent Per Session of Free Users`,
      COUNT(DISTINCT CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
                THEN user_id ELSE 0 END) as free_new_users,
      SUM(CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_free_new_users,
      SUM(CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
                THEN user_id ELSE 0 END) as time_result_free_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_free_new_users)) AS `Time Spent Per Free New User`,
      SUM(CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN sessions_count ELSE 0 END) as scan_sessions_new_free_users_count,
      CASE WHEN SUM(CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN sessions_count ELSE 0 END)=0 THEN 0 ELSE SUM(CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans=0 and install_date>=(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          and install_date<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 2=2 ), INTERVAL 1 DAY) 
          THEN sessions_count ELSE 0 END) END as time_result_session_free_new_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_free_new_users)) AS `Time Spent Per Session of Free New Users`,
      COUNT(DISTINCT CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                THEN user_id ELSE 0 END) as free_old_users,
      SUM(CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN total_time_spent ELSE 0 END) as sum_total_time_spent_free_old_users,
      SUM(CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN total_time_spent ELSE 0 END) / COUNT(DISTINCT CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
                THEN user_id ELSE 0 END) as time_result_free_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_free_old_users)) AS `Time Spent Per Free Old User`,
      SUM(CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN sessions_count ELSE 0 END) as scan_sessions_free_old_users_count,
      CASE WHEN SUM(CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN sessions_count ELSE 0 END)=0 THEN 0 ELSE SUM(CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN total_time_spent ELSE 0 END) / SUM(CASE WHEN scans=0 and install_date<(select min(server_date) from `applaydu.tbl_date_filter` where 2=2 ) 
          THEN sessions_count ELSE 0 END) END as time_result_session_free_old_users,
      --FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(time_result_session_free_old_users)) AS `Time Spent Per Session of Free Old Users`
  from t_users
  LEFT join tbl_install USING (user_id)
)
,result as (
select '01.Average Time per Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result AS INT64))) as value from tbl_launch_resume_src
union all select '02.Average Time Spent Per New Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_new_users AS INT64))) as value from tbl_launch_resume_src
union all select '03.Average Time Per Old Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_old_users AS INT64))) as value from tbl_launch_resume_src
union all select '04.Time Spent Per Scan User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_scan_users AS INT64))) as value from tbl_users_src
union all select '05.Time Spent Per Scan New User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_scan_new_users AS INT64))) as value from tbl_users_src
union all select '06.Time Spent Per Scan Old User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_scan_old_users AS INT64))) as value from tbl_users_src
union all select '07.Time Spent Per Free User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_free_users AS INT64))) as value from tbl_users_src
union all select '08.Time Spent Per Free New User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_free_new_users AS INT64))) as value from tbl_users_src
union all select '09.Time Spent Per Free Old User' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_free_old_users AS INT64))) as value from tbl_users_src
union all select '10.Average Time per Session' as kpi, FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_sessions AS INT64))) as value from tbl_launch_resume_src
union all select '11.Average Time per Session New Users' as kpi, FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_sessions_new_users AS INT64))) as value from tbl_launch_resume_src
union all select '12.Average Time per Session Old Users' as kpi, FORMAT_TIMESTAMP('%M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_sessions_old_users AS INT64))) as value from tbl_launch_resume_src
union all select '13.Time Spent Per Session of Scan Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_scan_users AS INT64))) as value from tbl_users_src
union all select '14.Time Spent Per Session of Scan New Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_scan_new_users AS INT64))) as value from tbl_users_src
union all select '15.Time Spent Per Session of Scan Old Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_scan_old_users AS INT64))) as value from tbl_users_src
union all select '16.Time Spent Per Session of Free Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_free_users AS INT64))) as value from tbl_users_src
union all select '17.Time Spent Per Session of Free New Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_free_new_users AS INT64))) as value from tbl_users_src
union all select '18.Time Spent Per Session of Free Old Users' as kpi, FORMAT_TIMESTAMP('%H hour %M min %S sec', TIMESTAMP_SECONDS(CAST(time_result_session_free_old_users AS INT64))) as value from tbl_users_src
union all select '19.Average Session per User' as kpi, CAST(`Average Session per User` as STRING) as value from tbl_launch_resume_src
union all select '20.Average Session per New User' as kpi, CAST(`Average Session per New User` as STRING) as value from tbl_launch_resume_src
union all select '21.Average Session per Old User' as kpi, CAST(`Average Session per Old User` as STRING) as value from tbl_launch_resume_src
order by kpi asc
)
--main query

select 319 as dashboard_id
            ,4250 as query_id
            ,timestamp('2025-02-01') as start_date
            ,timestamp('2025-02-15') as end_date
            ,current_timestamp() as load_time
            ,'Engagement KPIs:  Returning Users per New Users' as kpi_name
            ,`kpi` as value1_str,`value` as value2_str
        from
        (
        
select * from result
)