insert into `gcp-gfb-sai-tracking-gold.applaydu.tbl_users` (
    user_id,
    game_id,
    version,
    country,
    country_name,
    shop_filter,
    server_date,
    launch_resume_count,
    sessions_count,
    total_time_spent,
    toy_unlocked_count,
    new_toy_unlocked_count,
    toy_unlocked_by_scan_count,
    new_toy_unlocked_by_scan_count,
    toy_unlocked_by_deeplink,
    scan_mode_finished_count,
    new_scan_mode_finished_count,
    scan_mode_finished_toy_count,
    scan_mode_finished_leaflet_count
) 
select 
    user_id,
    game_id,
    version,
    country,
    coalesce(c.name, 'Unknown') as country_name,
    case 
        when country = 'cn' and game_id = 82471 then 'AppInChina-China country'
        when country <> 'cn' and game_id = 82471 then 'AppInChina-Other countries'
        when country = 'cn' and game_id = 81335 then 'App Store-China country'
        when country <> 'cn' and game_id = 81335 then 'App Store-Other countries'
        when country = 'cn' and game_id = 81337 then 'Google Play-China country'
        when country <> 'cn' and game_id = 81337 then 'Google Play-Other countries'
        when game_id = 84137 then 'China Market - UA Campaign'
        else 'Others'
    end as shop_filter,
    timestamp(server_date),
    sum(case when kpi_name = 'Launch Resume' then event_count else 0 end) as launch_resume_count,
    sum(case when kpi_name = 'sessions_count' then event_count else 0 end) as sessions_count,
    sum(case when kpi_name = 'total_time_spent' then event_count else 0 end) as total_time_spent,
    sum(case when kpi_name = 'Toy Unlocked' then event_count else 0 end) as toy_unlocked_count,
    sum(case when kpi_name = 'New Toy Unlocked' then event_count else 0 end) as new_toy_unlocked_count,
    sum(case when kpi_name = 'Toy Unlocked by scan' then event_count else 0 end) as toy_unlocked_by_scan_count,
    sum(case when kpi_name = 'New Toy Unlocked by scan' then event_count else 0 end) as new_toy_unlocked_by_scan_count,
    sum(case when kpi_name = 'Toy Unlocked by deeplink' then event_count else 0 end) as toy_unlocked_by_deeplink,
    sum(case when kpi_name = 'Scan Mode Finished' then event_count else 0 end) as scan_mode_finished_count,
    sum(case when kpi_name = 'New Scan Mode Finished' then event_count else 0 end) as new_scan_mode_finished_count,
    sum(case when kpi_name = 'Scan Mode Finished Toy' then event_count else 0 end) as scan_mode_finished_toy_count,
    sum(case when kpi_name = 'Scan Mode Finished Leaflet' then event_count else 0 end) as scan_mode_finished_leaflet_count
from (
    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'total_time_spent' as kpi_name,
        sum(case when time_spent >= 0 and time_spent < 86400 then time_spent else 0 end) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.launch_resume`
    where client_time >= '2025-02-01'
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'sessions_count' as kpi_name,
        sum(case when time_spent >= 0 and time_spent < 86400 and (session_id = 1 or cast(time_between_sessions as int) >= 30) then 1 else 0 end) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.launch_resume`
    where client_time >= '2025-02-01'
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'Launch Resume' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.launch_resume`
    where client_time >= '2025-02-01'
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'Toy Unlocked' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    where client_time >= '2025-02-01'
        and (
            (
                unlock_cause in ('Alternative Vignette', 'QR Code', 'Toy Scan', 'Vignette')
                and version in (select cast(ivalue as string) from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey = 'versions_use_toy_unlock')
            )
            or unlock_cause in ('Deep_Link', 'Experience')
        )
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'Toy Unlocked by scan' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    where client_time >= '2025-02-01'
        and (
            (
                unlock_cause in ('Alternative Vignette', 'QR Code', 'Toy Scan', 'Vignette')
                and version in (select cast(ivalue as string) from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey = 'versions_use_toy_unlock')
            )
            or unlock_cause in ('Deep_Link')
        )
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'New Toy Unlocked by scan' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    where client_time >= '2025-02-01'
        and (
            (
                unlock_cause in ('Alternative Vignette', 'QR Code', 'Toy Scan', 'Vignette')
                and version in (select cast(ivalue as string) from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey = 'versions_use_toy_unlock')
            )
            or unlock_cause in ('Deep_Link')
        )
        and isnewtoy = 1
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'Toy Unlocked by deeplink' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    where client_time >= '2025-02-01'
        and unlock_cause in ('Deep_Link')
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'New Toy Unlocked' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.toy_unlocked`
    where client_time >= '2025-02-01'
        and (
            (
                unlock_cause in ('Alternative Vignette', 'QR Code', 'Toy Scan', 'Vignette')
                and version in (select cast(ivalue as string) from `gcp-gfb-sai-tracking-gold.applaydu.tbl_variables` where ikey = 'versions_use_toy_unlock')
            )
            or unlock_cause in ('Deep_Link', 'Experience')
        )
        and isnewtoy = 1
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'Scan Mode Finished' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
    where 1=1 and client_time >= '2025-02-01' and scan_type in ('Alternative_Vignette', 'Scan_Toy', 'Scan_QR', 'Scan_Vignette')
        and client_time >= '2021-01-06'
        and scan_result in ('New_Toy', 'Old_Toy')
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'New Scan Mode Finished' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished`
    where 1=1 and client_time >= '2025-02-01' and scan_type in ('Alternative_Vignette', 'Scan_Toy', 'Scan_QR', 'Scan_Vignette')
        and client_time >= '2021-01-06'
        and scan_result = 'New_Toy'
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'Scan Mode Finished Toy' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished` t
    where 1=1 and client_time >= '2025-02-01' and (scan_type in ('Scan_Toy') or (t.toy_detected like '%_leftover' and t.reference not like 'http%' and t.version in ('3.1.0', '3.1.2', '3.2.0', '3.2.1')))
        and scan_result in ('New_Toy', 'Old_Toy')
        and client_time >= '2021-01-06'
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(client_time) as server_date,
        'Scan Mode Finished Leaflet' as kpi_name,
        count(0) as event_count
    from `gcp-bi-elephant-db-gold.applaydu.scan_mode_finished` t
    where 1=1 and client_time >= '2025-02-01' and  (scan_type in ('Alternative_Vignette', 'Scan_QR', 'Scan_Vignette', 'Easter Card') 
            and not (t.toy_detected like '%_leftover' and t.reference not like 'http%' and t.version in ('3.1.0', '3.1.2', '3.2.0', '3.2.1'))
            and scan_result in ('New_Toy', 'Old_Toy'))
        and client_time >= '2021-01-06'
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(server_date),
        'Scan Mode Finished' as kpi_name,
        sum(total_scan) as event_count
    from `gcp-gfb-sai-tracking-gold.applaydu.tbl_scan_mode_finished_24x`
	where 1=1 and server_date >= '2025-02-01'  
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(server_date),
        'Scan Mode Finished Toy' as kpi_name,
        sum(total_scan) as event_count
    from `gcp-gfb-sai-tracking-gold.applaydu.tbl_scan_mode_finished_24x`
	where 1=1 and server_date >= '2025-02-01'  
    group by user_id, game_id, version, country, server_date

    union all

    select 
        user_id,
        game_id,
        version,
        country,
        date(server_date),
        'New Scan Mode Finished' as kpi_name,
        sum(visenze_new_toy_count) as event_count
    from `gcp-gfb-sai-tracking-gold.applaydu.tbl_scan_mode_finished_24x`
	where 1=1 and server_date >= '2025-02-01' 
		and total_scan > 0
    group by user_id, game_id, version, country, server_date
) t
left join `gcp-bi-elephant-db-gold.dimensions.country` c on t.country = c.code
group by user_id, game_id, version, country, country_name, shop_filter, server_date;