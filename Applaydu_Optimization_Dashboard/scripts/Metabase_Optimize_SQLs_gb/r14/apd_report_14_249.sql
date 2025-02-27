insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
            ,249 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Total Downloads' as kpi_name
            ,CAST(`Shop` as STRING) as value1_str,`Total Installations` as value2
        from
        (
        
select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(t.game_id AS STRING), '81335', 'App Store'), '81337', 'Google Play'), '82471', 'AppInChina'), '85247', 'AppInChina'), '84515', 'Samsung'), '85837', 'Amazon') AS `Shop`, 
    SUM(event_count) AS `Total Installations`
   from 
    `gcp-gfb-sai-tracking-gold.applaydu.store_stats` t
   join 
    `applaydu.tbl_shop_filter` USING (game_id, country_name)
   where 1=1 and date(client_time) >= 'istart_date' and date(client_time) < date_add('iend_date', INTERVAL 1 DAY) 	
    and event_id=393584 
    and kpi_name in ('App Units', 'Install Events', 'Install events', 'New Downloads') 
    and version in ('1.0.0') 
   GROUP BY `Shop`
)