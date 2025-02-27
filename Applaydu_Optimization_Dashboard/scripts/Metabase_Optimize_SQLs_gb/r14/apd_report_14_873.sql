insert into `gcp-gfb-sai-tracking-gold.applaydu.apd_report_14` 
        (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2)
--main query

select 14 as dashboard_id
            ,873 as query_id
            ,timestamp('istart_date') as start_date
            ,timestamp('iend_date') as end_date
            ,current_timestamp() as load_time
            ,'Number of scans' as kpi_name
            ,CAST(`Scan type` as STRING) as value1_str,`Scans` as value2
        from
        (
        
select 
  CASE 
    WHEN scan_type in ('Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette') THEN 'Vignette'
    WHEN scan_type in ('Scan_QR', 'QR Code', 'Deep_Link') THEN 'QR Code'
    WHEN scan_type in ('Toy Scan', 'Scan_Toy') THEN 'Scan toy'
    ELSE 'Others' 
  END AS `Scan type`,
  SUM(total_scan) AS `Scans`
from 
  `applaydu.tbl_sum_scan_unlock` t
join 
  `applaydu.tbl_shop_filter` sf ON sf.game_id=t.game_id and sf.country=t.country
where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY)
  and scan_type in ('Alternative_Vignette', 'Scan_Vignette', 'Alternative Vignette', 'Vignette', 'Scan_QR', 'QR Code', 'Toy Scan', 'Scan_Toy', 'Deep_Link')
  and date(server_date)>=(select min(server_date) from `applaydu.tbl_date_filter` where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY) )
  and date(server_date)<date_add((select max(server_date) from `applaydu.tbl_date_filter` where 1=1 and date(server_date) >= 'istart_date' and date(server_date) < date_add('iend_date', INTERVAL 1 DAY) ), INTERVAL 1 DAY)
GROUP BY 
  `Scan type`
ORDER BY 
  `Scan type` ASC
)