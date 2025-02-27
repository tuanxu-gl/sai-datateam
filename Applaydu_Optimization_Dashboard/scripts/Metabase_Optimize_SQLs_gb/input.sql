
--main query
 select date_trunc('week',to_date(client_time)) as Week
    ,sum(event_count) as "New Installations"
from APPLAYDU_NOT_CERTIFIED.STORE_STATS t
    join tbl_shop_filter on tbl_shop_filter.game_id = t.game_id and tbl_shop_filter.country_name = t.country_name 
where event_id = 393584 
    and kpi_name in ('App Units','Install Events','Install events','New Downloads')
    and CLIENT_TIME >= '2020-08-10' and CLIENT_TIME < dateadd(day, -3, CURRENT_DATE()) 
    and VERSION IN ('1.0.0')
    and client_time >= (SELECT min(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] )
    and client_time < dateadd(day, 1,(SELECT max(SERVER_DATE) from tbl_date_filter where 2=2 [[AND {{idate}}]] ))
    and t.country_name in (select country_name from tbl_country_filter where 2=2  [[AND {{icountry}}]])    
	[[AND {{ishopfilter}}]]
group by Week