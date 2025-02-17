insert into APPLAYDU_NOT_CERTIFIED.apd_report_14 
    (dashboard_id,query_id,start_date,end_date,load_time,kpi_name,value1_str,value2_str,value3_str,value4)
with dim_date as (
  select server_date as grass_date
  from APPLAYDU_NOT_CERTIFIED.tbl_date_filter
  where day(server_date) = 1
  and server_date >= '2023-01-01'
)
, dim_user as (
  select user_id, min(to_date(client_time)) first_date
  from ELEPHANT_DB.APPLAYDU.LAUNCH_RESUME l
  where l.user_id is not null
  and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='persona_starting_date') 
  group by 1
)
, dim_user_date as (
  select u.user_id, d.grass_date
  from dim_user u
  join dim_date d
    on d.grass_date >= u.first_date
)
, unlock as (
  select to_date(client_time) client_date
  , user_id
  , count(1) as toy_unlock_cnt
  FROM "ELEPHANT_DB"."APPLAYDU"."TOY_UNLOCKED"
  where (("APPLAYDU"."TOY_UNLOCKED"."UNLOCK_CAUSE" = 'QR Code'
     OR "APPLAYDU"."TOY_UNLOCKED"."UNLOCK_CAUSE" = 'Toy Scan' 
     OR "APPLAYDU"."TOY_UNLOCKED"."UNLOCK_CAUSE" = 'Deep_Link')
    and "APPLAYDU"."TOY_UNLOCKED"."ISNEWTOY" = 1)
    and client_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey='persona_starting_date') 
  group by 1,2
)
, lifetime_unlock as (
  select d.grass_date, d.user_id, sum(toy_unlock_cnt) toy_unlock_cnt
  from dim_user_date d
  left join unlock u
    on d.user_id = u.user_id
    and u.client_date < d.grass_date
  group by 1,2
)
,persona as (
  select grass_date as grass_month, user_id
    , case
        when toy_unlock_cnt is null or toy_unlock_cnt = 0 then 'P1'
        when toy_unlock_cnt in (1,2,3) then 'P2'
        when toy_unlock_cnt > 3 then 'P3'
      end as persona
    , toy_unlock_cnt
  from lifetime_unlock
)
, mau as (
  select distinct user_id
    , country
    , date_from_parts(year(to_date(client_time)),month(to_date(client_time)),1) grass_month
    , min(date_from_parts(year(to_date(client_time)),month(to_date(client_time)),1)) over(partition by user_id) first_month
  from ELEPHANT_DB.APPLAYDU.LAUNCH_RESUME t
  where user_id is not null
   and not (game_id = 82471 and client_time <'2020-12-14')
   and t.country in (select country from applaydu_not_certified.tbl_country_filter)
	 and version in (select version from applaydu_not_certified.tbl_version_filter)
)
, final_agg as (
select m.grass_month
  , coalesce(p.persona,'P1') persona
  , count(distinct m.user_id) user_cnt
from mau m
left join persona p
  on m.user_id = p.user_id
  and m.grass_month = dateadd(month,-1,p.grass_month)
  -- and m.grass_month = p.grass_month
where m.grass_month >= '2023-01-01'
group by 1,2
)
, final_user as (
select m.grass_month
  , m.first_month
  , m.user_id
  , coalesce(p1.persona,'P1') month_end_persona
  , coalesce(p2.persona,'P1') month_start_persona
from mau m
left join persona p1
  on m.user_id = p1.user_id
  and m.grass_month = dateadd(month,-1,p1.grass_month)
  -- and m.grass_month = p.grass_month
left join persona p2
  on m.user_id = p2.user_id
  and m.grass_month = p2.grass_month
)
--main query

select 14 as dashboard_id
		,4413 as query_id
		,'istart_date' as start_date
		,'iend_date' as end_date
		,current_timestamp() as load_time
		,'P3 MAU by inflow source' as kpi_name
		,grass_month as value1_str,starting_persona as value2_str,persona as value3_str,user_cnt as value4
	from
	(
	
  select grass_month
    , case
      when grass_month = first_month then 'New-in-month'
      when month_start_persona in ('P1','P2') then 'P1,P2 inflow'
      else 'Existing P3'
    end as starting_persona
    , month_end_persona persona
    , count(distinct user_id) user_cnt
  from final_user
  where month_end_persona = 'P3'
  group by 1,2,3
)