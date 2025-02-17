select 'Minigame' as feature
, (select count (distinct user_id) from minigame
        join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[AND {{iINSTALL_SOURCE}}]]) using (user_id)) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

union
select 'Toy Friendship' as feature
, (select count (distinct user_id) from tbl_toy_friendship
        join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[AND {{iINSTALL_SOURCE}}]]) using (user_id)) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

union
select 'Avatar House' as feature
, (select count (distinct user_id) from tbl_avatar
        join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[AND {{iINSTALL_SOURCE}}]]) using (user_id)) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"

union
select 'New Feature' as feature
, (select count (distinct user_id) from new_feature_table
        join (select distinct user_id from ELEPHANT_DB.APPLAYDU.USER_ACTIVITY where 1=1 [[AND {{iINSTALL_SOURCE}}]]) using (user_id)) as Users
, Users /(select count (distinct user_id) from launch_resume ) as "% User Access the Feature"