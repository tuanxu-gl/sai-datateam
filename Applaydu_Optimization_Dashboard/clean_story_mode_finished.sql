
MERGE `gcp-gfb-sai-tracking-gold.applaydu.story_mode_finished` AS target
USING (
    SELECT
        user_id,
        game_id,
        event_id,
        client_time,
        version,
        country,
        session_id,
        token,
        end_cause,
        story_step,
        time_to_finish,
        activity_01,
        activity_01_value,
        environment_id,
        realtime_spent,
        load_time,
        from_scene
        ,count(0) as event_count
    FROM
        `gcp-bi-elephant-db-gold.applaydu.story_mode_finished`
    WHERE
        DATE(client_time) = '2025-02-01'
        AND (
        (version >='5.0.0' and version < '5.2.0' and (environment_id = 'Experience - Dino Museum' and version >= '4.7.0')) --q3094 line 41
        or ( --q3094 line 54
                environment_id like 'Natoons v4%' or
                (environment_id like '%Travel%' and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
                (environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
                (environment_id NOT IN ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land', 'Experience - Dino Museum') AND (environment_id not LIKE '%Travel%') ) or 
                (environment_id = 'Kinderini' and server_time >= '2024-10-19' ) or 
                (environment_id = 'Eduland Lets Story' and server_time >= '2024-08-28')
            )
        or((version<'5.0.0' or version>='5.2.0')
            and (environment_id='Experience - Dino Museum' and version>='4.7.0'))
        )
       
    GROUP BY ALL
) AS source
ON target.user_id = source.user_id AND
       target.game_id = source.game_id AND
       target.event_id = source.event_id AND
       target.client_time = source.client_time AND
       target.version = source.version AND
       target.country = source.country AND
       target.session_id = source.session_id AND
       target.token = source.token AND
       target.end_cause = source.end_cause AND
       target.story_step = source.story_step AND
       target.time_to_finish = source.time_to_finish AND
       target.activity_01 = source.activity_01 AND
       target.activity_01_value = source.activity_01_value AND
       target.environment_id = source.environment_id AND
       target.realtime_spent = source.realtime_spent AND
       target.load_time = source.load_time AND
       target.from_scene = source.from_scene
WHEN NOT MATCHED THEN
    INSERT (user_id, game_id, event_id, client_time, version, country, session_id, token, end_cause, story_step, time_to_finish, activity_01, activity_01_value, environment_id, realtime_spent, load_time, from_scene,event_count)
    VALUES (source.user_id, source.game_id, source.event_id, source.client_time, source.version, source.country, source.session_id, source.token, source.end_cause, source.story_step, source.time_to_finish, source.activity_01, source.activity_01_value, source.environment_id, source.realtime_spent, source.load_time, source.from_scene,event_count);
