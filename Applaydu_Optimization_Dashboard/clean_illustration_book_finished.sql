
MERGE `gcp-gfb-sai-tracking-gold.applaydu.illustration_book_finished` AS target
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
        realtime_spent
        ,count(0) as event_count
    FROM
        `gcp-bi-elephant-db-gold.applaydu.illustration_book_finished`
    WHERE
        DATE(client_time) = '2025-02-01'
        AND (
        (version >= '4.0.0' and version < '9.0.0') 
        
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
       target.realtime_spent = source.realtime_spent
WHEN NOT MATCHED THEN
    INSERT (user_id, game_id, event_id, client_time, version, country, session_id, token, realtime_spent,event_count)
    VALUES (source.user_id, source.game_id, source.event_id, source.client_time, source.version, source.country, source.session_id, source.token, source.realtime_spent,event_count);
