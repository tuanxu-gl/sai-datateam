def generate_merge_sql(source_table, target_table, date_filter, columns,special_conditions):
    select_columns = ",\n        ".join(columns)
    on_conditions = " AND\n       ".join([f"target.{col} = source.{col}" for col in columns])
    insert_columns = ", ".join(columns)
    values_columns = ", ".join([f"source.{col}" for col in columns])

    sql = f"""
MERGE `{target_table}` AS target
USING (
    SELECT
        {select_columns}
        ,count(0) as event_count
    FROM
        `{source_table}`
    WHERE
        DATE(client_time) = '{date_filter}'
        AND {special_conditions}
    GROUP BY ALL
) AS source
ON {on_conditions}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns},event_count)
    VALUES ({values_columns},event_count);
"""
    return sql

def main():
    clean_tables = [
    {
    'table_name' : "story_mode_finished",
    'source_table' : "gcp-bi-elephant-db-gold.applaydu.story_mode_finished",
    'target_table' : "gcp-gfb-sai-tracking-gold.applaydu.story_mode_finished",
    'date_filter' : "2025-02-01",
    'columns' : [
        "user_id",
        "game_id",
        "event_id",
        "client_time",
        "version",
        "country",
        "session_id",
        "token",
        "end_cause",
        "story_step",
        "time_to_finish",
        "activity_01",
        "activity_01_value",
        "environment_id",
        "realtime_spent",
        "load_time",
        "from_scene"
    ],

    'special_conditions' : """(
        (version >='5.0.0' and version < '5.2.0' and (environment_id = 'Experience - Dino Museum' and version >= '4.7.0')) --q3094 line 41
        or ( --q3094 line 54
                environment_id like 'Natoons v4%' or
                (environment_id like '%Travel%' and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
                (environment_id in ('Savannah','Space','Ocean','Jungle','Magic Land') and ( end_cause <> 'Finished' or (end_cause = 'Finished' and story_step = 'Ending') ) ) or
                (environment_id NOT IN ('Savannah', 'Space', 'Ocean', 'Jungle', 'Magic Land', 'Experience - Dino Museum') AND (environment_id not LIKE '%Travel%') ) or 
                (environment_id = 'Kinderini' and server_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date') ) or 
                (environment_id = 'Eduland Lets Story' and server_time >= (select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date'))
            )
        or((version<'5.0.0' or version>='5.2.0')
            and (environment_id='Experience - Dino Museum' and version>='4.7.0'))
        )
       """
    },
        {
    'table_name' : "illustration_book_finished",
    'source_table' : "gcp-bi-elephant-db-gold.applaydu.illustration_book_finished",
    'target_table' : "gcp-gfb-sai-tracking-gold.applaydu.illustration_book_finished",
    'date_filter' : "2025-02-01",
    'columns' : [
        "user_id",
        "game_id",
        "event_id",
        "client_time",
        "version",
        "country",
        "session_id",
        "token",
        "realtime_spent"
    ],

    'special_conditions' : """(
        (version >= '4.0.0' and version < '9.0.0') 
        
        )
       """
    }
    ]
    variable_replaces = [
        ["(select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_kinderini_start_date')", "'2024-10-19'"],
        ["(select ivalue from APPLAYDU_NOT_CERTIFIED.TBL_VARIABLES where ikey = 'apd_v5_lets_story_start_date')", "'2024-08-28'"],
    ]
    for clean_table in clean_tables:
        for variable_replace in variable_replaces:
            clean_table['special_conditions'] = clean_table['special_conditions'].replace(variable_replace[0], variable_replace[1])

        sql = generate_merge_sql(clean_table['source_table'],
                                  clean_table['target_table'],
                                  clean_table['date_filter'],
                                  clean_table['columns'],
                                  clean_table['special_conditions']
                                   )
        with open("clean_%s.sql"%clean_table['table_name'], "w") as f:
            f.write(sql)

if __name__ == "__main__":
    main()