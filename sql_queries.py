# DROP TABLES

songplay_table_drop = 'DROP TABLE IF EXISTS songplays'
user_table_drop = 'DROP TABLE IF EXISTS users'
song_table_drop = 'DROP TABLE IF EXISTS songs'
artist_table_drop = 'drop table if exists artist'
time_table_drop = 'drop table if exists time' 

# create tables

songplay_table_create = ("""
    create table if not exists songplays (
        songplay_id bigserial PRIMARY KEY,
        start_time bigint,
        user_id int,
        level text,
        song_id text,
        artist_id text,
        session_id int,
        location text,
        user_agent text
    )
""")

user_table_create = ("""
    create table if not exists users (
        user_id int PRIMARY KEY,
        first_name text,
        last_name text,
        gender text,
        level text
    )
""")

song_table_create = ("""
    create table if not exists songs (
        song_id text PRIMARY KEY,
        title text,
        artist_id text,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    create table if not exists artists (
        artist_id text PRIMARY KEY,
        name text,
        location text,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    create table if not exists time (
        start_time bigint PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays (
        start_time, user_id, level, 
        song_id, artist_id, session_id, 
        location, user_agent
    ) VALUES (
       %s, %s, %s, %s, %s, %s, %s, %s
    )
""")

user_table_insert = ("""
    insert into users (
        user_id, first_name, last_name,
        gender, level
    ) VALUES (
       %s, %s, %s, %s, %s
    )
    on conflict (user_id)
    do update 
    set level = 'paid'
""")

song_table_insert = ("""
    insert into songs (
        song_id, title, artist_id,
        year, duration
    ) VALUES (
       %s, %s, %s, %s, %s
    )
""")

artist_table_insert = ("""
    insert into artists (
        artist_id, name, location,
        latitude, longitude
    ) VALUES (
       %s, %s, %s, %s, %s
    )
    on conflict (artist_id) 
    do nothing
""")


time_table_insert = ("""
    insert into time (
        start_time, hour, day,
        week, month, year,
        weekday
    ) VALUES (
       %s, %s, %s, %s, %s, %s, %s
    )
    on conflict (start_time)
    do nothing
""")

# FIND SONGS

song_select = ("""
    SELECT song_id, songs.artist_id 
    FROM songs
    JOIN artists on songs.artist_id = artists.artist_id
    WHERE songs.title = %s 
    AND artists.name = %s 
    AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
