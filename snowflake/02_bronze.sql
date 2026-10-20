-- ============================================================
-- Spotify ELT Pipeline - Bronze Layer
-- Dynamic Tables that parse raw JSON into typed columns.
-- RAW is append-only (re-fetched records are loaded again), so each Bronze DT
-- keeps only the newest row per Spotify ID: updated metadata replaces the old
-- version instead of creating a duplicate.
-- TARGET_LAG = DOWNSTREAM: only refresh when Silver needs them.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.BRONZE.DT_ARTISTS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
AS
    SELECT
        SRC:"artist_id"::VARCHAR    AS ARTIST_ID,
        SRC:"artist_name"::VARCHAR  AS ARTIST_NAME,
        SRC:"genres"::VARIANT       AS GENRES,
        SRC:"followers"::INT        AS FOLLOWERS,
        SRC:"image_url"::VARCHAR    AS IMAGE_URL,
        SRC:"spotify_url"::VARCHAR  AS SPOTIFY_URL,
        LOADED_AT
    FROM MUSIC_DB.RAW.RAW_ARTISTS
    WHERE SRC:"artist_id" IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC:"artist_id"::VARCHAR ORDER BY LOADED_AT DESC) = 1;


CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.BRONZE.DT_ALBUMS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
AS
    SELECT
        SRC:"album_id"::VARCHAR              AS ALBUM_ID,
        SRC:"album_name"::VARCHAR            AS ALBUM_NAME,
        SRC:"artist_id"::VARCHAR             AS ARTIST_ID,
        SRC:"artist_name"::VARCHAR           AS ARTIST_NAME,
        SRC:"album_type"::VARCHAR            AS ALBUM_TYPE,
        SRC:"release_date"::VARCHAR          AS RELEASE_DATE,
        SRC:"release_date_precision"::VARCHAR AS RELEASE_DATE_PRECISION,
        SRC:"total_tracks"::INT              AS TOTAL_TRACKS,
        SRC:"image_url"::VARCHAR             AS IMAGE_URL,
        SRC:"spotify_url"::VARCHAR           AS SPOTIFY_URL,
        LOADED_AT
    FROM MUSIC_DB.RAW.RAW_RELEASES
    WHERE SRC:"album_id" IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC:"album_id"::VARCHAR ORDER BY LOADED_AT DESC) = 1;


CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.BRONZE.DT_TRACKS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
AS
    SELECT
        SRC:"track_id"::VARCHAR           AS TRACK_ID,
        SRC:"track_name"::VARCHAR         AS TRACK_NAME,
        SRC:"artist_id"::VARCHAR          AS ARTIST_ID,
        SRC:"artist_name"::VARCHAR        AS ARTIST_NAME,
        SRC:"album_id"::VARCHAR           AS ALBUM_ID,
        SRC:"album_name"::VARCHAR         AS ALBUM_NAME,
        SRC:"album_release_date"::VARCHAR AS ALBUM_RELEASE_DATE,
        SRC:"duration_ms"::INT            AS DURATION_MS,
        SRC:"explicit"::BOOLEAN           AS IS_EXPLICIT,
        SRC:"track_number"::INT           AS TRACK_NUMBER,
        SRC:"disc_number"::INT            AS DISC_NUMBER,
        SRC:"spotify_url"::VARCHAR        AS SPOTIFY_URL,
        LOADED_AT
    FROM MUSIC_DB.RAW.RAW_RECORDINGS
    WHERE SRC:"track_id" IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC:"track_id"::VARCHAR ORDER BY LOADED_AT DESC) = 1;
