-- ============================================================
-- Spotify ELT Pipeline - Silver Layer
-- Dynamic Tables that join Bronze tables and add derived columns.
-- Bronze is one row per ID, so these joins cannot fan out.
-- TARGET_LAG = DOWNSTREAM: only refresh when Gold needs them.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.SILVER.DT_TRACKS_ENRICHED
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
AS
    SELECT
        t.TRACK_ID,
        t.TRACK_NAME,
        t.ARTIST_ID,
        t.ARTIST_NAME,
        a.GENRES        AS ARTIST_GENRES,
        a.FOLLOWERS     AS ARTIST_FOLLOWERS,
        t.ALBUM_ID,
        t.ALBUM_NAME,
        t.ALBUM_RELEASE_DATE,
        TRY_TO_DATE(t.ALBUM_RELEASE_DATE)                          AS RELEASE_DATE,
        YEAR(TRY_TO_DATE(t.ALBUM_RELEASE_DATE))                    AS RELEASE_YEAR,
        t.DURATION_MS,
        ROUND(t.DURATION_MS / 60000.0, 2)                          AS DURATION_MIN,
        FLOOR(t.DURATION_MS / 60000) || ':' ||
            LPAD(FLOOR(MOD(t.DURATION_MS, 60000) / 1000), 2, '0')  AS DURATION_DISPLAY,
        t.IS_EXPLICIT,
        t.TRACK_NUMBER,
        t.DISC_NUMBER,
        t.SPOTIFY_URL,
        alb.ALBUM_TYPE,
        alb.TOTAL_TRACKS AS ALBUM_TOTAL_TRACKS
    FROM MUSIC_DB.BRONZE.TRACKS t
    LEFT JOIN MUSIC_DB.BRONZE.ARTISTS a
        ON t.ARTIST_ID = a.ARTIST_ID
    LEFT JOIN MUSIC_DB.BRONZE.ALBUMS alb
        ON t.ALBUM_ID = alb.ALBUM_ID;


CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.SILVER.DT_ALBUMS_ENRICHED
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = COMPUTE_WH
AS
    SELECT
        alb.ALBUM_ID,
        alb.ALBUM_NAME,
        alb.ARTIST_ID,
        alb.ARTIST_NAME,
        a.GENRES                              AS ARTIST_GENRES,
        a.FOLLOWERS                           AS ARTIST_FOLLOWERS,
        alb.ALBUM_TYPE,
        alb.RELEASE_DATE                      AS RELEASE_DATE_STR,
        TRY_TO_DATE(alb.RELEASE_DATE)         AS RELEASE_DATE,
        YEAR(TRY_TO_DATE(alb.RELEASE_DATE))   AS RELEASE_YEAR,
        alb.TOTAL_TRACKS,
        alb.IMAGE_URL,
        alb.SPOTIFY_URL
    FROM MUSIC_DB.BRONZE.ALBUMS alb
    LEFT JOIN MUSIC_DB.BRONZE.ARTISTS a
        ON alb.ARTIST_ID = a.ARTIST_ID;
