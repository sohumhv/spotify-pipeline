-- ============================================================
-- Spotify ELT Pipeline - Gold Layer
-- Dashboard-ready Dynamic Tables with aggregations and rankings.
-- TARGET_LAG = '1 hour': refresh at most once per hour.
-- ============================================================

-- Per-artist summary: track/album counts, duration stats, image URL for Power BI slicer
CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.GOLD.DT_ARTIST_OVERVIEW
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
    SELECT
        t.ARTIST_ID,
        t.ARTIST_NAME,
        a.IMAGE_URL                                                            AS ARTIST_IMAGE_URL,
        t.ARTIST_FOLLOWERS,
        COUNT(DISTINCT t.TRACK_ID)                                             AS TRACK_COUNT,
        COUNT(DISTINCT t.ALBUM_ID)                                             AS ALBUM_COUNT,
        COUNT(DISTINCT CASE WHEN t.ALBUM_TYPE = 'album' THEN t.ALBUM_ID END)  AS STUDIO_ALBUM_COUNT,
        COUNT(DISTINCT CASE WHEN t.ALBUM_TYPE = 'single' THEN t.ALBUM_ID END) AS SINGLE_COUNT,
        ROUND(AVG(t.DURATION_MIN), 2)                                          AS AVG_TRACK_DURATION_MIN,
        ROUND(MIN(t.DURATION_MIN), 2)                                          AS MIN_TRACK_DURATION_MIN,
        ROUND(MAX(t.DURATION_MIN), 2)                                          AS MAX_TRACK_DURATION_MIN,
        SUM(CASE WHEN t.IS_EXPLICIT THEN 1 ELSE 0 END)                        AS EXPLICIT_TRACK_COUNT,
        MIN(t.RELEASE_YEAR)                                                    AS EARLIEST_RELEASE_YEAR,
        MAX(t.RELEASE_YEAR)                                                    AS LATEST_RELEASE_YEAR
    FROM MUSIC_DB.SILVER.DT_TRACKS_ENRICHED t
    JOIN MUSIC_DB.BRONZE.DT_ARTISTS a
        ON t.ARTIST_ID = a.ARTIST_ID
    GROUP BY t.ARTIST_ID, t.ARTIST_NAME, t.ARTIST_FOLLOWERS, a.IMAGE_URL;


-- All tracks with single-version tagging and per-artist ranking.
-- TRACK_LABEL = "Artist Name (mm:ss)" for Power BI treemap display.
CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.GOLD.DT_TOP_TRACKS_RANKED
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
    WITH album_tracks AS (
        SELECT DISTINCT TRACK_NAME, ARTIST_NAME
        FROM MUSIC_DB.SILVER.DT_TRACKS_ENRICHED
        WHERE ALBUM_TYPE = 'album'
    )
    SELECT
        t.TRACK_ID,
        CASE
            WHEN t.ALBUM_TYPE = 'single' AND a.TRACK_NAME IS NOT NULL
                THEN t.TRACK_NAME || ' (Single Version)'
            ELSE t.TRACK_NAME
        END AS TRACK_NAME,
        t.ARTIST_NAME || ' (' || t.DURATION_DISPLAY || ')' AS TRACK_LABEL,
        t.ARTIST_NAME,
        t.ALBUM_NAME,
        t.ALBUM_TYPE,
        t.RELEASE_DATE,
        t.RELEASE_YEAR,
        t.DURATION_MS,
        t.DURATION_MIN,
        t.DURATION_DISPLAY,
        t.IS_EXPLICIT,
        t.SPOTIFY_URL,
        ROW_NUMBER() OVER (
            PARTITION BY t.ARTIST_NAME
            ORDER BY t.ALBUM_NAME, t.TRACK_NUMBER
        ) AS ARTIST_TRACK_NUM
    FROM MUSIC_DB.SILVER.DT_TRACKS_ENRICHED t
    LEFT JOIN album_tracks a
        ON t.TRACK_NAME = a.TRACK_NAME
        AND t.ARTIST_NAME = a.ARTIST_NAME;


-- All albums ranked by recency (overall and per-artist)
CREATE OR REPLACE DYNAMIC TABLE MUSIC_DB.GOLD.DT_ALBUM_ANALYTICS
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
    SELECT
        a.ALBUM_ID,
        a.ALBUM_NAME,
        a.ARTIST_NAME,
        a.ALBUM_TYPE,
        a.RELEASE_DATE,
        a.RELEASE_YEAR,
        a.TOTAL_TRACKS,
        a.ARTIST_FOLLOWERS,
        a.IMAGE_URL,
        a.SPOTIFY_URL,
        RANK() OVER (ORDER BY a.RELEASE_DATE DESC NULLS LAST)                            AS RECENCY_RANK,
        RANK() OVER (PARTITION BY a.ARTIST_NAME ORDER BY a.RELEASE_DATE DESC NULLS LAST) AS ARTIST_RECENCY_RANK
    FROM MUSIC_DB.SILVER.DT_ALBUMS_ENRICHED a;
