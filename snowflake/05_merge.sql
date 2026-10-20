-- ============================================================
-- Spotify ELT Pipeline - RAW -> Bronze MERGE
-- The Lambda runs these after each COPY INTO, with SINCE = the run's start time.
-- For a one-time backfill from all of RAW: SET SINCE = '1970-01-01';
-- Newest RAW row per ID wins; unchanged rows (same ROW_HASH) are skipped.
-- ============================================================

SET SINCE = '1970-01-01';

MERGE INTO MUSIC_DB.BRONZE.ARTISTS t
USING (
    SELECT
        SRC:artist_id::VARCHAR    AS artist_id,
        SRC:artist_name::VARCHAR  AS artist_name,
        SRC:genres                AS genres,
        SRC:followers::INT        AS followers,
        SRC:image_url::VARCHAR    AS image_url,
        SRC:spotify_url::VARCHAR  AS spotify_url,
        LOADED_AT,
        HASH(SRC:artist_name::VARCHAR, SRC:genres, SRC:followers::INT,
             SRC:image_url::VARCHAR, SRC:spotify_url::VARCHAR) AS row_hash
    FROM MUSIC_DB.RAW.RAW_ARTISTS
    WHERE LOADED_AT >= $SINCE::TIMESTAMP_LTZ AND SRC:artist_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC:artist_id::VARCHAR ORDER BY LOADED_AT DESC) = 1
) s
ON t.ARTIST_ID = s.artist_id
WHEN MATCHED AND t.ROW_HASH <> s.row_hash THEN UPDATE SET
    ARTIST_NAME = s.artist_name, GENRES = s.genres, FOLLOWERS = s.followers,
    IMAGE_URL = s.image_url, SPOTIFY_URL = s.spotify_url,
    ROW_HASH = s.row_hash, UPDATED_AT = s.LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (ARTIST_ID, ARTIST_NAME, GENRES, FOLLOWERS, IMAGE_URL, SPOTIFY_URL, ROW_HASH, FIRST_SEEN_AT, UPDATED_AT)
    VALUES (s.artist_id, s.artist_name, s.genres, s.followers, s.image_url, s.spotify_url,
            s.row_hash, s.LOADED_AT, s.LOADED_AT);


MERGE INTO MUSIC_DB.BRONZE.ALBUMS t
USING (
    SELECT
        SRC:album_id::VARCHAR               AS album_id,
        SRC:album_name::VARCHAR             AS album_name,
        SRC:artist_id::VARCHAR              AS artist_id,
        SRC:artist_name::VARCHAR            AS artist_name,
        SRC:album_type::VARCHAR             AS album_type,
        SRC:release_date::VARCHAR           AS release_date,
        SRC:release_date_precision::VARCHAR AS release_date_precision,
        SRC:total_tracks::INT               AS total_tracks,
        SRC:image_url::VARCHAR              AS image_url,
        SRC:spotify_url::VARCHAR            AS spotify_url,
        LOADED_AT,
        HASH(SRC:album_name::VARCHAR, SRC:album_type::VARCHAR, SRC:release_date::VARCHAR,
             SRC:total_tracks::INT, SRC:image_url::VARCHAR, SRC:spotify_url::VARCHAR) AS row_hash
    FROM MUSIC_DB.RAW.RAW_RELEASES
    WHERE LOADED_AT >= $SINCE::TIMESTAMP_LTZ AND SRC:album_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC:album_id::VARCHAR ORDER BY LOADED_AT DESC) = 1
) s
ON t.ALBUM_ID = s.album_id
-- Every album in the batch had its tracks re-fetched, so always stamp TRACKS_REFRESHED_AT
WHEN MATCHED THEN UPDATE SET
    ALBUM_NAME = s.album_name, ARTIST_ID = s.artist_id, ARTIST_NAME = s.artist_name,
    ALBUM_TYPE = s.album_type, RELEASE_DATE = s.release_date,
    RELEASE_DATE_PRECISION = s.release_date_precision, TOTAL_TRACKS = s.total_tracks,
    IMAGE_URL = s.image_url, SPOTIFY_URL = s.spotify_url, ROW_HASH = s.row_hash,
    UPDATED_AT = IFF(t.ROW_HASH <> s.row_hash, s.LOADED_AT, t.UPDATED_AT),
    TRACKS_REFRESHED_AT = s.LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (ALBUM_ID, ALBUM_NAME, ARTIST_ID, ARTIST_NAME, ALBUM_TYPE, RELEASE_DATE, RELEASE_DATE_PRECISION,
     TOTAL_TRACKS, IMAGE_URL, SPOTIFY_URL, ROW_HASH, FIRST_SEEN_AT, UPDATED_AT, TRACKS_REFRESHED_AT)
    VALUES (s.album_id, s.album_name, s.artist_id, s.artist_name, s.album_type, s.release_date,
            s.release_date_precision, s.total_tracks, s.image_url, s.spotify_url, s.row_hash,
            s.LOADED_AT, s.LOADED_AT, s.LOADED_AT);


MERGE INTO MUSIC_DB.BRONZE.TRACKS t
USING (
    SELECT
        SRC:track_id::VARCHAR           AS track_id,
        SRC:track_name::VARCHAR         AS track_name,
        SRC:artist_id::VARCHAR          AS artist_id,
        SRC:artist_name::VARCHAR        AS artist_name,
        SRC:album_id::VARCHAR           AS album_id,
        SRC:album_name::VARCHAR         AS album_name,
        SRC:album_release_date::VARCHAR AS album_release_date,
        SRC:duration_ms::INT            AS duration_ms,
        SRC:explicit::BOOLEAN           AS is_explicit,
        SRC:track_number::INT           AS track_number,
        SRC:disc_number::INT            AS disc_number,
        SRC:spotify_url::VARCHAR        AS spotify_url,
        LOADED_AT,
        HASH(SRC:track_name::VARCHAR, SRC:album_id::VARCHAR, SRC:album_name::VARCHAR,
             SRC:album_release_date::VARCHAR, SRC:duration_ms::INT, SRC:explicit::BOOLEAN,
             SRC:track_number::INT, SRC:disc_number::INT, SRC:spotify_url::VARCHAR) AS row_hash
    FROM MUSIC_DB.RAW.RAW_RECORDINGS
    WHERE LOADED_AT >= $SINCE::TIMESTAMP_LTZ AND SRC:track_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC:track_id::VARCHAR ORDER BY LOADED_AT DESC) = 1
) s
ON t.TRACK_ID = s.track_id
WHEN MATCHED AND t.ROW_HASH <> s.row_hash THEN UPDATE SET
    TRACK_NAME = s.track_name, ARTIST_ID = s.artist_id, ARTIST_NAME = s.artist_name,
    ALBUM_ID = s.album_id, ALBUM_NAME = s.album_name, ALBUM_RELEASE_DATE = s.album_release_date,
    DURATION_MS = s.duration_ms, IS_EXPLICIT = s.is_explicit, TRACK_NUMBER = s.track_number,
    DISC_NUMBER = s.disc_number, SPOTIFY_URL = s.spotify_url,
    ROW_HASH = s.row_hash, UPDATED_AT = s.LOADED_AT
WHEN NOT MATCHED THEN INSERT
    (TRACK_ID, TRACK_NAME, ARTIST_ID, ARTIST_NAME, ALBUM_ID, ALBUM_NAME, ALBUM_RELEASE_DATE,
     DURATION_MS, IS_EXPLICIT, TRACK_NUMBER, DISC_NUMBER, SPOTIFY_URL, ROW_HASH, FIRST_SEEN_AT, UPDATED_AT)
    VALUES (s.track_id, s.track_name, s.artist_id, s.artist_name, s.album_id, s.album_name,
            s.album_release_date, s.duration_ms, s.is_explicit, s.track_number, s.disc_number,
            s.spotify_url, s.row_hash, s.LOADED_AT, s.LOADED_AT);
