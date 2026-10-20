-- ============================================================
-- Spotify ELT Pipeline - Bronze Layer
-- Current-state tables: one row per Spotify ID, maintained by MERGE
-- (see 05_merge.sql). RAW stays append-only; Bronze is deduplicated.
-- ROW_HASH lets MERGE skip unchanged rows so downstream refreshes stay small.
-- ============================================================

CREATE TABLE IF NOT EXISTS MUSIC_DB.BRONZE.ARTISTS (
    ARTIST_ID       VARCHAR PRIMARY KEY,
    ARTIST_NAME     VARCHAR,
    GENRES          VARIANT,
    FOLLOWERS       INT,
    IMAGE_URL       VARCHAR,
    SPOTIFY_URL     VARCHAR,
    ROW_HASH        NUMBER,
    FIRST_SEEN_AT   TIMESTAMP_LTZ,
    UPDATED_AT      TIMESTAMP_LTZ
);


CREATE TABLE IF NOT EXISTS MUSIC_DB.BRONZE.ALBUMS (
    ALBUM_ID                VARCHAR PRIMARY KEY,
    ALBUM_NAME              VARCHAR,
    ARTIST_ID               VARCHAR,
    ARTIST_NAME             VARCHAR,
    ALBUM_TYPE              VARCHAR,
    RELEASE_DATE            VARCHAR,
    RELEASE_DATE_PRECISION  VARCHAR,
    TOTAL_TRACKS            INT,
    IMAGE_URL               VARCHAR,
    SPOTIFY_URL             VARCHAR,
    ROW_HASH                NUMBER,
    FIRST_SEEN_AT           TIMESTAMP_LTZ,
    UPDATED_AT              TIMESTAMP_LTZ,
    TRACKS_REFRESHED_AT     TIMESTAMP_LTZ   -- drives the Lambda's rotating track refresh
);


CREATE TABLE IF NOT EXISTS MUSIC_DB.BRONZE.TRACKS (
    TRACK_ID            VARCHAR PRIMARY KEY,
    TRACK_NAME          VARCHAR,
    ARTIST_ID           VARCHAR,
    ARTIST_NAME         VARCHAR,
    ALBUM_ID            VARCHAR,
    ALBUM_NAME          VARCHAR,
    ALBUM_RELEASE_DATE  VARCHAR,
    DURATION_MS         INT,
    IS_EXPLICIT         BOOLEAN,
    TRACK_NUMBER        INT,
    DISC_NUMBER         INT,
    SPOTIFY_URL         VARCHAR,
    ROW_HASH            NUMBER,
    FIRST_SEEN_AT       TIMESTAMP_LTZ,
    UPDATED_AT          TIMESTAMP_LTZ
);

-- Backfill: run 05_merge.sql with SET SINCE = '1970-01-01'; to seed from all of RAW.
