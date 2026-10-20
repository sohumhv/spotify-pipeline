"""
Spotify Data Pipeline v3 - Lambda Function (metadata refresh, deduplicated Bronze)
Fetches artist info, albums, and tracks (via albums) for a list of bands and
writes them to Snowflake via snowflake-connector-python.

Changes in v3:
  - Pure EL: Lambda only extracts and loads RAW. Bronze Dynamic Tables keep the
    newest RAW row per ID, so re-loaded artists/albums/tracks update in place
    (fixes duplicate artist rows that inflated Silver/Gold metrics)
  - Rotating metadata refresh: each run also re-fetches tracks for
    REFRESH_ALBUMS_PER_RUN known albums (incomplete first, then least recently loaded)

Changes from v2 (carried forward):
  - _api fails fast on long rate limits (RateLimited) and never returns None
  - Album tracks are paginated (v2 silently truncated at 10)
  - Incremental: only fetches tracks / loads albums that are not already in Snowflake
  - COPY INTO targets only the file just uploaded (no FORCE) -> no duplicate reloads
  - One bad artist no longer kills the run; the run fails loudly at the end
  - Stops gracefully before the Lambda timeout; the next run picks up the rest
  - Secrets: environment variables only (no Secrets Manager); missing vars fail clearly
  - Optional hardcoded artist IDs to skip search calls

Lambda deployment:
  - Runtime: Python 3.12
  - Layer: snowflake-connector-python
  - Timeout: 300-900 seconds
  - Memory: 512 MB
  - Env vars: SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SNOWFLAKE_PASSWORD
    (optional: SNOWFLAKE_ACCOUNT/USER/DATABASE/SCHEMA/WAREHOUSE)
  - Trigger: EventBridge cron (daily or weekly)

Snowflake requirement:
  JSON_FORMAT must have STRIP_OUTER_ARRAY = TRUE, e.g.
    CREATE OR REPLACE FILE FORMAT MUSIC_DB.RAW.JSON_FORMAT
      TYPE = JSON STRIP_OUTER_ARRAY = TRUE;

Local testing: python lambda_function.py --local
"""
import json
import os
import base64
import time
import tempfile
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

# ============================================================
# Config
# ============================================================

SF_STAGE = "@MUSIC_DB.RAW.LB_INTERNAL_STAGE"

PAGE_SIZE = 10            # Spotify page size used for albums and tracks
REQUEST_SLEEP = 0.5       # seconds between API calls
MAX_RETRY_AFTER = 30      # if Spotify asks for longer than this, abort the run
SAFETY_MARGIN_S = 45      # stop fetching this many seconds before Lambda timeout
LOCAL_TIME_BUDGET_S = 3600
REFRESH_ALBUMS_PER_RUN = 25   # known albums whose tracks are re-fetched each run

# Keep v2 behavior of dropping albums that share a name (deluxe/remaster variants
# with the same title, singles named like their album). Set False to keep them all.
DEDUPE_ALBUMS_BY_NAME = True

# Artist name -> optional Spotify artist ID. Leave the ID as None to search by name.
# The run logs "RESOLVED name -> id"; paste those IDs here to skip search calls.
ARTISTS = {
    "Geese": "0WCo84qtCKfbyIf1lqQWB4",
    "Maruja": None,
    "Fontaines D.C.": None,
    "Wednesday": None,
    "Magdalena Bay": None,
    "Black Country, New Road": None,
    "Big Thief": None,
    "Wolf Alice": None,
    "The Last Dinner Party": None,
    "MJ Lenderman": None,
}


# ============================================================
# Errors
# ============================================================

class StopRun(Exception):
    """Base: stop fetching, upload what we have, then report failure."""


class RateLimited(StopRun):
    pass


class OutOfTime(StopRun):
    pass


# ============================================================
# Secrets (environment variables only)
# ============================================================

_secrets_cache = None

REQUIRED_VARS = [
    "SPOTIFY_CLIENT_ID",
    "SPOTIFY_CLIENT_SECRET",
    "SNOWFLAKE_PASSWORD",
]


def _get_secrets():
    """Read config from environment variables (set in Lambda > Configuration > Environment variables)."""
    global _secrets_cache
    if _secrets_cache:
        return _secrets_cache

    secrets = {
        "SPOTIFY_CLIENT_ID": os.environ.get("SPOTIFY_CLIENT_ID", ""),
        "SPOTIFY_CLIENT_SECRET": os.environ.get("SPOTIFY_CLIENT_SECRET", ""),
        "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", ""),
        "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
        "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", "MUSIC_DB"),
        "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }

    # Lambda needs all of them; local mode can run without Snowflake
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        missing = [k for k in REQUIRED_VARS if not secrets[k]]
        if missing:
            raise RuntimeError(f"Missing Lambda environment variables: {', '.join(missing)}")

    _secrets_cache = secrets
    return secrets


# ============================================================
# Time budget
# ============================================================

_deadline = None


def _set_deadline(context):
    global _deadline
    if context is not None and hasattr(context, "get_remaining_time_in_millis"):
        remaining = context.get_remaining_time_in_millis() / 1000
    else:
        remaining = LOCAL_TIME_BUDGET_S
    _deadline = time.time() + remaining - SAFETY_MARGIN_S


def _check_time():
    if _deadline is not None and time.time() > _deadline:
        raise OutOfTime("Approaching Lambda timeout; stopping early (next run resumes)")


# ============================================================
# Spotify API helpers
# ============================================================

def get_token():
    secrets = _get_secrets()
    creds = base64.b64encode(
        f"{secrets['SPOTIFY_CLIENT_ID']}:{secrets['SPOTIFY_CLIENT_SECRET']}".encode()
    ).decode()
    req = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=b"grant_type=client_credentials",
        headers={
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())["access_token"]
    except urllib.error.HTTPError as e:
        if e.code == 429:
            wait = int(e.headers.get("Retry-After", 0))
            raise RateLimited(f"Token endpoint rate limited (Retry-After {wait}s)") from e
        raise


def _api(token, url, retries=3):
    """GET a Spotify URL. Returns parsed JSON or raises; never returns None."""
    for attempt in range(retries):
        _check_time()
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            time.sleep(REQUEST_SLEEP)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = int(e.headers.get("Retry-After", 2))
                if wait > MAX_RETRY_AFTER:
                    raise RateLimited(
                        f"Spotify wants a {wait}s ({wait / 3600:.1f}h) wait; aborting run"
                    ) from e
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif e.code in (500, 502, 503) and attempt < retries - 1:
                time.sleep(1 + attempt)
            else:
                raise
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(1 + attempt)
            else:
                raise RuntimeError(f"Network error after {retries} attempts: {url}: {e}") from e
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


def _paginate(token, base_url, page_size=PAGE_SIZE):
    """Yield every item from a Spotify offset-paginated endpoint."""
    sep = "&" if "?" in base_url else "?"
    offset = 0
    while True:
        data = _api(token, f"{base_url}{sep}limit={page_size}&offset={offset}")
        items = data.get("items", [])
        for item in items:
            yield item
        if len(items) < page_size or not data.get("next"):
            break
        offset += page_size


def search_artist(token, name):
    url = "https://api.spotify.com/v1/search?" + urllib.parse.urlencode(
        {"q": name, "type": "artist", "limit": 5, "market": "US"}
    )
    items = _api(token, url).get("artists", {}).get("items", [])
    for a in items:
        if a["name"].lower() == name.lower():
            return a
    return items[0] if items else None


def get_artist(token, name, artist_id):
    if artist_id:
        return _api(token, f"https://api.spotify.com/v1/artists/{artist_id}")
    artist = search_artist(token, name)
    if artist:
        print(f"  RESOLVED {name} -> {artist['id']}  (paste into ARTISTS to skip search)")
    return artist


def fetch_artist_data(token, name, artist_id, known_album_ids, refresh_album_ids):
    """
    Returns (artist_rec, tracks, albums).
    Albums returned are new ones plus known ones queued for refresh, and only if
    all of their tracks were fetched successfully (a failed album is retried next run).
    """
    artist = get_artist(token, name, artist_id)
    if not artist:
        print(f"  WARNING: Could not find {name}")
        return None, [], []

    artist_id = artist["id"]
    artist_rec = {
        "artist_id": artist_id,
        "artist_name": artist["name"],
        "genres": artist.get("genres", []),
        "followers": artist.get("followers", {}).get("total"),
        "image_url": artist["images"][0]["url"] if artist.get("images") else None,
        "spotify_url": artist.get("external_urls", {}).get("spotify"),
    }

    # --- Albums (paginated) ---
    candidate_albums = []
    seen_names = set()
    seen_ids = set()
    albums_base = (
        f"https://api.spotify.com/v1/artists/{artist_id}/albums"
        f"?include_groups=album,single&market=US"
    )
    for alb in _paginate(token, albums_base):
        if alb["id"] in seen_ids:
            continue
        seen_ids.add(alb["id"])
        if DEDUPE_ALBUMS_BY_NAME:
            key = alb["name"].lower().strip()
            if key in seen_names:
                continue
            seen_names.add(key)
        candidate_albums.append({
            "album_id": alb["id"],
            "album_name": alb["name"],
            "artist_id": artist_id,
            "artist_name": artist["name"],
            "album_type": alb["album_type"],
            "release_date": alb.get("release_date"),
            "release_date_precision": alb.get("release_date_precision"),
            "total_tracks": alb["total_tracks"],
            "image_url": alb["images"][0]["url"] if alb.get("images") else None,
            "spotify_url": alb.get("external_urls", {}).get("spotify"),
        })

    new_albums = [a for a in candidate_albums if a["album_id"] not in known_album_ids]
    refresh_albums = [a for a in candidate_albums if a["album_id"] in refresh_album_ids]
    print(f"  {len(candidate_albums)} albums on Spotify, {len(new_albums)} new, "
          f"{len(refresh_albums)} refreshing")

    # --- Tracks for new + refresh-queued albums (paginated) ---
    tracks, albums = [], []
    for alb in new_albums + refresh_albums:
        tracks_base = f"https://api.spotify.com/v1/albums/{alb['album_id']}/tracks?market=US"
        album_tracks = []
        try:
            for t in _paginate(token, tracks_base):
                album_tracks.append({
                    "track_id": t["id"],
                    "track_name": t["name"],
                    "artist_id": artist_id,
                    "artist_name": artist["name"],
                    "album_id": alb["album_id"],
                    "album_name": alb["album_name"],
                    "album_release_date": alb.get("release_date"),
                    "duration_ms": t["duration_ms"],
                    "explicit": t["explicit"],
                    "track_number": t["track_number"],
                    "disc_number": t["disc_number"],
                    "spotify_url": t.get("external_urls", {}).get("spotify"),
                })
        except urllib.error.HTTPError as e:
            # Skip this album entirely; it will be retried next run
            print(f"  WARNING: tracks failed for album '{alb['album_name']}' "
                  f"({alb['album_id']}): HTTP {e.code}")
            continue
        albums.append(alb)
        tracks.extend(album_tracks)

    return artist_rec, tracks, albums


# ============================================================
# Snowflake
# ============================================================

def _connect():
    import snowflake.connector
    s = _get_secrets()
    return snowflake.connector.connect(
        account=s["SNOWFLAKE_ACCOUNT"],
        user=s["SNOWFLAKE_USER"],
        password=s["SNOWFLAKE_PASSWORD"],
        database=s["SNOWFLAKE_DATABASE"],
        schema=s["SNOWFLAKE_SCHEMA"],
        warehouse=s["SNOWFLAKE_WAREHOUSE"],
    )


def get_album_state():
    """
    Returns (known_album_ids, refresh_album_ids) from the Bronze DTs.
    The refresh set is REFRESH_ALBUMS_PER_RUN albums: incomplete ones first
    (fewer loaded tracks than Spotify's total_tracks), then least recently loaded.
    Re-fetched tracks are appended to RAW; Bronze keeps only the newest row per ID.
    """
    db = _get_secrets()["SNOWFLAKE_DATABASE"]
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT a.ALBUM_ID,
                   ROW_NUMBER() OVER (
                       ORDER BY IFF(COUNT(t.TRACK_ID) < a.TOTAL_TRACKS, 0, 1),
                                MAX(t.LOADED_AT) NULLS FIRST
                   ) <= %s
            FROM {db}.BRONZE.DT_ALBUMS a
            LEFT JOIN {db}.BRONZE.DT_TRACKS t ON t.ALBUM_ID = a.ALBUM_ID
            GROUP BY a.ALBUM_ID, a.TOTAL_TRACKS
        """, (REFRESH_ALBUMS_PER_RUN,))
        rows = cur.fetchall()
        cur.close()
        known = {r[0] for r in rows}
        refresh = {r[0] for r in rows if r[1]}
        return known, refresh
    finally:
        conn.close()


def upload_to_snowflake(all_artists, all_tracks, all_albums):
    """Write JSON files to temp dir, PUT to stage, COPY INTO only those files."""
    s = _get_secrets()
    db, schema = s["SNOWFLAKE_DATABASE"], s["SNOWFLAKE_SCHEMA"]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # (filename, stage subdir, table, data) -- skip empty datasets
    batches = [
        (f"artists_{ts}.json", "artists", "RAW_ARTISTS", all_artists),
        (f"tracks_{ts}.json", "tracks", "RAW_RECORDINGS", all_tracks),
        (f"albums_{ts}.json", "albums", "RAW_RELEASES", all_albums),
    ]
    batches = [b for b in batches if b[3]]
    if not batches:
        print("Nothing new to load.")
        return

    conn = _connect()
    cur = conn.cursor()
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            for filename, subdir, table, data in batches:
                filepath = os.path.join(tmpdir, filename)
                with open(filepath, "w") as f:
                    json.dump(data, f)

                stage_path = f"{SF_STAGE}/{subdir}/"
                cur.execute(
                    f"PUT file://{filepath} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                print(f"PUT {filename} -> {stage_path} ({len(data)} records)")

                # Only the file we just uploaded; no FORCE, so load history prevents re-loads
                cur.execute(f"""
                    COPY INTO {db}.{schema}.{table} (SRC, LOADED_AT)
                    FROM (SELECT $1, CURRENT_TIMESTAMP() FROM {stage_path})
                    FILES = ('{filename}')
                    FILE_FORMAT = {db}.{schema}.JSON_FORMAT
                """)
                print(f"COPY INTO {table}: {cur.fetchone()}")

        print(f"Loaded {len(all_artists)} artists, {len(all_tracks)} tracks, "
              f"{len(all_albums)} albums (new + refreshed) into Snowflake")
    finally:
        cur.close()
        conn.close()


# ============================================================
# Orchestration
# ============================================================

def collect_data(token, known_album_ids, refresh_album_ids=frozenset()):
    """Fetch every artist. Returns (artists, tracks, albums, errors)."""
    all_artists, all_tracks, all_albums, errors = [], [], [], []

    for name, artist_id in ARTISTS.items():
        print(f"Fetching {name}...")
        try:
            artist, tracks, albums = fetch_artist_data(
                token, name, artist_id, known_album_ids, refresh_album_ids)
        except StopRun as e:
            errors.append(f"{name}: {e}")
            print(f"  STOPPING: {e}")
            break  # rate limit / timeout: don't keep hammering the API
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {e}")
            print(f"  ERROR on {name}: {type(e).__name__}: {e}")
            continue

        if artist:
            all_artists.append(artist)
            all_tracks.extend(tracks)
            all_albums.extend(albums)
            # Avoid re-fetching if the same album shows up under another artist
            known_album_ids.update(a["album_id"] for a in albums)
            print(f"  {len(tracks)} tracks, {len(albums)} albums to load")
        else:
            errors.append(f"{name}: artist not found")

    return all_artists, all_tracks, all_albums, errors


def lambda_handler(event=None, context=None):
    """Lambda entry point: fetch Spotify data, upload to Snowflake."""
    _set_deadline(context)

    known_album_ids, refresh_album_ids = get_album_state()
    print(f"{len(known_album_ids)} albums already in Snowflake, "
          f"{len(refresh_album_ids)} queued for refresh")

    token = get_token()
    artists, tracks, albums, errors = collect_data(token, known_album_ids, refresh_album_ids)

    # Load whatever completed successfully, even on partial failure
    upload_to_snowflake(artists, tracks, albums)

    summary = f"Loaded {len(artists)} artists, {len(tracks)} tracks, {len(albums)} albums (new + refreshed)"
    if errors:
        # Raise so Lambda reports a failure and CloudWatch alarms / retries fire
        raise RuntimeError(f"{summary}. Errors: {'; '.join(errors)}")

    return {"statusCode": 200, "body": summary}


def run_local():
    """Local mode: fetch Spotify data, write local backups, upload to Snowflake."""
    _set_deadline(None)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    secrets = _get_secrets()
    has_sf = bool(secrets.get("SNOWFLAKE_PASSWORD"))

    known_album_ids, refresh_album_ids = set(), set()
    if has_sf:
        known_album_ids, refresh_album_ids = get_album_state()
        print(f"{len(known_album_ids)} albums already in Snowflake, "
              f"{len(refresh_album_ids)} queued for refresh")
    else:
        print("SNOWFLAKE_PASSWORD not set: treating all albums as new, skipping upload")

    token = get_token()
    artists, tracks, albums, errors = collect_data(token, known_album_ids, refresh_album_ids)

    outdir = os.path.dirname(os.path.abspath(__file__))
    for name, data in [("artists", artists), ("tracks", tracks), ("albums", albums)]:
        path = os.path.join(outdir, f"{name}_{ts}.json")
        with open(path, "w") as f:
            json.dump(data, f)
        print(f"Wrote {path} ({len(data)} records)")

    if has_sf:
        print("\nUploading to Snowflake...")
        upload_to_snowflake(artists, tracks, albums)

    if errors:
        print("\nErrors:\n  " + "\n  ".join(errors))
    return artists, tracks, albums


if __name__ == "__main__":
    import sys
    if "--local" in sys.argv:
        run_local()
    else:
        print("Usage: python lambda_function.py --local")
        print("\nEnvironment variables:")
        print("  SPOTIFY_CLIENT_ID       - Spotify app client ID")
        print("  SPOTIFY_CLIENT_SECRET   - Spotify app client secret")
        print("  SNOWFLAKE_ACCOUNT       - e.g. MYORG-MYACCOUNT")
        print("  SNOWFLAKE_USER          - e.g. MY_USER")
        print("  SNOWFLAKE_PASSWORD      - Snowflake password")
        print("  SNOWFLAKE_DATABASE      - e.g. MUSIC_DB")
        print("  SNOWFLAKE_SCHEMA        - e.g. RAW")
        print("  SNOWFLAKE_WAREHOUSE     - e.g. COMPUTE_WH")
