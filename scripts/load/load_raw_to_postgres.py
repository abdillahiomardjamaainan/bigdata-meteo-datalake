import os
import json
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "datalake")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/datalake/raw")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE") or datetime.now().strftime("%Y-%m-%d")
RUN_ID = os.getenv("RUN_ID") or datetime.now().strftime("%Y%m%d%H%M%S")


def connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def unwrap(obj: dict) -> dict:
    """Accepte ancien format (payload direct) ou nouveau format { _meta, data }"""
    if isinstance(obj, dict) and "data" in obj and "_meta" in obj:
        return obj["data"]
    return obj


def ensure_schema_and_tables(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_tmdb_popular (
            snapshot_date DATE NOT NULL,
            tmdb_id       BIGINT NOT NULL,
            title         TEXT,
            payload       JSONB NOT NULL,
            created_at    TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (snapshot_date, tmdb_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_tmdb_details (
            snapshot_date DATE NOT NULL,
            tmdb_id       BIGINT NOT NULL,
            imdb_id       TEXT,
            title         TEXT,
            payload       JSONB NOT NULL,
            created_at    TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (snapshot_date, tmdb_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_omdb_ratings (
            snapshot_date DATE NOT NULL,
            imdb_id       TEXT NOT NULL,
            title         TEXT,
            payload       JSONB NOT NULL,
            created_at    TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (snapshot_date, imdb_id)
        );
    """)

    print("✅ Schéma et tables créés/vérifiés")


def load_tmdb_popular(cur, snapshot_date: str):
    popular_path = Path(DATA_DIR) / "tmdb" / "popular" / f"date={snapshot_date}" / "popular_movies.json"
    if not popular_path.exists():
        print(f"⚠️ TMDB popular introuvable: {popular_path}")
        return 0

    wrapped = read_json(popular_path)
    data = unwrap(wrapped)

    movies = data.get("results", []) if isinstance(data, dict) else []
    if not movies:
        print("⚠️ TMDB popular: aucun film")
        return 0

    inserted = 0
    for m in movies:
        tmdb_id = m.get("id")
        title = m.get("title")
        if not tmdb_id:
            continue

        cur.execute("""
            INSERT INTO raw.raw_tmdb_popular (snapshot_date, tmdb_id, title, payload)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (snapshot_date, tmdb_id)
            DO UPDATE SET
                title = EXCLUDED.title,
                payload = EXCLUDED.payload;
        """, (snapshot_date, tmdb_id, title, Json(m)))
        inserted += 1

    return inserted


def load_tmdb_details(cur, snapshot_date: str):
    details_dir = Path(DATA_DIR) / "tmdb" / "details" / f"date={snapshot_date}"
    if not details_dir.exists():
        print(f"⚠️ TMDB details introuvable: {details_dir}")
        return 0

    json_files = list(details_dir.glob("*.json"))
    if not json_files:
        print(f"⚠️ TMDB details: aucun fichier JSON dans {details_dir}")
        return 0

    print(f"📁 TMDB details: {len(json_files)} fichiers")
    inserted = 0

    for json_file in json_files:
        try:
            wrapped = read_json(json_file)
            details = unwrap(wrapped)

            tmdb_id = details.get("id")
            title = details.get("title")
            imdb_id = details.get("imdb_id")

            if not tmdb_id:
                continue

            cur.execute("""
                INSERT INTO raw.raw_tmdb_details (snapshot_date, tmdb_id, imdb_id, title, payload)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_date, tmdb_id)
                DO UPDATE SET
                    imdb_id = EXCLUDED.imdb_id,
                    title = EXCLUDED.title,
                    payload = EXCLUDED.payload;
            """, (snapshot_date, tmdb_id, imdb_id, title, Json(details)))
            inserted += 1

        except Exception as e:
            print(f"⚠️ Erreur lecture {json_file.name}: {e}")
            continue

    return inserted


def load_omdb_ratings(cur, snapshot_date: str):
    omdb_dir = Path(DATA_DIR) / "omdb" / "ratings" / f"date={snapshot_date}"
    if not omdb_dir.exists():
        print(f"⚠️ OMDb ratings introuvable: {omdb_dir}")
        return 0

    json_files = list(omdb_dir.glob("*.json"))
    if not json_files:
        print(f"⚠️ OMDb ratings: aucun fichier JSON dans {omdb_dir}")
        return 0

    print(f"📁 OMDb ratings: {len(json_files)} fichiers")
    inserted = 0

    for json_file in json_files:
        try:
            wrapped = read_json(json_file)
            omdb = unwrap(wrapped)

            imdb_id = json_file.stem
            title = omdb.get("Title")

            if omdb.get("Response") != "True":
                continue

            cur.execute("""
                INSERT INTO raw.raw_omdb_ratings (snapshot_date, imdb_id, title, payload)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (snapshot_date, imdb_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    payload = EXCLUDED.payload;
            """, (snapshot_date, imdb_id, title, Json(omdb)))
            inserted += 1

        except Exception as e:
            print(f"⚠️ Erreur lecture {json_file.name}: {e}")
            continue

    return inserted


def main():
    print(f"🐘 LOAD PostgreSQL (raw) | snapshot_date={SNAPSHOT_DATE} | run_id={RUN_ID}")
    print(f"Source: {DATA_DIR}")
    print(f"Connexion: {PG_HOST}:{PG_PORT} db={PG_DB} user={PG_USER}")

    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_schema_and_tables(cur)

                n_pop = load_tmdb_popular(cur, SNAPSHOT_DATE)
                print(f"✅ raw_tmdb_popular: {n_pop} lignes")

                n_det = load_tmdb_details(cur, SNAPSHOT_DATE)
                print(f"✅ raw_tmdb_details: {n_det} lignes")

                n_omd = load_omdb_ratings(cur, SNAPSHOT_DATE)
                print(f"✅ raw_omdb_ratings: {n_omd} lignes")

        print("🎉 LOAD terminé")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
