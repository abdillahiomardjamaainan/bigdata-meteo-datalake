"""
Chargement données JSON brutes dans PostgreSQL (schéma raw)
Lit datalake/raw/ et insère dans raw.raw_tmdb_*, raw.raw_omdb_*
"""

import os
import json
import glob
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# Config
load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
PG_DB = os.getenv("POSTGRES_DB", "datalake")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

BASE_DIR = Path("datalake/raw")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE") or datetime.now().strftime("%Y-%m-%d")


def connect():
    """Connexion PostgreSQL"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def read_json(path: Path) -> dict:
    """Lire fichier JSON"""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def ensure_schema_and_tables(cur):
    """Créer schéma raw et tables si nécessaire"""
    
    # Schéma
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # Table TMDB Popular
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

    # Table TMDB Details
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

    # Table OMDb Ratings
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
    """Charger TMDB popular movies"""
    
    popular_path = BASE_DIR / "tmdb" / "popular" / f"date={snapshot_date}" / "popular_movies.json"
    if not popular_path.exists():
        print(f"⚠️ TMDB popular introuvable: {popular_path}")
        return 0

    data = read_json(popular_path)
    movies = data.get("results", [])
    if not movies:
        print("⚠️ TMDB popular: results vide")
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
    """Charger TMDB details"""
    
    details_glob = BASE_DIR / "tmdb" / "details" / f"date={snapshot_date}" / "*.json"
    files = glob.glob(str(details_glob))
    if not files:
        print(f"⚠️ TMDB details introuvable: {details_glob}")
        return 0

    inserted = 0
    for fp in files:
        p = Path(fp)
        details = read_json(p)

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

    return inserted


def load_omdb_ratings(cur, snapshot_date: str):
    """Charger OMDb ratings"""
    
    omdb_glob = BASE_DIR / "omdb" / "ratings" / f"date={snapshot_date}" / "*.json"
    files = glob.glob(str(omdb_glob))
    if not files:
        print(f"⚠️ OMDb ratings introuvable: {omdb_glob}")
        return 0

    inserted = 0
    for fp in files:
        p = Path(fp)
        omdb = read_json(p)

        imdb_id = omdb.get("imdbID") or p.stem
        title = omdb.get("Title")

        if not imdb_id:
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

    return inserted


def main():
    """Point d'entrée principal"""
    
    print(f"📅 LOAD PostgreSQL (raw) pour snapshot_date={SNAPSHOT_DATE}")
    print(f"🐘 Connexion: {PG_HOST}:{PG_PORT} db={PG_DB} user={PG_USER}")

    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_schema_and_tables(cur)

                print("\n📂 Chargement données...")
                n_pop = load_tmdb_popular(cur, SNAPSHOT_DATE)
                print(f"✅ raw_tmdb_popular: {n_pop} lignes")

                n_det = load_tmdb_details(cur, SNAPSHOT_DATE)
                print(f"✅ raw_tmdb_details: {n_det} lignes")

                n_omd = load_omdb_ratings(cur, SNAPSHOT_DATE)
                print(f"✅ raw_omdb_ratings: {n_omd} lignes")

        print("\n🎉 LOAD terminé avec succès")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()