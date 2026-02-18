import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone
import requests

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY manquante")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/airflow/datalake/raw")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE") or datetime.now().strftime("%Y-%m-%d")

# optionnel (fourni par Airflow)
RUN_ID = os.getenv("RUN_ID") or datetime.now().strftime("%Y%m%d%H%M%S")

BASE_DIR = Path(OUTPUT_DIR)

TMDB_POPULAR_URL = "https://api.themoviedb.org/3/movie/popular"
TMDB_DETAILS_URL = "https://api.themoviedb.org/3/movie/{movie_id}"


def http_get(url, params):
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def save_json(path: Path, data: dict, source: str, endpoint: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    meta = {
        "snapshot_date": SNAPSHOT_DATE,
        "run_id": RUN_ID,
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "endpoint": endpoint,
    }

    payload = {"_meta": meta, "data": data}

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def main():
    print(f"🎬 TMDB extraction | snapshot_date={SNAPSHOT_DATE} | run_id={RUN_ID}")

    popular = http_get(
        TMDB_POPULAR_URL,
        {"api_key": TMDB_API_KEY, "page": 1, "language": "fr-FR"},
    )

    popular_path = BASE_DIR / "tmdb/popular" / f"date={SNAPSHOT_DATE}" / "popular_movies.json"
    save_json(popular_path, popular, source="tmdb", endpoint="popular")

    movies = popular.get("results", [])
    print(f"✅ {len(movies)} films récupérés (popular)")

    for m in movies:
        movie_id = m.get("id")
        if not movie_id:
            continue

        details = http_get(
            TMDB_DETAILS_URL.format(movie_id=movie_id),
            {"api_key": TMDB_API_KEY, "language": "fr-FR"},
        )

        details_path = BASE_DIR / "tmdb/details" / f"date={SNAPSHOT_DATE}" / f"{movie_id}.json"
        save_json(details_path, details, source="tmdb", endpoint="details")

        print(f"   ✔ {details.get('title')} | imdb={details.get('imdb_id')}")
        time.sleep(0.25)

    print("✅ TMDB terminé")


if __name__ == "__main__":
    main()
