import os
import json
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY manquante. Mets-la dans .env")

BASE_DIR = Path("datalake/raw")
TODAY = datetime.now().strftime("%Y-%m-%d")

TMDB_POPULAR_URL = "https://api.themoviedb.org/3/movie/popular"
TMDB_DETAILS_URL = "https://api.themoviedb.org/3/movie/{movie_id}"

POPULAR_PAGE = 1      # 1 page = 20 films (simple)
LANGUAGE = "fr-FR"    # ou "en-US"
SLEEP_SEC = 0.25      # throttle simple

def http_get_json(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main() -> None:
    print(f"📅 TMDB extraction du {TODAY}")

    # A) Popular
    print("\n🎬 A) TMDB - popular movies...")
    popular = http_get_json(
        TMDB_POPULAR_URL,
        {"api_key": TMDB_API_KEY, "page": POPULAR_PAGE, "language": LANGUAGE},
    )

    popular_path = BASE_DIR / "tmdb" / "popular" / f"date={TODAY}" / "popular_movies.json"
    save_json(popular_path, popular)

    movies = popular.get("results", [])
    print(f"✅ Films populaires récupérés : {len(movies)}")
    if not movies:
        print("⚠️ Aucun film → stop.")
        return

    # B) Details (1 appel par film)
    print("\n🔍 B) TMDB - details (imdb_id, runtime, genres...)")
    details_dir = BASE_DIR / "tmdb" / "details" / f"date={TODAY}"
    ok = 0

    for i, m in enumerate(movies, start=1):
        movie_id = m.get("id")
        if not movie_id:
            continue

        details = http_get_json(
            TMDB_DETAILS_URL.format(movie_id=movie_id),
            {"api_key": TMDB_API_KEY, "language": LANGUAGE},
        )

        save_json(details_dir / f"{movie_id}.json", details)
        print(f"✅ [{i}/{len(movies)}] {details.get('title','N/A')} | imdb_id={details.get('imdb_id')}")
        ok += 1
        time.sleep(SLEEP_SEC)

    print(f"\n🎉 TMDB terminé : popular=1 fichier, details={ok} fichiers")
    print(f"📂 Données : {BASE_DIR.resolve()}")

if __name__ == "__main__":
    main()
