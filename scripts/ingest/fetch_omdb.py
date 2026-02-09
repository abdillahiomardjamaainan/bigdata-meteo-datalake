import os
import json
import time
import glob
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
if not OMDB_API_KEY:
    raise RuntimeError("❌ OMDB_API_KEY manquante. Mets-la dans .env")

BASE_DIR = Path("datalake/raw")
TODAY = datetime.now().strftime("%Y-%m-%d")

OMDB_URL = "https://www.omdbapi.com/"
SLEEP_SEC = 1.0  # quota OMDb (gratuit)

def http_get_json(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main() -> None:
    print(f"📅 OMDb extraction du {TODAY}")

    # On lit les fichiers details TMDB du jour
    details_glob = str(BASE_DIR / "tmdb" / "details" / f"date={TODAY}" / "*.json")
    detail_files = glob.glob(details_glob)

    if not detail_files:
        raise RuntimeError(
            f"❌ Aucun fichier TMDB details trouvé pour {TODAY}. "
            "Lance d'abord scripts/ingest/fetch_tmdb_movies.py"
        )

    out_dir = BASE_DIR / "omdb" / "ratings" / f"date={TODAY}"

    ok = 0
    skip_no_imdb = 0
    not_found = 0

    for fpath in detail_files:
        with open(fpath, "r", encoding="utf-8") as f:
            details = json.load(f)

        imdb_id = details.get("imdb_id")
        title = details.get("title", "N/A")

        if not imdb_id:
            print(f"⚠️ {title} : pas d'imdb_id → skip")
            skip_no_imdb += 1
            continue

        omdb = http_get_json(OMDB_URL, {"apikey": OMDB_API_KEY, "i": imdb_id})

        if omdb.get("Response") != "True":
            print(f"⚠️ OMDb introuvable {imdb_id} ({title}) : {omdb.get('Error')}")
            not_found += 1
            continue

        save_json(out_dir / f"{imdb_id}.json", omdb)
        print(f"✅ {title} : IMDb {omdb.get('imdbRating','N/A')}/10")
        ok += 1
        time.sleep(SLEEP_SEC)

    print("\n🎉 OMDb terminé")
    print(f"  - OK : {ok}")
    print(f"  - Skip (no imdb_id) : {skip_no_imdb}")
    print(f"  - Not found : {not_found}")
    print(f"📂 Données : {BASE_DIR.resolve()}")

if __name__ == "__main__":
    main()
