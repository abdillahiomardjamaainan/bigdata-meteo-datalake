import os
import json
import glob
import time
from pathlib import Path
from datetime import datetime
import requests

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
if not OMDB_API_KEY:
    raise RuntimeError("❌ OMDB_API_KEY manquante")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/airflow/datalake/raw")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE") or datetime.now().strftime("%Y-%m-%d")

BASE_DIR = Path(OUTPUT_DIR)

def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    print(f"📅 OMDb extraction: {SNAPSHOT_DATE}")

    details_glob = BASE_DIR / "tmdb/details" / f"date={SNAPSHOT_DATE}" / "*.json"
    files = glob.glob(str(details_glob))

    if not files:
        raise RuntimeError("❌ Aucun fichier TMDB details trouvé")

    for fpath in files:
        with open(fpath, "r", encoding="utf-8") as f:
            details = json.load(f)

        imdb_id = details.get("imdb_id")
        if not imdb_id:
            continue

        response = requests.get(
            "https://www.omdbapi.com/",
            params={"apikey": OMDB_API_KEY, "i": imdb_id}
        )

        data = response.json()
        if data.get("Response") != "True":
            continue

        save_path = BASE_DIR / "omdb/ratings" / f"date={SNAPSHOT_DATE}" / f"{imdb_id}.json"
        save_json(save_path, data)

        print(f"   ✔ {data.get('Title')} | IMDb={data.get('imdbRating')}")
        time.sleep(1)

    print("🎉 OMDb terminé")

if __name__ == "__main__":
    main()
