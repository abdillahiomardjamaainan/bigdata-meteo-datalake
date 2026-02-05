import json
from datetime import datetime, timezone
from pathlib import Path

import requests


CITIES = [
    "Brussels",
    "Paris",
    "London",
    "Berlin",
    "Amsterdam",
    "Madrid",
    "Rome",
    "Lisbon",
    "Vienna",
    "Dublin",
]


def now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def geocode_city(city_name: str) -> dict:
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": city_name, "count": 5, "language": "en", "format": "json"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    out_dir = Path("datalake") / "raw" / "geography" / "open_meteo_geocoding" / f"dt={today_utc()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = now_utc_compact()

    for city in CITIES:
        data = geocode_city(city)

        data["_ingestion"] = {
            "ingested_at_utc": ts,
            "source": "open-meteo-geocoding",
            "query_city": city,
        }

        out_file = out_dir / f"geocoding_{city.lower().replace(' ', '_')}_{ts}.json"
        out_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"✅ Saved raw geocoding data to: {out_file}")


if __name__ == "__main__":
    main()
