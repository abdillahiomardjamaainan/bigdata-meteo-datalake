import json
from datetime import datetime, timezone
from pathlib import Path

import requests


GEO_DIR = Path("datalake/raw/geography/open_meteo_geocoding")
WEATHER_BASE_DIR = Path("datalake/raw/weather/open_meteo")


def now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def latest_dt_folder(base_dir: Path) -> Path:
    """
    Return the latest dt=YYYY-MM-DD folder inside base_dir.
    """
    dt_folders = sorted([p for p in base_dir.glob("dt=*") if p.is_dir()])
    if not dt_folders:
        raise FileNotFoundError(f"No dt=... folders found in {base_dir}")
    return dt_folders[-1]


def pick_best_result(geo_payload: dict) -> dict:
    """
    Open-Meteo geocoding can return multiple results (e.g., Brussels BE and Brussels US).
    We'll pick the most relevant by highest population.
    """
    results = geo_payload.get("results") or []
    if not results:
        raise ValueError("No results in geocoding payload")
    return max(results, key=lambda r: r.get("population") or 0)


def fetch_open_meteo_hourly(lat: float, lon: float) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "UTC",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def safe_slug(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


def main():
    # Find latest geocoding snapshot (dt=...)
    geo_dt_dir = latest_dt_folder(GEO_DIR)
    geo_files = sorted(geo_dt_dir.glob("geocoding_*.json"))

    if not geo_files:
        raise FileNotFoundError(f"No geocoding files found in {geo_dt_dir}")

    ts = now_utc_compact()
    out_dir = WEATHER_BASE_DIR / f"dt={today_utc()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    for geo_file in geo_files:
        geo_payload = json.loads(geo_file.read_text(encoding="utf-8"))
        best = pick_best_result(geo_payload)

        city = best.get("name", "unknown")
        country_code = best.get("country_code", "XX")
        lat = best["latitude"]
        lon = best["longitude"]

        weather = fetch_open_meteo_hourly(lat, lon)

        weather["_ingestion"] = {
            "ingested_at_utc": ts,
            "source": "open-meteo",
            "city": city,
            "country_code": country_code,
            "lat": lat,
            "lon": lon,
            "geo_source_file": geo_file.name,
        }

        out_file = out_dir / f"hourly_{safe_slug(city)}_{country_code}_{ts}.json"
        out_file.write_text(json.dumps(weather, indent=2, ensure_ascii=False), encoding="utf-8")

        print(f"✅ Weather saved for {city} ({country_code}) -> {out_file.name}")


if __name__ == "__main__":
    main()
