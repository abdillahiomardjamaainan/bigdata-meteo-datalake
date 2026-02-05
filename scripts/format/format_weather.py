import json
from pathlib import Path

import pandas as pd


RAW_WEATHER = Path("datalake/raw/weather/open_meteo")
OUT_WEATHER = Path("datalake/formatted/weather/open_meteo")


def latest_dt_folder(base_dir: Path) -> Path:
    dt_folders = sorted([p for p in base_dir.glob("dt=*") if p.is_dir()])
    if not dt_folders:
        raise FileNotFoundError(f"No dt=... folders found in {base_dir}")
    return dt_folders[-1]


def main():
    dt_dir = latest_dt_folder(RAW_WEATHER)
    dt = dt_dir.name.split("dt=")[-1]

    rows = []
    for f in sorted(dt_dir.glob("hourly_*.json")):
        payload = json.loads(f.read_text(encoding="utf-8"))
        meta = payload.get("_ingestion", {})

        city = meta.get("city")
        country_code = meta.get("country_code")

        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        hums = hourly.get("relative_humidity_2m", [])
        precs = hourly.get("precipitation", [])
        winds = hourly.get("wind_speed_10m", [])

        for i in range(len(times)):
            rows.append({
                "city": city,
                "country_code": country_code,
                "time_utc": times[i],  # already UTC because we requested timezone=UTC
                "temperature_2m": temps[i] if i < len(temps) else None,
                "humidity_2m": hums[i] if i < len(hums) else None,
                "precipitation": precs[i] if i < len(precs) else None,
                "wind_speed_10m": winds[i] if i < len(winds) else None,
            })

    df = pd.DataFrame(rows).dropna(subset=["city", "country_code", "time_utc"])

    # normalize types
    df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["time_utc"])

    out_dir = OUT_WEATHER / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / "weather_hourly.parquet"
    df.to_parquet(out_file, index=False)
    print(f"✅ Weather formatted: {out_file} ({len(df)} rows)")


if __name__ == "__main__":
    main()
