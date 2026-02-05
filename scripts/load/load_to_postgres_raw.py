from pathlib import Path
import re
import pandas as pd
from sqlalchemy import create_engine

# IMPORTANT: same port as your dbt debug (it showed 5433)
PG_URL = "postgresql+psycopg2://postgres:postgres@localhost:5433/datalake"

CITIES_BASE = Path("datalake/formatted/geography/open_meteo_geocoding")
WEATHER_BASE = Path("datalake/formatted/weather/open_meteo")


def latest_dt(base: Path) -> str:
    dts = sorted([p.name for p in base.glob("dt=*") if p.is_dir()])
    if not dts:
        raise FileNotFoundError(f"No dt=* folder found in {base}")
    return dts[-1].split("dt=")[-1]


def main():
    dt_cities = latest_dt(CITIES_BASE)
    dt_weather = latest_dt(WEATHER_BASE)

    # we expect them to match; if not, we load the latest of each
    cities_path = CITIES_BASE / f"dt={dt_cities}" / "cities.parquet"
    weather_path = WEATHER_BASE / f"dt={dt_weather}" / "weather_hourly.parquet"

    print("Using:")
    print(" -", cities_path)
    print(" -", weather_path)

    cities = pd.read_parquet(cities_path)
    weather = pd.read_parquet(weather_path)

    # ensure types
    weather["time_utc"] = pd.to_datetime(weather["time_utc"], utc=True, errors="coerce")
    weather = weather.dropna(subset=["time_utc"])

    engine = create_engine(PG_URL)

    # load (append)
    cities.to_sql("cities_raw", engine, schema="analytics", if_exists="append", index=False)
    weather.to_sql("weather_hourly_raw", engine, schema="analytics", if_exists="append", index=False)

    print(f"✅ Loaded cities_raw: {len(cities)} rows")
    print(f"✅ Loaded weather_hourly_raw: {len(weather)} rows")


if __name__ == "__main__":
    main()
