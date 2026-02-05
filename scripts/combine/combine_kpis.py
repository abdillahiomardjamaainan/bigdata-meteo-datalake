from pathlib import Path

import pandas as pd


FMT_GEO = Path("datalake/formatted/geography/open_meteo_geocoding")
FMT_WEATHER = Path("datalake/formatted/weather/open_meteo")
OUT_USAGE = Path("datalake/usage/weather_city_kpis")


def latest_dt_folder(base_dir: Path) -> Path:
    dt_folders = sorted([p for p in base_dir.glob("dt=*") if p.is_dir()])
    if not dt_folders:
        raise FileNotFoundError(f"No dt=... folders found in {base_dir}")
    return dt_folders[-1]


def comfort_score(temp, precip, wind, humidity) -> float:
    """
    Simple score 0..100 (no ML). Tunable and explainable.
    - Temp ideal around 22°C
    - Rain penalizes
    - Wind penalizes a bit
    - Humidity penalizes when far from 50%
    """
    if pd.isna(temp) or pd.isna(precip) or pd.isna(wind) or pd.isna(humidity):
        return None

    score = 100.0
    score -= abs(temp - 22) * 3.0          # temp penalty
    score -= precip * 8.0                  # rain penalty
    score -= max(wind - 10, 0) * 1.5       # wind penalty if >10
    score -= abs(humidity - 50) * 0.4      # humidity penalty

    return max(0.0, min(100.0, score))


def main():
    geo_dt_dir = latest_dt_folder(FMT_GEO)
    w_dt_dir = latest_dt_folder(FMT_WEATHER)

    dt_geo = geo_dt_dir.name.split("dt=")[-1]
    dt_weather = w_dt_dir.name.split("dt=")[-1]

    # choose one dt (they should be same day)
    dt = dt_weather

    cities = pd.read_parquet(geo_dt_dir / "cities.parquet")
    weather = pd.read_parquet(w_dt_dir / "weather_hourly.parquet")

    # normalize join keys
    cities["city"] = cities["city"].str.strip()
    weather["city"] = weather["city"].str.strip()

    # KPI aggregation (per city)
    kpi = (
        weather.groupby(["city", "country_code"], as_index=False)
        .agg(
            avg_temperature=("temperature_2m", "mean"),
            total_precipitation=("precipitation", "sum"),
            avg_wind_speed=("wind_speed_10m", "mean"),
            avg_humidity=("humidity_2m", "mean"),
        )
    )

    # join geography context
    final = kpi.merge(
        cities[["city", "country_code", "country", "admin1", "latitude", "longitude", "population"]],
        on=["city", "country_code"],
        how="left",
    )

    # compute comfort score
    final["comfort_score"] = final.apply(
        lambda r: comfort_score(
            r["avg_temperature"], r["total_precipitation"], r["avg_wind_speed"], r["avg_humidity"]
        ),
        axis=1,
    )

    # ranking
    final = final.sort_values("comfort_score", ascending=False).reset_index(drop=True)
    final["rank"] = final.index + 1

    out_dir = OUT_USAGE / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / "city_kpis.parquet"
    final.to_parquet(out_file, index=False)

    print(f"✅ Combined KPIs saved: {out_file} ({len(final)} rows)")
    print(final[["rank", "city", "country_code", "comfort_score"]].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
