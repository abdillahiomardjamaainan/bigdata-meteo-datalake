import json
from pathlib import Path

import pandas as pd


RAW_GEO = Path("datalake/raw/geography/open_meteo_geocoding")
OUT_GEO = Path("datalake/formatted/geography/open_meteo_geocoding")


def latest_dt_folder(base_dir: Path) -> Path:
    dt_folders = sorted([p for p in base_dir.glob("dt=*") if p.is_dir()])
    if not dt_folders:
        raise FileNotFoundError(f"No dt=... folders found in {base_dir}")
    return dt_folders[-1]


def pick_best_result(payload: dict) -> dict | None:
    results = payload.get("results") or []
    if not results:
        return None
    return max(results, key=lambda r: r.get("population") or 0)


def main():
    dt_dir = latest_dt_folder(RAW_GEO)
    dt = dt_dir.name.split("dt=")[-1]

    rows = []
    for f in sorted(dt_dir.glob("geocoding_*.json")):
        payload = json.loads(f.read_text(encoding="utf-8"))
        best = pick_best_result(payload)
        if not best:
            continue

        rows.append({
            "city": best.get("name"),
            "country": best.get("country"),
            "country_code": best.get("country_code"),
            "admin1": best.get("admin1"),
            "timezone": best.get("timezone"),
            "latitude": best.get("latitude"),
            "longitude": best.get("longitude"),
            "population": best.get("population"),
        })

    df = pd.DataFrame(rows).dropna(subset=["city", "country_code", "latitude", "longitude"])
    df["city"] = df["city"].str.strip()
    df["country_code"] = df["country_code"].str.strip()

    out_dir = OUT_GEO / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / "cities.parquet"
    df.to_parquet(out_file, index=False)
    print(f"✅ Geography formatted: {out_file} ({len(df)} rows)")


if __name__ == "__main__":
    main()
