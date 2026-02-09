import os
import json
from datetime import datetime, date
from pathlib import Path
import numpy as np

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200").rstrip("/")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE") or datetime.now().strftime("%Y-%m-%d")

DATALAKE_PATH = Path(__file__).parent.parent.parent / "datalake"
MOVIES_PARQUET = DATALAKE_PATH / "usage" / "movies_enriched" / f"snapshot_date={SNAPSHOT_DATE}" / "data.parquet"
KPIS_PARQUET = DATALAKE_PATH / "usage" / "kpi_daily" / f"snapshot_date={SNAPSHOT_DATE}" / "data.parquet"

INDEX_MOVIES = "movies_enriched_daily"
INDEX_KPIS = "movies_kpis_daily"

TIMEOUT = 60


def es_ok() -> None:
    """Vérifier connexion Elasticsearch"""
    try:
        r = requests.get(f"{ES_HOST}", timeout=TIMEOUT)
        r.raise_for_status()
        version = r.json().get('version', {}).get('number', 'unknown')
        print(f"✅ Elasticsearch OK: {version}")
    except Exception as e:
        print(f"❌ Elasticsearch inaccessible: {e}")
        print(f"   Vérifiez: docker compose ps")
        raise


def create_index_if_missing(index_name: str, mapping: dict) -> None:
    """Créer index Elasticsearch si absent"""
    r = requests.get(f"{ES_HOST}/{index_name}", timeout=TIMEOUT)
    
    if r.status_code == 200:
        print(f"ℹ️  Index déjà existant: {index_name}")
        return
    
    if r.status_code not in (404,):
        r.raise_for_status()

    r = requests.put(
        f"{ES_HOST}/{index_name}",
        headers={"Content-Type": "application/json"},
        data=json.dumps(mapping),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    print(f"✅ Index créé: {index_name}")


def convert_to_json_serializable(obj):
    """Convertir objets Python en types JSON sérialisables"""
    # Gérer None et NaN
    if obj is None:
        return None
    
    # Gérer pandas NA
    try:
        if pd.isna(obj):
            return None
    except (ValueError, TypeError):
        # pd.isna() échoue sur arrays/listes
        pass
    
    # Gérer numpy NaN
    if isinstance(obj, float) and np.isnan(obj):
        return None
    
    # Gérer dates
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.strftime('%Y-%m-%d')
    if isinstance(obj, date):
        return obj.strftime('%Y-%m-%d')
    
    # Gérer types numpy
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    
    # Gérer listes/arrays
    if isinstance(obj, (list, np.ndarray)):
        return [convert_to_json_serializable(item) for item in obj]
    
    # Type standard (str, int, float, bool, dict)
    return obj


def bulk_index(index_name: str, df: pd.DataFrame, id_cols: list[str]) -> None:
    """Indexation bulk Elasticsearch"""
    if df.empty:
        print(f"⚠️  Rien à indexer pour {index_name} (df vide)")
        return

    # Convertir toutes les colonnes datetime en string ISO
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d')
    
    # Convertir NaN -> None
    df = df.where(pd.notnull(df), None)

    # Bulk payload NDJSON
    lines = []
    for _, row in df.iterrows():
        # Convertir chaque valeur en type JSON sérialisable
        doc = {}
        for key, value in row.items():
            doc[key] = convert_to_json_serializable(value)
        
        # ID stable
        doc_id = "_".join([str(doc.get(c)) for c in id_cols])
        
        lines.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}))
        lines.append(json.dumps(doc, ensure_ascii=False, default=str))

    payload = "\n".join(lines) + "\n"

    r = requests.post(
        f"{ES_HOST}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=payload.encode("utf-8"),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    resp = r.json()

    if resp.get("errors"):
        # Afficher quelques erreurs
        errors = []
        for item in resp.get("items", []):
            action = item.get("index", {})
            if action.get("error"):
                errors.append(action.get("error"))
                if len(errors) >= 5:
                    break
        raise RuntimeError(f"❌ Bulk indexing errors (extraits): {errors}")

    print(f"✅ Bulk OK: {index_name} ({len(df)} docs)")


def main():
    """Indexation complète Elasticsearch"""
    
    print(f"\n🚀 INDEXATION ELASTICSEARCH | snapshot_date={SNAPSHOT_DATE}\n")
    print(f"🌐 ES_HOST: {ES_HOST}")
    print(f"📂 Movies: {MOVIES_PARQUET}")
    print(f"📂 KPIs: {KPIS_PARQUET}\n")

    # Vérifier Elasticsearch
    es_ok()

    # Vérifier fichiers
    if not MOVIES_PARQUET.exists():
        raise FileNotFoundError(f"❌ Fichier introuvable: {MOVIES_PARQUET}")
    if not KPIS_PARQUET.exists():
        raise FileNotFoundError(f"❌ Fichier introuvable: {KPIS_PARQUET}")

    # Mapping movies
    movies_mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "snapshot_date": {"type": "date"},
                "tmdb_id": {"type": "long"},
                "imdb_id": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "original_language": {"type": "keyword"},
                "release_date": {"type": "date"},
                "release_year": {"type": "integer"},
                "popularity": {"type": "double"},
                "tmdb_rating": {"type": "double"},
                "tmdb_vote_count": {"type": "integer"},
                "imdb_rating": {"type": "double"},
                "imdb_votes": {"type": "integer"},
                "metascore": {"type": "double"},
                "composite_score": {"type": "double"},
                "runtime_minutes": {"type": "integer"},
                "status": {"type": "keyword"},
                "rated": {"type": "keyword"},
                "director": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "actors": {"type": "text"},
                "missing_omdb_data": {"type": "boolean"},
                "is_overhyped": {"type": "boolean"},
                "is_hidden_gem": {"type": "boolean"},
            }
        },
    }

    # Mapping KPIs
    kpis_mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "snapshot_date": {"type": "date"},
                "nb_movies": {"type": "integer"},
                "nb_movies_with_omdb": {"type": "integer"},
                "omdb_coverage_ratio": {"type": "double"},
                "avg_tmdb_rating": {"type": "double"},
                "avg_imdb_rating": {"type": "double"},
                "avg_popularity": {"type": "double"},
                "nb_overhyped": {"type": "integer"},
                "nb_hidden_gems": {"type": "integer"},
            }
        },
    }

    # Créer indices
    print("📊 Création indices")
    print("=" * 50)
    create_index_if_missing(INDEX_MOVIES, movies_mapping)
    create_index_if_missing(INDEX_KPIS, kpis_mapping)

    # Lire Parquet
    print("\n📖 Lecture Parquet")
    print("=" * 50)
    df_movies = pd.read_parquet(MOVIES_PARQUET)
    df_kpis = pd.read_parquet(KPIS_PARQUET)
    
    print(f"✅ Movies: {len(df_movies)} lignes, {len(df_movies.columns)} colonnes")
    print(f"✅ KPIs: {len(df_kpis)} lignes, {len(df_kpis.columns)} colonnes")

    # Indexation bulk
    print("\n📤 Indexation Elasticsearch")
    print("=" * 50)
    bulk_index(INDEX_MOVIES, df_movies, id_cols=["snapshot_date", "tmdb_id"])
    bulk_index(INDEX_KPIS, df_kpis, id_cols=["snapshot_date"])

    # Résumé
    print("\n" + "=" * 50)
    print("🎉 INDEXATION TERMINÉE")
    print(f"   📊 Movies: {len(df_movies)} docs indexés")
    print(f"   📊 KPIs: {len(df_kpis)} docs indexés")
    print("=" * 50)
    
    print(f"\n📊 KIBANA")
    print(f"   URL: http://localhost:5601")
    print(f"   1. Aller dans 'Stack Management' > 'Data Views'")
    print(f"   2. Créer Data View '{INDEX_MOVIES}' (timestamp: snapshot_date)")
    print(f"   3. Créer Data View '{INDEX_KPIS}' (timestamp: snapshot_date)")
    print(f"   4. Aller dans 'Discover' pour explorer")


if __name__ == "__main__":
    main()