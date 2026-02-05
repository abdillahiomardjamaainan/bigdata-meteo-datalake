"""
Indexation des données Parquet (usage/) vers Elasticsearch
Format: datalake/usage/*.parquet → Elasticsearch
"""
import pandas as pd
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import warnings
warnings.filterwarnings('ignore')

# Chemins
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATALAKE_USAGE = PROJECT_ROOT / "datalake" / "usage"

# Connexion Elasticsearch
ES_HOST = "http://localhost:9200"


def get_latest_parquet(table_dir):
    """Récupère le fichier Parquet le plus récent"""
    parquet_files = list(table_dir.glob("*.parquet"))
    if not parquet_files:
        return None
    parquet_files.sort(reverse=True)
    return parquet_files[0]


# ...existing code...

def index_mart_city_overview(es):
    """Indexe mart_city_overview dans Elasticsearch"""
    print("\n📊 Indexation mart_city_overview...")
    
    # Lire le Parquet
    table_dir = DATALAKE_USAGE / "mart_city_overview"
    parquet_file = get_latest_parquet(table_dir)
    
    if not parquet_file:
        print("   ⚠️ Aucun fichier Parquet trouvé")
        return
    
    print(f"   📁 Lecture: {parquet_file.name}")
    df = pd.read_parquet(parquet_file)
    
    # Nettoyer les NaN
    df = df.where(pd.notnull(df), None)
    
    # 🌍 CRÉER LE CHAMP GEO_POINT (latitude + longitude combinés)
    df['location'] = df.apply(
        lambda row: {
            "lat": row['latitude'],
            "lon": row['longitude']
        } if pd.notnull(row['latitude']) and pd.notnull(row['longitude']) else None,
        axis=1
    )
    
    # Convertir les timestamps en chaînes ISO
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Nom de l'index
    index_name = "city_overview"
    
    # Supprimer l'ancien index (si existe)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"   🗑️ Ancien index supprimé")
    
    # Créer le mapping AVEC geo_point
    mapping = {
        "mappings": {
            "properties": {
                "city": {"type": "keyword"},
                "country": {"type": "keyword"},
                "country_code": {"type": "keyword"},
                "population": {"type": "long"},
                "city_size": {"type": "keyword"},
                "latitude": {"type": "float"},
                "longitude": {"type": "float"},
                "location": {"type": "geo_point"},  # ← CHAMP GÉO
                "aqi": {"type": "integer"},
                "air_quality_category": {"type": "keyword"},
                "pm2_5": {"type": "float"},
                "pm10": {"type": "float"},
                "no2": {"type": "float"},
                "temperature": {"type": "float"},
                "temperature_category": {"type": "keyword"},
                "humidity": {"type": "float"},
                "wind_speed": {"type": "float"},
                "weather_main": {"type": "keyword"},
                "livability_score": {"type": "keyword"},
                "population_millions": {"type": "float"},
                "air_measured_at": {"type": "date"},
                "weather_measured_at": {"type": "date"}
            }
        }
    }
    
    es.indices.create(index=index_name, body=mapping)
    print(f"   ✅ Index '{index_name}' créé avec geo_point")
    
    # Préparer les documents
    actions = []
    for idx, row in df.iterrows():
        doc = row.to_dict()
        action = {
            "_index": index_name,
            "_id": row['city'],
            "_source": doc
        }
        actions.append(action)
    
    # Indexer
    success, errors = bulk(es, actions, raise_on_error=False)
    
    print(f"   ✅ {success} documents indexés")
    
    # Rafraîchir
    es.indices.refresh(index=index_name)
    
    # Vérifier
    count = es.count(index=index_name)['count']
    print(f"   📊 Total: {count} villes")

# ...existing code...


def index_mart_pollution_alerts(es):
    """Indexe mart_pollution_alerts dans Elasticsearch"""
    print("\n📊 Indexation mart_pollution_alerts...")
    
    table_dir = DATALAKE_USAGE / "mart_pollution_alerts"
    parquet_file = get_latest_parquet(table_dir)
    
    if not parquet_file:
        print("   ⚠️ Aucun fichier Parquet trouvé")
        return
    
    print(f"   📁 Lecture: {parquet_file.name}")
    df = pd.read_parquet(parquet_file)
    
    if len(df) == 0:
        print("   ℹ️ Aucune alerte (table vide - c'est normal)")
        return
    
    df = df.where(pd.notnull(df), None)
    
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    index_name = "pollution_alerts"
    
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    
    mapping = {
        "mappings": {
            "properties": {
                "city": {"type": "keyword"},
                "country": {"type": "keyword"},
                "aqi": {"type": "integer"},
                "pm2_5": {"type": "float"},
                "alert_type": {"type": "keyword"},
                "alert_level": {"type": "keyword"}
            }
        }
    }
    
    es.indices.create(index=index_name, body=mapping)
    print(f"   ✅ Index créé")
    
    actions = [{"_index": index_name, "_source": row.to_dict()} for _, row in df.iterrows()]
    success, _ = bulk(es, actions, raise_on_error=False)
    
    print(f"   ✅ {success} alertes indexées")


# ...existing code...

def main():
    """Fonction principale"""
    print("=" * 70)
    print("🔍 INDEXATION ELASTICSEARCH")
    print("=" * 70)
    
    try:
        # Connexion (compatible Elasticsearch 8.x)
        es = Elasticsearch(
            [ES_HOST],
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        
        # Test avec info() au lieu de ping()
        try:
            info = es.info()
            print(f"✅ Connecté à Elasticsearch {info['version']['number']}")
        except Exception as e:
            print("❌ Impossible de se connecter à Elasticsearch")
            print(f"   Erreur: {e}")
            return
        
        # Indexer
        index_mart_city_overview(es)
        index_mart_pollution_alerts(es)
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 70)
    print("✅ INDEXATION TERMINÉE")
    print("=" * 70)
    print(f"\n🌐 Kibana: http://localhost:5601")
    print("   Index disponibles:")
    print("   - city_overview (10 villes)")
    print("   - pollution_alerts (alertes)")


if __name__ == "__main__":
    main()