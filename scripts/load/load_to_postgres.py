"""
Chargement des données brutes (JSON) vers PostgreSQL
Format: datalake/raw/ → PostgreSQL schema 'raw'
"""
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

# Chemins
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATALAKE_RAW = PROJECT_ROOT / "datalake" / "raw"

# Connexion PostgreSQL
DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/datalake"


def get_latest_partition(source_dir):
    """
    Trouve la partition la plus récente (dt=xxx/run=xxx)
    
    Args:
        source_dir: Chemin vers raw/airquality ou raw/weather
    
    Returns:
        Path de la partition la plus récente
    """
    partitions = []
    
    for dt_folder in source_dir.glob("dt=*"):
        for run_folder in dt_folder.glob("run=*"):
            partitions.append(run_folder)
    
    if not partitions:
        return None
    
    # Trier par date et heure (dt=2026-02-05/run=1333)
    partitions.sort(reverse=True)
    return partitions[0]


def load_airquality():
    """Charge les données Air Quality dans PostgreSQL"""
    print("\n📊 Chargement Air Quality...")
    
    source_dir = DATALAKE_RAW / "airquality"
    latest_partition = get_latest_partition(source_dir)
    
    if not latest_partition:
        print("⚠️ Aucune donnée Air Quality trouvée")
        return None
    
    print(f"📁 Partition: {latest_partition}")
    
    # Lire tous les fichiers JSON
    rows = []
    for json_file in latest_partition.glob("*.json"):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Parser les données
        city_name = json_file.stem.capitalize()
        
        for item in data.get('list', []):
            row = {
                'city': city_name,
                'latitude': data['coord']['lat'],
                'longitude': data['coord']['lon'],
                'timestamp': datetime.fromtimestamp(item['dt']),
                'aqi': item['main']['aqi'],
                'pm2_5': item['components'].get('pm2_5'),
                'pm10': item['components'].get('pm10'),
                'no2': item['components'].get('no2'),
                'o3': item['components'].get('o3'),
                'co': item['components'].get('co'),
                'loaded_at': datetime.now()
            }
            rows.append(row)
    
    df = pd.DataFrame(rows)
    print(f"✅ {len(df)} lignes extraites")
    
    return df


def load_weather():
    """Charge les données Weather dans PostgreSQL"""
    print("\n🌤️ Chargement Weather...")
    
    source_dir = DATALAKE_RAW / "weather"
    latest_partition = get_latest_partition(source_dir)
    
    if not latest_partition:
        print("⚠️ Aucune donnée Weather trouvée")
        return None
    
    print(f"📁 Partition: {latest_partition}")
    
    # Lire tous les fichiers JSON
    rows = []
    for json_file in latest_partition.glob("*.json"):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Parser les données
        row = {
            'city': data['name'],
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon'],
            'timestamp': datetime.fromtimestamp(data['dt']),
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'loaded_at': datetime.now()
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    print(f"✅ {len(df)} lignes extraites")
    
    return df


def load_population():
    """Charge les données Population dans PostgreSQL"""
    print("\n👥 Chargement Population...")
    
    source_dir = DATALAKE_RAW / "population"
    latest_partition = get_latest_partition(source_dir)
    
    if not latest_partition:
        print("⚠️ Aucune donnée Population trouvée")
        return None
    
    print(f"📁 Partition: {latest_partition}")
    
    # Lire le fichier JSON unique
    json_file = latest_partition / "population.json"
    
    with open(json_file, 'r', encoding='utf-8') as f:
        cities = json.load(f)
    
    # Créer DataFrame
    df = pd.DataFrame(cities)
    df['loaded_at'] = datetime.now()
    
    print(f"✅ {len(df)} villes chargées")
    
    return df


def create_schema(engine):
    """Crée le schéma 'raw' s'il n'existe pas"""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()
    print("✅ Schéma 'raw' créé")


def main():
    """Fonction principale"""
    print("=" * 70)
    print("🚀 CHARGEMENT POSTGRESQL")
    print("=" * 70)
    
    # Connexion à PostgreSQL
    engine = create_engine(DATABASE_URL)
    
    try:
        # Créer le schéma
        create_schema(engine)
        
        # Charger Air Quality
        df_airquality = load_airquality()
        if df_airquality is not None:
            df_airquality.to_sql(
                'airquality',
                engine,
                schema='raw',
                if_exists='replace',
                index=False
            )
            print("✅ Table raw.airquality créée")
        
        # Charger Weather
        df_weather = load_weather()
        if df_weather is not None:
            df_weather.to_sql(
                'weather',
                engine,
                schema='raw',
                if_exists='replace',
                index=False
            )
            print("✅ Table raw.weather créée")
        
        # Charger Population
        df_population = load_population()
        if df_population is not None:
            df_population.to_sql(
                'population',
                engine,
                schema='raw',
                if_exists='replace',
                index=False
            )
            print("✅ Table raw.population créée")
        
        print("\n" + "=" * 70)
        print("✅ CHARGEMENT TERMINÉ")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()