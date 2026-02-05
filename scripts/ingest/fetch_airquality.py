"""
Extraction des données de qualité de l'air depuis OpenWeatherMap
API: https://openweathermap.org/api/air-pollution
"""
import json
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import os

# Chemin explicite vers .env

PROJECT_ROOT = Path(__file__).parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)  # ← Spécifier le chemin
API_KEY = os.getenv('OPENWEATHER_API_KEY')

# Chemins
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "cities.json"
DATALAKE_RAW = PROJECT_ROOT / "datalake" / "raw" / "airquality"


def load_cities():
    """Charge la liste des villes depuis config/cities.json"""
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def fetch_air_quality(latitude, longitude, city_name):
    """
    Appelle l'API Air Pollution pour une ville
    
    Args:
        latitude: Latitude de la ville
        longitude: Longitude de la ville
        city_name: Nom de la ville (pour logging)
    
    Returns:
        dict: Données JSON de l'API ou None si erreur
    """
    url = "http://api.openweathermap.org/data/2.5/air_pollution"
    params = {
        'lat': latitude,
        'lon': longitude,
        'appid': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ {city_name}: AQI = {data['list'][0]['main']['aqi']}")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur pour {city_name}: {e}")
        return None


def save_to_raw(data, city_name, run_time):
    """
    Sauvegarde les données brutes dans datalake/raw/
    
    Format: raw/airquality/dt=YYYY-MM-DD/run=HHMM/city_name.json
    """
    if data is None:
        return
    
    # Créer le chemin partitionné
    date_str = run_time.strftime('%Y-%m-%d')
    run_str = run_time.strftime('%H%M')
    
    output_dir = DATALAKE_RAW / f"dt={date_str}" / f"run={run_str}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Nom du fichier
    output_file = output_dir / f"{city_name.lower()}.json"
    
    # Sauvegarder
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"📁 Sauvegardé: {output_file}")


def main():
    """Fonction principale"""
    print("=" * 70)
    print("🚀 EXTRACTION AIR QUALITY")
    print("=" * 70)
    
    # Vérifier la clé API
    if not API_KEY:
        print("❌ ERREUR: Clé API manquante dans .env")
        return
    
    # Timestamp d'exécution
    run_time = datetime.now()
    print(f"⏰ Heure d'exécution: {run_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Charger les villes
    cities = load_cities()
    print(f"🌍 {len(cities)} villes à traiter\n")
    
    # Extraire pour chaque ville
    for city in cities:
        city_name = city['city']
        lat = city['latitude']
        lon = city['longitude']
        
        # Appeler l'API
        data = fetch_air_quality(lat, lon, city_name)
        
        # Sauvegarder
        save_to_raw(data, city_name, run_time)
    
    print("\n" + "=" * 70)
    print("✅ EXTRACTION TERMINÉE")
    print("=" * 70)


if __name__ == "__main__":
    main()