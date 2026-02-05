"""
Extraction des données de population depuis config/cities.json
Note: Données statiques (pas d'API externe)
"""
import json
from pathlib import Path
from datetime import datetime

# Chemins
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "cities.json"
DATALAKE_RAW = PROJECT_ROOT / "datalake" / "raw" / "population"


def load_cities():
    """Charge la liste des villes depuis config/cities.json"""
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_to_raw(cities, run_time):
    """
    Sauvegarde les données de population dans datalake/raw/
    
    Format: raw/population/dt=YYYY-MM-DD/run=HHMM/population.json
    """
    # Créer le chemin partitionné
    date_str = run_time.strftime('%Y-%m-%d')
    run_str = run_time.strftime('%H%M')
    
    output_dir = DATALAKE_RAW / f"dt={date_str}" / f"run={run_str}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Nom du fichier
    output_file = output_dir / "population.json"
    
    # Sauvegarder
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cities, f, indent=2, ensure_ascii=False)
    
    print(f"📁 Sauvegardé: {output_file}")


def main():
    """Fonction principale"""
    print("=" * 70)
    print("👥 EXTRACTION POPULATION")
    print("=" * 70)
    
    # Timestamp d'exécution
    run_time = datetime.now()
    print(f"⏰ Heure d'exécution: {run_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Charger les villes
    cities = load_cities()
    print(f"🌍 {len(cities)} villes chargées\n")
    
    # Afficher les données
    for city in cities:
        city_name = city['city']
        population = city.get('population', 'N/A')
        country = city.get('country', 'N/A')
        print(f"✅ {city_name}, {country}: {population:,} habitants")
    
    # Sauvegarder
    save_to_raw(cities, run_time)
    
    print("\n" + "=" * 70)
    print("✅ EXTRACTION TERMINÉE")
    print("=" * 70)


if __name__ == "__main__":
    main()