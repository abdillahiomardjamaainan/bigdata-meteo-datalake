"""
Export des tables marts (PostgreSQL) vers Parquet (usage/)
Format: PostgreSQL analytics_marts.* → datalake/usage/
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine

# Chemins
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATALAKE_USAGE = PROJECT_ROOT / "datalake" / "usage"

# Connexion PostgreSQL
DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/datalake"


def export_table(engine, table_name, output_dir):
    """Exporte une table mart en Parquet"""
    print(f"\n📊 Export {table_name}...")
    
    # Lire depuis PostgreSQL
    query = f"SELECT * FROM analytics_marts.{table_name}"
    df = pd.read_sql(query, engine)
    
    print(f"   ✅ {len(df)} lignes extraites")
    
    # Créer le dossier de sortie
    output_path = output_dir / table_name
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Nom du fichier avec timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_path / f"{table_name}_{timestamp}.parquet"
    
    # Sauvegarder en Parquet
    df.to_parquet(output_file, index=False, compression='snappy')
    
    file_size_kb = output_file.stat().st_size / 1024
    print(f"   💾 Sauvegardé: {output_file}")
    print(f"   📦 Taille: {file_size_kb:.2f} KB")
    print(f"   📋 Colonnes: {list(df.columns)}")
    
    # Afficher un aperçu des données
    if len(df) > 0:
        print(f"   👁️ Aperçu (5 premières lignes):")
        print(df.head().to_string(index=False, max_colwidth=30))


def main():
    """Fonction principale"""
    print("=" * 70)
    print("📤 EXPORT MARTS → USAGE (Parquet)")
    print("=" * 70)
    
    engine = create_engine(DATABASE_URL)
    
    # Liste des tables marts
    tables = ['mart_city_overview', 'mart_pollution_alerts']
    
    for table in tables:
        try:
            export_table(engine, table, DATALAKE_USAGE)
        except Exception as e:
            print(f"   ❌ Erreur pour {table}: {e}")
    
    print("\n" + "=" * 70)
    print("✅ EXPORT MARTS TERMINÉ")
    print("=" * 70)
    print(f"📁 Destination: {DATALAKE_USAGE}")
    print("\n📊 Ces fichiers sont prêts pour:")
    print("   - Elasticsearch/Kibana")
    print("   - PowerBI/Tableau")
    print("   - Analyse Python/R")


if __name__ == "__main__":
    main()