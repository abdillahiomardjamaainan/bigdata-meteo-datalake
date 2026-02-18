"""
Export PostgreSQL → Parquet (formatted + usage)
"""

import os
import pandas as pd
import psycopg2
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq


DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'datalake'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}


DATALAKE_PATH = Path(os.getenv('OUTPUT_DIR', '/opt/airflow/datalake'))
SNAPSHOT_DATE = os.getenv('SNAPSHOT_DATE', datetime.now().strftime('%Y-%m-%d'))


EXPORTS = {
    'formatted': [
        ('analytics_staging.stg_tmdb_popular', 'tmdb_popular'),
        ('analytics_staging.stg_tmdb_details', 'tmdb_details'),
        ('analytics_staging.stg_omdb_ratings', 'omdb_ratings'),
    ],
    'usage': [
        ('analytics_marts.movies_enriched_daily', 'movies_enriched'),
        ('analytics_marts.kpi_daily_summary', 'kpi_daily'),
    ]
}

def export_table_to_parquet(conn, schema_table: str, output_path: Path):
    """Exporte table PostgreSQL vers Parquet"""
    
    print(f" Export {schema_table} → {output_path}")
    
    try:
        # Lire données
        query = f"SELECT * FROM {schema_table}"
        df = pd.read_sql(query, conn)
        
        print(f"   ✅ {len(df)} lignes extraites")
        
        # Créer dossier si nécessaire
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Exporter en Parquet (avec compression snappy)
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        # Afficher taille fichier
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"   ✅ Fichier créé : {size_mb:.2f} MB")
        
        return len(df)
    
    except Exception as e:
        print(f"   ⚠️  Erreur: {e}")
        return 0


def main():
    """Export complet PostgreSQL → Parquet"""
    
    print(f"\n EXPORT DATALAKE - {SNAPSHOT_DATE}\n")
    
    # Afficher config
    print(f" Datalake path: {DATALAKE_PATH.absolute()}")
    print(f" PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}\n")
    
    # Connexion PostgreSQL
    try:
        print(" Connexion PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connecté\n")
    except psycopg2.OperationalError as e:
        print(f"❌ ERREUR: Impossible de se connecter à PostgreSQL")
        print(f"   Erreur: {e}")
        raise
    
    stats = {'formatted': 0, 'usage': 0}
    
    try:
        # Export FORMATTED (staging)
        print(" FORMATTED (staging)")
        print("=" * 50)
        
        for schema_table, name in EXPORTS['formatted']:
            output_path = DATALAKE_PATH / 'formatted' / name / f'snapshot_date={SNAPSHOT_DATE}' / 'data.parquet'
            count = export_table_to_parquet(conn, schema_table, output_path)
            stats['formatted'] += count
        
        # Export USAGE (marts)
        print("\n USAGE (marts)")
        print("=" * 50)
        
        for schema_table, name in EXPORTS['usage']:
            output_path = DATALAKE_PATH / 'usage' / name / f'snapshot_date={SNAPSHOT_DATE}' / 'data.parquet'
            count = export_table_to_parquet(conn, schema_table, output_path)
            stats['usage'] += count
        
        # Résumé
        print("\n" + "=" * 50)
        print(f" EXPORT TERMINÉ")
        print(f"    Formatted: {stats['formatted']} lignes")
        print(f"    Usage: {stats['usage']} lignes")
        print("=" * 50)
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()