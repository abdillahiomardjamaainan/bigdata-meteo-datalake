{{ config(
    materialized='table',
    schema='staging',
    tags=['staging', 'tmdb']
) }}


with src as (
    select
        snapshot_date,
        tmdb_id,
        imdb_id,
        title,
        payload
    from {{ source('raw', 'raw_tmdb_details') }}
),

clean as (
    select
        snapshot_date,
        tmdb_id,
        imdb_id,
        title,

-- date
nullif(payload->>'release_date','')::date as release_date,

-- runtime
nullif(payload->>'runtime','')::int as runtime_minutes,  -- ✅ VIRGULE AJOUTÉE

-- métadonnées utiles
payload ->> 'status' as status,
payload ->> 'original_language' as original_language,

-- arrays JSON (utiles pour analyses futures)
payload->'genres' as genres_json,
        payload->'production_countries' as production_countries_json
        
    from src
)

select * from clean