{{ config(
    materialized='table',
    schema='staging',
    tags=['staging', 'tmdb']
) }}


with src as (
    select
        snapshot_date,
        tmdb_id,
        title,
        payload
    from {{ source('raw', 'raw_tmdb_popular') }}
),

clean as (
    select
        snapshot_date,
        tmdb_id,
        title,
        nullif(payload->>'release_date','')::date as release_date,
        (payload->>'popularity')::double precision as popularity,
        (payload->>'vote_average')::double precision as tmdb_rating,
        (payload->>'vote_count')::int as tmdb_vote_count,
        payload->>'original_language' as original_language,
        payload->'genre_ids' as genre_ids_json
    from src
)

select * from clean