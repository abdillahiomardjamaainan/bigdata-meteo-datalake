{{ config(
    materialized='table',
    schema='staging',
    tags=['staging', 'omdb']
) }}


with src as (
    select
        snapshot_date,
        imdb_id,
        title,
        payload
    from {{ source('raw', 'raw_omdb_ratings') }}
),

clean as (
    select
        snapshot_date,
        imdb_id,
        title as title_omdb,

-- Notes principales
case
            when payload->>'imdbRating' is null then null
            when payload->>'imdbRating' = 'N/A' then null
            else (payload->>'imdbRating')::double precision
        end as imdb_rating,

        case
            when payload->>'imdbVotes' is null then null
            when payload->>'imdbVotes' = 'N/A' then null
            else replace(payload->>'imdbVotes', ',', '')::bigint
        end as imdb_votes,

        case
            when payload->>'Metascore' is null then null
            when payload->>'Metascore' = 'N/A' then null
            else (payload->>'Metascore')::int
        end as metascore,

-- Métadonnées
payload ->> 'Rated' as rated,
payload ->> 'Type' as type,
payload ->> 'Year' as year_text,
payload ->> 'Country' as country,
payload ->> 'Genre' as genre,
payload ->> 'Director' as director,
payload ->> 'Actors' as actors,

-- Array des notes (JSON)
payload->'Ratings' as ratings_json from src )

select * from clean