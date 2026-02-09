{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'kpi']
) }}

with base as (
    select
        snapshot_date,
        popularity,
        tmdb_rating,
        imdb_rating,
        missing_omdb_data,
        is_overhyped,
        is_hidden_gem
    from {{ ref('movies_enriched_daily') }}
)

select snapshot_date,

-- Volumétrie
count(*) as nb_movies,

-- Couverture OMDb
sum(case when not missing_omdb_data then 1 else 0 end) as nb_movies_with_omdb,
    round(
        sum(case when not missing_omdb_data then 1 else 0 end)::numeric
        / count(*)::numeric,
        2
    ) as omdb_coverage_ratio,

-- Notes moyennes (✅ CAST ::numeric ajouté)
round(avg(tmdb_rating)::numeric, 2) as avg_tmdb_rating,
    round(avg(imdb_rating)::numeric, 2) as avg_imdb_rating,

-- Popularité moyenne (✅ CAST ::numeric ajouté)
round(avg(popularity)::numeric, 2) as avg_popularity,

-- Flags
sum(
    case
        when is_overhyped then 1
        else 0
    end
) as nb_overhyped,
sum(
    case
        when is_hidden_gem then 1
        else 0
    end
) as nb_hidden_gems
from base
group by
    snapshot_date