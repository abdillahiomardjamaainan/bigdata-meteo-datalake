{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'movies']
) }}


with pop as (
    select
        snapshot_date,
        tmdb_id,
        title,
        release_date,
        popularity,
        tmdb_rating,
        tmdb_vote_count,
        original_language,
        genre_ids_json
    from {{ ref('stg_tmdb_popular') }}
),

det as (
    select
        snapshot_date,
        tmdb_id,
        imdb_id,
        runtime_minutes,
        status,
        genres_json,
        production_countries_json
    from {{ ref('stg_tmdb_details') }}
),

tmdb as (
    select
        p.snapshot_date,
        p.tmdb_id,
        d.imdb_id,
        p.title,
        p.release_date,
        extract(year from p.release_date) as release_year,
        d.runtime_minutes,
        d.status,
        p.original_language,
        p.popularity,
        p.tmdb_rating,
        p.tmdb_vote_count,
        d.genres_json,
        d.production_countries_json
    from pop p
    left join det d
        on p.snapshot_date = d.snapshot_date
        and p.tmdb_id = d.tmdb_id
),

omdb as (
    select
        snapshot_date,
        imdb_id,
        imdb_rating,
        imdb_votes,
        metascore,
        rated,
        type,
        year_text,
        country,
        genre,
        director,
        actors,
        ratings_json
    from {{ ref('stg_omdb_ratings') }}
),

joined as (
    select
        t.*,

-- Notes OMDb (peuvent être NULL)
o.imdb_rating, o.imdb_votes, o.metascore,

-- Métadonnées OMDb
o.rated,
o.type,
o.country as omdb_country,
o.genre as omdb_genre,
o.director,
o.actors,
o.ratings_json as omdb_ratings_json,

-- Qualité données
case
    when o.imdb_id is null then true
    else false
end as missing_omdb_data,

-- Score composite (60% IMDb + 40% Metascore)
case
            when o.imdb_rating is not null and o.metascore is not null
                then round(((o.imdb_rating * 0.6) + ((o.metascore / 10.0) * 0.4))::numeric, 2)
            when o.imdb_rating is not null
                then o.imdb_rating::numeric
            else t.tmdb_rating::numeric
        end as composite_score,

-- Flags business
case
            when t.popularity >= 50 and coalesce(o.imdb_rating, t.tmdb_rating) < 6.0 then true
            else false
        end as is_overhyped,

        case
            when t.popularity < 30 and coalesce(o.imdb_rating, t.tmdb_rating) >= 7.5 then true
            else false
        end as is_hidden_gem

    from tmdb t
    left join omdb o
        on t.snapshot_date = o.snapshot_date
        and t.imdb_id = o.imdb_id
)

select * from joined