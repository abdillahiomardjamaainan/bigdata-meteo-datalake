{{ config(materialized='view') }}


WITH source AS (
    SELECT * FROM {{ source('analytics_raw', 'cities_raw') }}
),

cleaned AS (
    SELECT
        -- Identifiants
        city AS city_name,
        country AS country_name,
        country_code,

-- Géographie
admin1 AS region,
        timezone,
        ROUND(latitude::NUMERIC, 4) AS latitude,
        ROUND(longitude::NUMERIC, 4) AS longitude,

-- Démographie
population,

-- Catégorie de ville par population
CASE
    WHEN population >= 5000000 THEN 'Mégalopole'
    WHEN population >= 2000000 THEN 'Très grande ville'
    WHEN population >= 1000000 THEN 'Grande ville'
    ELSE 'Ville moyenne'
END AS city_category,

-- Métadonnées
CURRENT_TIMESTAMP AS loaded_at FROM source )

SELECT * FROM cleaned ORDER BY population DESC