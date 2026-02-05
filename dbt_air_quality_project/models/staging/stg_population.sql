{{
    config(
        materialized='table',
        schema='staging'
    )
}}

SELECT
    city,
    country,
    country_code,
    CAST(latitude AS FLOAT) AS latitude,
    CAST(longitude AS FLOAT) AS longitude,
    CAST(population AS INTEGER) AS population,
    CAST(loaded_at AS TIMESTAMP) AS loaded_at,

-- Colonnes calculées
CASE 
        WHEN population < 1000000 THEN 'Small'
        WHEN population BETWEEN 1000000 AND 3000000 THEN 'Medium'
        WHEN population BETWEEN 3000000 AND 5000000 THEN 'Large'
        ELSE 'Mega'
    END AS city_size

FROM {{ source('raw', 'population') }}