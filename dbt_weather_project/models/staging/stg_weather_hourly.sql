{{ config(materialized='view') }}


WITH weather_source AS (
    SELECT * FROM {{ source('analytics_raw', 'weather_hourly_raw') }}
),

cities_staging AS (
    SELECT * FROM {{ ref('stg_cities') }}  -- ← Changement ici !
),

cleaned AS (
    SELECT
        -- Identifiants principaux
        w.city AS city_name,
        w.country_code,
        w.time_utc AS observation_time,

-- Mesures météo nettoyées (arrondi 1 décimale)
ROUND(w.temperature_2m::NUMERIC, 1) AS temperature_celsius,
        ROUND(w.humidity_2m::NUMERIC, 1) AS humidity_percent,
        ROUND(w.precipitation::NUMERIC, 1) AS precipitation_mm,
        ROUND(w.wind_speed_10m::NUMERIC, 1) AS wind_speed_kmh,

-- Colonnes temporelles dérivées
DATE(w.time_utc) AS observation_date,
EXTRACT (
    HOUR
    FROM w.time_utc
) AS observation_hour,
EXTRACT (
    DOW
    FROM w.time_utc
) AS day_of_week,
TO_CHAR (w.time_utc, 'Day') AS day_name,
EXTRACT (
    WEEK
    FROM w.time_utc
) AS week_number,
EXTRACT (
    MONTH
    FROM w.time_utc
) AS month_number,
TO_CHAR (w.time_utc, 'Month') AS month_name,

-- Enrichissement avec données géographiques (depuis stg_cities)
c.country_name,
c.region,
c.timezone,
c.latitude,
c.longitude,
c.population,
c.city_category, -- ← Nouvelle colonne !

-- Métadonnées de chargement
CURRENT_TIMESTAMP AS loaded_at
        
    FROM weather_source w
    LEFT JOIN cities_staging c  -- ← Changement ici !
        ON w.city = c.city_name 
        AND w.country_code = c.country_code
    
    WHERE w.time_utc IS NOT NULL
      AND w.temperature_2m IS NOT NULL
)

SELECT * FROM cleaned ORDER BY observation_time DESC, city_name