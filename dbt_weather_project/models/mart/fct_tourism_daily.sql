{{ config(
    materialized='table',
    tags=['marts', 'tourism', 'daily']
) }}

WITH daily_weather AS (
    SELECT
        city_name,
        country_name,
        country_code,
        region,
        population,
        city_category,
        observation_date,

-- Agrégations météo quotidiennes
COUNT(*) AS nb_observations,
        ROUND(AVG(temperature_celsius)::NUMERIC, 1) AS avg_temperature,
        ROUND(MIN(temperature_celsius)::NUMERIC, 1) AS min_temperature,
        ROUND(MAX(temperature_celsius)::NUMERIC, 1) AS max_temperature,
        
        ROUND(AVG(humidity_percent)::NUMERIC, 1) AS avg_humidity,
        ROUND(MIN(humidity_percent)::NUMERIC, 1) AS min_humidity,
        ROUND(MAX(humidity_percent)::NUMERIC, 1) AS max_humidity,
        
        ROUND(SUM(precipitation_mm)::NUMERIC, 1) AS total_precipitation,
        ROUND(AVG(precipitation_mm)::NUMERIC, 1) AS avg_precipitation,
        ROUND(MAX(precipitation_mm)::NUMERIC, 1) AS max_precipitation_hourly,
        
        ROUND(AVG(wind_speed_kmh)::NUMERIC, 1) AS avg_wind_speed,
        ROUND(MAX(wind_speed_kmh)::NUMERIC, 1) AS max_wind_speed,

-- Comptages horaires
COUNT(CASE WHEN precipitation_mm > 0 THEN 1 END) AS hours_with_rain,
        COUNT(CASE WHEN precipitation_mm > 5 THEN 1 END) AS hours_heavy_rain,
        COUNT(CASE WHEN wind_speed_kmh > 30 THEN 1 END) AS hours_strong_wind
        
    FROM {{ ref('stg_weather_hourly') }}
    GROUP BY 
        city_name, 
        country_name, 
        country_code, 
        region, 
        population, 
        city_category, 
        observation_date
),

tourism_metrics AS (
    SELECT
        *,

-- KPI 1: Score de confort global (0-100)
ROUND(
            GREATEST(0, LEAST(100,
                50 -- Base score
                + (CASE WHEN avg_temperature BETWEEN 15 AND 25 THEN 20 ELSE 0 END) -- Température idéale
                + (CASE WHEN avg_humidity BETWEEN 40 AND 60 THEN 15 ELSE 0 END) -- Humidité confortable
                + (CASE WHEN total_precipitation = 0 THEN 15 ELSE -15 END) -- Pas de pluie
                + (CASE WHEN avg_wind_speed < 20 THEN 10 ELSE -10 END) -- Vent modéré
            ))::NUMERIC, 0
        ) AS tourism_comfort_score,

-- KPI 2: Qualité de la température (0-100)
ROUND(
            CASE
                WHEN avg_temperature BETWEEN 18 AND 24 THEN 100
                WHEN avg_temperature BETWEEN 15 AND 27 THEN 80
                WHEN avg_temperature BETWEEN 10 AND 30 THEN 60
                WHEN avg_temperature BETWEEN 5 AND 35 THEN 40
                ELSE 20
            END::NUMERIC, 0
        ) AS temperature_comfort_score,

-- KPI 3: Score de pluie (0-100, 100 = pas de pluie)
ROUND(
            GREATEST(0, 100 - (hours_with_rain * 4.17))::NUMERIC, 0
        ) AS rain_score,

-- KPI 4: Score de vent (0-100, 100 = calme)
ROUND(
            CASE
                WHEN avg_wind_speed < 10 THEN 100
                WHEN avg_wind_speed < 20 THEN 80
                WHEN avg_wind_speed < 30 THEN 60
                WHEN avg_wind_speed < 40 THEN 40
                ELSE 20
            END::NUMERIC, 0
        ) AS wind_comfort_score,

-- KPI 5: Catégorie de journée
CASE
    WHEN total_precipitation = 0
    AND avg_temperature BETWEEN 15 AND 25  THEN 'Idéale'
    WHEN total_precipitation < 5
    AND avg_temperature BETWEEN 10 AND 28  THEN 'Agréable'
    WHEN total_precipitation < 10
    OR (
        avg_temperature NOT BETWEEN 5 AND 32
    ) THEN 'Moyenne'
    ELSE 'Difficile'
END AS day_category,

-- KPI 6: Recommandation activités
CASE
    WHEN total_precipitation = 0
    AND avg_temperature BETWEEN 15 AND 25  THEN 'Visites extérieures, parcs'
    WHEN total_precipitation = 0
    AND avg_temperature < 15 THEN 'Marchés, balades urbaines'
    WHEN total_precipitation > 0
    AND total_precipitation < 5 THEN 'Musées avec sorties courtes'
    ELSE 'Musées, galeries, indoor'
END AS recommended_activities,

-- Métadonnées
CURRENT_TIMESTAMP AS created_at FROM daily_weather )

SELECT *
FROM tourism_metrics
ORDER BY
    observation_date DESC,
    tourism_comfort_score DESC