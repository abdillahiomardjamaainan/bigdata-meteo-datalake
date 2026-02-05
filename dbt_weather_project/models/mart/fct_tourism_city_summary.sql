{{ config(
    materialized='table',
    tags=['marts', 'tourism', 'summary']
) }}

WITH city_stats AS (
    SELECT
        city_name,
        country_name,
        country_code,
        region,
        population,
        city_category,

-- Nombre de jours analysés
COUNT(*) AS total_days,

-- Scores moyens
ROUND(AVG(tourism_comfort_score)::NUMERIC, 1) AS avg_comfort_score,
        ROUND(AVG(temperature_comfort_score)::NUMERIC, 1) AS avg_temperature_score,
        ROUND(AVG(rain_score)::NUMERIC, 1) AS avg_rain_score,
        ROUND(AVG(wind_comfort_score)::NUMERIC, 1) AS avg_wind_score,

-- Température
ROUND(AVG(avg_temperature)::NUMERIC, 1) AS avg_temperature,
        ROUND(MIN(min_temperature)::NUMERIC, 1) AS min_temperature_period,
        ROUND(MAX(max_temperature)::NUMERIC, 1) AS max_temperature_period,

-- Précipitations
ROUND(SUM(total_precipitation)::NUMERIC, 1) AS total_precipitation_period,
        ROUND(AVG(total_precipitation)::NUMERIC, 1) AS avg_daily_precipitation,
        ROUND(MAX(total_precipitation)::NUMERIC, 1) AS max_daily_precipitation,

-- Vent
ROUND(AVG(avg_wind_speed)::NUMERIC, 1) AS avg_wind_speed,
        ROUND(MAX(max_wind_speed)::NUMERIC, 1) AS max_wind_speed_period,

-- Comptage des jours par catégorie
COUNT(
    CASE
        WHEN day_category = 'Idéale' THEN 1
    END
) AS days_ideal,
COUNT(
    CASE
        WHEN day_category = 'Agréable' THEN 1
    END
) AS days_pleasant,
COUNT(
    CASE
        WHEN day_category = 'Moyenne' THEN 1
    END
) AS days_average,
COUNT(
    CASE
        WHEN day_category = 'Difficile' THEN 1
    END
) AS days_difficult,

-- Comptage jours de pluie
COUNT(
    CASE
        WHEN total_precipitation > 0 THEN 1
    END
) AS days_with_rain,
COUNT(
    CASE
        WHEN total_precipitation > 10 THEN 1
    END
) AS days_heavy_rain,

-- Meilleur et pire jour
MAX(tourism_comfort_score) AS best_day_score,
        MIN(tourism_comfort_score) AS worst_day_score
        
    FROM {{ ref('fct_tourism_daily') }}
    GROUP BY 
        city_name, 
        country_name, 
        country_code, 
        region, 
        population, 
        city_category
),

best_worst_dates AS (
    SELECT
        city_name,
        MAX(CASE WHEN rn_best = 1 THEN observation_date END) AS best_day_date,
        MAX(CASE WHEN rn_worst = 1 THEN observation_date END) AS worst_day_date
    FROM (
        SELECT
            city_name,
            observation_date,
            tourism_comfort_score,
            ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY tourism_comfort_score DESC) AS rn_best,
            ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY tourism_comfort_score ASC) AS rn_worst
        FROM {{ ref('fct_tourism_daily') }}
    ) ranked
    WHERE rn_best = 1 OR rn_worst = 1
    GROUP BY city_name
),

final_summary AS (
    SELECT
        cs.*,
        bw.best_day_date,
        bw.worst_day_date,

-- Classement de la ville
ROW_NUMBER() OVER ( ORDER BY cs.avg_comfort_score DESC ) AS city_rank,

-- Pourcentage de bons jours
ROUND(100.0 * (cs.days_ideal + cs.days_pleasant) / NULLIF(cs.total_days, 0)::NUMERIC, 1) AS good_days_percentage,

-- Recommandation globale
CASE
    WHEN cs.avg_comfort_score >= 80 THEN 'Excellente destination'
    WHEN cs.avg_comfort_score >= 70 THEN 'Très bonne destination'
    WHEN cs.avg_comfort_score >= 60 THEN 'Bonne destination'
    WHEN cs.avg_comfort_score >= 50 THEN 'Destination correcte'
    ELSE 'Destination peu recommandée'
END AS overall_recommendation,

-- Meilleure saison recommandée (basé sur les données actuelles)
CASE
    WHEN cs.avg_temperature > 15
    AND cs.avg_rain_score > 70 THEN 'Toute l''année'
    WHEN cs.avg_temperature > 20 THEN 'Printemps/Été'
    WHEN cs.avg_temperature < 10 THEN 'Automne/Hiver (prévoir habits chauds)'
    ELSE 'Printemps/Automne'
END AS best_season_recommendation,

-- Points forts
CASE
    WHEN cs.avg_temperature BETWEEN 15 AND 25  THEN 'Température idéale'
    WHEN cs.avg_rain_score > 80 THEN 'Peu de pluie'
    WHEN cs.avg_wind_score > 80 THEN 'Vent faible'
    ELSE 'Climat variable'
END AS main_strength,

-- Points faibles
CASE
    WHEN cs.days_with_rain > cs.total_days * 0.5 THEN 'Pluie fréquente'
    WHEN cs.avg_temperature < 10 THEN 'Températures fraîches'
    WHEN cs.avg_temperature > 25 THEN 'Températures élevées'
    WHEN cs.avg_wind_speed > 25 THEN 'Vent fort'
    ELSE 'Aucun point faible majeur'
END AS main_weakness,

-- Métadonnées
CURRENT_TIMESTAMP AS created_at
        
    FROM city_stats cs
    LEFT JOIN best_worst_dates bw ON cs.city_name = bw.city_name
)

SELECT * FROM final_summary ORDER BY city_rank