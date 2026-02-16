-- 1️⃣ Voir quelques films enrichis
SELECT
    snapshot_date,
    tmdb_id,
    title,
    tmdb_rating,
    imdb_rating,
    popularity,
    composite_score,
    is_overhyped,
    is_hidden_gem,
    missing_omdb_data
FROM analytics_marts.movies_enriched_daily
ORDER BY composite_score DESC
LIMIT 5;

-- 1️⃣ Voir quelques films enrichis
SELECT
    snapshot_date,
    tmdb_id,
    title,
    tmdb_rating,
    imdb_rating,
    popularity,
    composite_score,
    is_overhyped,
    is_hidden_gem,
    missing_omdb_data
FROM analytics_marts.movies_enriched_daily
ORDER BY composite_score DESC
LIMIT 5;

-- 2️⃣ Voir le KPI daily summary
SELECT * FROM analytics_marts.kpi_daily_summary;

-- 3️⃣ Films overhypés (popularité haute, rating bas)
SELECT
    title,
    popularity,
    tmdb_rating,
    imdb_rating,
    composite_score
FROM analytics_marts.movies_enriched_daily
WHERE
    is_overhyped = true
ORDER BY popularity DESC;