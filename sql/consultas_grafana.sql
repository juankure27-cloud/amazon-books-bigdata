-- Consultas SQL usadas por cada panel del tablero de Grafana (PostgreSQL)

-- Reseñas analizadas
SELECT total_reviews FROM kpis;

-- Títulos
SELECT n_titles FROM kpis;

-- Usuarios
SELECT n_users FROM kpis;

-- Puntaje promedio (1-5)
SELECT avg_score FROM kpis;

-- % reseñas negativas (≤2★)
SELECT pct_negative FROM kpis;

-- % reseñas de 5★
SELECT pct_five_stars FROM kpis;

-- Volumen de reseñas por año
SELECT make_date(review_year::int, 1, 1) AS time, n_reviews AS "Reseñas" FROM by_year ORDER BY 1;

-- Puntaje promedio y % negativas por año
SELECT make_date(review_year::int, 1, 1) AS time, avg_score AS "Puntaje promedio", pct_negative AS "% negativas" FROM by_year ORDER BY 1;

-- % de reseñas negativas por año – principales categorías
SELECT make_date(review_year::int, 1, 1) AS time, main_category AS metric, pct_negative AS value FROM by_year_category WHERE n_reviews >= 100 ORDER BY 1;

-- % de reseñas negativas por categoría (15 con más reseñas)
SELECT main_category AS "Categoría", pct_negative AS "% negativas" FROM (SELECT * FROM by_category WHERE main_category <> 'Sin categoría' ORDER BY n_reviews DESC LIMIT 15) t ORDER BY 2 DESC;

-- Editoriales con más reseñas
SELECT publisher AS "Editorial", n_reviews AS "Reseñas", avg_score AS "Puntaje", pct_negative AS "% negativas" FROM by_publisher ORDER BY n_reviews DESC LIMIT 20;

-- Libros más reseñados
SELECT title AS "Título", first_author AS "Autor", main_category AS "Categoría", n_reviews AS "Reseñas", avg_score AS "Puntaje", pct_negative AS "% negativas" FROM top_books ORDER BY n_reviews DESC LIMIT 15;

-- Distribución de puntajes
SELECT score::int::text || '★' AS "Puntaje", pct AS "%" FROM score_distribution ORDER BY score;

-- % negativas según longitud de la reseña (palabras)
SELECT text_len_bucket AS "Palabras", pct_negative AS "% negativas" FROM by_text_length ORDER BY orden;

-- % negativas según actividad del usuario
SELECT user_activity AS "Reseñas del usuario", pct_negative AS "% negativas" FROM by_user_activity ORDER BY orden;

-- % negativas según antigüedad del libro
SELECT book_age_bucket AS "Antigüedad", pct_negative AS "% negativas" FROM by_book_age ORDER BY orden;

-- Sentimiento del resumen vs. % negativas
SELECT sentiment_bucket AS "Sentimiento", pct_negative AS "% negativas" FROM by_sentiment ORDER BY orden;

-- Utilidad percibida (votos 'útil') por puntaje
SELECT score::int::text || '★' AS "Puntaje", avg_helpful_ratio AS "Proporción útil" FROM score_distribution ORDER BY score;

-- Variables más importantes para anticipar una reseña negativa
SELECT feature AS "Variable", importance AS "Importancia" FROM model_feature_importance ORDER BY importance DESC LIMIT 10;

-- Métricas del modelo base
SELECT metric AS "Métrica", value AS "Valor" FROM model_metrics;

-- Calidad de datos (% nulos)
SELECT table_name AS "Tabla", column_name AS "Columna", pct_nulls AS "% nulos" FROM data_quality WHERE pct_nulls > 0 ORDER BY pct_nulls DESC;

-- Dask: tiempo total por configuración (s)
SELECT profile || ' (' || n_workers || 'w x ' || threads_per_worker || 'h, ' || memory_limit || ')' AS "Perfil", seconds_total AS "Segundos" FROM dask_benchmark ORDER BY run_at DESC, profile LIMIT 3;

-- Historial de ejecuciones del ETL (Prefect)
SELECT run_at AS "Fecha", dask_profile AS "Perfil", files_converted AS "Archivos nuevos", rows_total AS "Reseñas", seconds_silver AS "Silver (s)", seconds_gold AS "Gold (s)", status AS "Estado" FROM etl_runs ORDER BY run_at DESC LIMIT 10;
