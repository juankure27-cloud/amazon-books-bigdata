"""Genera grafana/dashboard_amazon_books.json (importable en Grafana / Grafana Cloud)
y sql/consultas_grafana.sql con las mismas consultas, para documentarlas.

    python grafana/build_dashboard.py
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
DS = {"type": "grafana-postgresql-datasource", "uid": "${DS_POSTGRES}"}
panels, queries, _id = [], [], [0]


def target(sql, fmt="table"):
    return [{"refId": "A", "datasource": DS, "editorMode": "code", "rawQuery": True,
             "format": fmt, "rawSql": sql}]


def add(kind, title, sql, x, y, w, h, fmt="table", desc="", **extra):
    _id[0] += 1
    queries.append(f"-- {title}\n{sql};\n")
    p = {"id": _id[0], "type": kind, "title": title, "description": desc, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": target(sql, fmt)}
    p.update(extra)
    panels.append(p)


def row(title, y):
    _id[0] += 1
    panels.append({"id": _id[0], "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})


def stat(title, col, x, y, unit="short", decimals=0, color="blue", thresholds=None):
    steps = thresholds or [{"color": color, "value": None}]
    add("stat", title, f"SELECT {col} FROM kpis", x, y, 4, 4,
        fieldConfig={"defaults": {"unit": unit, "decimals": decimals,
                                  "color": {"mode": "thresholds"},
                                  "thresholds": {"mode": "absolute", "steps": steps}},
                     "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                 "colorMode": "background", "graphMode": "none", "textMode": "value_and_name",
                 "justifyMode": "center"})


def bars(title, sql, x, y, w, h, xfield, horizontal=False, unit="short", desc="", color="#5794F2"):
    add("barchart", title, sql, x, y, w, h, desc=desc,
        fieldConfig={"defaults": {"unit": unit, "color": {"mode": "fixed", "fixedColor": color},
                                  "custom": {"fillOpacity": 85, "lineWidth": 0}},
                     "overrides": []},
        options={"orientation": "horizontal" if horizontal else "vertical", "xField": xfield,
                 "showValue": "auto", "barWidth": 0.8, "legend": {"showLegend": False},
                 "tooltip": {"mode": "single"}, "xTickLabelRotation": 0 if horizontal else -30})


def table(title, sql, x, y, w, h, desc="", overrides=None):
    add("table", title, sql, x, y, w, h, desc=desc,
        fieldConfig={"defaults": {"custom": {"align": "auto"}}, "overrides": overrides or []},
        options={"showHeader": True, "cellHeight": "sm"})


def gauge_cell(field, red_high=True, unit="percent"):
    steps = ([{"color": "green", "value": None}, {"color": "orange", "value": 10},
              {"color": "red", "value": 15}] if red_high else
             [{"color": "red", "value": None}, {"color": "orange", "value": 3.8},
              {"color": "green", "value": 4.2}])
    return {"matcher": {"id": "byName", "options": field},
            "properties": [{"id": "custom.cellOptions", "value": {"type": "color-background"}},
                           {"id": "thresholds", "value": {"mode": "absolute", "steps": steps}},
                           {"id": "unit", "value": unit}]}


# ------------------------------------------------------------------ KPIs
row("Resumen ejecutivo", 0)
stat("Reseñas analizadas", "total_reviews", 0, 1)
stat("Títulos", "n_titles", 4, 1, color="purple")
stat("Usuarios", "n_users", 8, 1, color="purple")
stat("Puntaje promedio (1-5)", "avg_score", 12, 1, decimals=2, thresholds=[
    {"color": "red", "value": None}, {"color": "orange", "value": 3.8},
    {"color": "green", "value": 4.2}])
stat("% reseñas negativas (≤2★)", "pct_negative", 16, 1, unit="percent", decimals=1,
     thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 10},
                 {"color": "red", "value": 15}])
stat("% reseñas de 5★", "pct_five_stars", 20, 1, unit="percent", decimals=1, color="green")

# ------------------------------------------------------------------ Tendencia
row("Tendencia en el tiempo", 5)
add("timeseries", "Volumen de reseñas por año",
    "SELECT make_date(review_year::int, 1, 1) AS time, n_reviews AS \"Reseñas\" "
    "FROM by_year ORDER BY 1", 0, 6, 12, 8, fmt="time_series",
    fieldConfig={"defaults": {"custom": {"drawStyle": "bars", "fillOpacity": 80}},
                 "overrides": []})
add("timeseries", "Puntaje promedio y % negativas por año",
    "SELECT make_date(review_year::int, 1, 1) AS time, avg_score AS \"Puntaje promedio\", "
    "pct_negative AS \"% negativas\" FROM by_year ORDER BY 1", 12, 6, 12, 8, fmt="time_series",
    fieldConfig={"defaults": {"custom": {"drawStyle": "line", "lineWidth": 2, "pointSize": 5,
                                         "showPoints": "always"}},
                 "overrides": [{"matcher": {"id": "byName", "options": "% negativas"},
                                "properties": [{"id": "custom.axisPlacement", "value": "right"},
                                               {"id": "unit", "value": "percent"},
                                               {"id": "color", "value": {"mode": "fixed",
                                                "fixedColor": "red"}}]}]})
add("timeseries", "% de reseñas negativas por año – principales categorías",
    "SELECT make_date(review_year::int, 1, 1) AS time, main_category AS metric, "
    "pct_negative AS value FROM by_year_category WHERE n_reviews >= 100 ORDER BY 1",
    0, 14, 24, 8, fmt="time_series",
    fieldConfig={"defaults": {"unit": "percent", "custom": {"lineWidth": 2}}, "overrides": []})

# ------------------------------------------------------------------ Catálogo
row("Catálogo: categorías y editoriales", 22)
bars("% de reseñas negativas por categoría (15 con más reseñas)",
     "SELECT main_category AS \"Categoría\", pct_negative AS \"% negativas\" FROM "
     "(SELECT * FROM by_category WHERE main_category <> 'Sin categoría' "
     "ORDER BY n_reviews DESC LIMIT 15) t ORDER BY 2 DESC",
     0, 23, 12, 10, "Categoría", horizontal=True, unit="percent", color="#F2495C",
     desc="Categorías donde los lectores quedan más insatisfechos")
table("Editoriales con más reseñas",
      "SELECT publisher AS \"Editorial\", n_reviews AS \"Reseñas\", avg_score AS \"Puntaje\", "
      "pct_negative AS \"% negativas\" FROM by_publisher ORDER BY n_reviews DESC LIMIT 20",
      12, 23, 12, 10, overrides=[gauge_cell("% negativas"),
                                 gauge_cell("Puntaje", red_high=False, unit="none")])
table("Libros más reseñados",
      "SELECT title AS \"Título\", first_author AS \"Autor\", main_category AS \"Categoría\", "
      "n_reviews AS \"Reseñas\", avg_score AS \"Puntaje\", pct_negative AS \"% negativas\" "
      "FROM top_books ORDER BY n_reviews DESC LIMIT 15",
      0, 33, 24, 9, overrides=[gauge_cell("% negativas"),
                               gauge_cell("Puntaje", red_high=False, unit="none")])

# ------------------------------------------------------------------ Drivers
row("¿Qué se asocia con la insatisfacción?", 42)
bars("Distribución de puntajes", "SELECT score::int::text || '★' AS \"Puntaje\", pct AS \"%\" "
     "FROM score_distribution ORDER BY score", 0, 43, 6, 8, "Puntaje", unit="percent")
bars("% negativas según longitud de la reseña (palabras)",
     "SELECT text_len_bucket AS \"Palabras\", pct_negative AS \"% negativas\" FROM by_text_length "
     "ORDER BY orden", 6, 43, 6, 8, "Palabras", unit="percent", color="#FF9830")
bars("% negativas según actividad del usuario",
     "SELECT user_activity AS \"Reseñas del usuario\", pct_negative AS \"% negativas\" "
     "FROM by_user_activity ORDER BY orden", 12, 43, 6, 8, "Reseñas del usuario",
     unit="percent", color="#B877D9")
bars("% negativas según antigüedad del libro",
     "SELECT book_age_bucket AS \"Antigüedad\", pct_negative AS \"% negativas\" FROM by_book_age "
     "ORDER BY orden", 18, 43, 6, 8, "Antigüedad", unit="percent", color="#73BF69")
bars("Sentimiento del resumen vs. % negativas",
     "SELECT sentiment_bucket AS \"Sentimiento\", pct_negative AS \"% negativas\" "
     "FROM by_sentiment ORDER BY orden", 0, 51, 12, 8, "Sentimiento", unit="percent",
     color="#F2495C", desc="Sentimiento VADER del título de la reseña")
bars("Utilidad percibida (votos 'útil') por puntaje",
     "SELECT score::int::text || '★' AS \"Puntaje\", avg_helpful_ratio AS \"Proporción útil\" "
     "FROM score_distribution ORDER BY score", 12, 51, 12, 8, "Puntaje", unit="percentunit")

# ------------------------------------------------------------------ Modelo y operación
row("Modelo predictivo y operación del pipeline", 59)
bars("Variables más importantes para anticipar una reseña negativa",
     "SELECT feature AS \"Variable\", importance AS \"Importancia\" FROM model_feature_importance "
     "ORDER BY importance DESC LIMIT 10", 0, 60, 12, 9, "Variable", horizontal=True,
     color="#8F3BB8")
table("Métricas del modelo base", "SELECT metric AS \"Métrica\", value AS \"Valor\" "
      "FROM model_metrics", 12, 60, 6, 9)
table("Calidad de datos (% nulos)",
      "SELECT table_name AS \"Tabla\", column_name AS \"Columna\", pct_nulls AS \"% nulos\" "
      "FROM data_quality WHERE pct_nulls > 0 ORDER BY pct_nulls DESC",
      18, 60, 6, 9, overrides=[gauge_cell("% nulos")])
bars("Dask: tiempo total por configuración (s)",
     "SELECT profile || ' (' || n_workers || 'w x ' || threads_per_worker || 'h, ' || "
     "memory_limit || ')' AS \"Perfil\", seconds_total AS \"Segundos\" FROM dask_benchmark "
     "ORDER BY run_at DESC, profile LIMIT 3", 0, 69, 12, 8, "Perfil", unit="s", color="#FADE2A")
table("Historial de ejecuciones del ETL (Prefect)",
      "SELECT run_at AS \"Fecha\", dask_profile AS \"Perfil\", files_converted AS \"Archivos nuevos\", "
      "rows_total AS \"Reseñas\", seconds_silver AS \"Silver (s)\", seconds_gold AS \"Gold (s)\", "
      "status AS \"Estado\" FROM etl_runs ORDER BY run_at DESC LIMIT 10", 12, 69, 12, 8)

dashboard = {
    "__inputs": [{"name": "DS_POSTGRES", "label": "PostgreSQL (AWS RDS)", "type": "datasource",
                  "pluginId": "grafana-postgresql-datasource", "pluginName": "PostgreSQL"}],
    "__requires": [{"type": "datasource", "id": "grafana-postgresql-datasource",
                    "name": "PostgreSQL", "version": "1.0.0"}],
    "title": "Amazon Books Reviews – Tablero gerencial",
    "uid": "amazon-books-gerencial",
    "description": "Hallazgos del análisis de ~3 M de reseñas de libros (Proyecto Big Data)",
    "tags": ["big-data", "amazon-books", "crisp-dm"],
    "timezone": "browser", "editable": True, "graphTooltip": 1, "refresh": "",
    "time": {"from": "1995-01-01T00:00:00Z", "to": "2014-01-01T00:00:00Z"},
    "schemaVersion": 39, "version": 1, "panels": panels,
}
(HERE / "dashboard_amazon_books.json").write_text(
    json.dumps(dashboard, indent=2, ensure_ascii=False), encoding="utf-8")
(HERE.parent / "sql" / "consultas_grafana.sql").write_text(
    "-- Consultas SQL usadas por cada panel del tablero de Grafana (PostgreSQL)\n\n"
    + "\n".join(queries), encoding="utf-8")
print(f"OK: {len([p for p in panels if p['type'] != 'row'])} paneles")
