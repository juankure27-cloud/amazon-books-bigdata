"""Paso 3 (capa GOLD): tablas analíticas para la base de datos en AWS y Grafana.

Funcionan igual con un DataFrame de pandas o de Dask: se construye el cálculo y al
final se llama `compute`. Solo estas tablas (pequeñas) se suben a AWS.
"""
from __future__ import annotations

import pandas as pd

from src.transform import AGE_BUCKETS, SENT_BUCKETS, TEXT_BUCKETS, USER_BUCKETS
from src.utils import compute

METRICS = {
    "score": ["count", "mean"],
    "is_negative": "mean",
    "is_positive": "mean",
    "helpful_ratio": "mean",
    "text_words": "mean",
}
NAMES = {
    ("score", "count"): "n_reviews", ("score", "mean"): "avg_score",
    ("is_negative", "mean"): "pct_negative", ("is_positive", "mean"): "pct_positive",
    ("helpful_ratio", "mean"): "avg_helpful_ratio", ("text_words", "mean"): "avg_words",
    ("sentiment", "mean"): "avg_sentiment",
}


def _group(df, keys) -> pd.DataFrame:
    metrics = dict(METRICS)
    if "sentiment" in df.columns:
        metrics["sentiment"] = "mean"
    g = compute(df.groupby(keys, dropna=True).agg(metrics))
    g.columns = [NAMES[c] for c in g.columns.to_flat_index()]
    g = g.reset_index()
    for c in ("pct_negative", "pct_positive"):
        g[c] = (g[c] * 100).round(2)
    for c in ("avg_score", "avg_helpful_ratio", "avg_words", "avg_sentiment"):
        if c in g:
            g[c] = g[c].astype("float64").round(3)
    g["n_reviews"] = g["n_reviews"].astype("int64")
    return g


def _ordered(g: pd.DataFrame, col: str, order: list[str]) -> pd.DataFrame:
    g["orden"] = g[col].map({v: i for i, v in enumerate(order)})
    return g.sort_values("orden").reset_index(drop=True)


def build_gold(df, cfg: dict) -> dict[str, pd.DataFrame]:
    a = cfg["analysis"]
    t: dict[str, pd.DataFrame] = {}

    # 1. KPIs gerenciales (una fila)
    n = int(compute(df["score"].count()))
    k = {
        "total_reviews": n,
        "n_titles": int(compute(df["title_norm"].nunique())),
        "n_users": int(compute(df["user_id"].nunique())),
        "avg_score": round(float(compute(df["score"].mean())), 3),
        "pct_negative": round(float(compute(df["is_negative"].mean())) * 100, 2),
        "pct_positive": round(float(compute(df["is_positive"].mean())) * 100, 2),
        "pct_five_stars": round(float(compute((df["score"] == 5).mean())) * 100, 2),
        "pct_with_book_metadata": round(float(compute(df["has_metadata"].mean())) * 100, 2),
        "pct_anonymous": round(float(compute(df["is_anonymous"].mean())) * 100, 2),
        "first_year": int(compute(df["review_year"].min())),
        "last_year": int(compute(df["review_year"].max())),
        "avg_words": round(float(compute(df["text_words"].mean())), 1),
    }
    t["kpis"] = pd.DataFrame([k])

    # 2. Distribución de puntajes
    sd = compute(df.groupby("score")["score"].count()).rename("n_reviews").reset_index()
    sd["pct"] = (sd["n_reviews"] / sd["n_reviews"].sum() * 100).round(2)
    help_by_score = _group(df, "score")[["score", "avg_helpful_ratio", "avg_words"]]
    t["score_distribution"] = sd.merge(help_by_score, on="score").sort_values("score")

    # 3. Evolución anual
    y = _group(df, "review_year")
    t["by_year"] = y[y["review_year"] >= 1995].sort_values("review_year").reset_index(drop=True)

    # 4. Categorías
    c = _group(df, "main_category")
    titles = compute(df.groupby("main_category")["title_norm"].nunique()).rename("n_titles")
    c = c.merge(titles.reset_index(), on="main_category")
    c = c[c["n_reviews"] >= a["min_reviews_category"]]
    t["by_category"] = c.sort_values("n_reviews", ascending=False).reset_index(drop=True)

    # 5. Categoría x año (series de tiempo para las 8 categorías más grandes)
    top_cats = (t["by_category"].query("main_category != 'Sin categoría'")
                .head(8)["main_category"].tolist())
    yc = _group(df[df["main_category"].isin(top_cats)], ["review_year", "main_category"])
    t["by_year_category"] = yc[yc["review_year"] >= 1995].sort_values(
        ["review_year", "main_category"]).reset_index(drop=True)

    # 6. Editoriales
    p = _group(df, "publisher")
    p = p[(p["n_reviews"] >= a["min_reviews_publisher"]) & (p["publisher"] != "Sin editorial")]
    t["by_publisher"] = p.sort_values("n_reviews", ascending=False).reset_index(drop=True)

    # 7. Segmentos (buckets) -> ¿qué se asocia con la insatisfacción?
    t["by_text_length"] = _ordered(_group(df, "text_len_bucket"), "text_len_bucket", TEXT_BUCKETS)
    t["by_user_activity"] = _ordered(_group(df, "user_activity"), "user_activity",
                                     USER_BUCKETS + ["Anónimo"])
    t["by_book_age"] = _ordered(_group(df, "book_age_bucket"), "book_age_bucket", AGE_BUCKETS)
    if "sentiment_bucket" in df.columns:
        t["by_sentiment"] = _ordered(_group(df, "sentiment_bucket"), "sentiment_bucket",
                                     SENT_BUCKETS)

    # 8. Libros más reseñados
    b = _group(df, "title_norm").sort_values("n_reviews", ascending=False).head(a["top_books"])
    meta = compute(df[df["title_norm"].isin(b["title_norm"].tolist())]
                   [["title_norm", "title", "main_category", "first_author", "publisher",
                     "published_year"]].drop_duplicates(subset=["title_norm"]))
    t["top_books"] = b.merge(meta, on="title_norm", how="left").reset_index(drop=True)

    return t
