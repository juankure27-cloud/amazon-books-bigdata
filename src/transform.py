"""Paso 2 (capa SILVER): limpieza, ingeniería de características y cruce con libros.

Diseño: toda la lógica fila a fila son funciones puras de pandas
(`add_review_features`, `clean_books`, `add_derived`...). Dask las aplica en paralelo
partición por partición con `map_partitions`. Esto permite probarlas con pytest sobre
un DataFrame pequeño y escalarlas sin cambios a los ~3 millones de reseñas.
"""
from __future__ import annotations

import shutil

import numpy as np
import pandas as pd

TEXT_BUCKETS = ["<50", "50-149", "150-299", "300-599", "600+"]
USER_BUCKETS = ["1 reseña", "2-5", "6-20", "21-100", "100+"]
SENT_BUCKETS = ["Muy negativo", "Negativo", "Neutral", "Positivo", "Muy positivo"]
AGE_BUCKETS = ["<0 (preventa/reedición)", "0-1 años", "2-5 años", "6-15 años", "16+ años"]

# Columnas que se leen de BRONZE (se excluye 'text', la más pesada: ya tenemos su longitud)
BRONZE_REVIEW_COLS = ["book_id", "title", "price", "user_id", "helpfulness", "score",
                      "review_time", "summary", "text_len", "text_words"]


def normalize_title(s: pd.Series) -> pd.Series:
    return (s.astype("string").str.lower().str.strip()
             .str.replace(r"\s+", " ", regex=True))


def _bucket(values: pd.Series, edges: list[float], labels: list[str]) -> pd.Series:
    v = values.astype("float64").to_numpy()
    conds = [v < e for e in edges]
    out = np.select(conds, labels[:-1], default=labels[-1])
    out = pd.Series(out, index=values.index, dtype="string")
    return out.mask(values.isna())


# --------------------------------------------------------------------------- #
# Reseñas
# --------------------------------------------------------------------------- #
def add_review_features(pdf: pd.DataFrame, negative_threshold: int = 2) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf = pdf[pdf["score"].notna()]

    votes = pdf["helpfulness"].astype("string").str.extract(r"(\d+)\s*/\s*(\d+)")
    pdf["votes_helpful"] = pd.to_numeric(votes[0], errors="coerce").fillna(0).astype("int32")
    pdf["votes_total"] = pd.to_numeric(votes[1], errors="coerce").fillna(0).astype("int32")
    ratio = pdf["votes_helpful"] / pdf["votes_total"].where(pdf["votes_total"] > 0)
    pdf["helpful_ratio"] = ratio.astype("float32")

    ts = pd.to_datetime(pdf["review_time"].astype("float64"), unit="s", errors="coerce")
    pdf["review_date"] = ts
    pdf["review_year"] = ts.dt.year.astype("Int16")

    # VARIABLE OBJETIVO
    pdf["is_negative"] = (pdf["score"] <= negative_threshold).astype("int8")
    pdf["is_positive"] = (pdf["score"] >= 4).astype("int8")

    summ = pdf["summary"].astype("string").fillna("")
    pdf["summary_len"] = summ.str.len().astype("int32")
    pdf["summary_exclam"] = summ.str.count("!").astype("int16")
    pdf["summary_upper_ratio"] = (
        summ.str.count(r"[A-Z]") / summ.str.len().where(summ.str.len() > 0)).astype("float32")

    pdf["text_len_bucket"] = _bucket(pdf["text_words"], [50, 150, 300, 600], TEXT_BUCKETS)
    pdf["is_anonymous"] = pdf["user_id"].isna().astype("int8")
    pdf["title_norm"] = normalize_title(pdf["title"])
    return pdf.drop(columns=["helpfulness"])


_VADER = None


def add_sentiment(pdf: pd.DataFrame) -> pd.DataFrame:
    """Sentimiento VADER (compound, -1 a 1) del resumen de la reseña.

    Es una tarea intensiva en CPU y atada al GIL de Python: justo el caso donde
    varios *procesos* de Dask rinden más que varios *hilos* (ver benchmark).
    """
    global _VADER
    if _VADER is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _VADER = SentimentIntensityAnalyzer()
    pdf = pdf.copy()
    summ = pdf["summary"].astype("string").fillna("")
    pdf["sentiment"] = np.array(
        [_VADER.polarity_scores(s)["compound"] if s else np.nan for s in summ],
        dtype="float32")
    pdf["sentiment_bucket"] = _bucket(pdf["sentiment"], [-0.5, -0.05, 0.05, 0.5],
                                      SENT_BUCKETS)
    return pdf


# --------------------------------------------------------------------------- #
# Libros
# --------------------------------------------------------------------------- #
def clean_books(pdf: pd.DataFrame) -> pd.DataFrame:
    b = pd.DataFrame({"title_norm": normalize_title(pdf["title"])})
    first_item = r"\[\s*['\"](.+?)['\"]"
    b["main_category"] = (pdf["categories"].astype("string").str.extract(first_item)[0]
                          .str.strip())
    b["first_author"] = pdf["authors"].astype("string").str.extract(first_item)[0].str.strip()
    b["n_authors"] = pdf["authors"].astype("string").str.count(r"['\"],\s*['\"]").add(1) \
        .where(pdf["authors"].notna()).astype("Int16")
    b["publisher"] = (pdf["publisher"].astype("string").str.strip()
                      .str.replace(r'^"|"$', "", regex=True))
    b["published_year"] = pd.to_numeric(
        pdf["published_date"].astype("string").str.extract(r"(\d{4})")[0],
        errors="coerce").astype("Int16")
    b["ratings_count"] = pd.to_numeric(pdf["ratings_count"], errors="coerce").astype("float32")
    b["has_description"] = pdf["description"].notna().astype("int8")
    b["desc_len"] = pdf["description"].astype("string").str.len().fillna(0).astype("int32")
    b["has_metadata"] = np.int8(1)
    # títulos duplicados: se conserva la fila con más información
    b["_info"] = b.notna().sum(axis=1)
    b = (b.dropna(subset=["title_norm"])
          .sort_values("_info", ascending=False)
          .drop_duplicates("title_norm")
          .drop(columns="_info")
          .reset_index(drop=True))
    return b


# --------------------------------------------------------------------------- #
# Variables derivadas del cruce
# --------------------------------------------------------------------------- #
def add_derived(pdf: pd.DataFrame, user_counts: pd.Series, book_counts: pd.Series) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf["user_n_reviews"] = pdf["user_id"].map(user_counts).astype("float32")
    pdf["book_n_reviews"] = pdf["title_norm"].map(book_counts).fillna(1).astype("int32")
    pdf["user_activity"] = _bucket(pdf["user_n_reviews"], [2, 6, 21, 101], USER_BUCKETS)
    pdf["user_activity"] = pdf["user_activity"].fillna("Anónimo")
    age = pdf["review_year"].astype("float64") - pdf["published_year"].astype("float64")
    pdf["book_age_years"] = age.astype("float32")
    pdf["book_age_bucket"] = _bucket(age, [0, 2, 6, 16], AGE_BUCKETS)
    pdf["has_metadata"] = pdf["has_metadata"].fillna(0).astype("int8")
    pdf["main_category"] = pdf["main_category"].fillna("Sin categoría")
    pdf["publisher"] = pdf["publisher"].fillna("Sin editorial")
    return pdf


# --------------------------------------------------------------------------- #
# Orquestación con Dask
# --------------------------------------------------------------------------- #
def build_silver(cfg: dict, sentiment: bool | None = None, write: bool = True) -> dict:
    """Lee BRONZE con Dask, transforma y escribe SILVER. Requiere un Client activo
    (ver utils.dask_cluster) o usa el scheduler local por defecto."""
    import dask.dataframe as dd

    bronze, silver = cfg["paths"]["bronze_dir"], cfg["paths"]["silver_dir"]
    thr = cfg["analysis"]["negative_threshold"]
    sentiment = cfg["analysis"]["sentiment"] if sentiment is None else sentiment

    reviews = dd.read_parquet(bronze / "reviews", columns=BRONZE_REVIEW_COLS)
    rows_raw = int(reviews.shape[0].compute())
    reviews = reviews.map_partitions(add_review_features, thr)

    # Duplicados: misma persona, mismo libro, mismo momento y mismo resumen
    # (en Amazon una reseña se replica entre ediciones del mismo título)
    reviews = reviews.drop_duplicates(
        subset=["user_id", "title_norm", "review_time", "score", "summary"],
        split_out=max(1, reviews.npartitions // 4))
    if sentiment:
        reviews = reviews.map_partitions(add_sentiment)

    books = clean_books(pd.read_parquet(bronze / "books"))
    reviews = reviews.merge(books, on="title_norm", how="left")   # broadcast (books es pequeño)

    user_counts = reviews.groupby("user_id")["score"].count().compute()
    book_counts = reviews.groupby("title_norm")["score"].count().compute()
    rows_clean = int(book_counts.sum())

    reviews = reviews.map_partitions(add_derived, user_counts, book_counts)
    reviews = reviews.drop(columns=["summary"])

    if write:
        if silver.exists():
            shutil.rmtree(silver)
        reviews.repartition(partition_size="128MB").to_parquet(
            silver / "reviews_enriched", write_index=False)
        (silver / "books").mkdir(parents=True, exist_ok=True)
        books.to_parquet(silver / "books" / "books_clean.parquet", index=False)

    return {
        "rows_raw": rows_raw,
        "rows_clean": rows_clean,
        "rows_removed": rows_raw - rows_clean,   # duplicados + filas sin puntaje
        "n_users": int(len(user_counts)),
        "n_titles": int(len(book_counts)),
        "books_with_metadata": int(len(books)),
        "sentiment": bool(sentiment),
    }
