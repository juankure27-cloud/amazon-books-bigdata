"""Modelo base (fase de Modelado de CRISP-DM): ¿se puede anticipar una reseña negativa?

Se entrena un HistGradientBoostingClassifier sobre una muestra de SILVER. No se usan
variables derivadas del puntaje (eso sería fuga de información / data leakage).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import (average_precision_score, f1_score, precision_score,
                             recall_score, roc_auc_score)
from sklearn.model_selection import train_test_split

NUMERIC = ["text_words", "summary_len", "summary_exclam", "summary_upper_ratio",
           "votes_total", "helpful_ratio", "review_year", "user_n_reviews",
           "book_n_reviews", "published_year", "book_age_years", "ratings_count",
           "has_description", "desc_len", "n_authors", "is_anonymous", "has_metadata",
           "sentiment"]
CATEGORICAL = ["main_category"]
TARGET = "is_negative"


def prepare(pdf: pd.DataFrame, top_categories: int = 25) -> tuple[pd.DataFrame, pd.Series]:
    num = [c for c in NUMERIC if c in pdf.columns]
    X = pdf[num].astype("float64")
    cat = pdf["main_category"].astype("string").fillna("Sin categoría")
    top = cat.value_counts().head(top_categories).index
    X = X.assign(main_category=pd.Categorical(cat.where(cat.isin(top), "Otra")))
    return X, pdf[TARGET].astype(int)


def train_baseline(pdf: pd.DataFrame, seed: int = 42) -> dict[str, pd.DataFrame]:
    X, y = prepare(pdf)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, stratify=y, random_state=seed)
    model = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.08, class_weight="balanced",
        categorical_features="from_dtype", random_state=seed)
    model.fit(Xtr, ytr)
    proba = model.predict_proba(Xte)[:, 1]
    pred = (proba >= 0.5).astype(int)

    metrics = pd.DataFrame([
        {"metric": "ROC AUC", "value": roc_auc_score(yte, proba)},
        {"metric": "PR AUC (precisión promedio)", "value": average_precision_score(yte, proba)},
        {"metric": "F1 (clase negativa)", "value": f1_score(yte, pred)},
        {"metric": "Precisión (clase negativa)", "value": precision_score(yte, pred)},
        {"metric": "Recall (clase negativa)", "value": recall_score(yte, pred)},
        {"metric": "Tasa base de negativas (%)", "value": float(y.mean() * 100)},
        {"metric": "Filas de entrenamiento", "value": float(len(Xtr))},
        {"metric": "Filas de prueba", "value": float(len(Xte))},
    ])
    metrics["value"] = metrics["value"].round(4)

    n = min(20_000, len(Xte))
    idx = np.random.default_rng(seed).choice(len(Xte), n, replace=False)
    imp = permutation_importance(model, Xte.iloc[idx], yte.iloc[idx], scoring="roc_auc",
                                 n_repeats=3, random_state=seed)
    importance = (pd.DataFrame({"feature": X.columns, "importance": imp.importances_mean,
                                "std": imp.importances_std})
                  .sort_values("importance", ascending=False).reset_index(drop=True))
    importance[["importance", "std"]] = importance[["importance", "std"]].round(5)
    return {"model_metrics": metrics, "model_feature_importance": importance}


def sample_silver(cfg: dict) -> pd.DataFrame:
    import dask.dataframe as dd
    path = cfg["paths"]["silver_dir"] / "reviews_enriched"
    cols = list(dict.fromkeys(NUMERIC + CATEGORICAL + [TARGET]))
    df = dd.read_parquet(path)
    df = df[[c for c in cols if c in df.columns]]
    n_total = int(df.shape[0].compute())
    frac = min(1.0, cfg["analysis"]["model_sample_rows"] / max(n_total, 1))
    return df.sample(frac=frac, random_state=42).compute()
