"""Perfil de calidad de datos de la capa BRONZE (evidencia del análisis preliminar)."""
from __future__ import annotations

import pandas as pd

from src.utils import compute


def profile(df, table_name: str) -> pd.DataFrame:
    """Nulos, tipo y rango de cada columna. Funciona con pandas o Dask."""
    n = int(compute(df.shape[0]))
    nulls = compute(df.isna().sum())
    rows = []
    for col in df.columns:
        r = {"table_name": table_name, "column_name": col, "dtype": str(df[col].dtype),
             "n_rows": n, "n_nulls": int(nulls[col]),
             "pct_nulls": round(int(nulls[col]) / n * 100, 2) if n else 0.0,
             "min": None, "max": None, "mean": None}
        if pd.api.types.is_numeric_dtype(df[col].dtype):
            r["min"] = float(compute(df[col].min()))
            r["max"] = float(compute(df[col].max()))
            r["mean"] = round(float(compute(df[col].mean())), 4)
        rows.append(r)
    return pd.DataFrame(rows)


def bronze_quality(cfg: dict) -> pd.DataFrame:
    import dask.dataframe as dd
    bronze = cfg["paths"]["bronze_dir"]
    cols = ["book_id", "title", "price", "user_id", "profile_name", "helpfulness",
            "score", "review_time", "summary", "text_len", "text_words"]
    r = dd.read_parquet(bronze / "reviews", columns=cols)
    b = dd.read_parquet(bronze / "books")
    return pd.concat([profile(r, "reviews"), profile(b, "books")], ignore_index=True)
