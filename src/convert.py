"""Paso 1 (capa BRONZE): CSV -> Parquet, de forma incremental.

¿Por qué no leer el CSV directamente con Dask?
    El texto de las reseñas contiene saltos de línea dentro de comillas. Dask parte el
    CSV en bloques por bytes y un bloque puede empezar en mitad de una reseña, lo que
    produce filas corruptas. pandas, leyendo por *chunks* de forma secuencial, respeta
    las comillas. Se convierte una sola vez a Parquet (columnar, comprimido, tipado) y
    a partir de ahí todo el análisis se hace con Dask en paralelo.

Incremental:
    Se guarda un manifiesto (_manifest.json) con tamaño y fecha de cada CSV procesado.
    Si llega un CSV nuevo (p. ej. Books_rating_2024.csv) o cambia uno existente, solo
    se convierte ese archivo. Así el flujo de Prefect es recurrente y escalable.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REVIEW_COLS = {
    "Id": "book_id", "Title": "title", "Price": "price", "User_id": "user_id",
    "profileName": "profile_name", "review/helpfulness": "helpfulness",
    "review/score": "score", "review/time": "review_time",
    "review/summary": "summary", "review/text": "text",
}
BOOK_COLS = {
    "Title": "title", "description": "description", "authors": "authors",
    "image": "image", "previewLink": "preview_link", "publisher": "publisher",
    "publishedDate": "published_date", "infoLink": "info_link",
    "categories": "categories", "ratingsCount": "ratings_count",
}


# --------------------------------------------------------------------------- #
# Estandarización de cada bloque (funciones puras de pandas, fáciles de testear)
# --------------------------------------------------------------------------- #
def standardize_reviews_chunk(pdf: pd.DataFrame, source_file: str) -> pd.DataFrame:
    pdf = pdf.rename(columns=REVIEW_COLS)
    out = pd.DataFrame({
        "book_id": pdf["book_id"].astype("string"),
        "title": pdf["title"].astype("string"),
        "price": pd.to_numeric(pdf["price"], errors="coerce").astype("float32"),
        "user_id": pdf["user_id"].astype("string"),
        "profile_name": pdf["profile_name"].astype("string"),
        "helpfulness": pdf["helpfulness"].astype("string"),
        "score": pd.to_numeric(pdf["score"], errors="coerce").astype("float32"),
        "review_time": pd.to_numeric(pdf["review_time"], errors="coerce").astype("Int64"),
        "summary": pdf["summary"].astype("string"),
        "text": pdf["text"].astype("string"),
    })
    # métricas de texto precalculadas: evita volver a leer la columna pesada 'text'
    out["text_len"] = out["text"].str.len().fillna(0).astype("int32")
    out["text_words"] = out["text"].str.count(r"\S+").fillna(0).astype("int32")
    out["source_file"] = source_file
    return out


def standardize_books(pdf: pd.DataFrame, source_file: str) -> pd.DataFrame:
    pdf = pdf.rename(columns=BOOK_COLS)
    for c in BOOK_COLS.values():
        if c not in pdf:
            pdf[c] = pd.NA
    out = pdf[list(BOOK_COLS.values())].copy()
    for c in out.columns:
        if c != "ratings_count":
            out[c] = out[c].astype("string")
    out["ratings_count"] = pd.to_numeric(out["ratings_count"], errors="coerce").astype("float32")
    out["source_file"] = source_file
    return out


# --------------------------------------------------------------------------- #
# Manifiesto (control incremental)
# --------------------------------------------------------------------------- #
def _load_manifest(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def _fingerprint(f: Path) -> dict:
    st = f.stat()
    return {"size": st.st_size, "mtime": int(st.st_mtime)}


def pending_files(raw_dir: Path, pattern: str, manifest: dict) -> list[Path]:
    files = sorted(Path(raw_dir).glob(pattern))
    return [f for f in files if manifest.get(f.name, {}).get("fingerprint") != _fingerprint(f)]


# --------------------------------------------------------------------------- #
# Conversión
# --------------------------------------------------------------------------- #
def convert_reviews(csv: Path, out_dir: Path, chunksize: int) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob(f"{csv.stem}-*.parquet"):      # si el archivo cambió, se rehace
        old.unlink()
    n_rows, n_parts = 0, 0
    reader = pd.read_csv(csv, dtype=str, chunksize=chunksize, on_bad_lines="warn")
    for i, chunk in enumerate(reader):
        std = standardize_reviews_chunk(chunk, csv.name)
        std.to_parquet(out_dir / f"{csv.stem}-{i:05d}.parquet", index=False)
        n_rows += len(std)
        n_parts += 1
        print(f"  {csv.name}: bloque {i} ({n_rows:,} filas)")
    return {"rows": n_rows, "parts": n_parts}


def convert_books(csv: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf = pd.read_csv(csv, dtype=str, on_bad_lines="warn")
    std = standardize_books(pdf, csv.name)
    std.to_parquet(out_dir / f"{csv.stem}.parquet", index=False)
    return {"rows": len(std), "parts": 1}


def convert_all(cfg: dict) -> dict:
    """Convierte solo los CSV nuevos o modificados. Devuelve un resumen."""
    raw, bronze = cfg["paths"]["raw_dir"], cfg["paths"]["bronze_dir"]
    mpath = cfg["paths"]["manifest"]
    manifest = _load_manifest(mpath)
    summary = {"converted": [], "skipped": 0}

    jobs = [
        (cfg["files"]["reviews_pattern"], lambda f: convert_reviews(
            f, bronze / "reviews", cfg["conversion"]["chunksize"])),
        (cfg["files"]["books_pattern"], lambda f: convert_books(f, bronze / "books")),
    ]
    for pattern, fn in jobs:
        all_files = list(Path(raw).glob(pattern))
        todo = pending_files(raw, pattern, manifest)
        summary["skipped"] += len(all_files) - len(todo)
        for f in todo:
            print(f"[convert] {f.name}")
            stats = fn(f)
            manifest[f.name] = {
                "fingerprint": _fingerprint(f), **stats,
                "converted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            summary["converted"].append({"file": f.name, **stats})

    mpath.parent.mkdir(parents=True, exist_ok=True)
    mpath.write_text(json.dumps(manifest, indent=2))
    summary["total_review_rows"] = int(np.sum([
        v["rows"] for k, v in manifest.items()
        if Path(k).match(cfg["files"]["reviews_pattern"])]))
    return summary


if __name__ == "__main__":
    from src.utils import load_config
    print(convert_all(load_config()))
