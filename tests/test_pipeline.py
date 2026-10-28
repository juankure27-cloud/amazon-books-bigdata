"""Pruebas de la lógica del pipeline usando pandas (sin Dask ni Prefect).

La misma lógica que Dask aplica por partición se ejecuta aquí sobre una muestra,
por eso estas pruebas validan las transformaciones de punta a punta.

    python scripts/make_sample_data.py --rows 50000
    pytest -q
"""
from pathlib import Path

import pandas as pd
import pytest

from src.aggregations import build_gold
from src.convert import standardize_books, standardize_reviews_chunk
from src.model import train_baseline
from src.quality import profile
from src.transform import add_derived, add_review_features, clean_books
from src.utils import load_config

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"


def test_helpfulness_and_target():
    pdf = pd.DataFrame({
        "title": [" The  Hobbit", "x"], "user_id": ["A1", None],
        "helpfulness": ["3/4", "0/0"], "score": [1.0, 5.0],
        "review_time": [1000000000, 1300000000], "summary": ["Bad!!", "Good"],
        "text_words": [10, 700],
    })
    out = add_review_features(pdf, 2)
    assert out["votes_helpful"].tolist() == [3, 0]
    assert out["helpful_ratio"].iloc[0] == pytest.approx(0.75)
    assert pd.isna(out["helpful_ratio"].iloc[1])
    assert out["is_negative"].tolist() == [1, 0]
    assert out["summary_exclam"].tolist() == [2, 0]
    assert out["title_norm"].iloc[0] == "the hobbit"
    assert out["text_len_bucket"].tolist() == ["<50", "600+"]
    assert out["review_year"].tolist() == [2001, 2011]


def test_clean_books_parsing():
    raw = pd.DataFrame({
        "Title": ["A", "A", "B"], "description": [None, "d", None],
        "authors": ["['X']", "['X', 'Y']", None], "image": None, "previewLink": None,
        "publisher": ["P", "P", None], "publishedDate": ["1999-05", "1999*", "2004-01-01"],
        "infoLink": None, "categories": ["['Fiction']", "['Fiction']", None],
        "ratingsCount": ["3", None, None],
    })
    b = clean_books(standardize_books(raw, "books_data.csv"))
    assert len(b) == 2                                   # título duplicado eliminado
    a = b.set_index("title_norm").loc["a"]
    assert a["main_category"] == "Fiction" and a["published_year"] == 1999
    assert b.set_index("title_norm").loc["b", "published_year"] == 2004


@pytest.mark.skipif(not (RAW / "Books_rating.csv").exists(), reason="sin datos de muestra")
def test_end_to_end_pandas(tmp_path):
    cfg = load_config()
    cfg["analysis"].update(min_reviews_category=10, min_reviews_publisher=10,
                           model_sample_rows=50_000)
    raw = pd.read_csv(RAW / "Books_rating.csv", dtype=str, nrows=60_000)
    bronze = standardize_reviews_chunk(raw, "Books_rating.csv")
    books_bronze = standardize_books(pd.read_csv(RAW / "books_data.csv", dtype=str),
                                     "books_data.csv")
    q = profile(bronze.drop(columns=["text"]), "reviews")
    assert {"pct_nulls", "column_name"} <= set(q.columns)

    r = add_review_features(bronze.drop(columns=["text"]), 2)
    r = r.drop_duplicates(subset=["user_id", "title_norm", "review_time", "score", "summary"])
    books = clean_books(books_bronze)
    r = r.merge(books, on="title_norm", how="left")
    uc = r.groupby("user_id")["score"].count()
    bc = r.groupby("title_norm")["score"].count()
    silver = add_derived(r, uc, bc).drop(columns=["summary"])

    gold = build_gold(silver, cfg)
    assert gold["kpis"]["total_reviews"].iloc[0] == len(silver)
    assert gold["score_distribution"]["pct"].sum() == pytest.approx(100, abs=0.1)
    assert gold["by_year"]["review_year"].between(1995, 2013).all()
    for name, t in gold.items():
        assert len(t) > 0, name

    model = train_baseline(silver.sample(20_000, random_state=1))
    auc = model["model_metrics"].set_index("metric").loc["ROC AUC", "value"]
    assert 0.5 < auc <= 1.0
