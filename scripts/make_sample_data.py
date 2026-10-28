"""Genera CSV sintéticos con EXACTAMENTE el mismo esquema que el dataset de Kaggle
'Amazon Books Reviews' (Books_rating.csv y books_data.csv).

Sirve para probar todo el pipeline en minutos antes de correrlo con los ~3 GB reales.
Incluye a propósito los problemas del dataset real: textos con saltos de línea y
comillas, precios vacíos, utilidad en formato "x/y", fechas de publicación en
formatos distintos y títulos sin metadatos.

Uso:
    python scripts/make_sample_data.py --rows 200000 --out data/raw
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

CATEGORIES = ["Fiction", "Juvenile Fiction", "Biography & Autobiography", "History",
              "Religion", "Business & Economics", "Cooking", "Computers",
              "Self-Help", "Science", "Poetry", "Travel"]
PUBLISHERS = ["Penguin", "HarperCollins", "Random House", "Simon and Schuster",
              "Macmillan", "Scholastic", "Wiley", "Oxford University Press"]
POS = ["Great read", "Loved it!", "Wonderful book", "A classic", "Highly recommended"]
NEG = ["Terrible", "Boring and too long", "Waste of money!!", "Disappointing", "Did not finish"]


def main(rows: int, out: Path, seed: int = 42) -> None:
    rng = np.random.default_rng(seed)
    out.mkdir(parents=True, exist_ok=True)

    n_books = max(rows // 15, 50)
    titles = [f"Book Title {i}" for i in range(n_books)]
    books = pd.DataFrame({
        "Title": titles,
        "description": [f"Description of book {i}." for i in range(n_books)],
        "authors": [f"['Author {rng.integers(0, n_books // 3 + 1)}']" for _ in range(n_books)],
        "image": "http://books.google.com/img.jpg",
        "previewLink": "http://books.google.com/preview",
        "publisher": rng.choice(PUBLISHERS + [None], n_books),
        "publishedDate": [rng.choice([f"{y}", f"{y}-05", f"{y}-05-01", f"{y}*"])
                          for y in rng.integers(1950, 2013, n_books)],
        "infoLink": "http://books.google.com/info",
        "categories": [f"['{c}']" if c else None for c in rng.choice(CATEGORIES + [None], n_books)],
        "ratingsCount": np.where(rng.random(n_books) < 0.6, np.nan, rng.integers(1, 500, n_books)),
    })
    # 8 % de títulos sin metadatos (como en el dataset real)
    books = books.sample(frac=0.92, random_state=seed)
    books.to_csv(out / "books_data.csv", index=False)

    book_quality = rng.normal(4.1, 0.5, n_books)
    idx = rng.zipf(1.6, rows) % n_books                       # pocos libros muy populares
    score = np.clip(np.round(book_quality[idx] + rng.normal(0, 1.1, rows)), 1, 5)
    total = rng.poisson(3, rows)
    helpful = rng.binomial(total, np.where(score <= 2, 0.45, 0.75))
    neg = score <= 2
    summary = np.where(neg, rng.choice(NEG, rows), rng.choice(POS, rows))
    text_len = rng.lognormal(6, 0.8, rows).astype(int) + np.where(neg, 150, 0)
    text = [("This book was \"interesting\".\nSecond line, " + "word " * (n // 5))
            for n in text_len]

    reviews = pd.DataFrame({
        "Id": [f"{rng.integers(1e9, 9e9)}" for _ in range(rows)],
        "Title": [titles[i] for i in idx],
        "Price": np.where(rng.random(rows) < 0.85, np.nan, np.round(rng.uniform(5, 60, rows), 2)),
        "User_id": np.where(rng.random(rows) < 0.18, None,
                            [f"A{u}" for u in rng.zipf(1.8, rows) % (rows // 3)]),
        "profileName": "Reader",
        "review/helpfulness": [f"{h}/{t}" for h, t in zip(helpful, total)],
        "review/score": score,
        "review/time": rng.integers(820000000, 1362000000, rows),   # 1996 - 2013
        "review/summary": summary,
        "review/text": text,
    })
    reviews.to_csv(out / "Books_rating.csv", index=False)
    print(f"OK: {rows:,} reseñas y {len(books):,} libros en {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--out", type=Path, default=Path("data/raw"))
    a = p.parse_args()
    main(a.rows, a.out)
