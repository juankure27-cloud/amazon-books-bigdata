"""Paso 4: carga de las tablas GOLD a la base de datos (PostgreSQL en AWS RDS).

Solo se cargan resultados analíticos (decenas de KB), nunca el dataset original.
Además se dejan copias en CSV dentro de data/gold como respaldo y evidencia.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

APPEND_TABLES = {"etl_runs"}   # histórico: se acumula entre corridas del flujo


def save_csv(tables: dict[str, pd.DataFrame], gold_dir: Path) -> None:
    gold_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(gold_dir / f"{name}.csv", index=False)


def load_tables(tables: dict[str, pd.DataFrame], db_url: str, prefix: str = "") -> dict:
    from sqlalchemy import create_engine, inspect

    engine = create_engine(db_url, pool_pre_ping=True)
    loaded = {}
    with engine.begin() as conn:
        existing = set(inspect(conn).get_table_names())
        for name, df in tables.items():
            table = f"{prefix}{name}"
            mode = "append" if name in APPEND_TABLES and table in existing else "replace"
            df.to_sql(table, conn, if_exists=mode, index=False, chunksize=5000, method="multi")
            loaded[table] = {"rows": len(df), "mode": mode}
    engine.dispose()
    return loaded
