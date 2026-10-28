"""Comparación de configuraciones de Dask (punto 5 del proyecto).

Ejecuta el mismo trabajo (SILVER + GOLD) con distintos perfiles de workers y memoria
por worker definidos en config.yaml y guarda:
  * reports/dask_benchmark.csv        -> tabla comparativa (también va a la BD/Grafana)
  * reports/dask-report-<perfil>.html -> reporte de rendimiento de Dask (evidencia)

Uso:
    python -m src.dask_benchmark --profiles A B
"""
from __future__ import annotations

import argparse
import platform
from datetime import datetime, timezone

import pandas as pd

from src.aggregations import build_gold
from src.transform import build_silver
from src.utils import Timer, dask_cluster, load_config


def run_profile(cfg: dict, profile: str) -> dict:
    import dask.dataframe as dd
    from dask.distributed import performance_report

    reports = cfg["paths"]["reports_dir"]
    reports.mkdir(parents=True, exist_ok=True)
    with dask_cluster(cfg, profile) as (client, info):
        with performance_report(filename=str(reports / f"dask-report-{profile}.html")):
            with Timer() as t_silver:
                stats = build_silver(cfg)
            with Timer() as t_gold:
                df = dd.read_parquet(cfg["paths"]["silver_dir"] / "reviews_enriched")
                build_gold(df, cfg)
    return {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "profile": profile,
        "n_workers": info["n_workers"],
        "threads_per_worker": info["threads_per_worker"],
        "memory_limit": info["memory_limit"],
        "total_threads": info["n_workers"] * info["threads_per_worker"],
        "rows": stats["rows_clean"],
        "sentiment": stats["sentiment"],
        "seconds_silver": t_silver.seconds,
        "seconds_gold": t_gold.seconds,
        "seconds_total": round(t_silver.seconds + t_gold.seconds, 2),
        "machine": f"{platform.system()} {platform.machine()}",
    }


def main(profiles: list[str]) -> pd.DataFrame:
    cfg = load_config()
    rows = [run_profile(cfg, p) for p in profiles]
    res = pd.DataFrame(rows)
    res["rows_per_second"] = (res["rows"] / res["seconds_total"]).round(0)
    base = res["seconds_total"].max()
    res["speedup_vs_slowest"] = (base / res["seconds_total"]).round(2)
    out = cfg["paths"]["reports_dir"] / "dask_benchmark.csv"
    res.to_csv(out, mode="a", header=not out.exists(), index=False)
    print(res.to_string(index=False))
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles", nargs="+", default=["A", "B"])
    main(ap.parse_args().profiles)
