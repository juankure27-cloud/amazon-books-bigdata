"""Flujo ETL recurrente con Prefect (punto 4 del proyecto).

    data/raw (CSV)  ->  BRONZE (Parquet)  ->  SILVER (limpio + enriquecido, Dask)
                    ->  GOLD (tablas analíticas)  ->  PostgreSQL en AWS RDS  ->  Grafana

Recurrente y escalable:
  * Si se copian más CSV con el mismo esquema en data/raw (p. ej. Books_rating_2025.csv),
    la siguiente ejecución los detecta (manifiesto), los convierte y recalcula todo.
  * Si no hay archivos nuevos, se omite la conversión y la capa SILVER (a menos que
    force=True) y solo se recalcula GOLD y se recarga la BD.
  * El tamaño del cluster de Dask es un parámetro (dask_profile): con más datos basta
    con usar un perfil con más workers/memoria, sin tocar el código.

Ejecutar una vez:
    python -m flows.etl_flow
    python -m flows.etl_flow --profile A --force
Programar (ver flows/serve_schedule.py):
    python -m flows.serve_schedule
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.cache_policies import NO_CACHE

from src.aggregations import build_gold
from src.convert import convert_all
from src.load import load_tables, save_csv
from src.model import sample_silver, train_baseline
from src.quality import bronze_quality
from src.transform import build_silver
from src.utils import Timer, dask_cluster, get_db_url, load_config


@task(name="1-convertir-csv-a-parquet", retries=1, cache_policy=NO_CACHE)
def t_convert(cfg: dict) -> dict:
    return convert_all(cfg)


@task(name="2-calidad-de-datos", cache_policy=NO_CACHE)
def t_quality(cfg: dict) -> pd.DataFrame:
    return bronze_quality(cfg)


@task(name="3-silver-limpieza-y-features", cache_policy=NO_CACHE)
def t_silver(cfg: dict, sentiment: bool | None) -> dict:
    return build_silver(cfg, sentiment=sentiment)


@task(name="4-gold-tablas-analiticas", cache_policy=NO_CACHE)
def t_gold(cfg: dict) -> dict[str, pd.DataFrame]:
    import dask.dataframe as dd
    df = dd.read_parquet(cfg["paths"]["silver_dir"] / "reviews_enriched")
    return build_gold(df, cfg)


@task(name="5-modelo-base", cache_policy=NO_CACHE)
def t_model(cfg: dict) -> dict[str, pd.DataFrame]:
    return train_baseline(sample_silver(cfg))


@task(name="6-cargar-a-base-de-datos", retries=3, retry_delay_seconds=20, cache_policy=NO_CACHE)
def t_load(tables: dict[str, pd.DataFrame], cfg: dict) -> dict:
    save_csv(tables, cfg["paths"]["gold_dir"])
    return load_tables(tables, get_db_url(cfg), cfg["database"].get("schema_prefix") or "")


@flow(name="amazon-books-etl", log_prints=True)
def amazon_books_etl(dask_profile: str = "B", force: bool = False, run_model: bool = True,
                     sentiment: bool | None = None, config_path: str | None = None) -> dict:
    log = get_run_logger()
    cfg = load_config(config_path)
    run = {"run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "dask_profile": dask_profile}
    try:
        from prefect.runtime import flow_run
        run["flow_run_id"] = str(flow_run.id)
    except Exception:  # noqa: BLE001
        run["flow_run_id"] = None

    with Timer() as t:
        conv = t_convert(cfg)
    run.update(files_converted=len(conv["converted"]), seconds_convert=t.seconds)
    log.info(f"Archivos convertidos: {conv['converted'] or 'ninguno (sin datos nuevos)'}")

    silver_path = cfg["paths"]["silver_dir"] / "reviews_enriched"
    need_silver = force or bool(conv["converted"]) or not silver_path.exists()

    tables: dict[str, pd.DataFrame] = {}
    with dask_cluster(cfg, dask_profile) as (client, info):
        run.update(n_workers=info["n_workers"], threads_per_worker=info["threads_per_worker"],
                   memory_limit=info["memory_limit"])
        tables["data_quality"] = t_quality(cfg)

        with Timer() as t:
            if need_silver:
                stats = t_silver(cfg, sentiment)
                run.update(rows_raw=stats["rows_raw"], rows_clean=stats["rows_clean"],
                           rows_removed=stats["rows_removed"])
            else:
                log.info("Sin datos nuevos: se reutiliza la capa SILVER existente")
        run["seconds_silver"] = t.seconds

        with Timer() as t:
            tables.update(t_gold(cfg))
        run["seconds_gold"] = t.seconds

        if run_model:
            with Timer() as t:
                tables.update(t_model(cfg))
            run["seconds_model"] = t.seconds

    bench = cfg["paths"]["reports_dir"] / "dask_benchmark.csv"
    if bench.exists():
        tables["dask_benchmark"] = pd.read_csv(bench)

    run["status"] = "ok"
    run["rows_total"] = int(tables["kpis"]["total_reviews"].iloc[0])
    tables["etl_runs"] = pd.DataFrame([run])
    loaded = t_load(tables, cfg)

    # Evidencias visibles en la interfaz de Prefect (pestaña Artifacts)
    create_table_artifact(key="kpis", table=tables["kpis"].to_dict("records"),
                          description="KPIs gerenciales de la corrida")
    create_table_artifact(key="tablas-cargadas",
                          table=[{"tabla": k, **v} for k, v in loaded.items()],
                          description="Tablas GOLD cargadas a la base de datos")
    k = tables["kpis"].iloc[0]
    create_markdown_artifact(key="resumen-corrida", markdown=(
        f"# Corrida del ETL\n\n"
        f"* Perfil Dask: **{dask_profile}** ({run['n_workers']} workers x "
        f"{run['threads_per_worker']} hilos, {run['memory_limit']} por worker)\n"
        f"* Archivos nuevos convertidos: **{run['files_converted']}**\n"
        f"* Reseñas analizadas: **{int(k['total_reviews']):,}**\n"
        f"* Puntaje promedio: **{k['avg_score']}** | Reseñas negativas: **{k['pct_negative']} %**\n"
        f"* Tablas cargadas: {len(loaded)}\n"))
    log.info(f"Corrida terminada: {run}")
    return run


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="B")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--no-model", action="store_true")
    a = ap.parse_args()
    amazon_books_etl(dask_profile=a.profile, force=a.force, run_model=not a.no_model)
