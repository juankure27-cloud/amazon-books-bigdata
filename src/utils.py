"""Utilidades comunes: configuración, rutas, conexión a BD y cluster de Dask."""
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_config(path: str | Path | None = None) -> dict:
    path = Path(path) if path else ROOT / "config.yaml"
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # convertir rutas relativas en absolutas respecto a la raíz del repo
    for k, v in cfg["paths"].items():
        p = Path(v)
        cfg["paths"][k] = p if p.is_absolute() else ROOT / p
    return cfg


def get_db_url(cfg: dict) -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    url = os.getenv("DB_URL")
    if url:
        return url
    fallback = cfg["database"]["fallback_url"]
    # SQLite relativo -> absoluto
    if fallback.startswith("sqlite:///") and not fallback.startswith("sqlite:////"):
        rel = fallback.replace("sqlite:///", "")
        (ROOT / rel).parent.mkdir(parents=True, exist_ok=True)
        fallback = f"sqlite:///{ROOT / rel}"
    return fallback


@contextmanager
def dask_cluster(cfg: dict, profile: str | None = None):
    """Levanta un LocalCluster con el perfil de workers/memoria indicado.

    Devuelve (client, info). info se usa para dejar evidencia del perfil usado.
    """
    from dask.distributed import Client, LocalCluster

    profile = profile or cfg["dask"]["default_profile"]
    p = cfg["dask"]["profiles"][profile]
    cluster = LocalCluster(
        n_workers=p["n_workers"],
        threads_per_worker=p["threads_per_worker"],
        memory_limit=p["memory_limit"],
        processes=True,
        dashboard_address=":8787",
    )
    client = Client(cluster)
    info = {"profile": profile, **p, "dashboard": client.dashboard_link}
    print(f"[dask] perfil {profile}: {p} | dashboard: {client.dashboard_link}")
    try:
        yield client, info
    finally:
        client.close()
        cluster.close()


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = round(time.perf_counter() - self.t0, 2)


def compute(obj):
    """Ejecuta .compute() si es un objeto de Dask; si es pandas lo devuelve tal cual."""
    return obj.compute() if hasattr(obj, "compute") else obj
