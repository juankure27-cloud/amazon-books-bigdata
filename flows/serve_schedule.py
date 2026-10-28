"""Despliega el flujo con una programación recurrente.

Deja este proceso corriendo (o en un servidor/EC2) y Prefect ejecutará el ETL todos
los días a las 2:00 a. m. de Bogotá (07:00 UTC). Cualquier CSV nuevo que se copie en
data/raw será procesado en la siguiente corrida. También se puede lanzar a mano desde
la interfaz de Prefect (botón "Run").

    prefect server start          # en otra terminal: interfaz en http://127.0.0.1:4200
    python -m flows.serve_schedule
"""
from flows.etl_flow import amazon_books_etl

if __name__ == "__main__":
    amazon_books_etl.serve(
        name="etl-diario-amazon-books",
        cron="0 7 * * *",          # Prefect usa UTC: 07:00 UTC = 02:00 en Bogotá
        parameters={"dask_profile": "B", "force": False, "run_model": True},
        tags=["big-data", "amazon-books"],
    )
