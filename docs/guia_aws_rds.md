# Guía: base de datos PostgreSQL en AWS RDS (punto 6)

> **Regla del proyecto:** a AWS solo se suben las tablas GOLD (resultados de la analítica,
> unos pocos cientos de KB). El dataset original de ~3 GB **nunca** sale del computador.

La consola de AWS cambia de vez en cuando; los nombres de los botones pueden variar un poco.

## 0. Antes de empezar (5 min)
1. Usen una cuenta de AWS nueva (con la capa gratuita o los créditos de bienvenida) o el
   *AWS Academy Learner Lab* si la universidad lo ofrece. Revisen las condiciones vigentes en
   la página oficial de AWS Free Tier.
2. **Creen una alerta de facturación** (Billing → Budgets → *Zero spend budget*). Así les
   llega un correo si algo empieza a cobrar.
3. Elijan una región y úsenla siempre (p. ej. `us-east-1`).

## 1. Crear la instancia
RDS → **Create database**:

| Opción | Valor |
|---|---|
| Creation method | Standard create |
| Engine | **PostgreSQL** (versión por defecto) |
| Templates | **Free tier** |
| DB instance identifier | `amazon-books-db` |
| Master username | `admin_grupo` |
| Password | una clave fuerte (guárdenla en un gestor de contraseñas, no en el repo) |
| Instance class | `db.t4g.micro` o `db.t3.micro` |
| Storage | 20 GB gp3, **sin** autoescalado |
| Public access | **Yes** (Grafana Cloud y sus portátiles deben poder conectarse) |
| VPC security group | Create new → `amazon-books-sg` |
| Additional configuration → Initial database name | **`amazon_books`** |

Clic en *Create database* y esperar ~10 min hasta que el estado sea **Available**.
📸 *Evidencia: captura de la instancia en estado Available (ocultando el endpoint si quieren).*

## 2. Abrir el puerto 5432 (security group)
EC2 → Security Groups → `amazon-books-sg` → *Edit inbound rules* → *Add rule*:
* Type **PostgreSQL**, Source **My IP** (repetir para la IP de cada integrante).
* Para Grafana Cloud: agregar las IP de salida que Grafana publica para su región (están en
  la documentación de Grafana Cloud, "source IPs"). Solo como último recurso para la
  demostración: `0.0.0.0/0`, con clave fuerte y cerrándolo después de la calificación.

## 3. Conectar el proyecto
1. Copiar el **endpoint** (RDS → la instancia → *Connectivity & security*).
2. En la raíz del repo: `cp .env.example .env` y completar:
   ```
   DB_URL=postgresql+psycopg2://admin_grupo:CLAVE@ENDPOINT:5432/amazon_books
   ```
3. Probar la conexión:
   ```bash
   python -c "from sqlalchemy import create_engine,text; from src.utils import *; \
   e=create_engine(get_db_url(load_config())); print(e.connect().execute(text('select version()')).scalar())"
   ```
4. Correr el flujo: `python -m flows.etl_flow`. Las tablas aparecen solas.
5. Crear el usuario de solo lectura para Grafana con `sql/01_usuario_grafana.sql`
   (desde DBeaver, pgAdmin o `psql`).
   📸 *Evidencia: captura de la consulta final del script mostrando las tablas y filas.*

## 4. Tablas que quedan en AWS
| Tabla | Contenido |
|---|---|
| `kpis` | indicadores generales (1 fila) |
| `score_distribution` | distribución de puntajes y utilidad por puntaje |
| `by_year`, `by_year_category` | evolución temporal |
| `by_category`, `by_publisher`, `top_books` | catálogo |
| `by_text_length`, `by_user_activity`, `by_book_age`, `by_sentiment` | segmentos asociados a la insatisfacción |
| `model_metrics`, `model_feature_importance` | resultados del modelo base |
| `data_quality` | perfil de nulos y rangos de la capa bronze |
| `dask_benchmark` | comparación de configuraciones de Dask |
| `etl_runs` | historial de ejecuciones del flujo de Prefect (se acumula) |

## 5. Al terminar el curso
Detener la instancia no basta (AWS la vuelve a encender a los 7 días): **elimínenla**
(Actions → Delete) cuando ya no la necesiten, y borren el security group.
