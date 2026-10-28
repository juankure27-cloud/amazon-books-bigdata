# Guía: tablero gerencial en Grafana (punto 7)

El tablero ya está construido: `grafana/dashboard_amazon_books.json` (23 paneles en 5
secciones). Solo hay que conectarlo a la base de datos de AWS e importarlo.

## Opción A – Grafana Cloud (recomendada: queda en línea y se puede compartir)
1. Crear una cuenta gratuita en grafana.com y abrir su *stack*.
2. **Connections → Data sources → Add data source → PostgreSQL**
   * Host URL: `ENDPOINT-DE-RDS:5432`
   * Database: `amazon_books`
   * User / Password: `grafana_ro` / la clave de `sql/01_usuario_grafana.sql`
   * TLS/SSL Mode: `require`
   * **Save & test** → debe decir *Database Connection OK*.
   📸 *Evidencia: captura del "Save & test" exitoso.*
3. **Dashboards → New → Import → Upload JSON file** → elegir
   `grafana/dashboard_amazon_books.json` → en *PostgreSQL (AWS RDS)* seleccionar el data
   source del paso 2 → **Import**.
4. Si Grafana Cloud no logra conectarse: revisar que el security group de RDS permita las
   IP de Grafana Cloud (ver `docs/guia_aws_rds.md`, paso 2).

## Opción B – Grafana local con Docker
```bash
docker run -d -p 3000:3000 --name grafana grafana/grafana
```
Abrir http://localhost:3000 (usuario/clave inicial `admin`/`admin`) y seguir los pasos 2 y 3.

## Qué muestra el tablero
| Sección | Pregunta gerencial que responde |
|---|---|
| Resumen ejecutivo | ¿Qué tan grande es la base y qué tan satisfechos están los lectores? |
| Tendencia en el tiempo | ¿Cómo evolucionó el volumen y la satisfacción? ¿En qué categorías empeoró? |
| Catálogo | ¿Qué categorías, editoriales y libros concentran la insatisfacción? |
| ¿Qué se asocia con la insatisfacción? | Longitud de la reseña, tipo de usuario, antigüedad del libro, sentimiento |
| Modelo y operación | ¿Se puede anticipar una reseña negativa? ¿El pipeline corre bien? ¿Qué configuración de Dask rinde más? |

## Consejos para la presentación del 14 de noviembre
* Usar el tablero en vivo como hilo conductor del resumen ejecutivo (máx. 3–4 paneles).
* Exportar también un PDF o capturas por si falla la conexión el día de la presentación.
* Las consultas de cada panel están en `sql/consultas_grafana.sql` (para el anexo del documento).
* Si cambian algo del tablero en la interfaz, expórtenlo de nuevo (*Share → Export → Save to file*)
  y reemplacen el JSON del repo.
