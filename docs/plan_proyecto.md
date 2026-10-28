# Plan del proyecto

**Presentación:** viernes 14 de noviembre de 2026 (10 min, expositora al azar)
**Entrega final del documento:** una semana después, con la retroalimentación incorporada

## Cronograma

| Fechas | Entregable | Responsable |
|---|---|---|
| 27–29 oct | Repo en GitHub, entorno instalado, dataset descargado, prueba con datos sintéticos | todas |
| 30 oct – 3 nov | Conversión a Parquet + notebook 01 (EDA) ejecutado con datos reales | Datos/EDA |
| 30 oct – 3 nov | Cuenta AWS, alerta de facturación, instancia RDS creada | AWS/Grafana |
| 3–6 nov | Flujo de Prefect con datos reales + benchmark de Dask (notebook 02) | Dask/Prefect |
| 5–8 nov | Carga a RDS + tablero en Grafana Cloud | AWS/Grafana |
| 7–10 nov | Notebook 03, conclusiones, documento con evidencias | Documento |
| 11–13 nov | Ensayo de la presentación: **cada integrante la presenta al menos una vez** | todas |
| 14–21 nov | Ajustes por retroalimentación y entrega final | todas |

## Lista de verificación de la primera ejecución con los datos reales
El código se probó con datos sintéticos de igual esquema; con el dataset real verificar:
- [ ] `python -m src.convert` termina y `data/bronze/_manifest.json` reporta ~3 M de filas.
      Si aparecen avisos de líneas mal formadas, anotarlos como hallazgo de calidad.
- [ ] `pytest -q` pasa (3 pruebas).
- [ ] Con 8 GB de RAM: si los workers se reinician por memoria, usar el perfil A o bajar
      `model_sample_rows`. Con 16 GB todo debería correr sin cambios.
- [ ] El sentimiento VADER sobre 3 M de resúmenes es lo más lento: para pruebas rápidas,
      `sentiment: false` en `config.yaml`.
- [ ] Revisar en `01_EDA_dask.ipynb` la cobertura del cruce reseñas–libros por título.

## Evidencias (capturas) para el documento
- [ ] Tamaño de los archivos descargados (> 1.5 GB)
- [ ] Dashboard de Dask (http://localhost:8787) durante la corrida: *Task Stream*, *Workers*
- [ ] `reports/dask-report-A.html` y `-B.html` + tabla comparativa del benchmark
- [ ] Interfaz de Prefect: grafo del flujo, corridas exitosas, pestaña *Artifacts*, deployment programado
- [ ] Segunda corrida "incremental" (sin archivos nuevos → omite conversión) en el log de Prefect
- [ ] Consola de AWS RDS (instancia *Available*) y tablas en la BD (`sql/01_usuario_grafana.sql`)
- [ ] Grafana: conexión exitosa y cada sección del tablero
- [ ] Repositorio de GitHub (estructura y commits de todas)

## Estructura sugerida del documento (APA 7)
1. Portada · Resumen (abstract) · Palabras clave
2. Introducción y objetivo del análisis
3. Marco metodológico: CRISP-DM y arquitectura (diagrama del README)
4. **Comprensión del negocio** – objetivo, unidad de análisis, variable objetivo (a)
5. **Comprensión de los datos** – EDA y análisis preliminar, contexto adicional requerido (b, c)
6. **Preparación de los datos** – capas bronze/silver/gold, Dask y comparación de configuraciones (e)
7. **Modelado** – modelo base y variables más importantes
8. **Evaluación** – métricas e interpretación
9. **Despliegue** – flujo de Prefect (d), base de datos en AWS (f), tablero de Grafana (g)
10. Conclusiones y recomendaciones (h) · Limitaciones y trabajo futuro
11. Referencias · Anexos (consultas SQL, enlace al repositorio (l), capturas)

Referencias base a citar: el dataset de Kaggle; McAuley et al. sobre los datos de reseñas
de Amazon; Chapman et al. (2000) *CRISP-DM 1.0*; Hutto y Gilbert (2014) sobre VADER;
y la documentación oficial de Dask, Prefect, AWS RDS y Grafana.

## Guion de la presentación (10 min)
1. Problema y dataset (1 min) · 2. Arquitectura y CRISP-DM (1.5 min)
3. Hallazgos del EDA (2 min) · 4. Dask + Prefect en vivo o capturas (2 min)
5. Tablero de Grafana (2.5 min) · 6. Conclusiones y recomendaciones (1 min)
