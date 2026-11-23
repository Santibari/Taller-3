# Taller 3 – Parte II: Tendencias internacionales de YouTube

Pipeline ETL orquestado con Prefect sobre el dataset de Kaggle `datasnaek/youtube-new`:
extracción desde Kaggle, transformación con pandas, carga en PostgreSQL (OLTP) y analítica con DuckDB (OLAP).

## Archivos
- `youtube_pipeline.py`: las 11 tasks y el único `@flow` (`YouTube — Top-100 Multi-País`).
- `config.py`: nombre de tabla, rutas y conexión (las credenciales se leen de `.env`).
- `.env.example`: plantilla de credenciales; copiarla como `.env`.
- `analysis_outputs/`: CSV generados por `aggregate_insights` y la gráfica del clustering.
- `informe/ANALISIS.pdf`: reporte escrito con la captura del grafo del flow.

## Ejecución
1. Iniciar el PostgreSQL de la Sesión 2 (puerto 5433): `docker start docker_postgres`
2. Activar el entorno: `conda activate BigDataPipelines` (en Windows, activar el entorno es necesario para que KMeans encuentre las DLL de OpenMP).
3. Credenciales de Kaggle en `~/.kaggle/kaggle.json` y de PostgreSQL en `.env`.
4. (Opcional, para ver el grafo) `prefect server start` y `set PREFECT_API_URL=http://127.0.0.1:4200/api`
5. `python youtube_pipeline.py`

No se versionan `data/`, `kaggle.json`, `.env` ni el volumen de Docker.
