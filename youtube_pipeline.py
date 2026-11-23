"""Taller 3 - Parte II: Análisis de tendencias internacionales de YouTube.

Pipeline ETL orquestado con Prefect (un único @flow):
    Extracción   -> Kaggle (datasnaek/youtube-new), 10 CSV de países
    Transformación -> pandas (unificación, top 100 global, fechas, limpieza)
    Carga (OLTP) -> PostgreSQL dockerizado (contenedor de la Sesión 2, puerto 5433)
    Analítica (OLAP) -> DuckDB lee PostgreSQL con ATTACH ... (TYPE postgres, READ_ONLY)

Ejecución:
    python youtube_pipeline.py
"""
import os

os.environ.setdefault("OMP_NUM_THREADS", "1")  # evita problemas de KMeans en Windows

import time
from functools import wraps
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from psycopg2.extras import execute_values
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from config import (
    DATA_DIR,
    EXPECTED_COUNTRIES,
    K_RANGE,
    KAGGLE_DATASET,
    OUTPUT_DIR,
    PG_CONFIG,
    RANDOM_STATE,
    TABLE_NAME,
    TOP_N,
)

INVALID_VIDEO_IDS = ["#NAME?"]
NUMERIC_COLUMNS = ["views", "likes", "dislikes", "comment_count", "days_to_trending", "category_id"]
TABLE_COLUMNS = [
    "video_id", "country", "title", "category_id", "views", "likes", "dislikes",
    "comment_count", "days_to_trending", "trending_date_parsed", "publish_time_parsed",
]


# ---------------------------------------------------------------------------
# Decoradores (Taller 3, parte I)
# ---------------------------------------------------------------------------
def timing_decorator(func):
    """Registra la duración de la tarea, incluso si falla."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_run_logger()
        started_at = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - started_at
            logger.info(f"Task '{func.__name__}' completed in {elapsed:.2f} seconds")
    return wrapper


def get_directory_size(path=".") -> int:
    total_size = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if not os.path.islink(file_path):
                total_size += os.path.getsize(file_path)
    return total_size


def human_readable_size(size_bytes: int) -> str:
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}"
        size /= 1024


def report_size_decorator(func):
    """Reporta el tamaño en disco de la carpeta que retorna la función envuelta."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_run_logger()
        path = func(*args, **kwargs)
        size_bytes = get_directory_size(path)
        logger.info(
            f"Dataset size on disk ('{path}'): {size_bytes:,} bytes = {human_readable_size(size_bytes)}"
        )
        return path
    return wrapper


# ---------------------------------------------------------------------------
# 3.1 Extracción
# ---------------------------------------------------------------------------
@task(name="extract_data", retries=3, retry_delay_seconds=15, cache_policy=NO_CACHE)
@timing_decorator
@report_size_decorator
def extract_data(download_path: Path = DATA_DIR, dataset_id: str = KAGGLE_DATASET) -> Path:
    """3.1.1 Descarga el dataset de Kaggle y lo descomprime en data/."""
    logger = get_run_logger()
    try:
        from kaggle import KaggleApi  # autentica con ~/.kaggle/kaggle.json al importar

        download_path = Path(download_path)
        download_path.mkdir(parents=True, exist_ok=True)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset_id, path=str(download_path), unzip=True)
        n_files = sum(1 for _ in download_path.iterdir())
        logger.info(f"Downloaded '{dataset_id}' into '{download_path}' ({n_files} files).")
        return download_path
    except Exception as exc:
        logger.error(f"Kaggle download failed: {exc}")
        raise
    finally:
        logger.info("extract_data finished.")


@task(name="enumerate_country_files", cache_policy=NO_CACHE)
@timing_decorator
def enumerate_country_files(raw_path: Path) -> dict:
    """3.1.2 Mapea código de país -> ruta del CSV de videos (sin JSON ni *_comments.csv)."""
    logger = get_run_logger()
    try:
        files = {
            p.name[:2]: p
            for p in sorted(Path(raw_path).glob("*videos.csv"))
            if not p.name.endswith("_comments.csv")
        }
        logger.info(f"Country files found: {len(files)} -> {sorted(files)}")
        if len(files) != EXPECTED_COUNTRIES:
            raise ValueError(f"Expected {EXPECTED_COUNTRIES} country files, found {len(files)}")
        return files
    except Exception as exc:
        logger.error(f"Could not enumerate country files: {exc}")
        raise
    finally:
        logger.info("enumerate_country_files finished.")


# ---------------------------------------------------------------------------
# 3.2 Transformación
# ---------------------------------------------------------------------------
@task(name="unify_countries", cache_policy=NO_CACHE)
@timing_decorator
def unify_countries(files: dict) -> pd.DataFrame:
    """3.2.1 Lee los 10 CSV, agrega la columna country y concatena."""
    logger = get_run_logger()
    try:
        frames = []
        for country, path in files.items():
            # Algunos CSV (JP, KR, MX, RU) traen bytes UTF-8 inválidos: se reemplazan
            # en lugar de abortar la lectura.
            frame = pd.read_csv(path, encoding="utf-8", encoding_errors="replace")
            frame["country"] = country
            logger.info(f"{country}: {frame.shape[0]:,} rows")
            frames.append(frame)
        df_all = pd.concat(frames, ignore_index=True)
        logger.info(f"Unified shape: {df_all.shape[0]:,} rows x {df_all.shape[1]} columns")
        return df_all
    except Exception as exc:
        logger.error(f"Could not unify country files: {exc}")
        raise
    finally:
        logger.info("unify_countries finished.")


@task(name="top100_global", cache_policy=NO_CACHE)
@timing_decorator
def top100_global(df_all: pd.DataFrame, top_n: int = TOP_N) -> pd.DataFrame:
    """3.2.2 Una fila por video (la de máximo views) y los top_n con más vistas."""
    logger = get_run_logger()
    try:
        # "#NAME?" es un error de Excel en el CSV original: miles de videos distintos
        # comparten ese id, así que no identifican un video y se excluyen.
        invalid = df_all["video_id"].isin(INVALID_VIDEO_IDS) | df_all["video_id"].isna()
        logger.info(f"Rows with invalid video_id {INVALID_VIDEO_IDS}: {int(invalid.sum()):,} (excluded)")
        df_valid = df_all.loc[~invalid]
        idx_max = df_valid.groupby("video_id")["views"].idxmax()
        df_unique = df_valid.loc[idx_max]
        logger.info(
            f"De-duplicated by video_id: {len(df_valid):,} -> {len(df_unique):,} unique videos"
        )
        df_top = (
            df_unique.sort_values("views", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        distribution = df_top["country"].value_counts().to_dict()
        logger.info(f"Top {top_n} shape: {df_top.shape}. Country distribution: {distribution}")
        return df_top
    except Exception as exc:
        logger.error(f"top100_global failed: {exc}")
        raise
    finally:
        logger.info("top100_global finished.")


@task(name="standardize_dates", cache_policy=NO_CACHE)
@timing_decorator
def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """3.2.3 Parseo de fechas: forma vectorizada (apply+lambda) y verificación con iterrows."""
    logger = get_run_logger()
    try:
        df = df.copy()
        # Forma 1 — apply + lambda. trending_date viene como AA.DD.MM (día antes del mes).
        df["trending_date_parsed"] = df["trending_date"].apply(
            lambda s: pd.to_datetime(s, format="%y.%d.%m")
        )
        df["publish_time_parsed"] = df["publish_time"].apply(
            lambda s: pd.to_datetime(s, utc=True)
        )

        # Forma 2 — iterrows explícito (verificación cruzada fila por fila).
        df["publish_time_iter"] = pd.Series(pd.NaT, index=df.index, dtype=df["publish_time_parsed"].dtype)
        for idx, row in df.iterrows():
            df.at[idx, "publish_time_iter"] = pd.to_datetime(row["publish_time"], utc=True)

        try:
            assert (df["publish_time_parsed"] == df["publish_time_iter"]).all()
        except AssertionError:
            logger.error("Vectorized and iterrows parsing of publish_time do NOT match.")
            raise
        logger.info("assert OK: apply+lambda and iterrows parse publish_time identically.")

        sample = df.loc[0, ["trending_date", "trending_date_parsed"]].tolist()
        logger.info(f"Sample trending_date: {sample[0]} -> {sample[1].date()} (AA.DD.MM)")
        return df
    except Exception as exc:
        logger.error(f"standardize_dates failed: {exc}")
        raise
    finally:
        logger.info("standardize_dates finished.")


@task(name="compute_days_to_trending", cache_policy=NO_CACHE)
@timing_decorator
def compute_days_to_trending(df: pd.DataFrame) -> pd.DataFrame:
    """3.2.4 Días entre publicación y tendencia; rechaza duraciones negativas."""
    logger = get_run_logger()
    try:
        df = df.copy()
        df["days_to_trending"] = (
            df["trending_date_parsed"].dt.normalize()
            - df["publish_time_parsed"].dt.tz_localize(None).dt.normalize()
        ).dt.days
        d_min = df["days_to_trending"].min()
        d_med = df["days_to_trending"].median()
        d_max = df["days_to_trending"].max()
        logger.info(f"days_to_trending -> min={d_min}, median={d_med}, max={d_max}")
        if d_min < 0:
            logger.warning(f"Negative days_to_trending found (min={d_min}): date parsing error.")
            raise ValueError(f"Negative days_to_trending (min={d_min})")
        return df
    except Exception as exc:
        logger.error(f"compute_days_to_trending failed: {exc}")
        raise
    finally:
        logger.info("compute_days_to_trending finished.")


@task(name="clean_records", cache_policy=NO_CACHE)
@timing_decorator
def clean_records(df: pd.DataFrame) -> pd.DataFrame:
    """3.2.5 Elimina filas con faltantes en las columnas numéricas clave."""
    logger = get_run_logger()
    try:
        missing = df[NUMERIC_COLUMNS].isna().sum()
        for column, count in missing.items():
            logger.info(f"Missing values in '{column}': {count}")
        df_clean = df.dropna(subset=NUMERIC_COLUMNS).reset_index(drop=True)
        dropped = len(df) - len(df_clean)
        reasons = {c: int(n) for c, n in missing.items() if n > 0}
        logger.info(
            f"clean_records: {len(df)} -> {len(df_clean)} rows "
            f"({dropped} dropped; reasons: {reasons or 'no missing values'})"
        )
        return df_clean
    except Exception as exc:
        logger.error(f"clean_records failed: {exc}")
        raise
    finally:
        logger.info("clean_records finished.")


# ---------------------------------------------------------------------------
# 3.3 Carga (OLTP)
# ---------------------------------------------------------------------------
def _to_python(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        return value.item()
    return value


@task(name="load_data", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
@timing_decorator
def load_data(df: pd.DataFrame, table_name: str = TABLE_NAME) -> int:
    """3.3.1 DROP + CREATE idempotente e inserción masiva con execute_values."""
    logger = get_run_logger()
    conn = None
    cur = None
    try:
        records = [
            tuple(_to_python(v) for v in row)
            for row in df[TABLE_COLUMNS].itertuples(index=False, name=None)
        ]
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS public.{table_name};")
        cur.execute(
            f"""
            CREATE TABLE public.{table_name} (
                video_id             TEXT PRIMARY KEY,
                country              CHAR(2) NOT NULL,
                title                TEXT,
                category_id          INTEGER NOT NULL,
                views                BIGINT  NOT NULL,
                likes                BIGINT  NOT NULL,
                dislikes             BIGINT  NOT NULL,
                comment_count        BIGINT  NOT NULL,
                days_to_trending     INTEGER NOT NULL CHECK (days_to_trending >= 0),
                trending_date_parsed TIMESTAMP   NOT NULL,
                publish_time_parsed  TIMESTAMPTZ NOT NULL
            );
            """
        )
        execute_values(
            cur,
            f"INSERT INTO public.{table_name} ({', '.join(TABLE_COLUMNS)}) VALUES %s",
            records,
        )
        conn.commit()
        logger.info(f"Inserted {len(records)} rows into {table_name}")
        return len(records)
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.error(f"load_data failed, transaction rolled back: {exc}")
        raise
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        logger.info("load_data finished (PostgreSQL connection closed).")


# ---------------------------------------------------------------------------
# 3.4 Analítica (OLAP con DuckDB)
# ---------------------------------------------------------------------------
@task(name="duckdb_fetch", cache_policy=NO_CACHE)
@timing_decorator
def duckdb_fetch(table_name: str = TABLE_NAME, expected_rows: int | None = None) -> pd.DataFrame:
    """3.4.1 Lee la tabla de PostgreSQL a través de DuckDB (ATTACH READ_ONLY)."""
    logger = get_run_logger()
    con = None
    try:
        dsn = " ".join(f"{k}={v}" for k, v in PG_CONFIG.items())
        con = duckdb.connect()
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")
        con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_ONLY);")
        df = con.execute(
            f"""
            SELECT video_id, country, title, category_id, views, likes, dislikes,
                   comment_count, days_to_trending, trending_date_parsed, publish_time_parsed
            FROM pg.public.{table_name}
            ORDER BY views DESC
            """
        ).df()
        logger.info(f"DuckDB fetched shape: {df.shape}")
        if expected_rows is not None:
            assert len(df) == expected_rows, (
                f"DuckDB returned {len(df)} rows, expected {expected_rows}"
            )
            logger.info(f"assert OK: {len(df)} rows == {expected_rows} inserted rows.")
        return df
    except Exception as exc:
        logger.error(f"duckdb_fetch failed: {exc}")
        raise
    finally:
        if con is not None:
            con.close()
        logger.info("duckdb_fetch finished (DuckDB connection closed).")


@task(name="aggregate_insights", cache_policy=NO_CACHE)
@timing_decorator
def aggregate_insights(df: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> dict:
    """3.4.2 Tres agregaciones GROUP BY, registradas y guardadas en CSV."""
    logger = get_run_logger()
    try:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        df = df.assign(engagement_ratio=(df["likes"] + df["dislikes"]) / (df["views"] + 1))

        videos_per_country = (
            df.groupby("country").size().rename("videos")
            .sort_values(ascending=False).reset_index()
        )
        median_days_per_country = (
            df.groupby("country")["days_to_trending"].median().rename("median_days_to_trending")
            .sort_values(ascending=False).reset_index()
        )
        engagement_per_category = (
            df.groupby("category_id")
            .agg(median_engagement_ratio=("engagement_ratio", "median"), videos=("video_id", "count"))
            .sort_values("median_engagement_ratio", ascending=False).reset_index()
        )

        results = {
            "videos_per_country": videos_per_country,
            "median_days_to_trending_per_country": median_days_per_country,
            "engagement_ratio_per_category": engagement_per_category,
        }
        for name, table in results.items():
            path = output_dir / f"{name}.csv"
            table.to_csv(path, index=False)
            logger.info(f"{name} (saved to {path.name}):\n{table.to_string(index=False)}")
        return results
    except Exception as exc:
        logger.error(f"aggregate_insights failed: {exc}")
        raise
    finally:
        logger.info("aggregate_insights finished.")


@task(name="cluster_videos", cache_policy=NO_CACHE)
@timing_decorator
def cluster_videos(df: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> dict:
    """3.4.3 KMeans sobre days_to_trending y engagement_ratio; K por silhouette."""
    logger = get_run_logger()
    try:
        df = df.copy()
        if "engagement_ratio" not in df:
            df["engagement_ratio"] = (df["likes"] + df["dislikes"]) / (df["views"] + 1)
        features = ["days_to_trending", "engagement_ratio"]
        X = df[features].astype(float)

        scores = {}
        for k in K_RANGE:
            labels = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10).fit_predict(X)
            scores[k] = silhouette_score(X, labels)
        logger.info("Silhouette by K: " + str({k: round(v, 4) for k, v in scores.items()}))

        best_k = max(scores, key=scores.get)
        logger.info(f"Best K = {best_k} (silhouette = {scores[best_k]:.4f})")

        model = KMeans(n_clusters=best_k, random_state=RANDOM_STATE, n_init=10).fit(X)
        df["cluster"] = model.labels_
        centroids = pd.DataFrame(model.cluster_centers_, columns=features)
        centroids["videos"] = df["cluster"].value_counts().sort_index().values
        logger.info(f"Centroid means per cluster:\n{centroids.to_string()}")

        # Gráfica de apoyo para el informe.
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        fig, ax = plt.subplots(figsize=(8, 5.5))
        for c in sorted(df["cluster"].unique()):
            part = df[df["cluster"] == c]
            ax.scatter(part["days_to_trending"], part["engagement_ratio"], s=30, alpha=0.8,
                       label=f"Cluster {c} (n={len(part)})")
        ax.scatter(centroids["days_to_trending"], centroids["engagement_ratio"], marker="X",
                   s=220, c="black", edgecolors="white", linewidths=1.2, label="Centroides")
        ax.set_xlabel("days_to_trending (días)")
        ax.set_ylabel("engagement_ratio = (likes + dislikes) / (views + 1)")
        ax.set_title(f"KMeans top 100 (K={best_k}, silhouette={scores[best_k]:.3f})")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(Path(output_dir) / "clusters_top100.png", dpi=150)
        plt.close(fig)

        return {"best_k": best_k, "silhouette": scores, "centroids": centroids}
    except Exception as exc:
        logger.error(f"cluster_videos failed: {exc}")
        raise
    finally:
        logger.info("cluster_videos finished.")


# ---------------------------------------------------------------------------
# 3.5 Flow
# ---------------------------------------------------------------------------
@flow(name="YouTube — Top-100 Multi-País", log_prints=True)
def youtube_pipeline():
    raw_path = extract_data()
    files = enumerate_country_files(raw_path)
    df_all = unify_countries(files)
    df_top = top100_global(df_all)
    df_top = standardize_dates(df_top)
    df_top = compute_days_to_trending(df_top)
    df_top = clean_records(df_top)
    inserted = load_data(df_top, TABLE_NAME)
    df_analysis = duckdb_fetch(TABLE_NAME, expected_rows=inserted)
    aggregate_insights(df_analysis)
    clusters = cluster_videos(df_analysis)
    return {"inserted_rows": inserted, "best_k": clusters["best_k"]}


if __name__ == "__main__":
    youtube_pipeline()
