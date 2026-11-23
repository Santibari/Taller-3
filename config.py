"""Configuración del pipeline de YouTube (Taller 3, parte II).

Las credenciales no se escriben en las tareas: se leen de variables de entorno
o de un archivo .env local (no versionado). Los valores por defecto apuntan al
contenedor PostgreSQL de la Sesión 2 (puerto 5433).
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def _load_dotenv(path: Path) -> None:
    """Carga KEY=VALUE de un .env sin sobrescribir variables ya definidas."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_dotenv(BASE_DIR / ".env")

# --- Extracción ---
KAGGLE_DATASET = "datasnaek/youtube-new"
DATA_DIR = BASE_DIR / "data"
EXPECTED_COUNTRIES = 10

# --- Carga (OLTP: PostgreSQL) ---
TABLE_NAME = "youtube_top100_global"
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5433")),
    "dbname": os.getenv("PG_DB", "bigdatatools1"),
    "user": os.getenv("PG_USER", "psqluser"),
    "password": os.getenv("PG_PASS", ""),  # se define en .env
}

# --- Analítica (OLAP: DuckDB) ---
OUTPUT_DIR = BASE_DIR / "analysis_outputs"
TOP_N = 100
K_RANGE = range(2, 9)  # K = 2..8
RANDOM_STATE = 42
