# ==============================================================================
# rust_bridge.py  v2
# Puente entre Polars (lectura del parquet) y el motor Rust (plaza_rust.so)
#
# Responsabilidades:
#   1. Leer las columnas NUMÉRICAS del parquet con Polars (rápido, columnar)
#   2. Pasar esos arrays al motor Rust vía init_engine()
#   3. Exponer una API limpia a app.py para las 3 operaciones matemáticas
#   4. Traducir estado_id → nombre usando el ColumnTranslator existente
#
# Cambios v2:
#   - Warm-up de coordenadas desde JSON en _warm_coordenadas_json()
#     → evita tocar el DataFrame para el caché de coords al arranque
#   - distancias_cercanas usa parámetros posicionales para evitar ambigüedad
#     con la firma de PyO3 (Rust no soporta kwargs nativamente)
#   - _coordenadas_json_cache: lista en RAM para lookups de nombre/estado
#     sin acceder al DataFrame cuando Rust devuelve solo índice
# ==============================================================================
import logging
import os
from typing import Optional
import sys


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Importar la extensión Rust
# ---------------------------------------------------------------------------
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

try:
    import plaza_rust as _rust
    RUST_AVAILABLE = True
    logger.info("✅ rust_bridge: plaza_rust cargado")
except ImportError as e:
    # Intento 2: buscar el .pyd compilado en cualquier subcarpeta de _APP_DIR
    import glob
    _pyd_matches = glob.glob(os.path.join(_APP_DIR, "**", "plaza_rust*.pyd"), recursive=True) + \
                   glob.glob(os.path.join(_APP_DIR, "**", "plaza_rust*.so"),  recursive=True)
    _rust = None
    RUST_AVAILABLE = False
    if _pyd_matches:
        _pyd_dir = os.path.dirname(_pyd_matches[0])
        if _pyd_dir not in sys.path:
            sys.path.insert(0, _pyd_dir)
        try:
            import plaza_rust as _rust
            RUST_AVAILABLE = True
            logger.info(f"✅ rust_bridge: plaza_rust cargado desde {_pyd_dir}")
        except ImportError as e2:
            logger.warning(
                f"⚠️  rust_bridge: plaza_rust no disponible ({e2}). "
                "Ejecuta: python build_rust.py"
            )
    else:
        logger.warning(
            f"⚠️  rust_bridge: plaza_rust.pyd no encontrado ({e}). "
            "Ejecuta: python build_rust.py"
        )
# ---------------------------------------------------------------------------
# Importar Polars (solo para lectura del parquet)
# ---------------------------------------------------------------------------
try:
    import polars as pl
    POLARS_AVAILABLE = True
    logger.info("✅ rust_bridge: polars disponible")
except ImportError:
    pl = None
    POLARS_AVAILABLE = False
    logger.warning("⚠️  rust_bridge: polars no instalado. pip install polars")

# ---------------------------------------------------------------------------
# Importar orjson para leer el JSON de coordenadas (más rápido que stdlib)
# ---------------------------------------------------------------------------
try:
    import orjson as _orjson
    ORJSON_AVAILABLE = True
except ImportError:
    import json as _orjson          # fallback transparente
    ORJSON_AVAILABLE = False
    logger.warning("⚠️  rust_bridge: orjson no disponible — usando json stdlib")

# ---------------------------------------------------------------------------
# Importar el Translator para mapear estado_id → nombre
# ---------------------------------------------------------------------------
try:
    from translator import translator as _translator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    _translator = None
    TRANSLATOR_AVAILABLE = False
    logger.warning("⚠️  rust_bridge: translator.py no disponible")

# ---------------------------------------------------------------------------
# Columnas que Polars extrae del parquet NUEVO (ya optimizado, nombres cortos)
# ---------------------------------------------------------------------------
_COLUMNAS_RUST = {
    "lat":        "lat",
    "lng":        "lng",
    "estado_id":  "estado_id",
    "situacion":  "situacion",
    "inc_total":  "inc_total",
    "aten_total": "aten_total",
    "cn_total":   "cn_total",
    "cve_mes":    "Cve-mes",
}

# Columnas del parquet LEGACY (nombres ya traducidos por ColumnTranslator)
_COLUMNAS_RUST_LEGACY = {
    "lat":        "Latitud",
    "lng":        "Longitud",
    "estado_id":  "Clave_Edo",
    "situacion":  "Situación",
    "inc_total":  "Inc_Total",
    "aten_total": "Aten_Total",
    "cn_total":   "CN_Tot_Acum",
    "cve_mes":    "Cve-mes",
}

# ---------------------------------------------------------------------------
# FIX CRÍTICO: _initialized = False al inicio del módulo.
# El valor True anterior causaba que distancias_cercanas() llamara a Rust
# aunque ENGINE estuviera vacío → "Motor no init."
# ---------------------------------------------------------------------------
_initialized: bool = False
_n_registros: int  = 0

# Caché en RAM del JSON de coordenadas
_coordenadas_json_cache: list[dict] | None = None

# Ruta por defecto al JSON
ARCHIVO_COORDENADAS = "datos/coordenadasplazas.json"


# ===========================================================================
# DETECCIÓN AUTOMÁTICA DE ESQUEMA
# ===========================================================================
def _detectar_esquema_parquet(parquet_path: str) -> bool:
    """
    Lee el schema del parquet y decide si usar columnas NUEVO o LEGACY.
    Retorna True si debe usarse esquema LEGACY, False para esquema NUEVO.
    Si no puede leer el schema, retorna False (intenta NUEVO).
    """
    if not POLARS_AVAILABLE:
        return False
    try:
        schema = pl.read_parquet_schema(parquet_path)
        columnas = set(schema.keys())
        # Si tiene "lat" y "lng" (nombres cortos) → esquema NUEVO
        if "lat" in columnas and "lng" in columnas:
            logger.info("🔍 rust_bridge: esquema NUEVO detectado (lat/lng)")
            return False
        # Si tiene "Latitud" y "Longitud" (nombres largos) → esquema LEGACY
        if "Latitud" in columnas and "Longitud" in columnas:
            logger.info("🔍 rust_bridge: esquema LEGACY detectado (Latitud/Longitud)")
            return True
        # Fallback: si tiene Estado/Situación probablemente es legacy
        if "Estado" in columnas or "Situación" in columnas:
            logger.info("🔍 rust_bridge: esquema LEGACY detectado por columnas Estado/Situación")
            return True
        logger.warning("⚠️  rust_bridge: no se pudo determinar esquema, usando NUEVO por defecto")
        return False
    except Exception as exc:
        logger.warning(f"⚠️  rust_bridge: error detectando esquema: {exc} — usando NUEVO")
        return False


# ===========================================================================
# WARM-UP: JSON de coordenadas
# ===========================================================================
def warm_coordenadas_json(archivo: str | None = None) -> int:
    """
    Lee coordenadasplazas.json y lo guarda en _coordenadas_json_cache.
    Retorna el número de entradas cargadas (0 si falla o no existe).
    """
    global _coordenadas_json_cache

    path = archivo or ARCHIVO_COORDENADAS
    if not os.path.exists(path):
        logger.debug(f"rust_bridge.warm_coordenadas_json: {path} no encontrado")
        return 0

    try:
        if ORJSON_AVAILABLE:
            with open(path, "rb") as f:
                datos = _orjson.loads(f.read())
        else:
            with open(path, "r", encoding="utf-8") as f:
                datos = _orjson.load(f)

        if not isinstance(datos, list) or not datos:
            logger.warning("rust_bridge: coordenadasplazas.json vacío o no es lista")
            return 0

        muestra = datos[0]
        if "lat" not in muestra or "lng" not in muestra:
            logger.warning("rust_bridge: coordenadasplazas.json sin campos lat/lng")
            return 0

        resultado = []
        for item in datos:
            try:
                resultado.append({
                    "clave":     str(item.get("clave",     "") or ""),
                    "nombre":    str(item.get("nombre",    "") or ""),
                    "estado":    str(item.get("estado",    "") or ""),
                    "municipio": str(item.get("municipio", "") or ""),
                    "localidad": str(item.get("localidad", "") or ""),
                    "situacion": str(item.get("situacion", "") or ""),
                    "lat":       round(float(item["lat"]), 6),
                    "lng":       round(float(item["lng"]), 6),
                })
            except (KeyError, TypeError, ValueError):
                continue

        _coordenadas_json_cache = resultado
        logger.info(f"✅ rust_bridge: {len(resultado)} coords cargadas desde JSON")
        return len(resultado)

    except Exception as exc:
        logger.error(f"❌ rust_bridge.warm_coordenadas_json: {exc}")
        _coordenadas_json_cache = None
        return 0


def get_coordenadas_json() -> list[dict]:
    if _coordenadas_json_cache is None:
        warm_coordenadas_json()
    return _coordenadas_json_cache or []


def invalidar_coordenadas_json():
    global _coordenadas_json_cache
    _coordenadas_json_cache = None
    logger.debug("rust_bridge: caché de coordenadas JSON invalidado")


# ===========================================================================
# INICIALIZACIÓN
# ===========================================================================
def init(parquet_path: str, usar_legacy: bool | None = None) -> bool:
    """
    Lee las columnas numéricas del parquet con Polars y carga el motor Rust.

    Args:
        parquet_path : ruta al archivo .parquet
        usar_legacy  : None (autodetectar), True (legacy), False (nuevo)

    Returns:
        True si la inicialización fue exitosa.
    """
    global _initialized, _n_registros

    if not RUST_AVAILABLE:
        logger.error("rust_bridge.init: Rust no disponible")
        return False

    if not POLARS_AVAILABLE:
        logger.error("rust_bridge.init: Polars no disponible")
        return False

    if not os.path.exists(parquet_path):
        logger.error(f"rust_bridge.init: parquet no encontrado: {parquet_path}")
        return False

    # Warm-up JSON antes de inicializar el motor
    warm_coordenadas_json()

    # FIX: autodetección de esquema si usar_legacy no se especifica
    if usar_legacy is None:
        usar_legacy = _detectar_esquema_parquet(parquet_path)

    try:
        col_map = _COLUMNAS_RUST_LEGACY if usar_legacy else _COLUMNAS_RUST

        cve_mes_col     = col_map.get("cve_mes")
        columnas_a_leer = list(dict.fromkeys(v for v in col_map.values() if v is not None))

        # Filtrar a columnas que existen en el parquet para evitar errores
        schema_cols = set(pl.read_parquet_schema(parquet_path).keys())
        columnas_a_leer = [c for c in columnas_a_leer if c in schema_cols]
        cols_faltantes  = [c for c in col_map.values() if c and c not in schema_cols]
        if cols_faltantes:
            logger.warning(f"⚠️  rust_bridge: columnas no encontradas en parquet: {cols_faltantes}")

        if not columnas_a_leer:
            logger.error("❌ rust_bridge.init: ninguna columna necesaria encontrada en el parquet")
            _initialized = False
            return False

        logger.info(f"🔍 rust_bridge: leyendo columnas {columnas_a_leer} con Polars…")
        df_polars = pl.read_parquet(parquet_path, columns=columnas_a_leer)
        logger.info(f"✅ rust_bridge: Polars leyó {len(df_polars):,} filas")

        # Filtrar al último mes
        if cve_mes_col and cve_mes_col in df_polars.columns:
            df_polars = df_polars.with_columns(
                pl.col(cve_mes_col).cast(pl.Float64, strict=False).alias("__cve_mes_num")
            )
            max_mes = df_polars["__cve_mes_num"].max()
            df_polars = df_polars.filter(
                pl.col("__cve_mes_num") == max_mes
            ).drop("__cve_mes_num")
            logger.info(
                f"✅ rust_bridge: filtrado a último mes ({max_mes}): "
                f"{len(df_polars):,} filas"
            )
        else:
            logger.warning("⚠️  rust_bridge: Cve-mes no encontrada → se cargan todos los meses")

        if len(df_polars) == 0:
            logger.error("❌ rust_bridge.init: DataFrame vacío tras filtrar — motor no se inicializa")
            _initialized = False
            return False

        _I64_MIN = -9223372036854775808

        def get_col_list(key: str, dtype_cast=None) -> list:
            col_name = col_map.get(key)
            if col_name is None or col_name not in df_polars.columns:
                logger.debug(f"rust_bridge: columna '{key}' ({col_name}) no encontrada → centinelas")
                if key in ("lat", "lng"):
                    return [float("nan")] * len(df_polars)
                return [_I64_MIN] * len(df_polars)
            series = df_polars[col_name]
            if dtype_cast:
                series = series.cast(dtype_cast, strict=False)
            if key in ("lat", "lng"):
                return series.fill_null(float("nan")).to_list()
            return series.fill_null(_I64_MIN).to_list()

        lats         = get_col_list("lat")
        lngs         = get_col_list("lng")
        estado_ids   = get_col_list("estado_id",  pl.Int64)
        situaciones  = get_col_list("situacion",  pl.Int64)
        inc_totales  = get_col_list("inc_total",  pl.Int64)
        aten_totales = get_col_list("aten_total", pl.Int64)
        cn_totales   = get_col_list("cn_total",   pl.Int64)

        # Sanity check: al menos lat/lng deben tener datos válidos
        lats_validos = sum(1 for v in lats if v == v)  # NaN != NaN
        if lats_validos == 0:
            logger.error("❌ rust_bridge.init: todas las latitudes son NaN — revisa el esquema del parquet")
            _initialized = False
            return False

        logger.info(f"🔨 rust_bridge: inicializando motor Rust con {len(lats):,} filas ({lats_validos:,} coords válidas)…")
        n = _rust.init_engine(
            lats, lngs,
            estado_ids, situaciones,
            inc_totales, aten_totales, cn_totales,
        )

        _initialized = True
        _n_registros = n
        logger.info(f"✅ rust_bridge: motor Rust listo con {n:,} registros")
        return True

    except Exception as exc:
        # FIX: asegurar que _initialized quede False si init falla
        _initialized = False
        logger.error(f"❌ rust_bridge.init: {exc}", exc_info=True)
        return False


def rebuild(parquet_path: str, usar_legacy: bool | None = None) -> bool:
    """Reconstruye el motor Rust con datos frescos."""
    global _initialized
    _initialized = False
    invalidar_coordenadas_json()
    return init(parquet_path, usar_legacy)


# ===========================================================================
# API PÚBLICA
# ===========================================================================

def distancias_cercanas(
    lat:      float,
    lng:      float,
    radio_km: float = 50.0,
    limite:   int   = 20,
) -> list[dict]:
    """
    Plazas dentro de `radio_km` km del punto dado usando Haversine paralelo (Rayon).
    Retorna lista vacía si el motor no está listo (en lugar de propagar excepción).
    """
    if not _initialized or not RUST_AVAILABLE:
        return []

    try:
        resultados = _rust.distancias_cercanas(lat, lng, radio_km, limite)
        return [{"indice_df": idx, "distancia_km": dist} for idx, dist in resultados]
    except Exception as exc:
        logger.error(f"❌ rust_bridge.distancias_cercanas: {exc}")
        return []


def enriquecer_con_json(indice_df: int, distancia_km: float) -> dict | None:
    """
    Enriquece un resultado de distancias_cercanas con metadatos del JSON.
    Retorna None si el índice está fuera de rango.
    """
    coords = get_coordenadas_json()
    if not coords or indice_df >= len(coords):
        return None
    item = coords[indice_df]
    return {**item, "distancia_km": round(distancia_km, 2),
            "distancia_formateada": f"{distancia_km:.1f} km"}


def agregaciones_por_estado(filtro_situacion: int = -1) -> list[dict]:
    """Conteos y totales agrupados por estado."""
    if not _initialized or not RUST_AVAILABLE:
        return []

    try:
        raw = _rust.agregaciones_por_estado(filtro_situacion)
    except Exception as exc:
        logger.error(f"❌ rust_bridge.agregaciones_por_estado: {exc}")
        return []

    estado_map = {}
    if TRANSLATOR_AVAILABLE and _translator:
        estado_map = _translator.estado_map

    resultado = []
    for estado_id, metricas in raw.items():
        nombre = estado_map.get(int(estado_id), f"Estado_{estado_id}")
        resultado.append({
            "estado_id":  int(estado_id),
            "nombre":     nombre,
            "plazas":     int(metricas.get("plazas", 0)),
            "inc_total":  int(metricas.get("inc_total", 0)),
            "aten_total": int(metricas.get("aten_total", 0)),
            "cn_total":   int(metricas.get("cn_total", 0)),
        })

    resultado.sort(key=lambda x: x["plazas"], reverse=True)
    return resultado


def filtrar_indices(estado_id: int = -1, situacion: int = -1) -> list[int]:
    """Filtra por estado y/o situación y devuelve índices de filas."""
    if not _initialized or not RUST_AVAILABLE:
        return []

    try:
        return _rust.filtrar_indices(estado_id, situacion)
    except Exception as exc:
        logger.error(f"❌ rust_bridge.filtrar_indices: {exc}")
        return []


def engine_stats() -> dict:
    """Estadísticas del motor para health-check y monitoreo."""
    coords_json = get_coordenadas_json()
    base = {
        "rust_disponible":         RUST_AVAILABLE,
        "polars_disponible":       POLARS_AVAILABLE,
        "inicializado":            _initialized,
        "registros":               _n_registros,
        "coordenadas_json_listas": len(coords_json),
        "orjson_activo":           ORJSON_AVAILABLE,
    }
    if _initialized and RUST_AVAILABLE:
        try:
            base.update(_rust.engine_stats())
        except Exception:
            pass
    return base


def is_ready() -> bool:
    """True si el motor está inicializado y listo para responder."""
    return _initialized and RUST_AVAILABLE
