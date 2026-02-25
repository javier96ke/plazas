# ==============================================================================
# polars_precalc.py
# Pre-cálculo de estadísticas pesadas con Polars al arranque del servidor.
#
# Calcula UNA SOLA VEZ:
#   - Resumen nacional  (cn_resumen)
#   - Totales por estado con métricas CN + conectividad  (cn_por_estado)
#   - Top-N por métrica  (cn_top_estados)
#   - Estado destacado por métrica  (cn_estados_destacados)
#   - Top-5 todas las métricas  (cn_top5_todos)
#   - Distribución por situación  (situacion_distribucion)
#
# Los 5 endpoints de app.py leen directamente los dicts en memoria → O(1).
# Ningún endpoint vuelve a tocar el DataFrame.
#
# Uso:
#   from polars_precalc import stats_cache
#   stats_cache.build(parquet_path)          # al arranque
#   stats_cache.get_cn_resumen()             # en el endpoint
# ==============================================================================

import logging
import threading
import os
from typing import Optional, Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Importar Polars
# ---------------------------------------------------------------------------
try:
    import polars as pl
    POLARS_OK = True
except ImportError:
    pl = None
    POLARS_OK = False
    logger.warning("⚠️  polars_precalc: polars no instalado. pip install polars")

# ---------------------------------------------------------------------------
# FIX: El parquet usa nombres NUEVOS (cortos), NO los nombres legacy.
# polars_precalc lee el parquet directamente, antes del ColumnTranslator,
# por lo que debe usar los nombres originales del parquet.
# ---------------------------------------------------------------------------
_C = {
    "estado":      "estado_id",      # int64 → se traduce con _ESTADO_MAP
    "situacion":   "situacion",      # int64 → 1 = EN OPERACIÓN
    "conectividad":"conectividad",   # categorical
    "cn_ini":      "cn_inicial",     # int64
    "cn_prim":     "cn_prim",        # int64
    "cn_sec":      "cn_sec",         # int64
    "cve_mes":     "Cve-mes",        # int64
    "clave":       "clave",          # categorical
}

# FIX: situacion es int en el parquet nuevo, no string
_SITUACION_OPERACION_INT = 1   # 1 = "EN OPERACIÓN"

# Mapa estado_id (int) → nombre textual
# (mismo que usa ColumnTranslator — mantenido en sync)
_ESTADO_MAP: dict[int, str] = {
    1:  "Aguascalientes",
    2:  "Baja California",
    3:  "Baja California Sur",
    4:  "Campeche",
    5:  "Coahuila",
    6:  "Colima",
    7:  "Chiapas",
    8:  "Chihuahua",
    9:  "Ciudad de México",
    10: "Durango",
    11: "Guanajuato",
    12: "Guerrero",
    13: "Hidalgo",
    14: "Jalisco",
    15: "México",
    16: "Michoacán",
    17: "Morelos",
    18: "Nayarit",
    19: "Nuevo León",
    20: "Oaxaca",
    21: "Puebla",
    22: "Querétaro",
    23: "Quintana Roo",
    24: "San Luis Potosí",
    25: "Sinaloa",
    26: "Sonora",
    27: "Tabasco",
    28: "Tamaulipas",
    29: "Tlaxcala",
    30: "Veracruz",
    31: "Yucatán",
    32: "Zacatecas",
}

# Mapa situacion (int) → texto
_SITUACION_MAP: dict[int, str] = {
    0: "SUSPENSIÓN TEMPORAL",
    1: "EN OPERACIÓN",
    2: "EN PROCESO DE APERTURA",
    3: "BAJA DEFINITIVA",
    4: "REUBICACIÓN",
}


# ---------------------------------------------------------------------------
# Helper: traducir estado_id int → nombre en un DataFrame Polars
# ---------------------------------------------------------------------------
def _agregar_nombre_estado(df: "pl.DataFrame", col_id: str, col_out: str = "__estado_nombre") -> "pl.DataFrame":
    """
    Añade una columna de texto con el nombre del estado a partir de estado_id (int).
    Usa pl.when/then en cadena para evitar UDFs lentos.
    """
    expr = pl.lit("Desconocido")
    for k, v in _ESTADO_MAP.items():
        expr = pl.when(pl.col(col_id) == k).then(pl.lit(v)).otherwise(expr)
    return df.with_columns(expr.alias(col_out))


# ---------------------------------------------------------------------------
# Clase principal
# ---------------------------------------------------------------------------

class StatsCache:
    """
    Caché de estadísticas pre-calculadas con Polars.
    Thread-safe: RLock para lecturas concurrentes.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._ready = False

        self._cn_resumen:            Optional[dict] = None
        self._cn_por_estado:         Optional[dict] = None
        self._cn_top_estados:        Optional[dict] = None
        self._cn_estados_destacados: Optional[dict] = None
        self._cn_top5_todos:         Optional[dict] = None
        self._situacion_dist:        Optional[dict] = None

    # ------------------------------------------------------------------
    # Construcción
    # ------------------------------------------------------------------

    def build(self, parquet_path: str) -> bool:
        """
        Lee el parquet con Polars y pre-calcula todas las estadísticas.
        """
        if not POLARS_OK:
            logger.error("polars_precalc: Polars no disponible.")
            return False

        if not os.path.exists(parquet_path):
            logger.error(f"polars_precalc: parquet no encontrado: {parquet_path}")
            return False

        logger.info(f"🔨 polars_precalc: iniciando pre-cálculo desde {parquet_path}…")

        try:
            # ── Leer solo columnas necesarias ──────────────────────────
            cols_necesarias = list(dict.fromkeys(
                v for v in _C.values() if v is not None
            ))
            # Filtrar a columnas que realmente existen en el parquet
            schema = pl.read_parquet_schema(parquet_path)
            cols_a_leer = [c for c in cols_necesarias if c in schema]
            cols_faltantes = [c for c in cols_necesarias if c not in schema]
            if cols_faltantes:
                logger.warning(f"⚠️  polars_precalc: columnas no encontradas en parquet: {cols_faltantes}")

            df_full = pl.read_parquet(parquet_path, columns=cols_a_leer)
            logger.info(f"  DF completo: {len(df_full):,} filas")

            # ── Asegurar tipos numéricos en columnas CN ─────────────────
            for key in ["cn_ini", "cn_prim", "cn_sec"]:
                col = _C[key]
                if col in df_full.columns:
                    df_full = df_full.with_columns(
                        pl.col(col).cast(pl.Float64, strict=False).fill_null(0).alias(col)
                    )

            # ── Filtrar al último mes ───────────────────────────────────
            df_actual = df_full
            cve_col = _C["cve_mes"]
            if cve_col in df_full.columns:
                df_actual = df_full.with_columns(
                    pl.col(cve_col).cast(pl.Float64, strict=False).alias("__cve_mes_num")
                )
                max_mes = df_actual["__cve_mes_num"].max()
                df_actual = (df_actual
                             .filter(pl.col("__cve_mes_num") == max_mes)
                             .drop("__cve_mes_num"))
                logger.info(f"  Último mes ({max_mes}): {len(df_actual):,} filas")

            # ── Calcular bloques ────────────────────────────────────────
            cn_resumen            = self._calc_cn_resumen(df_full, df_actual)
            cn_por_estado         = self._calc_cn_por_estado(df_full, df_actual)
            cn_top_estados        = self._calc_cn_top_estados(df_full)
            cn_estados_destacados = self._calc_cn_estados_destacados(df_full)
            cn_top5_todos         = self._calc_cn_top5_todos(df_full)
            situacion_dist        = self._calc_situacion_dist(df_actual)

            # ── Commit atómico ──────────────────────────────────────────
            with self._lock:
                self._cn_resumen            = cn_resumen
                self._cn_por_estado         = cn_por_estado
                self._cn_top_estados        = cn_top_estados
                self._cn_estados_destacados = cn_estados_destacados
                self._cn_top5_todos         = cn_top5_todos
                self._situacion_dist        = situacion_dist
                self._ready                 = True

            logger.info("✅ polars_precalc: todas las estadísticas pre-calculadas y listas.")
            return True

        except Exception as exc:
            logger.error(f"❌ polars_precalc.build: {exc}", exc_info=True)
            return False

    def rebuild(self, parquet_path: str) -> bool:
        """Re-calcula con datos frescos sin interrumpir lecturas activas."""
        logger.info("🔄 polars_precalc: reconstruyendo estadísticas…")
        tmp = StatsCache()
        ok = tmp.build(parquet_path)
        if ok:
            with self._lock:
                self._cn_resumen            = tmp._cn_resumen
                self._cn_por_estado         = tmp._cn_por_estado
                self._cn_top_estados        = tmp._cn_top_estados
                self._cn_estados_destacados = tmp._cn_estados_destacados
                self._cn_top5_todos         = tmp._cn_top5_todos
                self._situacion_dist        = tmp._situacion_dist
                self._ready                 = True
            logger.info("✅ polars_precalc: reconstrucción completada.")
        return ok

    # ------------------------------------------------------------------
    # Cálculos internos
    # ------------------------------------------------------------------

    def _calc_cn_resumen(self, df_full: "pl.DataFrame", df_actual: "pl.DataFrame") -> dict:
        col_ini  = _C["cn_ini"]
        col_prim = _C["cn_prim"]
        col_sec  = _C["cn_sec"]
        col_sit  = _C["situacion"]

        def _resumen_col(col: str, df: "pl.DataFrame", df_act: "pl.DataFrame") -> dict:
            n = len(df)
            if col not in df.columns:
                return {"total_registros": n, "nulos": n, "pct_nulos": 100.0,
                        "suma": 0.0, "plazasOperacion": 0}
            nulos    = int(df[col].is_null().sum())
            suma     = float(df[col].fill_null(0).sum())
            plazas_op = 0
            if col in df_act.columns and col_sit in df_act.columns:
                # FIX: situacion es int, comparar con int
                plazas_op = int(
                    df_act.filter(
                        (pl.col(col_sit) == _SITUACION_OPERACION_INT)
                        & (pl.col(col).fill_null(0) > 0)
                    ).height
                )
            return {
                "total_registros": n,
                "nulos":           nulos,
                "pct_nulos":       round(nulos / n * 100, 2) if n else 0.0,
                "suma":            round(suma, 2),
                "plazasOperacion": plazas_op,
            }

        resumen = {
            "CN_Inicial_Acum": _resumen_col(col_ini,  df_full, df_actual),
            "CN_Prim_Acum":    _resumen_col(col_prim, df_full, df_actual),
            "CN_Sec_Acum":     _resumen_col(col_sec,  df_full, df_actual),
        }

        cn_total = sum(v["suma"] for v in resumen.values())
        plazas_op_total = 0
        if col_sit in df_actual.columns:
            # FIX: comparar int directamente
            plazas_op_total = int(
                df_actual.filter(pl.col(col_sit) == _SITUACION_OPERACION_INT).height
            )

        resumen["CN_Total"] = {
            "total_registros": len(df_full),
            "nulos":           0,
            "pct_nulos":       0.0,
            "suma":            round(cn_total, 2),
            "plazasOperacion": plazas_op_total,
        }

        # Top-5 estados por CN_Total — con nombres textuales
        top5 = []
        col_est = _C["estado"]
        if col_est in df_full.columns:
            cols_cn = [c for c in [col_ini, col_prim, col_sec] if c in df_full.columns]
            df_top = (
                df_full
                .with_columns(
                    sum(pl.col(c).fill_null(0) for c in cols_cn).alias("__cn_total")
                )
                .group_by(col_est)
                .agg(pl.col("__cn_total").sum().alias("suma_CN_Total"))
                .sort("suma_CN_Total", descending=True)
                .head(5)
            )
            top5 = [
                {
                    "estado":        _ESTADO_MAP.get(int(r[col_est]), f"Estado_{r[col_est]}"),
                    "suma_CN_Total": float(r["suma_CN_Total"]),
                }
                for r in df_top.iter_rows(named=True)
            ]

        return {"resumen_nacional": resumen, "top5_estados_por_CN_Total": top5}

    def _calc_cn_por_estado(self, df_full: "pl.DataFrame", df_actual: "pl.DataFrame") -> dict:
        col_est  = _C["estado"]
        col_sit  = _C["situacion"]
        col_con  = _C["conectividad"]
        col_ini  = _C["cn_ini"]
        col_prim = _C["cn_prim"]
        col_sec  = _C["cn_sec"]

        if col_est not in df_actual.columns:
            return {"status": "error", "estados": [], "metadata": {}}

        # Conteo total por estado_id (último mes)
        conteo = (df_actual.group_by(col_est)
                  .agg(pl.len().alias("total_plazas"))
                  .to_pandas().set_index(col_est)["total_plazas"].to_dict())

        # Plazas en operación
        plazas_op = {}
        if col_sit in df_actual.columns:
            # FIX: comparar int directamente
            plazas_op = (
                df_actual
                .filter(pl.col(col_sit) == _SITUACION_OPERACION_INT)
                .group_by(col_est)
                .agg(pl.len().alias("op"))
                .to_pandas().set_index(col_est)["op"].to_dict()
            )

        # Conectividad (en operación, último mes)
        conect_dict = {}
        if col_con in df_actual.columns and col_sit in df_actual.columns:
            df_op = df_actual.filter(pl.col(col_sit) == _SITUACION_OPERACION_INT)
            # conectividad es categorical: valores "positivos" son cualquier valor
            # que no sea vacío, "0", "false", "no", "nan", "none"
            conect_dict = (
                df_op.filter(
                    ~pl.col(col_con)
                    .cast(pl.Utf8)
                    .str.strip_chars()
                    .str.to_lowercase()
                    .is_in(["nan", "none", "0", "false", "no", ""])
                )
                .group_by(col_est)
                .agg(pl.len().alias("conect"))
                .to_pandas().set_index(col_est)["conect"].to_dict()
            )

        # Totales CN histórico por estado
        cols_cn = [c for c in [col_ini, col_prim, col_sec] if c in df_full.columns]
        agg_exprs = [pl.col(c).fill_null(0).sum().alias(c) for c in cols_cn]
        df_grp = df_full.group_by(col_est).agg(agg_exprs)

        cn_nacional = float(sum(df_full[c].fill_null(0).sum() for c in cols_cn))

        summary = []
        for row in df_grp.iter_rows(named=True):
            eid    = row[col_est]
            # FIX: traducir estado_id int → nombre textual
            nombre = _ESTADO_MAP.get(int(eid), f"Estado_{eid}")
            total  = int(conteo.get(eid, 0))
            op     = int(plazas_op.get(eid, 0))
            conect = int(conect_dict.get(eid, 0))
            si     = float(row.get(col_ini,  0) or 0)
            sp     = float(row.get(col_prim, 0) or 0)
            ss     = float(row.get(col_sec,  0) or 0)
            st     = si + sp + ss
            summary.append({
                "estado":               nombre,
                "estado_id":            int(eid),
                "total_plazas":         total,
                "plazas_operacion":     op,
                "conectados_actual":    conect,
                "pct_conectividad":     round(conect / op * 100, 1) if op else 0.0,
                "suma_CN_Inicial_Acum": int(si),
                "suma_CN_Prim_Acum":    int(sp),
                "suma_CN_Sec_Acum":     int(ss),
                "suma_CN_Total":        int(st),
                "pct_sobre_nacional":   round(st / cn_nacional * 100, 2) if cn_nacional else 0.0,
            })

        summary.sort(key=lambda x: x["suma_CN_Total"], reverse=True)
        return {
            "status":   "success",
            "estados":  summary,
            "metadata": {
                "cn_total_nacional":        int(cn_nacional),
                "total_estados_procesados": len(summary),
            },
        }

    def _calc_cn_top_estados(self, df_full: "pl.DataFrame") -> dict:
        col_est = _C["estado"]
        metricas = {
            "inicial": _C["cn_ini"],
            "prim":    _C["cn_prim"],
            "sec":     _C["cn_sec"],
        }
        result = {}
        for metric_key, col in metricas.items():
            if col not in df_full.columns or col_est not in df_full.columns:
                result[metric_key] = []
                continue
            grp = (df_full.group_by(col_est)
                   .agg(pl.col(col).fill_null(0).sum().alias("valor"))
                   .sort("valor", descending=True))
            result[metric_key] = [
                {
                    # FIX: traducir estado_id → nombre
                    "estado": _ESTADO_MAP.get(int(r[col_est]), f"Estado_{r[col_est]}"),
                    "valor":  float(r["valor"]),
                }
                for r in grp.iter_rows(named=True)
            ]
        return result

    def _calc_cn_estados_destacados(self, df_full: "pl.DataFrame") -> dict:
        col_est = _C["estado"]
        metricas = {
            "CN_Inicial_Acum": _C["cn_ini"],
            "CN_Prim_Acum":    _C["cn_prim"],
            "CN_Sec_Acum":     _C["cn_sec"],
        }
        result = {}
        for nombre, col in metricas.items():
            if col not in df_full.columns or col_est not in df_full.columns:
                result[nombre] = {"estado": "", "valor": 0.0}
                continue
            grp = (df_full.group_by(col_est)
                   .agg(pl.col(col).fill_null(0).sum().alias("valor"))
                   .sort("valor", descending=True)
                   .head(1))
            if grp.height > 0:
                row = grp.row(0, named=True)
                eid = row[col_est]
                result[nombre] = {
                    # FIX: traducir
                    "estado": _ESTADO_MAP.get(int(eid), f"Estado_{eid}"),
                    "valor":  float(row["valor"]),
                }
            else:
                result[nombre] = {"estado": "", "valor": 0.0}
        return result

    def _calc_cn_top5_todos(self, df_full: "pl.DataFrame") -> dict:
        col_est = _C["estado"]
        pares = [
            ("inicial",    _C["cn_ini"]),
            ("primaria",   _C["cn_prim"]),
            ("secundaria", _C["cn_sec"]),
        ]
        result = {}
        for cat, col in pares:
            if col not in df_full.columns or col_est not in df_full.columns:
                result[cat] = []
                continue
            grp = (df_full.group_by(col_est)
                   .agg(pl.col(col).fill_null(0).sum().alias("valor"))
                   .sort("valor", descending=True)
                   .head(5))
            result[cat] = [
                {
                    # FIX: traducir
                    "estado": _ESTADO_MAP.get(int(r[col_est]), f"Estado_{r[col_est]}"),
                    "valor":  float(r["valor"]),
                }
                for r in grp.iter_rows(named=True)
            ]
        return result

    def _calc_situacion_dist(self, df_actual: "pl.DataFrame") -> dict:
        """Distribución de plazas por situación en el último mes."""
        col_sit = _C["situacion"]
        if col_sit not in df_actual.columns:
            return {}

        grp = (df_actual.group_by(col_sit)
               .agg(pl.len().alias("cantidad"))
               .sort("cantidad", descending=True))

        total = len(df_actual)
        return {
            "total": total,
            "distribucion": [
                {
                    # FIX: traducir int → texto de situación
                    "situacion":  _SITUACION_MAP.get(int(r[col_sit]), f"Situación_{r[col_sit]}"),
                    "cantidad":   int(r["cantidad"]),
                    "porcentaje": round(r["cantidad"] / total * 100, 2) if total else 0.0,
                }
                for r in grp.iter_rows(named=True)
            ],
        }

    # ------------------------------------------------------------------
    # Getters públicos — O(1)
    # ------------------------------------------------------------------

    @property
    def is_ready(self) -> bool:
        return self._ready

    def get_cn_resumen(self) -> Optional[dict]:
        with self._lock:
            return self._cn_resumen

    def get_cn_por_estado(self) -> Optional[dict]:
        with self._lock:
            return self._cn_por_estado

    def get_cn_top_estados(self, metric: str, n: int) -> Optional[dict]:
        with self._lock:
            if self._cn_top_estados is None:
                return None
            lista = self._cn_top_estados.get(metric.lower(), [])
            return {"metric": metric, "top": lista[:n]}

    def get_cn_estados_destacados(self) -> Optional[dict]:
        with self._lock:
            return self._cn_estados_destacados

    def get_cn_top5_todos(self) -> Optional[dict]:
        with self._lock:
            return self._cn_top5_todos

    def get_situacion_dist(self) -> Optional[dict]:
        with self._lock:
            return self._situacion_dist


# ---------------------------------------------------------------------------
# Instancia global
# ---------------------------------------------------------------------------
stats_cache = StatsCache()