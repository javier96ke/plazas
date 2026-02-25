# ==============================================================================
# comparativas_engine.py  v5.2
#
# CAMBIOS v5.2 respecto a v5.1:
#   - NUEVO: comparar_años() en ComparativasEngine — compatible con
#     _normalizarRespuestaAnual() del frontend JS.
#   - periodos_disponibles() corregido: años históricos sin los 12 meses
#     completos NO aparecen en el selector (Regla ⑤ del frontend JS).
#   - _calc_rust() hace fallback limpio a _calc_pandas() en caso de error
#     (ya no intentaba rellamar con agr vacío).
#   - _construir_comparacion(): plazas_nuevas/eliminadas calculadas con
#     conjuntos reales de Clave_Plaza (no solo diferencia de conteos).
#   - cargar_indice(): soporta estructura con o sin clave "index" en el JSON;
#     descarta entradas sin URL con log explícito.
#   - asegurar(): reintentos configurables (DOWNLOAD_MAX_RETRIES) con
#     backoff lineal entre intentos.
#   - _parse_bytes(): prueba múltiples encodings en CSV.
#   - _cargar_df_rust(): compresión "snappy" en lugar de "None".
#   - set_main_df(): logging mejorado, detección robusta de col numérica de mes.
#   - Watchdog: log de diagnóstico más completo; stop() disponible.
#   - Tipos Optional explícitos para compatibilidad con Python < 3.10.
#
# Arquitectura de caché de dos niveles:
#   Nivel 1 (Rust / RESULT_CACHE):
#       Resultado numérico ya calculado por par de periodo_keys.
#       Si existe → respuesta inmediata.
#   Nivel 2 (Rust / ENGINE_PERIODOS + Python _df_actual):
#       Arrays numéricos del parquet. Si existe pero no el resultado → calcula.
#       Si no existe + histórico → descarga Drive → carga en Rust.
#   Python mantiene:
#       - Strings (mapa eid→nombre_estado) por periodo
#       - _claves_protegidas: periodos del Parquet local (inmortales)
#       - Watchdog TTL/RAM
#       - Índice Drive: {"2024-03": {download_url, name, …}}
# ==============================================================================

from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests

# ── resource: módulo Unix-only → stub en Windows ──────────────────────────────
if sys.platform == "win32":
    class _resource:  # noqa: N801
        RUSAGE_SELF     = 0
        RUSAGE_CHILDREN = -1
        RLIM_INFINITY   = 2 ** 63 - 1
        RLIMIT_AS       = 9
        RLIMIT_NOFILE   = 7

        class _rusage:
            ru_maxrss = 0; ru_utime = 0.0; ru_stime = 0.0
            ru_minflt = 0; ru_majflt = 0

        @staticmethod
        def getrusage(who: int = 0) -> "_resource._rusage":
            r = _resource._rusage()
            try:
                import psutil
                r.ru_maxrss = psutil.Process(os.getpid()).memory_info().rss // 1024
            except ImportError:
                pass
            return r

        @staticmethod
        def getrlimit(resource_id: int) -> tuple:
            return (_resource.RLIM_INFINITY, _resource.RLIM_INFINITY)

        @staticmethod
        def setrlimit(resource_id: int, limits: tuple) -> None:
            pass

    resource = _resource()  # type: ignore[assignment]
else:
    import resource  # type: ignore[no-redef]
# ─────────────────────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────
CURRENT_YEAR: int = datetime.now().year

RESULT_TTL_S:            int   = 4 * 3600   # TTL result cache Rust: 4 horas
WATCHDOG_INTERVAL_S:     float = 30.0       # ciclo watchdog
MAX_PERIODOS_HISTORICOS: int   = 12         # máximo periodos Drive en Python
RAM_WARN_BYTES:          int   = 600 * 1024 * 1024
RAM_KILL_BYTES:          int   = 900 * 1024 * 1024
DOWNLOAD_TIMEOUT_S:      int   = 90
DOWNLOAD_MAX_RETRIES:    int   = 2          # reintentos en descarga (v5.2)

_COLS_NUM   = ["Clave_Edo", "Situación", "Inc_Total", "Aten_Total",
               "CN_Tot_Acum", "CN_Inicial_Acum", "CN_Prim_Acum", "CN_Sec_Acum"]
_COLS_COORD = ["Latitud", "Longitud"]
METRICAS_CN = ["CN_Inicial_Acum", "CN_Prim_Acum", "CN_Sec_Acum", "CN_Tot_Acum"]

_MESES: Dict[str, str] = {
    "01": "Enero",    "02": "Febrero",  "03": "Marzo",
    "04": "Abril",    "05": "Mayo",     "06": "Junio",
    "07": "Julio",    "08": "Agosto",   "09": "Septiembre",
    "10": "Octubre",  "11": "Noviembre","12": "Diciembre",
}


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS GENERALES
# ──────────────────────────────────────────────────────────────────────────────

def periodo_key(año: Any, mes: Any) -> int:
    a = int(año);  m = int(mes)
    if a < 100:
        a = 2000 + a
    return a * 100 + m


def parse_key(key: int) -> Tuple[int, str]:
    return key // 100, f"{key % 100:02d}"


def _pad(mes: Any) -> str:
    return f"{int(mes):02d}"


def _label(año: int, mes: Any) -> str:
    m = _pad(mes)
    return f"{_MESES.get(m, m)} {año}"


def _detectar_col(df: pd.DataFrame, candidatos: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in df.columns}
    for c in candidatos:
        if c in df.columns:
            return c
        if c.lower() in low:
            return low[c.lower()]
    return None


def _ram_bytes() -> int:
    try:
        import platform
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return rss * 1024 if platform.system() == "Linux" else rss
    except Exception:
        return 0


# ──────────────────────────────────────────────────────────────────────────────
# WATCHDOG
# ──────────────────────────────────────────────────────────────────────────────

class Watchdog:
    """
    Hilo daemon de mantenimiento del caché.

    - TTL    : limpia resultados Rust expirados cada `interval` segundos.
    - RAM    : evicta periodos Drive (nunca los protegidos) si RAM > RAM_WARN_BYTES.
    - SIGTERM: si RAM supera RAM_KILL_BYTES, termina el proceso.
    """

    def __init__(
        self,
        cache: "ParquetPeriodoCache",
        interval: float = WATCHDOG_INTERVAL_S,
    ):
        self._cache    = cache
        self._interval = interval
        self._running  = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        t = threading.Thread(
            target=self._loop, daemon=True, name="ComparativasWatchdog"
        )
        t.start()
        log.info(
            f"🐕 Watchdog ON  TTL={RESULT_TTL_S}s  "
            f"interval={self._interval}s  "
            f"ram_warn={RAM_WARN_BYTES // 1024 // 1024}MB  "
            f"ram_kill={RAM_KILL_BYTES // 1024 // 1024}MB"
        )

    def stop(self) -> None:
        self._running = False
        log.info("🛑 Watchdog detenido")

    def _loop(self) -> None:
        while self._running:
            time.sleep(self._interval)
            try:
                self._ciclo()
            except Exception as exc:
                log.error(f"Watchdog._ciclo error: {exc}", exc_info=True)

    def _ciclo(self) -> None:
        rust = self._cache._rust

        # 1. Limpiar resultados Rust expirados (TTL)
        if rust is not None:
            try:
                n = rust.limpiar_resultados_expirados(RESULT_TTL_S)
                if n:
                    log.info(f"🧹 Watchdog: {n} resultados Rust expirados eliminados")
            except Exception as exc:
                log.warning(f"limpiar_resultados_expirados: {exc}")

        # 2. Sincronizar mapa de strings (respeta claves protegidas)
        self._cache.sincronizar_mapa_estados()

        # 3. Diagnóstico RAM
        ram = _ram_bytes()
        with self._cache._lock:
            n_python    = len(self._cache._df_actual)
            n_prot      = len(self._cache._claves_protegidas)
            n_historico = n_python - n_prot
            n_indice    = len(self._cache._indice)
        log.debug(
            f"📊 Watchdog: RAM={ram // 1024 // 1024}MB  "
            f"pandas_cache={n_python} ({n_prot} protegidos + {n_historico} Drive)  "
            f"indice={n_indice}"
        )

        if ram < RAM_WARN_BYTES:
            return

        log.warning(f"⚠️ RAM alta: {ram // 1024 // 1024} MB")

        # 4. Evicción LRU Rust (solo históricos)
        if rust is not None:
            try:
                n_evict = rust.limpiar_periodos_lru(MAX_PERIODOS_HISTORICOS, CURRENT_YEAR)
                if n_evict:
                    log.warning(f"♻️  LRU Rust: {n_evict} periodos históricos evictados")
            except Exception as exc:
                log.warning(f"limpiar_periodos_lru: {exc}")

        # 5. Evicción Python (nunca toca _claves_protegidas)
        self._cache.evictar_historicos_python()

        # 6. SIGTERM si RAM sigue crítica
        ram = _ram_bytes()
        if ram > RAM_KILL_BYTES:
            log.critical(f"🔴 RAM crítica {ram // 1024 // 1024} MB — enviando SIGTERM")
            os.kill(os.getpid(), signal.SIGTERM)


# ──────────────────────────────────────────────────────────────────────────────
# CACHE DE PERIODOS
# ──────────────────────────────────────────────────────────────────────────────

class ParquetPeriodoCache:
    """
    Gestiona el índice de periodos disponibles y su caché en memoria.

    Parámetros
    ----------
    parquet_path    : ruta al .parquet local (datos del año actual)
    excel_tree_json : ruta al JSON de índice de Drive (excel_tree_real.json)
    plaza_rust      : módulo plaza_rust (PyO3) o None → modo pandas
    """

    def __init__(
        self,
        parquet_path:    str,
        excel_tree_json: str = "excel_tree_real.json",
        plaza_rust=None,
    ):
        self._parquet_path   = parquet_path
        self._tree_path      = excel_tree_json
        self._rust           = plaza_rust

        # Datos en Python por periodo_key
        self._df_actual: Dict[int, pd.DataFrame]   = {}
        self._mapa:      Dict[int, Dict[int, str]]  = {}
        self._acceso:    Dict[int, float]            = {}

        # Índice Drive: {"2023-05": {"download_url": "...", "name": "x.parquet"}}
        self._indice:         Dict[str, dict] = {}
        self._indice_cargado: bool            = False

        # Claves del Parquet local → inmortales, nunca evictadas
        self._claves_protegidas: Set[int] = set()

        self._lock = threading.Lock()

    # ──────────────────────────────────────────────────────────────────────────
    # set_main_df
    # ──────────────────────────────────────────────────────────────────────────
    def set_main_df(self, df: pd.DataFrame) -> None:
        """
        Indexa TODOS los años/meses del Parquet en el caché Python y Rust.
        Los periodos encontrados se marcan como protegidos (nunca eviccionados).
        """
        if df is None or df.empty:
            log.warning("⚠️  set_main_df: DataFrame vacío — no se indexa nada")
            return

        log.info(f"set_main_df: columnas disponibles = {list(df.columns)}")

        col_anio = _detectar_col(df, ["Año", "anio", "ANIO", "año"])
        col_mes  = _detectar_col(df, ["Cve-mes", "cve_mes", "Mes", "mes"])

        if col_anio is None or col_mes is None:
            log.warning(
                f"⚠️  set_main_df: columnas año/mes no encontradas "
                f"(col_anio={col_anio}, col_mes={col_mes}) — no se indexa"
            )
            return

        # Si la columna detectada no es numérica (ej: "Mes" = "Enero") → buscar Cve-mes
        muestra = pd.to_numeric(df[col_mes].dropna().head(10), errors="coerce")
        if muestra.isna().all():
            alt = _detectar_col(df, ["Cve-mes", "cve_mes"])
            if alt is not None and alt != col_mes:
                col_mes = alt
                log.info(f"set_main_df: usando '{col_mes}' como mes numérico")
            else:
                log.warning(
                    f"⚠️  set_main_df: '{col_mes}' no es numérica y no hay Cve-mes — no se indexa"
                )
                return

        indexados: List[str] = []
        for (anio_v, mes_v), sub in df.groupby([col_anio, col_mes], dropna=True):
            try:
                a = int(float(str(anio_v)))
                m = int(float(str(mes_v)))
                if a < 100:
                    a = 2000 + a
                key = periodo_key(a, m)

                sub_reset = sub.reset_index(drop=True)
                with self._lock:
                    self._df_actual[key] = sub_reset
                    self._mapa[key]      = _construir_mapa(sub_reset)
                    self._claves_protegidas.add(key)

                self._cargar_df_rust(key, sub_reset)
                indexados.append(f"{a}-{m:02d}")

            except (ValueError, TypeError) as exc:
                log.warning(f"set_main_df: error indexando {anio_v}-{mes_v}: {exc}")

        log.info(
            f"✅ Parquet local indexado y protegido: "
            f"{len(indexados)} periodos → {sorted(indexados)}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # cargar_indice
    # ──────────────────────────────────────────────────────────────────────────
    def cargar_indice(self) -> bool:
        """
        Lee excel_tree_real.json y construye el índice de periodos Drive.

        Estructuras soportadas:
          { "index": { "2023-05": {"download_url":...}, ... } }   ← preferida
          { "2023-05": {"download_url":...}, ... }                ← raíz directa
        """
        if not os.path.exists(self._tree_path):
            log.warning(f"⚠️  cargar_indice: {self._tree_path} no encontrado")
            return False

        try:
            with open(self._tree_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Detectar estructura
            if isinstance(data, dict) and "index" in data:
                raw_index: dict = data["index"]
            elif isinstance(data, dict):
                primera = next(iter(data), "")
                if len(primera) == 7 and primera[4] == "-":
                    raw_index = data
                else:
                    log.warning(f"⚠️  cargar_indice: estructura JSON inesperada ({self._tree_path})")
                    raw_index = {}
            else:
                log.warning(f"⚠️  cargar_indice: JSON no es dict ({self._tree_path})")
                raw_index = {}

            # Solo guardar entradas con URL válida
            validas: Dict[str, dict] = {}
            sin_url = 0
            for k, v in raw_index.items():
                if isinstance(v, dict) and (
                    "download_url" in v or "view_url" in v or "url" in v
                ):
                    validas[k] = v
                else:
                    sin_url += 1

            with self._lock:
                self._indice         = validas
                self._indice_cargado = True

            log.info(
                f"✅ Índice Drive: {len(validas)} entradas válidas"
                + (f" ({sin_url} sin URL descartadas)" if sin_url else "")
            )
            return True

        except Exception as exc:
            log.error(f"❌ cargar_indice: {exc}", exc_info=True)
            return False

    # ──────────────────────────────────────────────────────────────────────────
    # asegurar — garantiza disponibilidad del período (con reintentos)
    # ──────────────────────────────────────────────────────────────────────────
    def asegurar(self, año: Any, mes: Any) -> Tuple[bool, str]:
        """
        Garantiza que el periodo está disponible para cálculo.
        Orden:
          1. Rust ya lo tiene en cache         → cache_hit
          2. Python _df_actual (cualquier año) → ok_local
          3. Drive (con reintentos)            → ok_rust / ok_python
        """
        a = int(año);  m = int(mes)
        if a < 100:
            a = 2000 + a
        key = periodo_key(a, m)

        with self._lock:
            self._acceso[key] = time.monotonic()

        # Nivel 1: Rust ya tiene el periodo
        if self._rust is not None:
            try:
                if self._rust.periodo_en_cache(key):
                    return True, "cache_hit"
            except Exception as exc:
                log.warning(f"periodo_en_cache({key}): {exc}")

        # Nivel 2: Python local (Parquet de cualquier año)
        with self._lock:
            df = self._df_actual.get(key)
        if df is not None:
            ok = self._cargar_df_rust(key, df)
            return (True, "ok_local_rust") if ok else (True, "ok_pandas")

        # Nivel 3: Drive con reintentos y backoff
        for intento in range(1, DOWNLOAD_MAX_RETRIES + 1):
            ok, msg = self._descargar(a, m, key)
            if ok:
                return True, msg
            if intento < DOWNLOAD_MAX_RETRIES:
                espera = 2 * intento
                log.warning(
                    f"asegurar({a}-{_pad(m)}): reintento {intento}/{DOWNLOAD_MAX_RETRIES} "
                    f"en {espera}s ({msg})"
                )
                time.sleep(espera)

        return False, f"fallo_tras_{DOWNLOAD_MAX_RETRIES}_intentos"

    # ──────────────────────────────────────────────────────────────────────────
    # _descargar
    # ──────────────────────────────────────────────────────────────────────────
    def _descargar(self, año: int, mes: int, key: int) -> Tuple[bool, str]:
        ik = f"{año}-{_pad(mes)}"
        with self._lock:
            info = self._indice.get(ik)
        if info is None:
            return False, f"sin_entrada_{ik}"

        url = info.get("download_url") or info.get("view_url") or info.get("url")
        if not url:
            return False, f"sin_url_{ik}"

        log.info(f"⬇️  Descargando {ik}…")
        t0 = time.monotonic()
        try:
            resp = requests.get(
                url,
                headers={"Accept-Encoding": "gzip, deflate"},
                timeout=DOWNLOAD_TIMEOUT_S,
            )
            resp.raise_for_status()
        except requests.Timeout:
            return False, f"timeout_{ik}"
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "?"
            return False, f"http_{code}_{ik}"
        except Exception as exc:
            return False, f"error_red_{ik}: {exc}"

        raw    = resp.content
        nombre = info.get("name", "archivo.parquet")
        elapsed = time.monotonic() - t0
        log.info(f"✅ {ik}: {len(raw) // 1024} KB en {elapsed:.1f}s")

        # Intentar carga directa en Rust (más eficiente)
        if self._rust is not None:
            ok = self._cargar_bytes_rust(key, raw)
            if ok:
                mapa = self._extraer_mapa_bytes(raw, nombre)
                with self._lock:
                    self._mapa[key] = mapa
                return True, f"ok_rust_{ik}"

        # Fallback: parsear en Python
        log.warning(f"Rust rechazó {ik}; parseando en Python…")
        df = _parse_bytes(raw, nombre)
        if df is None or df.empty:
            return False, f"parse_fallido_{ik}"

        mapa = _construir_mapa(df)
        with self._lock:
            self._mapa[key]      = mapa
            self._df_actual[key] = df

        # Segundo intento Rust con DF convertido
        self._cargar_df_rust(key, df)
        return True, f"ok_python_{ik}"

    # ──────────────────────────────────────────────────────────────────────────
    # Carga en Rust
    # ──────────────────────────────────────────────────────────────────────────
    def _cargar_bytes_rust(self, key: int, raw: bytes) -> bool:
        try:
            n = self._rust.cargar_periodo_parquet(raw, key)
            log.info(f"🦀 {key}: {n} filas (bytes directos)")
            return True
        except Exception as exc:
            log.error(f"cargar_periodo_parquet({key}): {exc}")
            return False

    def _cargar_df_rust(self, key: int, df: pd.DataFrame) -> bool:
        if self._rust is None:
            return False
        try:
            cols = list(dict.fromkeys(
                c for c in (_COLS_COORD + _COLS_NUM + METRICAS_CN)
                if c in df.columns
            ))
            if not cols:
                return False
            buf = io.BytesIO()
            df[cols].to_parquet(buf, index=False, compression="snappy")
            n = self._rust.cargar_periodo_parquet(buf.getvalue(), key)
            log.info(f"🦀 {key}: {n} filas (DF→parquet)")
            return True
        except Exception as exc:
            log.error(f"_cargar_df_rust({key}): {exc}")
            return False

    # ──────────────────────────────────────────────────────────────────────────
    # Evicción (nunca toca _claves_protegidas)
    # ──────────────────────────────────────────────────────────────────────────
    def evictar_historicos_python(self) -> int:
        """
        Evicta periodos Drive del caché Python (LRU).
        NUNCA evicta claves en _claves_protegidas (Parquet local).
        Retorna cantidad eliminada.
        """
        with self._lock:
            historicos = [
                (k, ts) for k, ts in self._acceso.items()
                if k not in self._claves_protegidas
            ]
        historicos.sort(key=lambda x: x[1])  # más antiguo primero
        a_evictar = max(0, len(historicos) - MAX_PERIODOS_HISTORICOS)

        for key, _ in historicos[:a_evictar]:
            with self._lock:
                self._mapa.pop(key, None)
                self._df_actual.pop(key, None)
                self._acceso.pop(key, None)

        if a_evictar:
            log.info(
                f"♻️  Python evicción: {a_evictar} periodos Drive eliminados "
                f"(Parquet local intacto: {len(self._claves_protegidas)} protegidos)"
            )
        return a_evictar

    # ──────────────────────────────────────────────────────────────────────────
    # Sincronizar mapa con Rust
    # ──────────────────────────────────────────────────────────────────────────
    def sincronizar_mapa_estados(self) -> None:
        """
        Limpia del mapa Python claves que Rust ya no tiene.
        Respeta _claves_protegidas.
        """
        if self._rust is None:
            return
        with self._lock:
            keys_python = list(self._mapa.keys())
        for key in keys_python:
            if key in self._claves_protegidas:
                continue
            try:
                if not self._rust.periodo_en_cache(key):
                    with self._lock:
                        self._mapa.pop(key, None)
                        self._df_actual.pop(key, None)
                        self._acceso.pop(key, None)
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers de mapa
    # ──────────────────────────────────────────────────────────────────────────
    def get_mapa(self, key: int) -> Dict[int, str]:
        with self._lock:
            if key in self._mapa:
                return dict(self._mapa[key])
            if key in self._df_actual:
                m = _construir_mapa(self._df_actual[key])
                self._mapa[key] = m
                return dict(m)
        return {}

    def _extraer_mapa_bytes(self, raw: bytes, nombre: str) -> Dict[int, str]:
        try:
            df = pd.read_parquet(io.BytesIO(raw), columns=["Clave_Edo", "Estado"])
            return _construir_mapa(df)
        except Exception:
            try:
                df = _parse_bytes(raw, nombre)
                return _construir_mapa(df) if df is not None else {}
            except Exception:
                return {}

    # ──────────────────────────────────────────────────────────────────────────
    # periodos_disponibles — CORREGIDO v5.2
    # ──────────────────────────────────────────────────────────────────────────
    def periodos_disponibles(self) -> Dict[str, List[str]]:
        """
        Devuelve dict { "2024": ["01","02",...], "2026": [...] }

        Regla ⑤ (Regla JS): años históricos sin los 12 meses completos
        NO aparecen en el selector. El año actual siempre se incluye.
        """
        mp: Dict[str, List[str]] = {}

        with self._lock:
            # Parquet local
            for key in self._df_actual:
                a, m = parse_key(key)
                if m == "99":
                    continue
                mp.setdefault(str(a), [])
                if m not in mp[str(a)]:
                    mp[str(a)].append(m)
            # Índice Drive
            for ik in self._indice:
                parts = ik.split("-")
                if len(parts) != 2:
                    continue
                a_s, m_s = parts[0], parts[1].zfill(2)
                mp.setdefault(a_s, [])
                if m_s not in mp[a_s]:
                    mp[a_s].append(m_s)

        for a in mp:
            mp[a] = sorted(set(mp[a]))

        # Filtrar: históricos solo si tienen los 12 meses
        anio_actual_str = str(CURRENT_YEAR)
        resultado: Dict[str, List[str]] = {}
        for anio, meses in mp.items():
            if anio == anio_actual_str:
                resultado[anio] = meses
            else:
                nums = {int(m) for m in meses}
                if all(i in nums for i in range(1, 13)):
                    resultado[anio] = meses

        return resultado


# ──────────────────────────────────────────────────────────────────────────────
# MOTOR PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────

class ComparativasEngine:
    """
    Motor de comparativas entre periodos o entre años completos.

    Parámetros
    ----------
    cache      : instancia de ParquetPeriodoCache
    plaza_rust : módulo plaza_rust (PyO3) o None → modo pandas
    """

    def __init__(self, cache: ParquetPeriodoCache, plaza_rust=None):
        self._cache = cache
        self._rust  = plaza_rust

    # ──────────────────────────────────────────────────────────────────────────
    # API pública
    # ──────────────────────────────────────────────────────────────────────────

    def periodos_disponibles(self) -> dict:
        mp = self._cache.periodos_disponibles()
        return {
            "status":         "success",
            "years":          sorted(mp.keys(), reverse=True),
            "meses_por_anio": mp,
        }

    def comparar(
        self,
        año1: Any,
        mes1: Any,
        año2: Any,
        mes2: Any,
        filtro_estado: str = "Todos",
    ) -> dict:
        """
        Compara dos períodos (pueden ser años distintos).
        Compatible con _normalizarRespuestaPeriodo() del frontend JS.
        """
        a1, a2 = int(año1), int(año2)
        if a1 < 100: a1 = 2000 + a1
        if a2 < 100: a2 = 2000 + a2
        m1, m2 = _pad(mes1), _pad(mes2)
        key1, key2 = periodo_key(a1, m1), periodo_key(a2, m2)

        # Verificar resultado cacheado en Rust
        resultado_cacheado = False
        if self._rust is not None:
            try:
                resultado_cacheado = self._rust.resultado_en_cache(key1, key2, -1)
            except Exception:
                pass

        if not resultado_cacheado:
            ok1, msg1 = self._cache.asegurar(a1, m1)
            if not ok1:
                return {"status": "error", "error": f"No se pudo cargar {a1}-{m1}: {msg1}"}
            ok2, msg2 = self._cache.asegurar(a2, m2)
            if not ok2:
                return {"status": "error", "error": f"No se pudo cargar {a2}-{m2}: {msg2}"}

        raw   = self._calcular(key1, key2)
        mapa1 = self._cache.get_mapa(key1)
        mapa2 = self._cache.get_mapa(key2)
        mapa  = {**mapa2, **mapa1}

        # Resolver filtro_estado → eid numérico
        filtro_eid = -1
        if filtro_estado and filtro_estado.lower() not in ("todos", "all", ""):
            norm = filtro_estado.lower().strip()
            for eid, nom in mapa.items():
                if nom.lower().strip() == norm:
                    filtro_eid = eid
                    break

        claves1 = self._claves_plaza(key1)
        claves2 = self._claves_plaza(key2)
        comp    = _construir_comparacion(raw, mapa, filtro_eid, claves1, claves2)

        return {
            "status":               "success",
            "cache_hit":            resultado_cacheado,
            "label1":               _label(a1, m1),
            "label2":               _label(a2, m2),
            "comparacion":          comp,
            "metricas_principales": _metricas_principales(comp),
        }

    def comparar_años(self, año1: Any, año2: Any) -> dict:
        """
        Compara dos años completos (acumula todos los meses disponibles).
        Compatible con _normalizarRespuestaAnual() del frontend JS.

        Respuesta:
          resumen_año1 / resumen_año2 : totales acumulados del año
          diferencias.metricas        : cambio entre años
          por_estado                  : desglose por estado
        """
        a1, a2 = int(año1), int(año2)
        if a1 < 100: a1 = 2000 + a1
        if a2 < 100: a2 = 2000 + a2

        mp     = self._cache.periodos_disponibles()
        meses1 = mp.get(str(a1), [])
        meses2 = mp.get(str(a2), [])

        if not meses1:
            return {"status": "error", "error": f"Sin meses disponibles para el año {a1}"}
        if not meses2:
            return {"status": "error", "error": f"Sin meses disponibles para el año {a2}"}

        # Asegurar todos los periodos en caché
        for m in meses1:
            ok, msg = self._cache.asegurar(a1, m)
            if not ok:
                log.warning(f"comparar_años: no se pudo cargar {a1}-{m}: {msg}")
        for m in meses2:
            ok, msg = self._cache.asegurar(a2, m)
            if not ok:
                log.warning(f"comparar_años: no se pudo cargar {a2}-{m}: {msg}")

        # Acumular agregaciones de todos los meses de cada año
        agr1 = self._acumular_agr(a1, meses1)
        agr2 = self._acumular_agr(a2, meses2)

        # Mapa de estados fusionado
        mapa: Dict[int, str] = {}
        for meses, año in [(meses1, a1), (meses2, a2)]:
            for m in meses:
                mapa.update(self._cache.get_mapa(periodo_key(año, m)))

        t1 = _sumar(agr1)
        t2 = _sumar(agr2)

        # Métricas globales — formato que espera _normalizarRespuestaAnual
        metricas_diff: Dict[str, dict] = {}
        _pares_metricas = [
            ("CN_Tot_Acum",     "cn_total"),
            ("CN_Inicial_Acum", "cn_ini"),
            ("CN_Prim_Acum",    "cn_prim"),
            ("CN_Sec_Acum",     "cn_sec"),
            ("Inc_Total",       "inc_total"),
            ("Aten_Total",      "aten_total"),
        ]
        for col_out, campo in _pares_metricas:
            v1 = t1.get(campo, 0)
            v2 = t2.get(campo, 0)
            cambio = v2 - v1
            metricas_diff[col_out] = {
                "año1":              v1,
                "año2":              v2,
                "cambio":            cambio,
                "porcentaje_cambio": round(cambio / v1 * 100, 2) if v1 else 0.0,
            }

        # Desglose por estado
        por_estado: Dict[str, Any] = {}
        for eid in set(list(agr1.keys()) + list(agr2.keys())):
            nombre = mapa.get(int(eid), f"Estado_{eid}")
            d1 = agr1.get(eid, {})
            d2 = agr2.get(eid, {})
            metricas_e: Dict[str, dict] = {}
            for col_out, campo in _pares_metricas:
                v1 = int(d1.get(campo, 0))
                v2 = int(d2.get(campo, 0))
                cambio = v2 - v1
                metricas_e[col_out] = {
                    "año1":              v1,
                    "año2":              v2,
                    "cambio":            cambio,
                    "porcentaje_cambio": round(cambio / v1 * 100, 2) if v1 else 0.0,
                }
            por_estado[nombre] = {
                "resumen_año1": {
                    "total_plazas": int(d1.get("plazas", 0)),
                    "metricas":     {k: v["año1"] for k, v in metricas_e.items()},
                },
                "resumen_año2": {
                    "total_plazas": int(d2.get("plazas", 0)),
                    "plazas_op":    int(d2.get("plazas", 0)),
                    "metricas":     {k: v["año2"] for k, v in metricas_e.items()},
                },
                "diferencias": {"metricas": metricas_e},
            }

        return {
            "status":       "success",
            "año1":         a1,
            "año2":         a2,
            "resumen_año1": {
                "total_plazas": t1["plazas"],
                "plazas_op":    t1["plazas"],
                "metricas":     {k: v["año1"] for k, v in metricas_diff.items()},
            },
            "resumen_año2": {
                "total_plazas": t2["plazas"],
                "plazas_op":    t2["plazas"],
                "metricas":     {k: v["año2"] for k, v in metricas_diff.items()},
            },
            "diferencias":  {"metricas": metricas_diff},
            "por_estado":   por_estado,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Internos
    # ──────────────────────────────────────────────────────────────────────────

    def _calcular(self, key1: int, key2: int) -> dict:
        """Calcula agregaciones via Rust (con fallback a pandas)."""
        if self._rust is not None:
            return self._calc_rust(key1, key2)
        return self._calc_pandas(key1, key2)

    def _calc_rust(self, key1: int, key2: int) -> dict:
        try:
            raw = self._rust.comparar_periodos(key1, key2, -1)
            return {
                "agr1": raw.get("periodo1", {}),
                "agr2": raw.get("periodo2", {}),
            }
        except Exception as exc:
            log.error(f"comparar_periodos Rust({key1},{key2}): {exc} — fallback pandas")
            return self._calc_pandas(key1, key2)

    def _calc_pandas(self, key1: int, key2: int) -> dict:
        with self._cache._lock:
            df1 = self._cache._df_actual.get(key1)
            df2 = self._cache._df_actual.get(key2)
        return {
            "agr1": _agrupar_pandas(df1) if df1 is not None else {},
            "agr2": _agrupar_pandas(df2) if df2 is not None else {},
        }

    def _acumular_agr(self, año: int, meses: List[str]) -> Dict[int, dict]:
        """Suma las agregaciones de todos los meses de un año."""
        acum: Dict[int, dict] = {}
        for m in meses:
            key = periodo_key(año, m)
            with self._cache._lock:
                df = self._cache._df_actual.get(key)
            if df is None:
                continue
            for eid, vals in _agrupar_pandas(df).items():
                if eid not in acum:
                    acum[eid] = {k: 0 for k in vals}
                for k, v in vals.items():
                    acum[eid][k] = acum[eid].get(k, 0) + v
        return acum

    def _claves_plaza(self, key: int) -> Set[str]:
        """Conjunto de Clave_Plaza del período (para plazas nuevas/eliminadas)."""
        with self._cache._lock:
            df = self._cache._df_actual.get(key)
        if df is None:
            return set()
        col = _detectar_col(df, ["Clave_Plaza", "clave", "Clave"])
        if col is None:
            return set()
        return set(df[col].dropna().astype(str).unique())


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS PUROS
# ──────────────────────────────────────────────────────────────────────────────

def _construir_mapa(df: Optional[pd.DataFrame]) -> Dict[int, str]:
    """Construye dict {clave_edo_int: nombre_estado_str}."""
    if df is None or df.empty:
        return {}
    ce = _detectar_col(df, ["Clave_Edo", "clave_edo"])
    cn = _detectar_col(df, ["Estado", "estado"])
    if ce is None or cn is None:
        return {}
    mapa: Dict[int, str] = {}
    for _, row in df[[ce, cn]].drop_duplicates().iterrows():
        try:
            eid = int(float(row[ce]))
            nom = str(row[cn]).strip()
            if nom and nom.lower() not in ("nan", "none", ""):
                mapa[eid] = nom
        except (ValueError, TypeError):
            pass
    return mapa


def _parse_bytes(raw: bytes, nombre: str) -> Optional[pd.DataFrame]:
    """Parsea bytes a DataFrame detectando el formato por extensión."""
    nom = nombre.lower()
    try:
        if nom.endswith(".parquet"):
            return pd.read_parquet(io.BytesIO(raw))
        if nom.endswith((".xlsx", ".xls")):
            return pd.read_excel(io.BytesIO(raw))
        if nom.endswith(".csv"):
            for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
                try:
                    return pd.read_csv(
                        io.BytesIO(raw), encoding=enc, low_memory=False
                    )
                except UnicodeDecodeError:
                    continue
        # Formato desconocido: parquet primero, luego excel
        try:
            return pd.read_parquet(io.BytesIO(raw))
        except Exception:
            return pd.read_excel(io.BytesIO(raw))
    except Exception as exc:
        log.error(f"_parse_bytes '{nombre}': {exc}")
        return None


def _agrupar_pandas(df: Optional[pd.DataFrame]) -> Dict[int, dict]:
    """Agrega métricas por Clave_Edo."""
    if df is None or df.empty:
        return {}
    col_e = _detectar_col(df, ["Clave_Edo", "clave_edo"])
    if col_e is None:
        return {}
    mapeo = {
        "inc_total":  _detectar_col(df, ["Inc_Total"]),
        "aten_total": _detectar_col(df, ["Aten_Total"]),
        "cn_total":   _detectar_col(df, ["CN_Tot_Acum"]),
        "cn_ini":     _detectar_col(df, ["CN_Inicial_Acum"]),
        "cn_prim":    _detectar_col(df, ["CN_Prim_Acum"]),
        "cn_sec":     _detectar_col(df, ["CN_Sec_Acum"]),
    }
    agr: Dict[int, dict] = {}
    for eid, grp in df.groupby(col_e, dropna=True):
        try:
            ei = int(float(eid))
        except (ValueError, TypeError):
            continue
        entry: dict = {"plazas": len(grp)}
        for campo, col in mapeo.items():
            if col and col in grp.columns:
                entry[campo] = int(
                    pd.to_numeric(grp[col], errors="coerce").fillna(0).sum()
                )
            else:
                entry[campo] = 0
        agr[ei] = entry
    return agr


def _sumar(agr: Dict[int, dict]) -> dict:
    """Suma todas las agregaciones en un dict de totales."""
    p = inc = aten = cn = ini = prim = sec = 0
    for v in agr.values():
        p    += v.get("plazas",     0)
        inc  += v.get("inc_total",  0)
        aten += v.get("aten_total", 0)
        cn   += v.get("cn_total",   0)
        ini  += v.get("cn_ini",     0)
        prim += v.get("cn_prim",    0)
        sec  += v.get("cn_sec",     0)
    return {
        "plazas":     p,
        "inc_total":  inc,
        "aten_total": aten,
        "cn_total":   cn,
        "cn_ini":     ini,
        "cn_prim":    prim,
        "cn_sec":     sec,
    }


def _construir_comparacion(
    raw:        dict,
    mapa:       Dict[int, str],
    filtro_eid: int,
    claves1:    Set[str],
    claves2:    Set[str],
) -> dict:
    """
    Construye el dict de comparación completo compatible con el frontend JS.

    v5.2: plazas_nuevas/eliminadas con conjuntos reales de Clave_Plaza.
    """
    agr1 = raw.get("agr1", {})
    agr2 = raw.get("agr2", {})

    if filtro_eid >= 0:
        agr1 = {k: v for k, v in agr1.items() if int(k) == filtro_eid}
        agr2 = {k: v for k, v in agr2.items() if int(k) == filtro_eid}

    t1 = _sumar(agr1)
    t2 = _sumar(agr2)

    # Plazas nuevas/eliminadas — conjuntos reales si están disponibles
    if claves1 and claves2:
        plazas_nuevas     = len(claves2 - claves1)
        plazas_eliminadas = len(claves1 - claves2)
    else:
        plazas_nuevas     = max(0, t2["plazas"] - t1["plazas"])
        plazas_eliminadas = max(0, t1["plazas"] - t2["plazas"])

    analisis_plazas = {
        "total_plazas_periodo1":     t1["plazas"],
        "total_plazas_periodo2":     t2["plazas"],
        "plazas_nuevas":             plazas_nuevas,
        "plazas_eliminadas":         plazas_eliminadas,
        "plazas_operacion_periodo2": t2["plazas"],
    }

    _pares = [
        ("CN_Tot_Acum",     "cn_total"),
        ("CN_Inicial_Acum", "cn_ini"),
        ("CN_Prim_Acum",    "cn_prim"),
        ("CN_Sec_Acum",     "cn_sec"),
        ("Inc_Total",       "inc_total"),
        ("Aten_Total",      "aten_total"),
    ]

    metricas_globales: Dict[str, dict] = {}
    for col_out, campo in _pares:
        v1 = t1.get(campo, 0)
        v2 = t2.get(campo, 0)
        c  = v2 - v1
        metricas_globales[col_out] = {
            "periodo1":          v1,
            "periodo2":          v2,
            "incremento":        c,
            "cambio":            c,
            "porcentaje_cambio": round(c / v1 * 100, 2) if v1 else 0.0,
            "tipo":              "numerica",
        }

    analisis_por_estado: Dict[str, Any] = {}
    for eid in set(list(agr1.keys()) + list(agr2.keys())):
        nombre = mapa.get(int(eid), f"Estado_{eid}")
        d1 = agr1.get(eid, {})
        d2 = agr2.get(eid, {})
        metricas: Dict[str, dict] = {}
        for col_out, campo in _pares:
            v1 = int(d1.get(campo, 0))
            v2 = int(d2.get(campo, 0))
            c  = v2 - v1
            metricas[col_out] = {
                "periodo1":          v1,
                "periodo2":          v2,
                "cambio":            c,
                "porcentaje_cambio": round(c / v1 * 100, 2) if v1 else 0.0,
            }
        analisis_por_estado[nombre] = {
            "total_plazas_periodo1":     int(d1.get("plazas", 0)),
            "total_plazas_periodo2":     int(d2.get("plazas", 0)),
            "plazas_operacion_periodo2": int(d2.get("plazas", 0)),
            "metricas":                  metricas,
        }

    return {
        "analisis_plazas":     analisis_plazas,
        "metricas_globales":   metricas_globales,
        "analisis_por_estado": analisis_por_estado,
    }


def _metricas_principales(comp: dict) -> dict:
    """Resumen rápido para las cards del frontend."""
    ap = comp.get("analisis_plazas", {})
    cn = comp.get("metricas_globales", {}).get("CN_Tot_Acum", {})
    c  = cn.get("cambio", 0)
    return {
        "plazas_nuevas":       ap.get("plazas_nuevas",     0),
        "plazas_eliminadas":   ap.get("plazas_eliminadas", 0),
        "incremento_cn_total": c,
        "resumen_cambios":     f"CN Total {'+' if c >= 0 else ''}{c:,}",
    }