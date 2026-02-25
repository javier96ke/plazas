import os
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, jsonify, send_from_directory, redirect, Response
import json
import gzip
from unidecode import unidecode
from datetime import datetime
import traceback
import math
from functools import lru_cache
import orjson
import threading
import polars as pl
import io


 # ==============================================================================
# IMPORTACIONES CONDICIONALES: DRIVE + TRANSLATOR (MODO LEGACY)
# ==============================================================================
DRIVE_MODULES_AVAILABLE = False
logging.info("ℹ️ Módulos legacy de Drive desactivados (usando v5.1)")

class DummyDriveReader:
    def get_available_years(self): return []
    def get_available_months(self, year): return []
    def query_excel_data_readonly(self, *a, **kw): return {'status': 'error', 'message': 'Módulo no disponible'}
    def get_stats(self): return {'total_requests':0,'cache_hits':0,'drive_downloads':0,'cache_hit_ratio':0,'currently_loaded_files':0,'tree_loaded':False}
    def load_excel_strict(self, year, month): return None, {'error': 'Módulo no disponible'}
    def get_excel_info(self, *a, **kw): return None
    def clear_all_cache(self): pass
    def get_loaded_files_count(self): return 0
    def load_tree(self): return False

class DummyComparator:
    def comparar_periodos_avanzado(self, *a, **kw): return {'status': 'error', 'message': 'Módulo no disponible'}
    def comparar_periodos_avanzado_con_años_diferentes(self, *a, **kw): return {'status': 'error', 'message': 'Módulo no disponible'}
    def obtener_estados_disponibles(self, *a, **kw): return []
    def obtener_metricas_disponibles(self, *a, **kw): return []

drive_excel_reader_readonly = DummyDriveReader()
drive_excel_comparator      = DummyComparator()

def safe_json_serialize(obj):
    if isinstance(obj, (np.integer, np.int64)):   return int(obj)
    if isinstance(obj, (np.floating, np.float64)):return float(obj) if not np.isnan(obj) else None
    if isinstance(obj, np.ndarray):               return obj.tolist()
    if isinstance(obj, pd.DataFrame):             return obj.to_dict('records')
    if isinstance(obj, pd.Series):                return obj.to_dict()
    if isinstance(obj, datetime):                 return obj.isoformat()
    try:
        if pd.isna(obj): return None
    except Exception:
        pass
    return obj

def obtener_años_desde_arbol_json(): return [], {}
def obtener_nombre_mes(n: str) -> str:
    return {'01':'Enero','02':'Febrero','03':'Marzo','04':'Abril','05':'Mayo','06':'Junio',
            '07':'Julio','08':'Agosto','09':'Septiembre','10':'Octubre','11':'Noviembre','12':'Diciembre'}.get(n, f'Mes {n}')
# ==============================================================================
# IMPORTACIONES CONDICIONALES: TRANSLATOR + OPTIMIZACIONES
# ==============================================================================
try:
    from translator import translator as _col_translator
    from translator import traducir_json_coordenadas as _traducir_json
    TRANSLATOR_AVAILABLE = True
    logging.info("✅ ColumnTranslator cargado")
except ImportError:
    _col_translator = None
    _traducir_json = None
    TRANSLATOR_AVAILABLE = False
    logging.warning("⚠️ translator.py no disponible — se usará el parquet tal cual")

try:
    from plaza_index import plaza_index as _plaza_index
    PLAZA_INDEX_AVAILABLE = True
    logging.info("✅ PlazaIndex cargado")
except ImportError:
    _plaza_index = None
    PLAZA_INDEX_AVAILABLE = False
    logging.warning("⚠️ plaza_index.py no disponible — cascada usará filtrado normal")

try:
    from polars_precalc import stats_cache as _stats_cache
    PRECALC_AVAILABLE = True
    logging.info("✅ polars_precalc cargado")
except ImportError:
    _stats_cache = None
    PRECALC_AVAILABLE = False
    logging.warning("⚠️ polars_precalc no disponible — endpoints usarán pandas")

try:
    import rust_bridge
    RUST_BRIDGE_AVAILABLE = rust_bridge.RUST_AVAILABLE
    logging.info("✅ rust_bridge cargado")
except ImportError:
    rust_bridge = None
    RUST_BRIDGE_AVAILABLE = False
    logging.warning("⚠️ rust_bridge no disponible — operaciones matemáticas usarán pandas")

# ==============================================================================
# IMPORTACIONES CONDICIONALES: COMPARATIVAS ENGINE v5.1
# ==============================================================================
try:
    from comparativas_engine import (
        ParquetPeriodoCache,
        ComparativasEngine,
        Watchdog,
        CURRENT_YEAR,
    )
    COMPARATIVAS_ENGINE_AVAILABLE = True
    logging.info("✅ ComparativasEngine v5.1 cargado desde drive_excel_reader")
except ImportError as e:
    COMPARATIVAS_ENGINE_AVAILABLE = False
    logging.warning(f"⚠️ ComparativasEngine no disponible: {e}")

try:
    import plaza_rust as _plaza_rust_mod
    PLAZA_RUST_AVAILABLE = True
    logging.info("✅ plaza_rust (PyO3) cargado")
except ImportError:
    _plaza_rust_mod = None
    PLAZA_RUST_AVAILABLE = False
    logging.warning("⚠️ plaza_rust no disponible — comparativas usarán pandas")

# ==============================================================================
# CONFIGURACIÓN Y LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8',
)
logging.getLogger().addHandler(
    RotatingFileHandler('app.log', maxBytes=1_024_000, backupCount=5, encoding='utf-8')
)

app = Flask(__name__, template_folder='../templates', static_folder='../static')

# ==============================================================================
# SERIALIZACIÓN: orjson + Gzip ADAPTATIVO
# ==============================================================================
def json_response(data, status: int = 200) -> Response:
    body = orjson.dumps(data, option=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY)
    accept = request.headers.get("Accept-Encoding", "")
    if "gzip" in accept and len(body) > 1024:
        level = 1 if len(body) < 100_000 else 6
        body = gzip.compress(body, compresslevel=level)
        return Response(body, status=status, headers={
            "Content-Encoding": "gzip",
            "Content-Type": "application/json",
            "Vary": "Accept-Encoding",
        })
    return Response(body, status=status, mimetype="application/json")


def convertir_a_serializable(obj):
    if isinstance(obj, dict):   return {k: convertir_a_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):   return [convertir_a_serializable(v) for v in obj]
    if isinstance(obj, (np.integer, np.int64)):    return int(obj)
    if isinstance(obj, (np.floating, np.float64)): return float(obj) if not np.isnan(obj) else None
    if isinstance(obj, np.ndarray): return obj.tolist()
    try:
        if pd.isna(obj): return None
    except Exception:
        pass
    return obj


# ==============================================================================
# CONFIG
# ==============================================================================
class Config:
    EXCEL_PATH           = 'datos/datos_plazas.xlsx'
    PARQUET_PATH         = 'datos/datos_plazas.parquet'
    IMAGES_BASE_PATH     = 'fotos_de_plazas'
    ARCHIVO_COORDENADAS  = 'datos/coordenadasplazas.json'
    EXCEL_TREE_PATH      = 'datos/excel_tree_real.json'
    DRIVE_TREE_PATH      = 'datos/drive_tree.json'

    COLUMNA_CLAVE                  = 'Clave_Plaza'
    COLUMNA_ESTADO                 = 'Estado'
    COLUMNA_MUNICIPIO              = 'Municipio'
    COLUMNA_LOCALIDAD              = 'Localidad'
    COLUMNA_COORD_ZONA             = 'Coord. Zona'
    COLUMNA_LATITUD                = 'Latitud'
    COLUMNA_LONGITUD               = 'Longitud'
    COLUMNA_CVE_MES                = 'Cve-mes'
    COLUMNA_MES                    = 'Mes'
    COLUMNA_SITUACION              = 'Situación'
    COLUMNA_ANO                    = 'Año'
    COLUMNA_CLAVE_EDO              = 'Clave_Edo'
    COLUMNA_NOMBRE_PC              = 'Nombre_PC'
    COLUMNA_COLONIA                = 'Colonia'
    COLUMNA_CALLE                  = 'Calle'
    COLUMNA_NUM                    = 'Num'
    COLUMNA_COD_POST               = 'Cod_Post'
    COLUMNA_TIPO_LOCAL             = 'Tipo_local'
    COLUMNA_INST_ALIADA            = 'Inst_aliada'
    COLUMNA_ARQ_DISCAP             = 'Arq_Discap.'
    COLUMNA_CONECT_INSTALADA       = 'Conect_Instalada'
    COLUMNA_TIPO_CONECT            = 'Tipo_Conect'
    COLUMNA_INC_INICIAL            = 'Inc_Inicial'
    COLUMNA_INC_PRIM               = 'Inc_Prim'
    COLUMNA_INC_SEC                = 'Inc_Sec'
    COLUMNA_INC_TOTAL              = 'Inc_Total'
    COLUMNA_ATEN_INICIAL           = 'Aten_Inicial'
    COLUMNA_ATEN_PRIM              = 'Aten_Prim'
    COLUMNA_ATEN_SEC               = 'Aten_Sec'
    COLUMNA_ATEN_TOTAL             = 'Aten_Total'
    COLUMNA_EXAMENES_APLICADOS     = 'Exámenes aplicados'
    COLUMNA_CN_INICIAL_ACUM        = 'CN_Inicial_Acum'
    COLUMNA_CN_PRIM_ACUM           = 'CN_Prim_Acum'
    COLUMNA_CN_SEC_ACUM            = 'CN_Sec_Acum'
    COLUMNA_CN_TOT_ACUM            = 'CN_Tot_Acum'
    COLUMNA_CERT_EMITIDOS          = 'Cert_Emitidos'
    COLUMNA_TEC_DOC                = 'Tec_Doc'
    COLUMNA_NOM_PVS_1              = 'Nom_PVS_1'
    COLUMNA_NOM_PVS_2              = 'Nom_PVS_2'
    COLUMNA_TOTAL_EQUIPOS_COMPUTO  = 'Total de equipos de cómputo en la plaza'
    COLUMNA_EQUIPOS_COMPUTO_OPERAN = 'Equipos de cómputo que operan'
    COLUMNA_TIPOS_EQUIPOS_COMPUTO  = 'Tipos de equipos de cómputo'
    COLUMNA_IMPRESORAS_FUNCIONAN   = 'Impresoras que funcionan'
    COLUMNA_IMPRESORAS_SUMINISTROS = 'Impresoras con suministros (toner, hojas)'
    COLUMNA_TOTAL_SERVIDORES       = 'Total de servidores en la plaza'
    COLUMNA_SERVIDORES_FUNCIONAN   = 'Número de servidores que funcionan correctamente'
    COLUMNA_MESAS_FUNCIONAN        = 'Cuantas mesas funcionan'
    COLUMNA_SILLAS_FUNCIONAN       = 'Cuantas sillas funcionan'
    COLUMNA_ANAQUELES_FUNCIONAN    = 'Cuantos Anaqueles funcionan'

    _MAP = {
        'CLAVE_PLAZA':            'Clave_Plaza',
        'ESTADO':                 'Estado',
        'MUNICIPIO':              'Municipio',
        'LOCALIDAD':              'Localidad',
        'COORD_ZONA':             'Coord. Zona',
        'LATITUD':                'Latitud',
        'LONGITUD':               'Longitud',
        'CVE_MES':                'Cve-mes',
        'MES':                    'Mes',
        'SITUACION':              'Situación',
        'ANO':                    'Año',
        'CLAVE_EDO':              'Clave_Edo',
        'NOMBRE_PC':              'Nombre_PC',
        'COLONIA':                'Colonia',
        'CALLE':                  'Calle',
        'NUM':                    'Num',
        'COD_POST':               'Cod_Post',
        'TIPO_LOCAL':             'Tipo_local',
        'INST_ALIADA':            'Inst_aliada',
        'ARQ_DISCAP':             'Arq_Discap.',
        'CONECT_INSTALADA':       'Conect_Instalada',
        'TIPO_CONECT':            'Tipo_Conect',
        'INC_INICIAL':            'Inc_Inicial',
        'INC_PRIM':               'Inc_Prim',
        'INC_SEC':                'Inc_Sec',
        'INC_TOTAL':              'Inc_Total',
        'ATEN_INICIAL':           'Aten_Inicial',
        'ATEN_PRIM':              'Aten_Prim',
        'ATEN_SEC':               'Aten_Sec',
        'ATEN_TOTAL':             'Aten_Total',
        'EXAMENES_APLICADOS':     'Exámenes aplicados',
        'CN_INICIAL_ACUM':        'CN_Inicial_Acum',
        'CN_PRIM_ACUM':           'CN_Prim_Acum',
        'CN_SEC_ACUM':            'CN_Sec_Acum',
        'CN_TOT_ACUM':            'CN_Tot_Acum',
        'CERT_EMITIDOS':          'Cert_Emitidos',
        'TEC_DOC':                'Tec_Doc',
        'NOM_PVS_1':              'Nom_PVS_1',
        'NOM_PVS_2':              'Nom_PVS_2',
        'TOTAL_EQUIPOS_COMPUTO':  'Total de equipos de cómputo en la plaza',
        'EQUIPOS_COMPUTO_OPERAN': 'Equipos de cómputo que operan',
        'TIPOS_EQUIPOS_COMPUTO':  'Tipos de equipos de cómputo',
        'IMPRESORAS_FUNCIONAN':   'Impresoras que funcionan',
        'IMPRESORAS_SUMINISTROS': 'Impresoras con suministros (toner, hojas)',
        'TOTAL_SERVIDORES':       'Total de servidores en la plaza',
        'SERVIDORES_FUNCIONAN':   'Número de servidores que funcionan correctamente',
        'MESAS_FUNCIONAN':        'Cuantas mesas funcionan',
        'SILLAS_FUNCIONAN':       'Cuantas sillas funcionan',
        'ANAQUELES_FUNCIONAN':    'Cuantos Anaqueles funcionan',
    }

    @classmethod
    def get_col(cls, key: str) -> str:
        return cls._MAP.get(key, key)


# ==============================================================================
# FUNCIONES AUXILIARES GLOBALES
# ==============================================================================
def normalizar_texto(texto: str) -> str:
    if not isinstance(texto, str):
        return ""
    return unidecode(texto).strip().upper()


def obtener_valor_seguro(fila: pd.Series, clave: str, default=None):
    nombre_col = Config.get_col(clave)
    if nombre_col and nombre_col in fila.index:
        val = fila[nombre_col]
        try:
            if pd.isna(val): return default
        except Exception:
            pass
        return val
    return default


def obtener_opciones_unicas(df: pd.DataFrame, columna: str) -> list:
    if df is None or columna not in df.columns:
        return []
    if df[columna].dtype.name == 'category':
        opciones = df[columna].cat.categories.tolist()
    else:
        opciones = df[columna].dropna().unique()
    limpias = [convertir_a_serializable(o) for o in opciones if str(o).strip()]
    return sorted([x for x in limpias if x is not None], key=str)


def _cargar_coordenadas_desde_json() -> list:
    archivo = Config.ARCHIVO_COORDENADAS
    if not os.path.exists(archivo):
        return []
    try:
        with open(archivo, 'rb') as f:
            datos = orjson.loads(f.read())
        if not isinstance(datos, list) or not datos:
            return []
        muestra = datos[0]
        if not ('lat' in muestra and 'lng' in muestra):
            return []
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
        return resultado
    except Exception as exc:
        logging.error(f"❌ _cargar_coordenadas_desde_json: {exc}")
        return []


# ==============================================================================
# CACHÉ DE ESTADÍSTICAS TTL 1 hora
# ==============================================================================
_ESTADISTICAS_CACHE:    dict             = {}
_ESTADISTICAS_CACHE_TS: datetime | None  = None
_ESTADISTICAS_TTL_SEG:  int              = 3600

def invalidar_cache_estadisticas() -> None:
    global _ESTADISTICAS_CACHE, _ESTADISTICAS_CACHE_TS
    _ESTADISTICAS_CACHE    = {}
    _ESTADISTICAS_CACHE_TS = None

def _mask_por_clave(df: pd.DataFrame, clave_raw: str):
    clave_norm = unidecode(clave_raw).strip().upper()
    if "normalized_clave" in df.columns:
        return df["normalized_clave"] == clave_norm
    return df[Config.COLUMNA_CLAVE].astype(str).str.strip().str.upper() == clave_raw.upper()


# ==============================================================================
# COMPARATIVAS FIX — integrado directamente (corrige bugs A/B/C/D de v5.1)
# ==============================================================================

# Columnas numéricas para Rust — variantes con y sin tilde
_COLS_RUST_PARA_COMPARATIVAS = [
    "Latitud", "Longitud",
    "Clave_Edo",
    "Situación", "Situacion",
    "Inc_Total",
    "Aten_Total",
    "CN_Tot_Acum", "CN_Inicial_Acum", "CN_Prim_Acum", "CN_Sec_Acum",
]


def _comparativas_detectar_col_mes(df: pd.DataFrame):
    """
    Detecta la columna de mes NUMÉRICO.
    BUG-A fix: prioriza 'Cve-mes' (int) sobre 'Mes' (string 'Enero'...).
    BUG-B fix: búsqueda robusta con múltiples variantes del nombre.
    """
    for candidato in ["Cve-mes", "cve_mes", "cve-mes", "CVE-MES", "CVE_MES"]:
        if candidato in df.columns:
            check = pd.to_numeric(df[candidato], errors="coerce")
            if not check.isna().all():
                return candidato
    # Fallback: 'Mes' solo si contiene valores numéricos
    for candidato in ["Mes", "mes", "MES"]:
        if candidato in df.columns:
            check = pd.to_numeric(df[candidato], errors="coerce")
            if not check.isna().all():
                logging.warning(f"_comparativas_detectar_col_mes: usando '{candidato}' como fallback numérico")
                return candidato
    return None


def _comparativas_detectar_col_anio(df: pd.DataFrame):
    """
    Detecta la columna de año de forma robusta.
    BUG-D fix: incluye búsqueda case-insensitive.
    """
    for candidato in ["Año", "anio", "ANIO", "año", "AÑO", "year", "Year", "YEAR"]:
        if candidato in df.columns:
            return candidato
    cols_lower = {c.lower(): c for c in df.columns}
    for candidato in ["año", "anio", "year"]:
        if candidato in cols_lower:
            encontrado = cols_lower[candidato]
            logging.warning(f"_comparativas_detectar_col_anio: encontrado via lowercase: '{encontrado}'")
            return encontrado
    return None


def _construir_mapa_comparativas(df: pd.DataFrame) -> dict:
    """Construye mapa {clave_edo_int: nombre_estado_str} desde DF traducido."""
    if df is None or df.empty:
        return {}
    col_clave  = next((c for c in ["Clave_Edo", "clave_edo"] if c in df.columns), None)
    col_nombre = next((c for c in ["Estado", "estado"]       if c in df.columns), None)
    if col_clave is None or col_nombre is None:
        logging.warning(
            f"_construir_mapa_comparativas: "
            f"Clave_Edo={'✅' if col_clave else '❌'} Estado={'✅' if col_nombre else '❌'}"
        )
        return {}
    mapa = {}
    for _, row in df[[col_clave, col_nombre]].drop_duplicates().iterrows():
        try:
            eid = int(float(row[col_clave]))
            nom = str(row[col_nombre]).strip()
            if nom and nom.lower() not in ("nan", "none", ""):
                mapa[eid] = nom
        except (ValueError, TypeError):
            pass
    return mapa


def _cargar_periodo_en_rust(periodo_cache, key: int, df: pd.DataFrame) -> bool:
    """
    Carga el DF en Rust usando solo las columnas existentes.
    BUG-C fix: evita fallar cuando 'Situación' con tilde no existe en el parquet.
    """
    if periodo_cache._rust is None:
        return False
    cols = [c for c in _COLS_RUST_PARA_COMPARATIVAS if c in df.columns]
    if len(cols) < 2:
        logging.warning(f"_cargar_periodo_en_rust({key}): solo {len(cols)} cols disponibles")
        return False
    try:
        buf = io.BytesIO()
        df[cols].to_parquet(buf, index=False, compression="None")
        n = periodo_cache._rust.cargar_periodo_parquet(buf.getvalue(), key)
        logging.info(f"  🦀 Rust key={key}: {n} filas | cols={cols}")
        return True
    except Exception as exc:
        logging.error(f"  _cargar_periodo_en_rust({key}): {exc}")
        return False


def _set_main_df_robusto(periodo_cache, df: pd.DataFrame) -> None:
    """
    Reemplaza ParquetPeriodoCache.set_main_df() con detección robusta de columnas.
    Corrige BUG-A (Mes string), BUG-B (Cve-mes lookup), BUG-C (Situación tilde), BUG-D (Año ausente).
    """
    if df is None or df.empty:
        logging.warning("_set_main_df_robusto: DF vacío")
        return

    cols_df = [c for c in df.columns if not c.startswith("normalized_")]
    logging.info(f"_set_main_df_robusto: {len(df):,} filas | cols({len(cols_df)}): {cols_df[:20]}")

    col_mes  = _comparativas_detectar_col_mes(df)
    col_anio = _comparativas_detectar_col_anio(df)

    if col_mes is None:
        logging.error(
            f"❌ _set_main_df_robusto: columna numérica de MES no encontrada. "
            f"Columnas disponibles: {cols_df}"
        )
        return

    if col_anio is None:
        logging.error(
            f"❌ _set_main_df_robusto: columna de AÑO no encontrada. "
            f"Columnas disponibles: {cols_df}"
        )
        return

    logging.info(
        f"  col_mes='{col_mes}' ej={df[col_mes].dropna().iloc[0]!r} | "
        f"col_anio='{col_anio}' ej={df[col_anio].dropna().iloc[0]!r}"
    )

    cols_rust_ok = [c for c in _COLS_RUST_PARA_COMPARATIVAS if c in df.columns]
    logging.info(f"  Columnas Rust: {len(cols_rust_ok)}/{len(_COLS_RUST_PARA_COMPARATIVAS)} → {cols_rust_ok}")

    meses_indexados = []
    errores         = []

    try:
        grupos = df.groupby([col_anio, col_mes], dropna=True)
    except Exception as exc:
        logging.error(f"_set_main_df_robusto: groupby falló — {exc}")
        return

    for (anio_v, mes_v), sub in grupos:
        try:
            a = int(float(str(anio_v)))
            m = int(float(str(mes_v)))
            if a < 100:
                a = 2000 + a
            key = a * 100 + m
            sub_reset = sub.reset_index(drop=True)

            with periodo_cache._lock:
                periodo_cache._df_actual[key]        = sub_reset
                periodo_cache._mapa[key]             = _construir_mapa_comparativas(sub_reset)
                periodo_cache._claves_protegidas.add(key)

            _cargar_periodo_en_rust(periodo_cache, key, sub_reset)
            meses_indexados.append(f"{a}-{m:02d}")

        except (ValueError, TypeError) as exc:
            errores.append(f"{anio_v}-{mes_v}: {exc}")

    if errores:
        logging.warning(f"  Errores indexación ({len(errores)}): {errores[:5]}")

    if meses_indexados:
        logging.info(
            f"✅ _set_main_df_robusto: {len(meses_indexados)} periodos indexados "
            f"→ {sorted(meses_indexados)}"
        )
    else:
        logging.error(
            f"❌ _set_main_df_robusto: NINGÚN periodo indexado. "
            f"'{col_mes}' únicos: {df[col_mes].dropna().unique()[:10].tolist()} | "
            f"'{col_anio}' únicos: {df[col_anio].dropna().unique()[:10].tolist()}"
        )


# ==============================================================================
# DATAFRAME CACHE
# ==============================================================================
class DataframeCache:
    def __init__(self):
        self._df                = None
        self._df_ultimo_mes     = None
        self._cache_ts          = None
        self._estados_cache     = None
        self._zonas_cache       = {}
        self._municipios_cache  = {}
        self._localidades_cache = {}
        self._coordenadas_cache: list | None = None

    def cargar_dataframe(self, force_reload: bool = False) -> pd.DataFrame:
        if self._df is not None and not force_reload:
            return self._df
        try:
            if os.path.exists(Config.PARQUET_PATH):
                logging.info(f"📂 Cargando parquet: {Config.PARQUET_PATH}")
                df_raw = pd.read_parquet(Config.PARQUET_PATH)
                logging.info(f"✅ Parquet leído: {len(df_raw)} filas")
                if TRANSLATOR_AVAILABLE and _col_translator is not None:
                    report = _col_translator.check_schema(df_raw)
                    logging.info(f"🔎 Esquema: {report}")
                    df_raw = _col_translator.translate(df_raw)
                    logging.info("✅ Columnas traducidas al esquema legacy")
                else:
                    logging.warning("⚠️ Sin traductor — se usa parquet tal cual")
            elif os.path.exists(Config.EXCEL_PATH):
                logging.warning(f"⚠️ Parquet no encontrado, usando excel: {Config.EXCEL_PATH}")
                df_raw = pd.read_excel(Config.EXCEL_PATH)
                try:
                    df_raw.to_parquet(Config.PARQUET_PATH)
                    logging.info("💾 Excel convertido a parquet")
                except Exception as ex:
                    logging.error(f"❌ No se pudo guardar parquet: {ex}")
            else:
                logging.critical("❌ No hay parquet ni excel")
                self._df = pd.DataFrame()
                return self._df

            self._df       = self._preparar_dataframe(df_raw)
            self._cache_ts = datetime.now()
            self._clear_secondary_caches()
            logging.info(f"🎉 DataFrame listo: {len(self._df)} filas")
            return self._df

        except Exception as exc:
            logging.error(f"❌ Error cargando DataFrame: {exc}\n{traceback.format_exc()}")
            self._df = pd.DataFrame()
            return self._df

    def _preparar_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        _norm_map = {
            Config.COLUMNA_ESTADO:     'normalized_estado',
            Config.COLUMNA_COORD_ZONA: 'normalized_zona',
            Config.COLUMNA_MUNICIPIO:  'normalized_municipio',
            Config.COLUMNA_LOCALIDAD:  'normalized_localidad',
            Config.COLUMNA_CLAVE:      'normalized_clave',
        }
        for col_real, col_norm in _norm_map.items():
            if col_real in df.columns:
                df[col_norm] = df[col_real].fillna('').astype(str).apply(normalizar_texto).astype('category')
            else:
                logging.warning(f"⚠️ Columna '{col_real}' no encontrada para normalizar")
                df[col_norm] = pd.Categorical([''] * len(df))
        for col in [Config.COLUMNA_LATITUD, Config.COLUMNA_LONGITUD]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def get_dataframe(self) -> pd.DataFrame:
        return self.cargar_dataframe()

    def get_ultimo_mes(self) -> pd.DataFrame:
        if self._df_ultimo_mes is None:
            self._df_ultimo_mes = _filtrar_ultimo_mes(self.get_dataframe())
        return self._df_ultimo_mes

    def get_estados_cache(self) -> list:
        if self._estados_cache is None:
            df = self.get_ultimo_mes()
            if Config.COLUMNA_ESTADO in df.columns:
                cnts = df.groupby(Config.COLUMNA_ESTADO)[Config.COLUMNA_CLAVE].nunique()
                self._estados_cache = sorted(
                    [{'nombre': str(e), 'cantidad': int(c)} for e, c in cnts.items()],
                    key=lambda x: x['cantidad'], reverse=True,
                )
            else:
                self._estados_cache = []
        return self._estados_cache

    def get_coordenadas(self) -> list:
        if self._coordenadas_cache is not None:
            return self._coordenadas_cache
        plazas = _cargar_coordenadas_desde_json()
        if plazas:
            self._coordenadas_cache = plazas
            logging.info(f"📍 Coordenadas desde JSON: {len(plazas)} plazas")
            return plazas
        logging.warning("⚠️ coordenadasplazas.json no disponible — generando desde DataFrame")
        plazas = self._generar_coordenadas_desde_df()
        self._coordenadas_cache = plazas
        return plazas

    def _generar_coordenadas_desde_df(self) -> list:
        df = self.get_ultimo_mes()
        plazas: list[dict] = []
        cols_necesarias = {
            "clave": Config.COLUMNA_CLAVE, "nombre": Config.COLUMNA_NOMBRE_PC,
            "estado": Config.COLUMNA_ESTADO, "municipio": Config.COLUMNA_MUNICIPIO,
            "localidad": Config.COLUMNA_LOCALIDAD, "situacion": Config.COLUMNA_SITUACION,
            "lat": Config.COLUMNA_LATITUD, "lng": Config.COLUMNA_LONGITUD,
        }
        cols_presentes = {k: v for k, v in cols_necesarias.items() if v in df.columns}
        col_lat = cols_presentes.get("lat")
        col_lng = cols_presentes.get("lng")
        if col_lat and col_lng:
            df_sub = df.dropna(subset=[col_lat, col_lng]).copy()
            for _, row in df_sub.iterrows():
                try:
                    plazas.append({
                        "clave":     str(row.get(cols_presentes.get("clave",    ""), "") or ""),
                        "nombre":    str(row.get(cols_presentes.get("nombre",   ""), "") or ""),
                        "estado":    str(row.get(cols_presentes.get("estado",   ""), "") or ""),
                        "municipio": str(row.get(cols_presentes.get("municipio",""), "") or ""),
                        "localidad": str(row.get(cols_presentes.get("localidad",""), "") or ""),
                        "situacion": str(row.get(cols_presentes.get("situacion",""), "") or ""),
                        "lat":       round(float(row[col_lat]), 6),
                        "lng":       round(float(row[col_lng]), 6),
                    })
                except (TypeError, ValueError):
                    continue
        return plazas

    def _clear_secondary_caches(self):
        self._df_ultimo_mes     = None
        self._estados_cache     = None
        self._coordenadas_cache = None
        self._zonas_cache.clear()
        self._municipios_cache.clear()
        self._localidades_cache.clear()

    def refresh_cache(self) -> pd.DataFrame:
        logging.info("🔄 Refrescando cache")
        self._df = None
        self._clear_secondary_caches()
        df = self.cargar_dataframe(force_reload=True)
        def _rebuild():
            if PLAZA_INDEX_AVAILABLE and _plaza_index is not None:
                _plaza_index.rebuild(
                    self.get_ultimo_mes(),
                    col_estado=Config.COLUMNA_ESTADO, col_zona=Config.COLUMNA_COORD_ZONA,
                    col_municipio=Config.COLUMNA_MUNICIPIO, col_localidad=Config.COLUMNA_LOCALIDAD,
                    col_clave=Config.COLUMNA_CLAVE,
                )
            if PRECALC_AVAILABLE and _stats_cache is not None:
                _stats_cache.rebuild(Config.PARQUET_PATH)
            if RUST_BRIDGE_AVAILABLE and rust_bridge is not None:
                rust_bridge.rebuild(Config.PARQUET_PATH)
            if COMPARATIVAS_ENGINE_AVAILABLE and _periodo_cache is not None:
                try:
                    _set_main_df_robusto(_periodo_cache, df)
                    logging.info("🔄 ComparativasEngine: DF actualizado")
                except Exception as exc:
                    logging.warning(f"⚠️ ComparativasEngine refresh: {exc}")
            invalidar_cache_estadisticas()
        threading.Thread(target=_rebuild, daemon=True).start()
        return df


dataframe_cache = DataframeCache()


# ==============================================================================
# COMPARATIVAS ENGINE v5.1
# ==============================================================================
_periodo_cache = None
_comparativas_engine = None
_watchdog = None

if COMPARATIVAS_ENGINE_AVAILABLE:
    _periodo_cache = ParquetPeriodoCache(
        parquet_path=Config.PARQUET_PATH,
        excel_tree_json=Config.EXCEL_TREE_PATH,
        plaza_rust=_plaza_rust_mod,
    )

    _comparativas_engine = ComparativasEngine(
        cache=_periodo_cache,
        plaza_rust=_plaza_rust_mod,
    )

    _watchdog = Watchdog(_periodo_cache)

    logging.info("✅ ComparativasEngine v5.1 instanciado")
else:
    logging.warning("⚠️ ComparativasEngine no disponible")
    
def _init_comparativas():
    """
    Inicializa el motor de comparativas usando _set_main_df_robusto
    en lugar de _periodo_cache.set_main_df directo.
    Corrige bugs A/B/C/D de set_main_df v5.1.
    """
    if not COMPARATIVAS_ENGINE_AVAILABLE or _periodo_cache is None:
        return

    _periodo_cache.cargar_indice()

    df = dataframe_cache.get_dataframe()
    if df is not None and not df.empty:
        cols_criticas = {"Cve-mes": "mes numérico", "Año": "año",
                         "Clave_Edo": "clave estado", "Estado": "nombre estado"}
        for col, desc in cols_criticas.items():
            if col not in df.columns:
                logging.error(f"  ❌ Columna crítica ausente: '{col}' ({desc})")
            else:
                ej = df[col].dropna().iloc[0] if not df[col].dropna().empty else "vacío"
                logging.info(f"  ✅ '{col}' ({desc}): ej={ej!r}")

        _set_main_df_robusto(_periodo_cache, df)
    else:
        logging.warning("⚠️ DF principal vacío al init comparativas")

    _watchdog.start()
    logging.info(
        f"✅ ComparativasEngine v5.1 listo "
        f"({len(_periodo_cache._claves_protegidas)} periodos locales protegidos)"
    )


# ==============================================================================
# FILTROS Y UTILIDADES INTERNAS
# ==============================================================================
def _filtrar_ultimo_mes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or Config.COLUMNA_CVE_MES not in df.columns:
        return df.copy()
    try:
        temp = pd.to_numeric(df[Config.COLUMNA_CVE_MES], errors='coerce')
        max_mes = temp.max()
        if pd.isna(max_mes):
            return df.copy()
        return df[temp == max_mes].copy()
    except Exception as exc:
        logging.error(f"Error filtrando último mes: {exc}")
        return df.copy()


def filtrar_df_cascada(df: pd.DataFrame, filtros: dict) -> pd.DataFrame:
    if df.empty:
        return df
    _col_norm = {
        'ESTADO':      'normalized_estado',
        'COORD_ZONA':  'normalized_zona',
        'MUNICIPIO':   'normalized_municipio',
        'LOCALIDAD':   'normalized_localidad',
        'CLAVE_PLAZA': 'normalized_clave',
    }
    result = df
    for clave, valor in filtros.items():
        if not valor:
            continue
        val_norm = normalizar_texto(str(valor))
        col_opt  = _col_norm.get(clave)
        col_real = Config.get_col(clave)
        if col_opt and col_opt in result.columns:
            result = result[result[col_opt] == val_norm]
        elif col_real and col_real in result.columns:
            result = result[result[col_real].fillna('').astype(str).apply(normalizar_texto) == val_norm]
        else:
            logging.error(f"❌ filtrar_df_cascada: columna '{clave}' no encontrada")
            return pd.DataFrame()
    return result


# ==============================================================================
# RUTAS
# ==============================================================================
DRIVE_TREE_FILE = Config.DRIVE_TREE_PATH

@app.route('/')
def inicio():
    return render_template('inicio.html')

@app.route('/ejecutiva')
def vista_ejecutiva():
    return render_template('ejecutiva.html')

@app.route('/avanzada')
def vista_avanzada():
    return render_template('index.html')


@app.route('/api/estados')
def get_estados():
    if PLAZA_INDEX_AVAILABLE and _plaza_index and _plaza_index.is_ready:
        estados = _plaza_index.get_estados()
        if estados:
            return jsonify(estados)
    df = dataframe_cache.get_dataframe()
    estados = obtener_opciones_unicas(df, Config.COLUMNA_ESTADO)
    if not estados:
        return jsonify({'error': 'Información de estados no disponible'}), 500
    return jsonify(estados)


@app.route('/api/estados_con_conteo')
def get_estados_con_conteo():
    try:
        if PLAZA_INDEX_AVAILABLE and _plaza_index and _plaza_index.is_ready:
            data = _plaza_index.get_estados_con_conteo()
            if data:
                return json_response(data)
        data = dataframe_cache.get_estados_cache()
        if not data:
            return jsonify({'error': 'No hay datos'}), 503
        return json_response(data)
    except Exception as exc:
        logging.error(f"get_estados_con_conteo: {exc}")
        return jsonify({'error': str(exc)}), 500


@app.route('/api/estados_populares')
def get_estados_populares():
    try:
        if PLAZA_INDEX_AVAILABLE and _plaza_index and _plaza_index.is_ready:
            data = _plaza_index.get_estados_populares(n=8)
            if data:
                return jsonify(data)
        df = dataframe_cache.get_ultimo_mes()
        if df.empty:
            return jsonify({'error': 'No hay datos'}), 503
        cnts = df[Config.COLUMNA_ESTADO].value_counts()
        return jsonify([{'nombre': str(e), 'cantidad': int(c)} for e, c in cnts.head(8).items()])
    except Exception as exc:
        logging.error(f"get_estados_populares: {exc}")
        return jsonify({'error': str(exc)}), 500


@app.route('/api/zonas')
def get_zonas_por_estado():
    estado = request.args.get('estado', '')
    if not estado:
        return jsonify([])
    if PLAZA_INDEX_AVAILABLE and _plaza_index and _plaza_index.is_ready:
        return jsonify(_plaza_index.get_zonas(estado))
    df = filtrar_df_cascada(dataframe_cache.get_ultimo_mes(), {'ESTADO': estado})
    return jsonify(obtener_opciones_unicas(df, Config.COLUMNA_COORD_ZONA))


@app.route('/api/municipios')
def get_municipios_por_zona():
    estado = request.args.get('estado', '')
    zona   = request.args.get('zona', '')
    if not (estado and zona):
        return jsonify([])
    if PLAZA_INDEX_AVAILABLE and _plaza_index and _plaza_index.is_ready:
        return jsonify(_plaza_index.get_municipios(estado, zona))
    df = filtrar_df_cascada(dataframe_cache.get_ultimo_mes(), {'ESTADO': estado, 'COORD_ZONA': zona})
    return jsonify(obtener_opciones_unicas(df, Config.COLUMNA_MUNICIPIO))


@app.route('/api/localidades')
def get_localidades_por_municipio():
    estado    = request.args.get('estado', '')
    zona      = request.args.get('zona', '')
    municipio = request.args.get('municipio', '')
    if not all([estado, zona, municipio]):
        return jsonify([])
    if PLAZA_INDEX_AVAILABLE and _plaza_index and _plaza_index.is_ready:
        return jsonify(_plaza_index.get_localidades(estado, zona, municipio))
    df = filtrar_df_cascada(dataframe_cache.get_ultimo_mes(), {'ESTADO': estado, 'COORD_ZONA': zona, 'MUNICIPIO': municipio})
    return jsonify(obtener_opciones_unicas(df, Config.COLUMNA_LOCALIDAD))


@app.route('/api/claves_plaza')
def get_claves_por_localidad():
    estado    = request.args.get('estado', '')
    zona      = request.args.get('zona', '')
    municipio = request.args.get('municipio', '')
    localidad = request.args.get('localidad', '')
    if not all([estado, zona, municipio, localidad]):
        return jsonify([])
    if PLAZA_INDEX_AVAILABLE and _plaza_index and _plaza_index.is_ready:
        return jsonify(_plaza_index.get_claves(estado, zona, municipio, localidad))
    df = filtrar_df_cascada(dataframe_cache.get_ultimo_mes(),
                            {'ESTADO': estado, 'COORD_ZONA': zona, 'MUNICIPIO': municipio, 'LOCALIDAD': localidad})
    return jsonify(obtener_opciones_unicas(df, Config.COLUMNA_CLAVE))


@app.route('/api/search')
def api_search_plaza():
    try:
        clave_busqueda = request.args.get('clave', '').strip().upper()
        if not clave_busqueda:
            return jsonify({'error': 'Proporciona una clave.'}), 400
        df = dataframe_cache.get_ultimo_mes()
        if df.empty:
            return jsonify({'error': 'Base de datos no cargada.'}), 503
        if Config.COLUMNA_CLAVE not in df.columns:
            return jsonify({'error': 'Columna Clave_Plaza no encontrada'}), 500
        mask = _mask_por_clave(df, clave_busqueda)
        plaza_data = df[mask]
        if plaza_data.empty:
            return jsonify({'error': f'No se encontraron resultados para: {clave_busqueda}'}), 404
        fila = plaza_data.iloc[0]
        def gv(key):
            return convertir_a_serializable(obtener_valor_seguro(fila, key))
        partes_dir = [str(gv('COLONIA') or ''), str(gv('CALLE') or ''),
                      str(gv('NUM') or ''), str(gv('COD_POST') or '')]
        direccion  = ', '.join(p for p in partes_dir if p.strip())
        lat, lon = gv('LATITUD'), gv('LONGITUD')
        maps_url = None
        if lat and lon:
            try:
                maps_url = f"https://www.google.com/maps/search/?api=1&query={float(lat)},{float(lon)}"
            except Exception:
                pass
        images = []
        try:
            images = find_image_urls(clave_busqueda)
        except Exception as exc:
            logging.warning(f"Error buscando imágenes: {exc}")
        datos_organizados = {
            'informacion_general': {
                'Clave_Plaza': gv('CLAVE_PLAZA'), 'Nombre_PC': gv('NOMBRE_PC'),
                'Situación': gv('SITUACION'), 'Tipo_local': gv('TIPO_LOCAL'),
                'Inst_aliada': gv('INST_ALIADA'), 'Arq_Discap.': gv('ARQ_DISCAP'),
                'Conect_Instalada': gv('CONECT_INSTALADA'), 'Tipo_Conect': gv('TIPO_CONECT'),
            },
            'ubicacion': {
                'Estado': gv('ESTADO'), 'Clave_Edo': gv('CLAVE_EDO'),
                'Coord. Zona': gv('COORD_ZONA'), 'Municipio': gv('MUNICIPIO'),
                'Localidad': gv('LOCALIDAD'), 'Colonia': gv('COLONIA'),
                'Calle': gv('CALLE'), 'Num': gv('NUM'), 'Cod_Post': gv('COD_POST'),
                'Direccion_Completa': direccion, 'Latitud': lat, 'Longitud': lon,
            },
            'fecha_periodo': {'Año': gv('ANO'), 'Cve-mes': gv('CVE_MES'), 'Mes': gv('MES')},
            'incripciones': {
                'Inc_Inicial': gv('INC_INICIAL'), 'Inc_Prim': gv('INC_PRIM'),
                'Inc_Sec': gv('INC_SEC'), 'Inc_Total': gv('INC_TOTAL'),
            },
            'atenciones': {
                'Aten_Inicial': gv('ATEN_INICIAL'), 'Aten_Prim': gv('ATEN_PRIM'),
                'Aten_Sec': gv('ATEN_SEC'), 'Aten_Total': gv('ATEN_TOTAL'),
                'Exámenes aplicados': gv('EXAMENES_APLICADOS'),
            },
            'certificaciones': {
                'CN_Inicial_Acum': gv('CN_INICIAL_ACUM'), 'CN_Prim_Acum': gv('CN_PRIM_ACUM'),
                'CN_Sec_Acum': gv('CN_SEC_ACUM'), 'CN_Tot_Acum': gv('CN_TOT_ACUM'),
                'Cert_Emitidos': gv('CERT_EMITIDOS'),
            },
            'personal': {'Tec_Doc': gv('TEC_DOC'), 'Nom_PVS_1': gv('NOM_PVS_1'), 'Nom_PVS_2': gv('NOM_PVS_2')},
            'equipamiento': {
                Config.COLUMNA_TOTAL_EQUIPOS_COMPUTO:  gv('TOTAL_EQUIPOS_COMPUTO'),
                Config.COLUMNA_EQUIPOS_COMPUTO_OPERAN: gv('EQUIPOS_COMPUTO_OPERAN'),
                Config.COLUMNA_TIPOS_EQUIPOS_COMPUTO:  gv('TIPOS_EQUIPOS_COMPUTO'),
                Config.COLUMNA_IMPRESORAS_FUNCIONAN:   gv('IMPRESORAS_FUNCIONAN'),
                Config.COLUMNA_IMPRESORAS_SUMINISTROS: gv('IMPRESORAS_SUMINISTROS'),
                Config.COLUMNA_TOTAL_SERVIDORES:       gv('TOTAL_SERVIDORES'),
                Config.COLUMNA_SERVIDORES_FUNCIONAN:   gv('SERVIDORES_FUNCIONAN'),
            },
            'mobiliario': {
                Config.COLUMNA_MESAS_FUNCIONAN:     gv('MESAS_FUNCIONAN'),
                Config.COLUMNA_SILLAS_FUNCIONAN:    gv('SILLAS_FUNCIONAN'),
                Config.COLUMNA_ANAQUELES_FUNCIONAN: gv('ANAQUELES_FUNCIONAN'),
            },
        }
        todos = {k: v for k, v in convertir_a_serializable(fila.to_dict()).items()
                 if not str(k).startswith('normalized_')}
        return jsonify({
            'datos_organizados': datos_organizados, 'direccion_completa': direccion,
            'images': images, 'google_maps_url': maps_url,
            'todos_los_datos': todos, 'excel_info': todos,
        })
    except Exception as exc:
        logging.error(f"api_search_plaza: {exc}\n{traceback.format_exc()}")
        return jsonify({'error': f'Error interno: {exc}'}), 500


@app.route('/api/plaza-detallada/<clave>')
def get_plaza_detallada(clave):
    try:
        df = dataframe_cache.get_dataframe()
        if df.empty:
            return jsonify({'error': 'No hay datos'}), 503
        clave_busqueda = clave.strip().upper()
        plaza_data = df[_mask_por_clave(df, clave_busqueda)]
        if plaza_data.empty:
            return jsonify({'error': f'No se encontró: {clave}'}), 404
        plaza_dict = {k: convertir_a_serializable(v)
                      for k, v in plaza_data.iloc[0].replace({np.nan: None}).to_dict().items()}
        categorias = {
            'informacion_general': [Config.COLUMNA_CLAVE, Config.COLUMNA_NOMBRE_PC, Config.COLUMNA_SITUACION,
                                    Config.COLUMNA_TIPO_LOCAL, Config.COLUMNA_INST_ALIADA, Config.COLUMNA_ARQ_DISCAP],
            'conectividad':        [Config.COLUMNA_CONECT_INSTALADA, Config.COLUMNA_TIPO_CONECT],
            'ubicacion':           [Config.COLUMNA_ESTADO, Config.COLUMNA_CLAVE_EDO, Config.COLUMNA_COORD_ZONA,
                                    Config.COLUMNA_MUNICIPIO, Config.COLUMNA_LOCALIDAD, Config.COLUMNA_COLONIA,
                                    Config.COLUMNA_CALLE, Config.COLUMNA_NUM, Config.COLUMNA_COD_POST,
                                    Config.COLUMNA_LATITUD, Config.COLUMNA_LONGITUD],
            'fecha_periodo':       [Config.COLUMNA_ANO, Config.COLUMNA_CVE_MES, Config.COLUMNA_MES],
            'incripciones':        [Config.COLUMNA_INC_INICIAL, Config.COLUMNA_INC_PRIM,
                                    Config.COLUMNA_INC_SEC, Config.COLUMNA_INC_TOTAL],
            'atenciones':          [Config.COLUMNA_ATEN_INICIAL, Config.COLUMNA_ATEN_PRIM,
                                    Config.COLUMNA_ATEN_SEC, Config.COLUMNA_ATEN_TOTAL,
                                    Config.COLUMNA_EXAMENES_APLICADOS],
            'certificaciones':     [Config.COLUMNA_CN_INICIAL_ACUM, Config.COLUMNA_CN_PRIM_ACUM,
                                    Config.COLUMNA_CN_SEC_ACUM, Config.COLUMNA_CN_TOT_ACUM,
                                    Config.COLUMNA_CERT_EMITIDOS],
            'personal':            [Config.COLUMNA_TEC_DOC, Config.COLUMNA_NOM_PVS_1, Config.COLUMNA_NOM_PVS_2],
            'equipamiento':        [Config.COLUMNA_TOTAL_EQUIPOS_COMPUTO, Config.COLUMNA_EQUIPOS_COMPUTO_OPERAN,
                                    Config.COLUMNA_TIPOS_EQUIPOS_COMPUTO, Config.COLUMNA_IMPRESORAS_FUNCIONAN,
                                    Config.COLUMNA_IMPRESORAS_SUMINISTROS, Config.COLUMNA_TOTAL_SERVIDORES,
                                    Config.COLUMNA_SERVIDORES_FUNCIONAN],
            'mobiliario':          [Config.COLUMNA_MESAS_FUNCIONAN, Config.COLUMNA_SILLAS_FUNCIONAN,
                                    Config.COLUMNA_ANAQUELES_FUNCIONAN],
        }
        datos_completos = {
            cat: {col: plaza_dict[col] for col in cols if col in plaza_dict}
            for cat, cols in categorias.items()
        }
        return jsonify({
            'status': 'success', 'clave': clave_busqueda, 'datos': datos_completos,
            'images': find_image_urls(clave),
            'datos_completos': {k: v for k, v in plaza_dict.items() if not k.startswith('normalized_')},
        })
    except Exception as exc:
        logging.error(f"get_plaza_detallada: {exc}")
        return jsonify({'error': str(exc)}), 500


@app.route('/api/plaza-historial')
def get_plaza_historial():
    try:
        clave_busqueda = request.args.get('clave', '').strip().upper()
        if not clave_busqueda:
            return jsonify([])
        df = dataframe_cache.get_dataframe()
        if df.empty or Config.COLUMNA_CLAVE not in df.columns:
            return jsonify([])
        df_h = df[_mask_por_clave(df, clave_busqueda)].copy()
        if df_h.empty:
            return jsonify([])
        if Config.COLUMNA_ANO in df_h.columns and Config.COLUMNA_CVE_MES in df_h.columns:
            df_h['__sa'] = pd.to_numeric(df_h[Config.COLUMNA_ANO], errors='coerce').fillna(0)
            df_h['__sm'] = pd.to_numeric(df_h[Config.COLUMNA_CVE_MES], errors='coerce').fillna(0)
            df_h = df_h.sort_values(['__sa', '__sm'], ascending=[False, False])
        return jsonify([
            {k: v for k, v in convertir_a_serializable(row.to_dict()).items()
             if not k.startswith(('normalized_', '__'))}
            for _, row in df_h.iterrows()
        ])
    except Exception as exc:
        logging.error(f"get_plaza_historial: {exc}")
        return jsonify([])


@app.route('/api/plazas_por_estado/<estado>')
def get_plazas_por_estado(estado):
    try:
        df = dataframe_cache.get_ultimo_mes()
        if df.empty:
            return jsonify({'error': 'No hay datos'}), 503
        df_f = df[df[Config.COLUMNA_ESTADO] == estado]
        if df_f.empty:
            return jsonify([])
        plazas = []
        for _, p in df_f.iterrows():
            partes = [str(p.get(c, '') or '').strip()
                      for c in [Config.COLUMNA_COLONIA, Config.COLUMNA_CALLE,
                                 Config.COLUMNA_NUM, Config.COLUMNA_COD_POST]]
            plazas.append({
                'clave': p[Config.COLUMNA_CLAVE], 'direccion': ', '.join(x for x in partes if x),
                'municipio': p.get(Config.COLUMNA_MUNICIPIO, ''), 'localidad': p.get(Config.COLUMNA_LOCALIDAD, ''),
            })
        return jsonify(plazas)
    except Exception as exc:
        logging.error(f"get_plazas_por_estado: {exc}")
        return jsonify({'error': str(exc)}), 500


@app.route('/api/busqueda_global')
def busqueda_global():
    try:
        query = request.args.get('q', '').strip().lower()
        if not query or len(query) < 2:
            return jsonify([])
        df = dataframe_cache.get_ultimo_mes()
        if df.empty:
            return jsonify({'error': 'No hay datos'}), 503
        cols_busqueda = [Config.COLUMNA_CLAVE, Config.COLUMNA_ESTADO, Config.COLUMNA_MUNICIPIO,
                         Config.COLUMNA_LOCALIDAD, Config.COLUMNA_COLONIA, Config.COLUMNA_CALLE]
        mask = pd.Series(False, index=df.index)
        for col in cols_busqueda:
            if col in df.columns:
                mask |= df[col].fillna("").astype(str).str.lower().str.contains(query, regex=False, na=False)
        matches = df[mask].drop_duplicates(subset=[Config.COLUMNA_CLAVE]).head(20)
        resultados = []
        for _, plaza in matches.iterrows():
            partes = [str(plaza.get(c, "") or "").strip()
                      for c in [Config.COLUMNA_COLONIA, Config.COLUMNA_CALLE, Config.COLUMNA_NUM, Config.COLUMNA_COD_POST]]
            resultados.append({
                "tipo": "Plaza", "clave": plaza[Config.COLUMNA_CLAVE],
                "estado": plaza.get(Config.COLUMNA_ESTADO, ""), "municipio": plaza.get(Config.COLUMNA_MUNICIPIO, ""),
                "direccion": ", ".join(x for x in partes if x),
            })
        return jsonify(resultados)
    except Exception as exc:
        logging.error(f"busqueda_global: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route('/api/columnas-disponibles')
def get_columnas_disponibles():
    try:
        df = dataframe_cache.get_dataframe()
        if df.empty:
            return jsonify({'error': 'No hay datos'}), 503
        cols = [c for c in df.columns if not c.startswith('normalized_')]
        esperadas = list(Config._MAP.values())
        return jsonify({
            'total_columnas': len(cols), 'columnas': cols,
            'columnas_esperadas': esperadas,
            'columnas_faltantes': [c for c in esperadas if c not in cols],
        })
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/estadisticas')
def get_estadisticas():
    global _ESTADISTICAS_CACHE, _ESTADISTICAS_CACHE_TS
    ahora = datetime.now()
    if (_ESTADISTICAS_CACHE and _ESTADISTICAS_CACHE_TS
            and (ahora - _ESTADISTICAS_CACHE_TS).seconds < _ESTADISTICAS_TTL_SEG):
        return json_response(_ESTADISTICAS_CACHE)
    try:
        resultado = _calcular_estadisticas()
        if resultado:
            _ESTADISTICAS_CACHE    = resultado
            _ESTADISTICAS_CACHE_TS = ahora
        return json_response(resultado)
    except Exception as exc:
        logging.error(f"get_estadisticas: {exc}\n{traceback.format_exc()}")
        return jsonify({'error': str(exc)}), 500


def _calcular_estadisticas() -> dict:
    df = dataframe_cache.get_ultimo_mes()
    if df.empty:
        return {'error': 'No hay datos'}
    mes_usado = "Todos (Histórico)"
    if Config.COLUMNA_CVE_MES in df.columns:
        max_mes = pd.to_numeric(df[Config.COLUMNA_CVE_MES], errors='coerce').max()
        if pd.notna(max_mes):
            mes_usado = str(int(max_mes))
    total_plazas     = df[Config.COLUMNA_CLAVE].nunique()
    plazas_operacion = 0
    if Config.COLUMNA_SITUACION in df.columns:
        plazas_operacion = df[
            df[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
        ][Config.COLUMNA_CLAVE].nunique()
    estado_mas_plazas   = {'nombre': 'N/A', 'cantidad': 0}
    estado_menos_plazas = {'nombre': 'N/A', 'cantidad': 0}
    total_estados       = 0
    if Config.COLUMNA_ESTADO in df.columns:
        cnts = df.groupby(Config.COLUMNA_ESTADO)[Config.COLUMNA_CLAVE].nunique().sort_values(ascending=False)
        total_estados = len(cnts)
        if not cnts.empty:
            estado_mas_plazas   = {'nombre': str(cnts.index[0]),  'cantidad': int(cnts.iloc[0])}
            estado_menos_plazas = {'nombre': str(cnts.index[-1]), 'cantidad': int(cnts.iloc[-1])}
    estado_mayor_conectividad = {'nombre': 'N/A', 'porcentaje': 0}
    if Config.COLUMNA_CONECT_INSTALADA in df.columns:
        tmp = df.copy()
        tmp['_c'] = tmp[Config.COLUMNA_CONECT_INSTALADA].apply(
            lambda v: 1 if pd.notna(v) and str(v).strip().lower() not in ['','nan','na','none','null','0'] else 0)
        top = tmp.groupby(Config.COLUMNA_ESTADO)['_c'].mean().sort_values(ascending=False)
        if not top.empty:
            estado_mayor_conectividad = {'nombre': str(top.index[0]), 'porcentaje': round(float(top.iloc[0])*100, 2)}
    estado_mas_operacion  = {'nombre': 'N/A', 'porcentaje': 0}
    estado_mas_suspension = {'nombre': 'N/A', 'porcentaje': 0}
    if Config.COLUMNA_SITUACION in df.columns:
        tmp = df.copy()
        tmp['_sn'] = tmp[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper()
        tbl = tmp.groupby([Config.COLUMNA_ESTADO, '_sn'])[Config.COLUMNA_CLAVE].nunique().unstack(fill_value=0)
        tbl['_tot'] = tbl.sum(axis=1)
        if 'EN OPERACIÓN' in tbl.columns:
            tbl['_po'] = tbl['EN OPERACIÓN'] / tbl['_tot']
            top_op = tbl['_po'].idxmax()
            estado_mas_operacion = {'nombre': str(top_op), 'porcentaje': round(float(tbl.loc[top_op, '_po'])*100, 2)}
        if 'SUSPENSIÓN TEMPORAL' in tbl.columns:
            tbl['_ps'] = tbl['SUSPENSIÓN TEMPORAL'] / tbl['_tot']
            top_su = tbl['_ps'].idxmax()
            estado_mas_suspension = {'nombre': str(top_su), 'porcentaje': round(float(tbl.loc[top_su, '_ps'])*100, 2)}
    est_equip = {}
    if Config.COLUMNA_TOTAL_EQUIPOS_COMPUTO in df.columns:
        tot = pd.to_numeric(df[Config.COLUMNA_TOTAL_EQUIPOS_COMPUTO], errors='coerce').sum()
        op  = pd.to_numeric(df[Config.COLUMNA_EQUIPOS_COMPUTO_OPERAN], errors='coerce').sum()
        est_equip['equipos_computo'] = {'total': float(tot), 'operando': float(op),
                                         'porcentaje_operativos': round(op/tot*100, 2) if tot else 0}
    if Config.COLUMNA_TOTAL_SERVIDORES in df.columns:
        tot = pd.to_numeric(df[Config.COLUMNA_TOTAL_SERVIDORES], errors='coerce').sum()
        op  = pd.to_numeric(df[Config.COLUMNA_SERVIDORES_FUNCIONAN], errors='coerce').sum()
        est_equip['servidores'] = {'total': float(tot), 'operando': float(op),
                                    'porcentaje_operativos': round(op/tot*100, 2) if tot else 0}
    est_mob = {}
    for k, col in [('mesas', Config.COLUMNA_MESAS_FUNCIONAN), ('sillas', Config.COLUMNA_SILLAS_FUNCIONAN),
                    ('anaqueles', Config.COLUMNA_ANAQUELES_FUNCIONAN)]:
        if col in df.columns:
            est_mob[k] = float(pd.to_numeric(df[col], errors='coerce').sum())
    est_cert = {}
    for col in [Config.COLUMNA_CN_INICIAL_ACUM, Config.COLUMNA_CN_PRIM_ACUM,
                Config.COLUMNA_CN_SEC_ACUM, Config.COLUMNA_CN_TOT_ACUM, Config.COLUMNA_CERT_EMITIDOS]:
        if col in df.columns:
            est_cert[col] = float(pd.to_numeric(df[col], errors='coerce').sum())
    return {
        'meta': {'mes_reportado': mes_usado}, 'totalPlazas': int(total_plazas),
        'plazasOperacion': int(plazas_operacion), 'totalEstados': int(total_estados),
        'estadoMasPlazas': estado_mas_plazas, 'estadoMenosPlazas': estado_menos_plazas,
        'estadoMayorConectividad': estado_mayor_conectividad,
        'estadoMasOperacion': estado_mas_operacion, 'estadoMasSuspension': estado_mas_suspension,
        'estadisticasEquipamiento': est_equip, 'estadisticasMobiliario': est_mob,
        'estadisticasCertificaciones': est_cert,
    }


@app.route('/api/cn_resumen')
def cn_resumen():
    try:
        if PRECALC_AVAILABLE and _stats_cache and _stats_cache.is_ready:
            data = _stats_cache.get_cn_resumen()
            if data:
                return json_response(data)
        df_completo = dataframe_cache.get_dataframe()
        df_actual   = dataframe_cache.get_ultimo_mes()
        if df_completo.empty:
            return jsonify({'error': 'No hay datos'}), 503
        cols_map = {'CN_Inicial_Acum': Config.COLUMNA_CN_INICIAL_ACUM,
                    'CN_Prim_Acum': Config.COLUMNA_CN_PRIM_ACUM, 'CN_Sec_Acum': Config.COLUMNA_CN_SEC_ACUM}
        df_tmp = df_completo.copy()
        mask_op = (pd.Series([False]*len(df_actual), index=df_actual.index)
                   if Config.COLUMNA_SITUACION not in df_actual.columns
                   else df_actual[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN')
        resumen  = {}
        cn_total = 0.0
        for clave, col in cols_map.items():
            num_col = f'__{clave}_num'
            if col in df_tmp.columns:
                df_tmp[num_col] = pd.to_numeric(df_tmp[col], errors='coerce')
                df_actual[num_col] = pd.to_numeric(df_actual[col], errors='coerce') if col in df_actual.columns else 0
                n_nulos = int(df_tmp[num_col].isna().sum())
                suma    = float(df_tmp[num_col].fillna(0).sum())
            else:
                df_tmp[num_col] = 0; df_actual[num_col] = 0
                n_nulos = len(df_tmp); suma = 0.0
            cn_total += suma
            plazas_op = int(len(df_actual[mask_op & (df_actual[num_col].fillna(0) > 0)])) if mask_op.any() else 0
            n = len(df_tmp)
            resumen[clave] = {'total_registros': n, 'nulos': n_nulos,
                               'pct_nulos': round(n_nulos/n*100, 2) if n else 0,
                               'suma': round(suma, 2), 'plazasOperacion': plazas_op}
        resumen['CN_Total'] = {'total_registros': len(df_tmp), 'nulos': 0, 'pct_nulos': 0.0,
                                'suma': round(cn_total, 2), 'plazasOperacion': int(mask_op.sum())}
        top5 = []
        if Config.COLUMNA_ESTADO in df_tmp.columns:
            df_tmp['__CN_Total_num'] = sum(df_tmp.get(f'__{c}_num', pd.Series(0, index=df_tmp.index)).fillna(0) for c in cols_map)
            grp = df_tmp.groupby(Config.COLUMNA_ESTADO)['__CN_Total_num'].sum().sort_values(ascending=False)
            top5 = [{'estado': str(i), 'suma_CN_Total': float(v)} for i, v in grp.head(5).items()]
        return json_response({'resumen_nacional': resumen, 'top5_estados_por_CN_Total': top5})
    except Exception as exc:
        logging.error(f"cn_resumen: {exc}\n{traceback.format_exc()}")
        return jsonify({'error': str(exc)}), 500


@app.route('/api/cn_por_estado')
def cn_por_estado():
    try:
        if PRECALC_AVAILABLE and _stats_cache and _stats_cache.is_ready:
            data = _stats_cache.get_cn_por_estado()
            if data:
                return json_response(data)
        df_h = dataframe_cache.get_dataframe().copy()
        df_a = dataframe_cache.get_ultimo_mes().copy()
        if df_h.empty:
            return jsonify({'error': 'No hay datos'}), 503
        col_estado    = Config.COLUMNA_ESTADO
        col_situacion = Config.COLUMNA_SITUACION
        col_conect    = Config.COLUMNA_CONECT_INSTALADA
        if col_estado not in df_a.columns:
            return jsonify({'error': f'Columna {col_estado} no encontrada'}), 500
        conteo_plazas = df_a.groupby(col_estado).size().to_dict()
        plazas_op_dict = {}
        if col_situacion in df_a.columns:
            mask = df_a[col_situacion].astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
            plazas_op_dict = df_a[mask].groupby(col_estado).size().to_dict()
        conectividad_dict = {}
        if col_conect in df_a.columns and col_situacion in df_a.columns:
            mask  = df_a[col_situacion].astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
            df_op = df_a[mask].copy()
            valid = df_op[col_conect].apply(lambda v: str(v).lower().strip() not in ['nan','none','0','false','no',''])
            conectividad_dict = df_op[valid].groupby(col_estado).size().to_dict()
        cols_cn = {'CN_Inicial_Acum': Config.COLUMNA_CN_INICIAL_ACUM,
                   'CN_Prim_Acum': Config.COLUMNA_CN_PRIM_ACUM, 'CN_Sec_Acum': Config.COLUMNA_CN_SEC_ACUM}
        cn_nacional = 0.0
        for key, col in cols_cn.items():
            tmp_col = f'__{key}_num'
            df_h[tmp_col] = pd.to_numeric(df_h[col], errors='coerce').fillna(0) if col in df_h.columns else 0
            cn_nacional += df_h[tmp_col].sum()
        summary = []
        for estado, grp in df_h.groupby(col_estado):
            ek = estado[0] if isinstance(estado, tuple) else estado
            total = int(conteo_plazas.get(ek, 0)); op = int(plazas_op_dict.get(ek, 0))
            conect = int(conectividad_dict.get(ek, 0))
            si = float(grp['__CN_Inicial_Acum_num'].sum()); sp = float(grp['__CN_Prim_Acum_num'].sum())
            ss = float(grp['__CN_Sec_Acum_num'].sum()); st = si + sp + ss
            summary.append({'estado': str(ek).strip(), 'total_plazas': total, 'plazas_operacion': op,
                'conectados_actual': conect, 'pct_conectividad': round(conect/op*100, 1) if op else 0.0,
                'suma_CN_Inicial_Acum': int(si), 'suma_CN_Prim_Acum': int(sp),
                'suma_CN_Sec_Acum': int(ss), 'suma_CN_Total': int(st),
                'pct_sobre_nacional': round(st/cn_nacional*100, 2) if cn_nacional else 0.0})
        summary.sort(key=lambda x: x['suma_CN_Total'], reverse=True)
        return json_response({'status': 'success', 'estados': summary,
                               'metadata': {'cn_total_nacional': int(cn_nacional), 'total_estados_procesados': len(summary)}})
    except Exception as exc:
        logging.error(f"cn_por_estado: {exc}\n{traceback.format_exc()}")
        return jsonify({'error': str(exc)}), 500


@app.route('/api/cn_top_estados')
def cn_top_estados():
    try:
        metric = request.args.get('metric', 'inicial').lower()
        n      = int(request.args.get('n', 5))
        if PRECALC_AVAILABLE and _stats_cache and _stats_cache.is_ready:
            data = _stats_cache.get_cn_top_estados(metric, n)
            if data is not None:
                return jsonify(data)
        col_map = {'inicial': Config.COLUMNA_CN_INICIAL_ACUM, 'prim': Config.COLUMNA_CN_PRIM_ACUM, 'sec': Config.COLUMNA_CN_SEC_ACUM}
        col = col_map.get(metric)
        if not col:
            return jsonify({'error': f'Métrica inválida: {metric}'}), 400
        df = dataframe_cache.get_dataframe().copy()
        if col not in df.columns:
            return jsonify({'error': f'Columna {col} no encontrada'}), 400
        num_col = f'__{metric}_num'
        df[num_col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        grp = df.groupby(Config.COLUMNA_ESTADO)[num_col].sum().sort_values(ascending=False)
        return jsonify({'metric': metric, 'top': [{'estado': str(i), 'valor': float(v)} for i, v in grp.head(n).items()]})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/cn_estados_destacados')
def cn_estados_destacados():
    try:
        if PRECALC_AVAILABLE and _stats_cache and _stats_cache.is_ready:
            data = _stats_cache.get_cn_estados_destacados()
            if data:
                return jsonify(data)
        df = dataframe_cache.get_dataframe().copy()
        if df.empty:
            return jsonify({'error': 'No hay datos'}), 503
        cols = {'CN_Inicial_Acum': Config.COLUMNA_CN_INICIAL_ACUM,
                'CN_Prim_Acum': Config.COLUMNA_CN_PRIM_ACUM, 'CN_Sec_Acum': Config.COLUMNA_CN_SEC_ACUM}
        faltantes = [v for v in cols.values() if v not in df.columns]
        if faltantes:
            return jsonify({'error': 'Faltan columnas', 'faltantes': faltantes}), 400
        result = {}
        for nombre, col in cols.items():
            tmp = f'__{nombre}_num'
            df[tmp] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            grp = df.groupby(Config.COLUMNA_ESTADO)[tmp].sum()
            result[nombre] = {'estado': str(grp.idxmax()), 'valor': float(grp.max())}
        return jsonify(result)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/cn_top5_todos')
def cn_top5_todos():
    try:
        if PRECALC_AVAILABLE and _stats_cache and _stats_cache.is_ready:
            data = _stats_cache.get_cn_top5_todos()
            if data:
                return jsonify(data)
        df = dataframe_cache.get_dataframe().copy()
        if df.empty:
            return jsonify({'error': 'No hay datos'}), 503
        pares = [('inicial', Config.COLUMNA_CN_INICIAL_ACUM), ('primaria', Config.COLUMNA_CN_PRIM_ACUM),
                 ('secundaria', Config.COLUMNA_CN_SEC_ACUM)]
        result = {}
        for cat, col in pares:
            if col in df.columns:
                tmp = f'__{cat}_num'
                df[tmp] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                grp = df.groupby(Config.COLUMNA_ESTADO)[tmp].sum().sort_values(ascending=False)
                result[cat] = [{'estado': str(i), 'valor': float(v)} for i, v in grp.head(5).items()]
        return jsonify(result)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/situacion_distribucion')
def situacion_distribucion():
    try:
        if PRECALC_AVAILABLE and _stats_cache and _stats_cache.is_ready:
            data = _stats_cache.get_situacion_dist()
            if data:
                return json_response(data)
        return jsonify({'error': 'Datos no disponibles aún'}), 503
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/metricas-por-estado/<estado>')
def get_metricas_por_estado(estado):
    try:
        df = filtrar_df_cascada(dataframe_cache.get_ultimo_mes(), {'ESTADO': estado})
        if df.empty:
            return jsonify({'plazas': [], 'municipios': []})
        cols_metricas = [Config.COLUMNA_ATEN_INICIAL, Config.COLUMNA_ATEN_PRIM, Config.COLUMNA_ATEN_SEC,
                         Config.COLUMNA_ATEN_TOTAL, Config.COLUMNA_EXAMENES_APLICADOS,
                         Config.COLUMNA_CN_INICIAL_ACUM, Config.COLUMNA_CN_PRIM_ACUM,
                         Config.COLUMNA_CN_SEC_ACUM, Config.COLUMNA_CN_TOT_ACUM, Config.COLUMNA_CERT_EMITIDOS]
        cols_info = [Config.COLUMNA_CLAVE, Config.COLUMNA_NOMBRE_PC, Config.COLUMNA_MUNICIPIO]
        datos_plazas = []
        for _, row in df.iterrows():
            fila = {}
            for c in cols_info:
                val = row.get(c)
                fila[c] = str(val).strip() if pd.notna(val) else 'N/D'
            for c in cols_metricas:
                val = row.get(c)
                try:
                    fila[c] = int(float(val)) if pd.notna(val) else 0
                except (ValueError, TypeError):
                    fila[c] = 0
            datos_plazas.append(fila)
        municipios_dict = {}
        for plaza in datos_plazas:
            mun = plaza.get(Config.COLUMNA_MUNICIPIO, 'Sin Municipio')
            if mun not in municipios_dict:
                municipios_dict[mun] = {Config.COLUMNA_MUNICIPIO: mun, **{c: 0 for c in cols_metricas}}
            for c in cols_metricas:
                municipios_dict[mun][c] += plaza.get(c, 0)
        return jsonify({'plazas': datos_plazas, 'municipios': list(municipios_dict.values())})
    except Exception as exc:
        logging.error(f"get_metricas_por_estado: {exc}\n{traceback.format_exc()}")
        return jsonify({'plazas': [], 'municipios': []}), 500


@app.route('/api/refresh-cache', methods=['POST'])
def refresh_cache():
    try:
        dataframe_cache.refresh_cache()
        return jsonify({'status': 'success', 'message': 'Cache refrescado', 'timestamp': datetime.now().isoformat()})
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 500


@app.route('/api/cache/status')
def get_cache_status():
    try:
        df = dataframe_cache.get_dataframe()
        rust_comp = {}
        if PLAZA_RUST_AVAILABLE and _plaza_rust_mod:
            try:
                rust_comp = _plaza_rust_mod.engine_recursos()
            except Exception:
                pass
        periodos_protegidos = len(_periodo_cache._claves_protegidas) if _periodo_cache else 0
        return jsonify({
            'status': 'success',
            'cache_info': {
                'dataframe_en_memoria':    not df.empty,
                'total_registros':         len(df),
                'columnas_disponibles':    [c for c in df.columns if not c.startswith('normalized_')] if not df.empty else [],
                'ultimo_mes_registros':    len(dataframe_cache.get_ultimo_mes()) if not df.empty else 0,
                'estados_cacheados':       len(dataframe_cache.get_estados_cache()) if not df.empty else 0,
                'zonas_cacheadas':         len(dataframe_cache._zonas_cache),
                'timestamp':               dataframe_cache._cache_ts.isoformat() if dataframe_cache._cache_ts else None,
                'translator_activo':       TRANSLATOR_AVAILABLE,
                'plaza_index_activo':      PLAZA_INDEX_AVAILABLE and bool(_plaza_index and _plaza_index.is_ready),
                'precalc_activo':          PRECALC_AVAILABLE and bool(_stats_cache and _stats_cache.is_ready),
                'rust_activo':             RUST_BRIDGE_AVAILABLE and bool(rust_bridge and rust_bridge.is_ready()) if rust_bridge else False,
                'comparativas_engine':     COMPARATIVAS_ENGINE_AVAILABLE,
                'plaza_rust_activo':       PLAZA_RUST_AVAILABLE,
                'periodos_locales_protegidos': periodos_protegidos,
                'estadisticas_cacheadas':  bool(_ESTADISTICAS_CACHE),
            },
            'comparativas_rust': rust_comp,
            'archivos': {
                'parquet_existe':     os.path.exists(Config.PARQUET_PATH),
                'excel_existe':       os.path.exists(Config.EXCEL_PATH),
                'coordenadas_existe': os.path.exists(Config.ARCHIVO_COORDENADAS),
                'excel_tree_existe':  os.path.exists(Config.EXCEL_TREE_PATH),
            },
        })
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 500


@app.route('/api/excel/last-update')
def get_last_update():
    try:
        archivo = Config.PARQUET_PATH if os.path.exists(Config.PARQUET_PATH) else Config.EXCEL_PATH
        if not os.path.exists(archivo):
            return jsonify({'status': 'archivo_no_encontrado', 'last_modified': None}), 404
        fecha_real = datetime.fromtimestamp(os.path.getmtime(archivo))
        año, mes   = fecha_real.year, fecha_real.month
        if mes == 1:    año -= 1; mes = 12
        elif mes != 12: mes -= 1
        try:
            fecha_aj = datetime(año, mes, fecha_real.day)
        except ValueError:
            import calendar
            fecha_aj = datetime(año, mes, calendar.monthrange(año, mes)[1])
        return jsonify({
            'status': 'success', 'last_modified_real': fecha_real.isoformat(),
            'last_modified': fecha_aj.isoformat(), 'formatted': fecha_aj.strftime('%d/%m/%Y'),
            'archivo_fuente': os.path.basename(archivo),
        }), 200
    except Exception as exc:
        return jsonify({'status': 'error', 'mensaje': str(exc), 'last_modified': None}), 500


def find_image_urls(clave_original: str) -> list:
    try:
        if not os.path.exists(Config.DRIVE_TREE_PATH):
            return []
        with open(Config.DRIVE_TREE_PATH, 'r', encoding='utf-8') as f:
            drive_data = json.load(f)
        clave_lower = clave_original.strip().lower()
        image_list  = []
        def search(tree, target):
            if tree.get('type') == 'folder' and tree.get('name', '').lower() == target:
                for ch in tree.get('children', []):
                    if ch.get('type') == 'file' and ch.get('mimeType', '').startswith('image/'):
                        url = ch.get('webContentLink')
                        if url:
                            image_list.append(url.replace('&export=download','').replace('?usp=drivesdk',''))
                return True
            return any(search(ch, target) for ch in tree.get('children', []))
        search(drive_data['structure'], clave_lower)
        return image_list
    except Exception as exc:
        logging.error(f"find_image_urls: {exc}")
        return []


@app.route('/imagenes/<path:filename>')
def serve_image(filename: str):
    return send_from_directory(Config.IMAGES_BASE_PATH, filename)


@app.route('/api/drive-tree')
def get_drive_tree():
    try:
        if not os.path.exists(Config.DRIVE_TREE_PATH):
            return jsonify({'error': 'Árbol de Drive no disponible', 'status': 'not_generated'}), 503
        with open(Config.DRIVE_TREE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if (datetime.now() - datetime.fromisoformat(data['generated_at'])).days > 500:
            return jsonify({'error': 'Árbol desactualizado', 'status': 'stale'}), 503
        return jsonify(data)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/drive-image/<path:image_path>')
def serve_drive_image(image_path):
    try:
        if not os.path.exists(Config.DRIVE_TREE_PATH):
            return jsonify({'error': 'Árbol de Drive no disponible'}), 503
        with open(Config.DRIVE_TREE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        def find_file(tree, path):
            if tree.get('type') == 'file' and tree.get('path') == path: return tree
            for ch in tree.get('children', []):
                r = find_file(ch, path)
                if r: return r
        info = find_file(data['structure'], image_path)
        if not info:
            return jsonify({'error': 'Imagen no encontrada'}), 404
        fid = info.get('id')
        if fid:
            return redirect(f"https://drive.google.com/uc?id={fid}&export=download")
        return jsonify({'error': 'URL no disponible'}), 404
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@app.route('/api/drive-excel/years')
def get_drive_excel_years():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        return jsonify({'status':'success','data_type':'metadata_only','years': drive_excel_reader_readonly.get_available_years()})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-excel/years/<year>/months')
def get_drive_excel_months(year):
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        return jsonify({'status':'success','data_type':'metadata_only','year':year,
                        'months': drive_excel_reader_readonly.get_available_months(year)})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-excel/years/<year>/months/<month>/files')
def get_drive_excel_files(year, month):
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        files = drive_excel_reader_readonly.get_excel_files_by_date(year, month)
        return jsonify({'status':'success','data_type':'metadata_only','year':year,'month':month,'files':files,'count':len(files)})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-excel/query/<year>/<month>')
def query_drive_excel_data(year, month):
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        result = drive_excel_reader_readonly.query_excel_data_readonly(
            year, month, request.args.get('filename'), request.args.get('query','basic_stats'))
        result['data_source']    = 'google_drive_readonly'
        result['requested_file'] = f"{year}/{month}"
        return jsonify(result)
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc),'data_source':'google_drive_readonly'}), 500

@app.route('/api/drive-excel/info/<year>/<month>')
def get_drive_excel_info(year, month):
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        info = drive_excel_reader_readonly.get_excel_info(year, month, request.args.get('filename'))
        if info:
            return jsonify({'status':'success','data_type':'metadata_only','file_info':info})
        return jsonify({'status':'error','message':'Archivo no encontrado'}), 404
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-excel/stats')
def get_drive_excel_stats():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        return jsonify({'status':'success','data_type':'usage_stats','stats': drive_excel_reader_readonly.get_stats()})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-excel/cleanup')
def cleanup_drive_cache():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        before = drive_excel_reader_readonly.get_loaded_files_count()
        drive_excel_reader_readonly.clear_all_cache()
        after  = drive_excel_reader_readonly.get_loaded_files_count()
        return jsonify({'status':'success','message':f'Cache Drive limpiado: {before} -> {after}','cleaned_files':before-after})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


# ==============================================================================
# COMPARATIVAS — endpoints v5.1
# ==============================================================================

@app.route('/api/drive-comparativas/periodos')
def get_comparativa_periodos():
    if COMPARATIVAS_ENGINE_AVAILABLE and _comparativas_engine is not None:
        try:
            return json_response(_comparativas_engine.periodos_disponibles())
        except Exception as exc:
            logging.error(f"periodos v5: {exc}\n{traceback.format_exc()}")
    if not DRIVE_MODULES_AVAILABLE:
        return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        years, meses = obtener_años_desde_arbol_json()
        return jsonify({'status':'success','years':years,'meses_por_anio':meses})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/drive-comparativas/comparar-avanzado')
def comparar_periodos_avanzado_unificado():
    year1  = request.args.get('year1', '') or request.args.get('year', '')
    year2  = request.args.get('year2', '') or year1
    p1     = request.args.get('periodo1', '')
    p2     = request.args.get('periodo2', '')
    filtro = request.args.get('filtro_estado', 'Todos')
    if not all([year1, p1, year2, p2]):
        return json_response({'status': 'error', 'message': 'Se requieren year1, periodo1, year2 y periodo2'}, 400)
    if COMPARATIVAS_ENGINE_AVAILABLE and _comparativas_engine is not None:
        try:
            res = _comparativas_engine.comparar(año1=year1, mes1=p1, año2=year2, mes2=p2, filtro_estado=filtro)
            return json_response(res, 200 if res.get('status') == 'success' else 400)
        except Exception as exc:
            logging.error(f"comparar-avanzado v5: {exc}\n{traceback.format_exc()}")
    if not DRIVE_MODULES_AVAILABLE:
        return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        metricas = request.args.getlist('metricas') or ['CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum','CN_Tot_Acum','Situación']
        years_disp = drive_excel_reader_readonly.get_available_years()
        for y in [year1, year2]:
            if y not in years_disp:
                return jsonify({'status':'error','message':f'Año {y} no disponible','available_years':years_disp}), 404
        m1 = drive_excel_reader_readonly.get_available_months(year1)
        m2 = drive_excel_reader_readonly.get_available_months(year2)
        if p1 not in m1: return jsonify({'status':'error','message':f'Mes {p1} no disponible para {year1}','available_months':m1}), 404
        if p2 not in m2: return jsonify({'status':'error','message':f'Mes {p2} no disponible para {year2}','available_months':m2}), 404
        if year1 != year2:
            res = drive_excel_comparator.comparar_periodos_avanzado_con_años_diferentes(year1,p1,year2,p2,filtro,metricas)
        else:
            res = drive_excel_comparator.comparar_periodos_avanzado(year1,p1,p2,filtro,metricas)
        if res.get('status') == 'success':
            try:
                json.dumps(res)
                return jsonify(res)
            except (TypeError, ValueError):
                return jsonify(safe_json_serialize(res))
        return jsonify({'status':'error','message':res.get('error','Error desconocido')}), 400
    except Exception as exc:
        logging.error(f"comparar-avanzado legado: {exc}\n{traceback.format_exc()}")
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/drive-comparativas/comparar')
def comparar_periodos_acumulativos():
    year = request.args.get('year', '')
    p1   = request.args.get('periodo1', '')
    p2   = request.args.get('periodo2', '')
    if not all([year, p1, p2]):
        return json_response({'status': 'error', 'message': 'Se requieren year, periodo1 y periodo2'}, 400)
    if COMPARATIVAS_ENGINE_AVAILABLE and _comparativas_engine is not None:
        try:
            res = _comparativas_engine.comparar(año1=year, mes1=p1, año2=year, mes2=p2)
            return json_response(res, 200 if res.get('status') == 'success' else 400)
        except Exception as exc:
            logging.error(f"comparar v5: {exc}")
    if not DRIVE_MODULES_AVAILABLE:
        return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        res = drive_excel_comparator.comparar_periodos_avanzado(year,p1,p2,'Todos',
              ['CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum','CN_Tot_Acum'])
        return jsonify(res) if res.get('status')=='success' else (jsonify(res), 400)
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/drive-comparativas/status')
def get_status_comparativas():
    if COMPARATIVAS_ENGINE_AVAILABLE and _comparativas_engine is not None:
        try:
            rust_res = {}
            if PLAZA_RUST_AVAILABLE and _plaza_rust_mod:
                try:
                    rust_res = _plaza_rust_mod.engine_recursos()
                except Exception:
                    pass
            periodos = _comparativas_engine.periodos_disponibles()
            periodos_protegidos = len(_periodo_cache._claves_protegidas) if _periodo_cache else 0
            return json_response({
                'status': 'operational', 'motor': 'rust' if PLAZA_RUST_AVAILABLE else 'pandas',
                'engine_version': 'v5.1', 'año_actual_local': CURRENT_YEAR,
                'años_disponibles': len(periodos.get('years', [])),
                'periodos_cargados_rust': rust_res.get('periodos_cargados', 0),
                'resultados_cacheados': rust_res.get('resultados_cacheados', 0),
                'cache_hits_total': rust_res.get('cache_hits_total', 0),
                'ram_datos_kb': rust_res.get('ram_datos_kb', 0),
                'max_resultados_cache': rust_res.get('max_resultados', 0),
                'indice_drive_cargado': _periodo_cache._indice_cargado if _periodo_cache else False,
                'periodos_locales_protegidos': periodos_protegidos,
                'watchdog_activo': _watchdog._running if _watchdog else False,
                'timestamp': datetime.now().isoformat(),
            })
        except Exception as exc:
            return json_response({'status': 'error', 'message': str(exc)}, 500)
    if not DRIVE_MODULES_AVAILABLE:
        return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        stats = drive_excel_reader_readonly.get_stats()
        years = drive_excel_reader_readonly.get_available_years()
        return jsonify({'status':'success','engine_version':'legado',
            'drive_reader':{'total_requests':stats['total_requests'],'cache_hits':stats['cache_hits'],
                'drive_downloads':stats['drive_downloads'],'cache_hit_ratio':stats['cache_hit_ratio'],
                'currently_loaded_files':stats['currently_loaded_files'],'tree_loaded':stats['tree_loaded']},
            'datos_disponibles':{'total_años':len(years),'años':years},
            'system':{'timestamp':datetime.now().isoformat(),'status':'operational'}})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/drive-comparativas/cache-info')
def cache_info_comparativas():
    try:
        info = []
        if PLAZA_RUST_AVAILABLE and _plaza_rust_mod:
            try:
                info = _plaza_rust_mod.cache_info()
            except Exception:
                pass
        for entry in info:
            for campo in ("key1", "key2"):
                k = entry.get(campo, 0)
                if k:
                    a, m = k // 100, f"{k % 100:02d}"
                    entry[f"{campo}_label"] = f"{a}-{m}"
        return json_response({'status': 'success', 'cache': info, 'total': len(info)})
    except Exception as exc:
        return json_response({'status': 'error', 'message': str(exc)}, 500)


@app.route('/api/drive-comparativas/limpiar-cache', methods=['POST'])
def limpiar_cache_comparativas():
    if COMPARATIVAS_ENGINE_AVAILABLE and _periodo_cache is not None:
        try:
            elim_res = 0; elim_per = 0
            if PLAZA_RUST_AVAILABLE and _plaza_rust_mod:
                try:
                    elim_res = _plaza_rust_mod.limpiar_resultados_expirados(0)
                except Exception:
                    pass
                try:
                    elim_per = _plaza_rust_mod.limpiar_periodos_lru(0, CURRENT_YEAR)
                except Exception:
                    pass
            _periodo_cache.evictar_historicos_python()
            return json_response({
                'status': 'success', 'resultados_eliminados': elim_res, 'periodos_eliminados': elim_per,
                'periodos_protegidos': len(_periodo_cache._claves_protegidas),
                'mensaje': 'Cache Drive limpiado; Parquet local intacto',
            })
        except Exception as exc:
            return json_response({'status': 'error', 'message': str(exc)}, 500)
    if not DRIVE_MODULES_AVAILABLE:
        return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        if hasattr(drive_excel_reader_readonly, 'loaded_excels'):
            drive_excel_reader_readonly.loaded_excels.clear()
        return jsonify({'status':'success','message':'Cache limpiado','timestamp':datetime.now().isoformat()})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/drive-comparativas/recargar-arbol', methods=['POST'])
def recargar_arbol_comparativas():
    if COMPARATIVAS_ENGINE_AVAILABLE and _periodo_cache is not None:
        try:
            ok = _periodo_cache.cargar_indice()
            if ok:
                return json_response({'status': 'success', 'entradas': len(_periodo_cache._indice), 'mensaje': 'Índice Drive recargado'})
            return json_response({'status': 'error', 'message': 'excel_tree_real.json no encontrado'}, 404)
        except Exception as exc:
            return json_response({'status': 'error', 'message': str(exc)}, 500)
    if not DRIVE_MODULES_AVAILABLE:
        return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        ok = drive_excel_reader_readonly.load_tree()
        if ok:
            return jsonify({'status':'success','message':'Árbol recargado','timestamp':datetime.now().isoformat()})
        return jsonify({'status':'error','message':'Error al recargar árbol'}), 500
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/drive-comparativas/cn-resumen-comparativo')
def get_cn_resumen_comparativo():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year, p1, p2 = request.args.get('year',''), request.args.get('periodo1',''), request.args.get('periodo2','')
        if not all([year,p1,p2]):
            return jsonify({'status':'error','message':'Se requieren year, periodo1 y periodo2'}), 400
        res = drive_excel_comparator.comparar_periodos_avanzado(year,p1,p2,'Todos',
              ['CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum','CN_Tot_Acum'])
        if res.get('status')=='success':
            comp = res.get('comparacion',{}); mp = res.get('metricas_principales',{})
            return jsonify({'status':'success','year':year,'periodo1':p1,'periodo2':p2,
                'resumen_comparativo':{'comparacion_general':{'periodo1':p1,'periodo2':p2,
                    'plazas_nuevas':mp.get('plazas_nuevas',0),'plazas_eliminadas':mp.get('plazas_eliminadas',0),
                    'incremento_cn_total':mp.get('incremento_cn_total',0)},
                    'metricas_detalladas':comp.get('metricas_globales',{}),'resumen_cambios':mp.get('resumen_cambios','')}})
        return jsonify({'status':'error','message':res.get('error','Error')}), 400
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/top-estados-comparativo')
def get_top_estados_comparativo():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year   = request.args.get('year','')
        p1, p2 = request.args.get('periodo1',''), request.args.get('periodo2','')
        metric = request.args.get('metric','CN_Tot_Acum')
        n      = int(request.args.get('n',5))
        if not all([year,p1,p2]):
            return jsonify({'status':'error','message':'Se requieren year, periodo1 y periodo2'}), 400
        res = drive_excel_comparator.comparar_periodos_avanzado(year,p1,p2,'Todos',[metric])
        if res.get('status')=='success':
            analisis = res.get('comparacion',{}).get('analisis_por_estado',{})
            estados  = []
            for estado, datos in analisis.items():
                md = datos.get('metricas',{}).get(metric,{})
                estados.append({'estado':estado,'periodo1':md.get('periodo1',0),'periodo2':md.get('periodo2',0),
                                 'cambio':md.get('cambio',0),'porcentaje_cambio':md.get('porcentaje_cambio',0)})
            estados.sort(key=lambda x: abs(x['cambio']), reverse=True)
            return jsonify({'status':'success','year':year,'periodo1':p1,'periodo2':p2,
                            'metric':metric,'top_comparativo':estados[:n]})
        return jsonify({'status':'error','message':res.get('error','Error')}), 400
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/estadisticas-comparativas')
def get_estadisticas_comparativas():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year, p1, p2 = request.args.get('year',''), request.args.get('periodo1',''), request.args.get('periodo2','')
        if not all([year,p1,p2]):
            return jsonify({'status':'error','message':'Se requieren year, periodo1 y periodo2'}), 400
        res = drive_excel_comparator.comparar_periodos_avanzado(year,p1,p2,'Todos')
        if res.get('status')=='success':
            comp = res.get('comparacion',{})
            return jsonify({'status':'success','year':year,'periodo1':p1,'periodo2':p2,
                'estadisticas_comparativas':{'general':comp.get('resumen_general',{}),
                    'analisis_plazas':comp.get('analisis_plazas',{}),'metricas_globales':comp.get('metricas_globales',{}),
                    'analisis_por_estado':comp.get('analisis_por_estado',{}),'resumen_cambios':res.get('metricas_principales',{})}})
        return jsonify({'status':'error','message':res.get('error','Error')}), 400
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/analisis-tendencia')
def get_analisis_tendencia():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year  = request.args.get('year','')
        p_ini = request.args.get('periodo_inicio','01')
        p_fin = request.args.get('periodo_fin','12')
        if not year:
            return jsonify({'status':'error','message':'Se requiere year'}), 400
        meses = [m for m in drive_excel_reader_readonly.get_available_months(year) if p_ini <= m <= p_fin]
        if not meses:
            return jsonify({'status':'error','message':'No hay meses en el rango'}), 400
        tendencia = []
        for mes in sorted(meses):
            df, _ = drive_excel_reader_readonly.load_excel_strict(year, mes)
            if df is not None:
                metricas = {col: int(round(pd.to_numeric(df[col], errors='coerce').fillna(0).sum()))
                            for col in ['CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum','CN_Tot_Acum'] if col in df.columns}
                tendencia.append({'mes':mes,'nombre_mes':obtener_nombre_mes(mes),
                                  'total_plazas':len(df),'metricas_cn':metricas,
                                  'periodo':f"{obtener_nombre_mes(mes)} {year}"})
        return jsonify({'status':'success','year':year,'periodo_inicio':p_ini,'periodo_fin':p_fin,'tendencia':tendencia})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/consulta-plazas')
def consulta_plazas_especificas():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year, periodo = request.args.get('year',''), request.args.get('periodo','')
        clave_plaza   = request.args.get('clave_plaza','')
        if not all([year,periodo]):
            return jsonify({'status':'error','message':'Se requieren year y periodo'}), 400
        df, info = drive_excel_reader_readonly.load_excel_strict(year, periodo)
        if df is None:
            return jsonify({'status':'error','message':f'No se pudo cargar {periodo}-{year}'}), 400
        if clave_plaza and 'Clave_Plaza' in df.columns:
            df = df[df['Clave_Plaza'].astype(str).str.contains(clave_plaza, na=False)]
        datos = df.head(100).fillna('').to_dict('records')
        return jsonify({'status':'success','year':year,'periodo':periodo,'clave_plaza':clave_plaza,
                        'total_resultados':len(datos),'datos':datos,'metadata':info.get('file_info',{})})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/estadisticas-rapidas')
def get_estadisticas_rapidas():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year, periodo = request.args.get('year',''), request.args.get('periodo','')
        if not all([year,periodo]):
            return jsonify({'status':'error','message':'Se requieren year y periodo'}), 400
        res = drive_excel_reader_readonly.query_excel_data_readonly(year, periodo, query_type='basic_stats')
        if res.get('status')=='success':
            return jsonify({'status':'success','year':year,'periodo':periodo,
                'estadisticas':{'total_registros':res.get('total_rows',0),'total_columnas':res.get('total_columns',0),
                                'columnas':res.get('column_names',[]),'muestra':res.get('sample_data',[])},'drive_file':res.get('drive_file',{})})
        return jsonify({'status':'error','message':res.get('error','Error desconocido')}), 400
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/estados-disponibles')
def estados_disponibles_comparativas():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year, periodo = request.args.get('year',''), request.args.get('periodo','')
        if not (year and periodo): return jsonify({'error':'Se requieren year y periodo'}), 400
        return jsonify({'estados': drive_excel_comparator.obtener_estados_disponibles(year, periodo)})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

@app.route('/api/drive-comparativas/metricas-disponibles')
def metricas_disponibles_comparativas():
    if not DRIVE_MODULES_AVAILABLE: return jsonify({'error': 'Módulos Drive no disponibles'}), 503
    try:
        year, periodo = request.args.get('year',''), request.args.get('periodo','')
        if not (year and periodo): return jsonify({'error':'Se requieren year y periodo'}), 400
        return jsonify({'metricas': drive_excel_comparator.obtener_metricas_disponibles(year, periodo)})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

@app.route('/api/drive-comparativas/datos-completos')
def get_datos_completos():
    try:
        años, meses_por_año = obtener_años_desde_arbol_json() if DRIVE_MODULES_AVAILABLE else ([],{})
        df = dataframe_cache.get_dataframe()
        data = {'años_disponibles':años,'meses_por_año':meses_por_año,
                'drive_modules_available':DRIVE_MODULES_AVAILABLE,
                'cache_local':{'registros':len(df),'columnas':list(df.columns) if not df.empty else [],
                               'muestra':df.head(5).fillna('').to_dict('records') if not df.empty else []},
                'ultima_actualizacion':datetime.now().isoformat()}
        if DRIVE_MODULES_AVAILABLE:
            data['estadisticas_sistema'] = drive_excel_reader_readonly.get_stats()
        return jsonify({'status':'success','datos':data})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/buscar-estados')
def buscar_estados_comparativa():
    try:
        query = request.args.get('q','').strip().lower()
        if not query:
            return jsonify({'status':'success','query':query,'resultados':[],'total_resultados':0})
        df = dataframe_cache.get_dataframe()
        if df.empty:
            return jsonify({'status':'error','message':'No hay datos'}), 503
        if Config.COLUMNA_ESTADO not in df.columns:
            return jsonify({'status':'error','message':'Columna estado no disponible'}), 400
        resultados = []
        for e in df[Config.COLUMNA_ESTADO].dropna().unique():
            es = str(e).strip()
            if query in es.lower():
                resultados.append({'nombre':es,'match_type':'exact' if query==es.lower() else 'partial'})
        resultados.sort(key=lambda x:(x['match_type']!='exact', x['nombre']))
        return jsonify({'status':'success','query':query,'resultados':resultados,'total_resultados':len(resultados)})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/drive-comparativas/comparar-años')
def comparar_años_endpoint():
    return comparar_periodos_avanzado_unificado()


# ── Sistema / Info ────────────────────────────────────────────────────────────
@app.route('/api/system/info')
def get_system_info():
    try:
        drive_stats  = drive_excel_reader_readonly.get_stats() if DRIVE_MODULES_AVAILABLE else {'tree_loaded':False,'cache_hit_ratio':0}
        years        = drive_excel_reader_readonly.get_available_years() if DRIVE_MODULES_AVAILABLE else []
        total_months = sum(len(drive_excel_reader_readonly.get_available_months(y)) for y in years) if DRIVE_MODULES_AVAILABLE else 0
        df = dataframe_cache.get_dataframe()
        rust_comp = {}
        if PLAZA_RUST_AVAILABLE and _plaza_rust_mod:
            try:
                rust_comp = _plaza_rust_mod.engine_recursos()
            except Exception:
                pass
        return json_response({'status':'success',
            'system':{'timestamp':datetime.now().isoformat(),'python_version':os.sys.version,'platform':os.sys.platform},
            'optimizaciones':{
                'plaza_index':    PLAZA_INDEX_AVAILABLE and bool(_plaza_index and _plaza_index.is_ready),
                'polars_precalc': PRECALC_AVAILABLE and bool(_stats_cache and _stats_cache.is_ready),
                'rust_bridge':    RUST_BRIDGE_AVAILABLE and bool(rust_bridge and rust_bridge.is_ready()) if rust_bridge else False,
                'comparativas_engine_v5': COMPARATIVAS_ENGINE_AVAILABLE,
                'plaza_rust':     PLAZA_RUST_AVAILABLE,
            },
            'comparativas_rust': rust_comp,
            'drive_system':{'available':DRIVE_MODULES_AVAILABLE,'tree_loaded':drive_stats['tree_loaded'],
                'total_years':len(years),'total_months':total_months,
                'cache_performance':{'hit_ratio':drive_stats['cache_hit_ratio'],
                    'total_requests':drive_stats.get('total_requests',0),'cache_hits':drive_stats.get('cache_hits',0),
                    'drive_downloads':drive_stats.get('drive_downloads',0)}},
            'local_cache':{'dataframe_en_memoria':not df.empty,'registros':len(df),
                'columnas':len(df.columns) if not df.empty else 0,
                'ultimo_mes_registros':len(dataframe_cache.get_ultimo_mes()) if not df.empty else 0,
                'estados_cacheados':len(dataframe_cache.get_estados_cache()) if not df.empty else 0,
                'translator_activo':TRANSLATOR_AVAILABLE},
            'archivos_disco':{
                'parquet':    os.path.exists(Config.PARQUET_PATH),
                'excel':      os.path.exists(Config.EXCEL_PATH),
                'excel_tree': os.path.exists(Config.EXCEL_TREE_PATH),
            },
            'available_years':years})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/safe-serialize-test')
def safe_serialize_test():
    try:
        test = {'string':'Texto','integer':42,'float':3.14,'nan':float('nan'),
                'inf':float('inf'),'none':None,'list':[1,2,float('nan')],'timestamp':datetime.now()}
        return jsonify({'status':'success','serialized': safe_json_serialize(test)})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


@app.route('/api/optimizaciones/status')
def get_optimizaciones_status():
    rust_comp = {}
    if PLAZA_RUST_AVAILABLE and _plaza_rust_mod:
        try:
            rust_comp = _plaza_rust_mod.engine_recursos()
        except Exception:
            pass
    status = {
        'plaza_index':    {'disponible': PLAZA_INDEX_AVAILABLE, 'listo': bool(_plaza_index and _plaza_index.is_ready)},
        'polars_precalc': {'disponible': PRECALC_AVAILABLE,     'listo': bool(_stats_cache and _stats_cache.is_ready)},
        'rust_bridge':    {'disponible': RUST_BRIDGE_AVAILABLE, 'listo': bool(rust_bridge and rust_bridge.is_ready()) if rust_bridge else False},
        'comparativas_engine_v5': {
            'disponible':          COMPARATIVAS_ENGINE_AVAILABLE,
            'listo':               _comparativas_engine is not None,
            'watchdog':            bool(_watchdog and _watchdog._running),
            'plaza_rust':          PLAZA_RUST_AVAILABLE,
            'periodos_protegidos': len(_periodo_cache._claves_protegidas) if _periodo_cache else 0,
            'rust_stats':          rust_comp,
        },
        'estadisticas_cache': {
            'activo': bool(_ESTADISTICAS_CACHE),
            'timestamp': _ESTADISTICAS_CACHE_TS.isoformat() if _ESTADISTICAS_CACHE_TS else None,
            'ttl_seg': _ESTADISTICAS_TTL_SEG,
        },
    }
    if rust_bridge:
        status['rust_bridge']['stats'] = rust_bridge.engine_stats()
    return jsonify(status)


# ── Mapa ──────────────────────────────────────────────────────────────────────
@app.route('/ver-mapa')
def vista_mapa():
    return render_template('mapa_cluster.html')

@app.route('/api/version-coordenadas')
def check_version():
    archivo = Config.ARCHIVO_COORDENADAS
    if not os.path.exists(archivo):
        return jsonify({'version': None}), 404
    try:
        return jsonify({'version': os.path.getmtime(archivo)})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

@app.route('/api/coordenadas-lazy')
def get_coordenadas_lazy():
    archivo = Config.ARCHIVO_COORDENADAS
    if not os.path.exists(archivo):
        return jsonify({'error': 'Archivo no disponible', 'datos': []}), 404
    try:
        with open(archivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("JSON no es lista")
        return jsonify(data)
    except json.JSONDecodeError:
        return jsonify({'error': 'JSON corrupto', 'datos': []}), 500
    except Exception as exc:
        return jsonify({'error': str(exc), 'datos': []}), 500

@app.route('/api/mapa/coordenadas-optimizadas')
def get_coordenadas_optimizadas():
    try:
        plazas = dataframe_cache.get_coordenadas()
        if not plazas:
            return jsonify({'error': 'Datos no disponibles', 'datos': []}), 404
        return json_response({'status':'success','total_plazas':len(plazas),'plazas':plazas,'cached':True})
    except Exception as exc:
        return jsonify({'error': str(exc), 'datos': []}), 500

@app.route('/api/mapa/coordenadas-completas')
def get_coordenadas_completas():
    try:
        plazas = dataframe_cache.get_coordenadas()
        if not plazas:
            return jsonify({'datos': []}), 200
        estados = {}
        for pz in plazas:
            estados[pz['estado']] = estados.get(pz['estado'], 0) + 1
        return json_response({'status':'success','total_plazas':len(plazas),
                               'total_estados':len(estados),'estadisticas_estados':estados,'plazas':plazas})
    except Exception as exc:
        logging.error(f"get_coordenadas_completas: {exc}")
        return jsonify({'status':'error','message':str(exc),'plazas':[]}), 500

@app.route('/api/mapa/seguro')
def mapa_seguro_endpoint():
    action = request.args.get('action')
    handlers = {
        'cercanos':           handle_cercanos,
        'calcular-distancia': handle_calcular_distancia,
        'ruta':               handle_ruta,
        'buscar':             handle_buscar,
        'filtro-estados':     handle_filtro_estados,
    }
    fn = handlers.get(action)
    if fn is None:
        return jsonify({'status':'error','message':'Acción no válida'}), 400
    try:
        return fn()
    except Exception as exc:
        logging.error(f"mapa_seguro_endpoint [{action}]: {exc}")
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/mapa/ubicar-plaza-cercana')
def ubicar_plaza_cercana():
    try:
        lat = request.args.get('lat', type=float)
        lng = request.args.get('lng', type=float)
        if lat is None or lng is None:
            return jsonify({'status':'error','message':'Se requieren lat, lng'}), 400
        if RUST_BRIDGE_AVAILABLE and rust_bridge and rust_bridge.is_ready():
            cercanas = rust_bridge.distancias_cercanas(lat, lng, 5000.0, 1)
            if cercanas:
                item  = cercanas[0]
                d     = item["distancia_km"]
                mejor = rust_bridge.enriquecer_con_json(item["indice_df"], d)
                if mejor is None:
                    df = dataframe_cache.get_ultimo_mes()
                    if not df.empty:
                        try:
                            fila = df.iloc[item["indice_df"]]
                            plat = obtener_valor_seguro(fila, 'LATITUD')
                            plng = obtener_valor_seguro(fila, 'LONGITUD')
                            mejor = {
                                'clave':                str(obtener_valor_seguro(fila,'CLAVE_PLAZA','') or ''),
                                'nombre':               str(obtener_valor_seguro(fila,'NOMBRE_PC','') or ''),
                                'estado':               str(obtener_valor_seguro(fila,'ESTADO','') or ''),
                                'municipio':            str(obtener_valor_seguro(fila,'MUNICIPIO','') or ''),
                                'lat':                  float(plat) if plat is not None else None,
                                'lng':                  float(plng) if plng is not None else None,
                                'distancia_km':         round(d, 2),
                                'distancia_formateada': f"{d:.1f} km",
                            }
                        except IndexError:
                            pass
                if mejor is not None:
                    return jsonify({'status':'success','ubicacion_usuario':{'lat':lat,'lng':lng},
                                    'plaza_mas_cercana':mejor,'distancia_minima_km':round(d,2),'motor':'rust'})
        df = dataframe_cache.get_ultimo_mes()
        if df.empty:
            return jsonify({'status':'error','message':'Sin datos'}), 503
        mejor, dist_min = None, float('inf')
        for _, p in df.iterrows():
            plat = obtener_valor_seguro(p,'LATITUD'); plng = obtener_valor_seguro(p,'LONGITUD')
            if plat is None or plng is None: continue
            try:
                d = calcular_distancia_km(lat, lng, float(plat), float(plng))
            except (TypeError, ValueError):
                continue
            if d < dist_min:
                dist_min = d
                mejor = {
                    'clave':                str(obtener_valor_seguro(p,'CLAVE_PLAZA','') or ''),
                    'nombre':               str(obtener_valor_seguro(p,'NOMBRE_PC','') or ''),
                    'estado':               str(obtener_valor_seguro(p,'ESTADO','') or ''),
                    'municipio':            str(obtener_valor_seguro(p,'MUNICIPIO','') or ''),
                    'lat':                  float(plat), 'lng': float(plng),
                    'distancia_km':         round(d, 2), 'distancia_formateada': f"{d:.1f} km",
                }
        if mejor is None:
            return jsonify({'status':'error','message':'Sin plazas cercanas'}), 404
        return jsonify({'status':'success','ubicacion_usuario':{'lat':lat,'lng':lng},
                        'plaza_mas_cercana':mejor,'distancia_minima_km':round(dist_min,2),'motor':'pandas_fallback'})
    except Exception as exc:
        logging.error(f"ubicar_plaza_cercana: {exc}")
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/mapa/generar-linea-ruta')
def generar_linea_ruta():
    try:
        olat = request.args.get('origen_lat', type=float); olng = request.args.get('origen_lng', type=float)
        dlat = request.args.get('destino_lat', type=float); dlng = request.args.get('destino_lng', type=float)
        if None in [olat,olng,dlat,dlng]:
            return jsonify({'status':'error','message':'Faltan coordenadas'}), 400
        d = calcular_distancia_km(olat,olng,dlat,dlng)
        puntos = [[olat+(dlat-olat)*i/10, olng+(dlng-olng)*i/10] for i in range(11)]
        return jsonify({'status':'success','origen':{'lat':olat,'lng':olng},'destino':{'lat':dlat,'lng':dlng},
                        'puntos_ruta':puntos,'distancia_km':round(d,2),
                        'estilo_linea':{'color':'#007bff','weight':3,'opacity':0.7,'dashArray':'10, 10'}})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500

@app.route('/api/mapa/opciones-navegacion')
def opciones_navegacion():
    try:
        dlat  = request.args.get('destino_lat', type=float); dlng = request.args.get('destino_lng', type=float)
        dname = request.args.get('destino_nombre','Destino')
        olat  = request.args.get('origen_lat', type=float);  olng = request.args.get('origen_lng', type=float)
        if dlat is None or dlng is None:
            return jsonify({'status':'error','message':'Faltan coordenadas destino'}), 400
        opciones = {'ver_ubicacion':{
            'google_maps': f"https://www.google.com/maps/search/?api=1&query={dlat},{dlng}",
            'waze':        f"https://www.waze.com/ul?ll={dlat},{dlng}&navigate=yes"}}
        if olat is not None and olng is not None:
            nc = dname.replace(' ','+'); d = calcular_distancia_km(olat,olng,dlat,dlng)
            opciones['crear_ruta'] = {
                'google_maps': f"https://www.google.com/maps/dir/?api=1&origin={olat},{olng}&destination={dlat},{dlng}&destination_place_id={nc}&travelmode=driving",
                'waze':        f"https://www.waze.com/ul?ll={dlat},{dlng}&navigate=yes&to={nc}"}
            opciones['informacion'] = {'distancia_km': round(d,2), 'tiempo_estimado': estimar_tiempo_viaje(d)}
        return jsonify({'status':'success','destino':{'lat':dlat,'lng':dlng,'nombre':dname},'opciones':opciones})
    except Exception as exc:
        return jsonify({'status':'error','message':str(exc)}), 500


# ── Helpers de mapa ───────────────────────────────────────────────────────────
def calcular_distancia_km(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    la1,lo1,la2,lo2 = map(math.radians,[lat1,lng1,lat2,lng2])
    a = math.sin((la2-la1)/2)**2 + math.cos(la1)*math.cos(la2)*math.sin((lo2-lo1)/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def estimar_tiempo_viaje(distancia_km: float) -> str:
    h = int(distancia_km / 40); m = int((distancia_km/40 - h) * 60)
    return f"{h}h {m}min" if h else f"{m} min"


def handle_cercanos():
    lat    = request.args.get('lat', type=float)
    lng    = request.args.get('lng', type=float)
    dmax   = request.args.get('distancia_max', 50, type=float)
    limite = request.args.get('limite', 10, type=int)
    if lat is None or lng is None:
        return jsonify({'status':'error','message':'Se requieren lat, lng'}), 400
    df = dataframe_cache.get_ultimo_mes()
    if df.empty:
        return jsonify({'status':'error','message':'Sin datos'}), 503
    if RUST_BRIDGE_AVAILABLE and rust_bridge and rust_bridge.is_ready():
        cercanas = rust_bridge.distancias_cercanas(lat, lng, dmax, limite)
        resultados = []
        for item in cercanas:
            try:
                fila = df.iloc[item["indice_df"]]
                resultados.append({
                    'clave':                str(obtener_valor_seguro(fila,'CLAVE_PLAZA','') or ''),
                    'nombre':               str(obtener_valor_seguro(fila,'NOMBRE_PC','') or ''),
                    'estado':               str(obtener_valor_seguro(fila,'ESTADO','') or ''),
                    'municipio':            str(obtener_valor_seguro(fila,'MUNICIPIO','') or ''),
                    'lat':                  obtener_valor_seguro(fila,'LATITUD'),
                    'lng':                  obtener_valor_seguro(fila,'LONGITUD'),
                    'distancia_km':         item["distancia_km"],
                    'distancia_formateada': f"{item['distancia_km']:.1f} km",
                })
            except IndexError:
                continue
        return jsonify({'status':'success','ubicacion_usuario':{'lat':lat,'lng':lng},
                        'distancia_maxima_km':dmax,'total_encontradas':len(resultados),
                        'plazas_cercanas':resultados,'motor':'rust'})
    resultados = []
    for _, p in df.iterrows():
        plat = obtener_valor_seguro(p,'LATITUD'); plng = obtener_valor_seguro(p,'LONGITUD')
        if plat is None or plng is None: continue
        try:
            d = calcular_distancia_km(lat, lng, float(plat), float(plng))
        except (TypeError, ValueError):
            continue
        if d <= dmax:
            resultados.append({'clave': str(obtener_valor_seguro(p,'CLAVE_PLAZA','') or ''),
                'nombre': str(obtener_valor_seguro(p,'NOMBRE_PC','') or ''),
                'estado': str(obtener_valor_seguro(p,'ESTADO','') or ''),
                'municipio': str(obtener_valor_seguro(p,'MUNICIPIO','') or ''),
                'lat':plat,'lng':plng,'distancia_km':round(d,2),'distancia_formateada':f"{d:.1f} km"})
    resultados.sort(key=lambda x: x['distancia_km'])
    return jsonify({'status':'success','ubicacion_usuario':{'lat':lat,'lng':lng},
                    'distancia_maxima_km':dmax,'total_encontradas':len(resultados[:limite]),
                    'plazas_cercanas':resultados[:limite],'motor':'pandas_fallback'})


def handle_calcular_distancia():
    la1=request.args.get('lat1',type=float); lo1=request.args.get('lng1',type=float)
    la2=request.args.get('lat2',type=float); lo2=request.args.get('lng2',type=float)
    if None in [la1,lo1,la2,lo2]:
        return jsonify({'status':'error','message':'Faltan coordenadas'}), 400
    d = calcular_distancia_km(la1,lo1,la2,lo2)
    return jsonify({'status':'success','punto1':{'lat':la1,'lng':lo1},'punto2':{'lat':la2,'lng':lo2},
                    'distancia_km':round(d,2),'distancia_metros':round(d*1000),'distancia_formateada':f"{d:.1f} km"})

def handle_ruta():
    olat=request.args.get('origen_lat',type=float); olng=request.args.get('origen_lng',type=float)
    dlat=request.args.get('destino_lat',type=float); dlng=request.args.get('destino_lng',type=float)
    dname=request.args.get('destino_nombre','Destino')
    if None in [olat,olng,dlat,dlng]:
        return jsonify({'status':'error','message':'Faltan coordenadas'}), 400
    d = calcular_distancia_km(olat,olng,dlat,dlng); nc = dname.replace(' ','+')
    return jsonify({'status':'success','origen':{'lat':olat,'lng':olng},
        'destino':{'lat':dlat,'lng':dlng,'nombre':dname},
        'distancia':{'km':round(d,2),'metros':round(d*1000),'formateada':f"{d:.1f} km"},
        'urls_navegacion':{
            'google_maps': f"https://www.google.com/maps/dir/?api=1&origin={olat},{olng}&destination={dlat},{dlng}&destination_place_id={nc}&travelmode=driving",
            'waze':        f"https://www.waze.com/ul?ll={dlat},{dlng}&navigate=yes&to={nc}",
            'google_maps_directo': f"https://www.google.com/maps/search/?api=1&query={dlat},{dlng}",
            'waze_directo':        f"https://www.waze.com/ul?ll={dlat},{dlng}&navigate=yes"},
        'estimacion_tiempo': estimar_tiempo_viaje(d)})

def handle_buscar():
    query  = request.args.get('q','').strip()
    tipo   = request.args.get('tipo','todas')
    limite = request.args.get('limite',20,type=int)
    if not query or len(query) < 2:
        return jsonify({'status':'error','message':'Búsqueda demasiado corta'}), 400
    df = dataframe_cache.get_ultimo_mes()
    if df.empty:
        return jsonify({'status':'error','message':'Sin datos'}), 503
    qn = normalizar_texto(query)
    resultados = []
    for _, p in df.iterrows():
        try:
            clave     = str(obtener_valor_seguro(p,'CLAVE_PLAZA','') or '')
            nombre    = str(obtener_valor_seguro(p,'NOMBRE_PC','') or '')
            estado    = str(obtener_valor_seguro(p,'ESTADO','') or '')
            municipio = str(obtener_valor_seguro(p,'MUNICIPIO','') or '')
            localidad = str(obtener_valor_seguro(p,'LOCALIDAD','') or '')
            lat = obtener_valor_seguro(p,'LATITUD'); lng = obtener_valor_seguro(p,'LONGITUD')
            coincidencia, score = None, 0
            if normalizar_texto(clave) == qn:       coincidencia, score = 'exacta', 100
            elif qn in normalizar_texto(clave):     coincidencia, score = 'clave_parcial', 90
            elif qn in normalizar_texto(estado):    coincidencia, score = 'estado', 80
            elif qn in normalizar_texto(municipio): coincidencia, score = 'municipio', 70
            elif qn in normalizar_texto(localidad): coincidencia, score = 'localidad', 60
            elif qn in normalizar_texto(nombre):    coincidencia, score = 'nombre', 50
            if coincidencia and (tipo=='todas' or tipo==coincidencia):
                resultados.append({'clave':clave,'nombre':nombre,'estado':estado,'municipio':municipio,
                    'localidad':localidad,'lat':lat,'lng':lng,'tipo_coincidencia':coincidencia,'score':score})
        except Exception:
            continue
    resultados.sort(key=lambda x: x['score'], reverse=True)
    return jsonify({'status':'success','query':query,'tipo_busqueda':tipo,
                    'total_encontradas':len(resultados[:limite]),'resultados':resultados[:limite]})

def handle_filtro_estados():
    if RUST_BRIDGE_AVAILABLE and rust_bridge and rust_bridge.is_ready():
        data = rust_bridge.agregaciones_por_estado(filtro_situacion=-1)
        if data:
            estados = [{'nombre': item['nombre'], 'cantidad': item['plazas'],
                        'codigo': normalizar_texto(item['nombre'])[:10]} for item in data]
            return jsonify({'status':'success','total_estados':len(estados),'estados':estados,'motor':'rust'})
    df = dataframe_cache.get_ultimo_mes()
    if df.empty:
        return jsonify({'status':'error','message':'Sin datos'}), 503
    if Config.COLUMNA_ESTADO not in df.columns:
        return jsonify({'status':'error','message':'Columna estado no disponible'}), 400
    cnts = df[Config.COLUMNA_ESTADO].value_counts().to_dict()
    estados = sorted(
        [{'nombre':str(e),'cantidad':int(c),'codigo':normalizar_texto(str(e))[:10]}
         for e, c in cnts.items() if pd.notna(e)],
        key=lambda x: x['nombre'])
    return jsonify({'status':'success','total_estados':len(estados),'estados':estados,'motor':'pandas_fallback'})


# ==============================================================================
# HEADERS DE CACHÉ con ETag dinámico
# ==============================================================================
def _parquet_etag(parquet_path: str = None) -> str:
    path = parquet_path or Config.PARQUET_PATH
    try:
        mtime = os.path.getmtime(path)
        return f'"{int(mtime)}"'
    except OSError:
        return '"v1"'

@app.after_request
def add_header(response: Response) -> Response:
    if (request.path.startswith('/api/mapa/') or request.path.startswith('/api/estadisticas')):
        etag = _parquet_etag()
        response.headers['Cache-Control'] = 'public, max-age=3600'
        response.headers['ETag']          = etag
        if request.headers.get('If-None-Match') == etag:
            response.status_code = 304
            response.data        = b""
    return response


# ==============================================================================
# PRE-CÁLCULO
# ==============================================================================
STATS_CACHE = None

def precalcular_datos():
    global STATS_CACHE
    try:
        logging.info("🔄 Pre-cálculo iniciado...")
        if TRANSLATOR_AVAILABLE and _traducir_json is not None:
            try:
                n_registros = _traducir_json(Config.ARCHIVO_COORDENADAS)
                if n_registros:
                    logging.info(f"✅ coordenadasplazas.json traducido: {n_registros} registros")
            except Exception as e:
                logging.error(f"❌ Error al traducir el JSON: {e}")

        df = dataframe_cache.cargar_dataframe()
        if df.empty:
            logging.warning("⚠️ Pre-cálculo: DataFrame vacío")
            return

        STATS_CACHE = {
            'total_plazas':         int(df[Config.COLUMNA_CLAVE].nunique()),
            'total_registros':      len(df),
            'total_estados':        int(df[Config.COLUMNA_ESTADO].nunique()) if Config.COLUMNA_ESTADO in df.columns else 0,
            'ultimo_mes_registros': len(dataframe_cache.get_ultimo_mes()),
        }

        coords = dataframe_cache.get_coordenadas()
        fuente = "JSON" if _cargar_coordenadas_desde_json() else "DataFrame"
        logging.info(f"📍 Coordenadas listas ({fuente}): {len(coords)} plazas")

        if RUST_BRIDGE_AVAILABLE and rust_bridge is not None:
            n_json = rust_bridge.warm_coordenadas_json()
            if n_json:
                logging.info(f"🦀 rust_bridge JSON warm-up: {n_json} coords listas")

        if PLAZA_INDEX_AVAILABLE and _plaza_index is not None:
            _plaza_index.build(
                dataframe_cache.get_ultimo_mes(),
                col_estado=Config.COLUMNA_ESTADO, col_zona=Config.COLUMNA_COORD_ZONA,
                col_municipio=Config.COLUMNA_MUNICIPIO, col_localidad=Config.COLUMNA_LOCALIDAD,
                col_clave=Config.COLUMNA_CLAVE,
            )

        if PRECALC_AVAILABLE and _stats_cache is not None:
            _stats_cache.build(Config.PARQUET_PATH)

        if RUST_BRIDGE_AVAILABLE and rust_bridge is not None:
            rust_bridge.init(Config.PARQUET_PATH, usar_legacy=False)

        _init_comparativas()

        logging.info(f"✅ Pre-cálculo: {STATS_CACHE['total_plazas']} plazas listas")

    except Exception as exc:
        logging.error(f"❌ Pre-cálculo: {exc}\n{traceback.format_exc()}")


# ==============================================================================
# ARRANQUE
# ==============================================================================
logging.info("🚀 Iniciando app...")
threading.Thread(target=precalcular_datos, daemon=True).start()
df_inicial = dataframe_cache.cargar_dataframe()
if df_inicial.empty:
    logging.critical("⚠️ App iniciada sin datos")
else:
    logging.info(f"✅ App lista: {len(df_inicial)} registros")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
