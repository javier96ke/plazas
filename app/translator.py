# ==============================================================================
# column_translator.py
# ColumnTranslator — Capa de traducción entre el nuevo parquet optimizado
# y la app legacy. Cero cambios en app.py.
#
# Nuevo parquet                   →  Nombre legacy (Config / app.py)
# ─────────────────────────────────────────────────────────────────────────────
# Cve-mes (int64)                 →  Cve-mes
# Mes (categorical/int8)          →  Mes
# Clave_Edo (int64)               →  Clave_Edo
# zona (categorical/int16)        →  Coord. Zona
# municipio (categorical/int16)   →  Municipio
# localidad (categorical/int16)   →  Localidad
# colonia (categorical/int16)     →  Colonia
# cp (categorical/int16)          →  Cod_Post
# calle (categorical/int16)       →  Calle
# num (categorical/int16)         →  Num
# clave (categorical/int16)       →  Clave_Plaza
# nombre (categorical/int16)      →  Nombre_PC
# inc_inicial (int64)             →  Inc_Inicial
# inc_prim (int64)                →  Inc_Prim
# inc_sec (int64)                 →  Inc_Sec
# inc_total (int64)               →  Inc_Total
# aten_inicial (int64)            →  Aten_Inicial
# aten_prim (int64)               →  Aten_Prim
# aten_sec (int64)                →  Aten_Sec
# aten_total (int64)              →  Aten_Total
# examenes (int64)                →  Exámenes aplicados
# cn_inicial (int64)              →  CN_Inicial_Acum
# cn_prim (int64)                 →  CN_Prim_Acum
# cn_sec (int64)                  →  CN_Sec_Acum
# cn_total (int64)                →  CN_Tot_Acum
# certificados (int64)            →  Cert_Emitidos
# tec_doc (categorical/int16)     →  Tec_Doc
# pvs1 (categorical/int16)        →  Nom_PVS_1
# pvs2 (categorical/int8)         →  Nom_PVS_2
# tipo_local (categorical/int8)   →  Tipo_local
# inst_aliada (categorical/int16) →  Inst_aliada
# arq_discap (categorical/int8)   →  Arq_Discap.
# conectividad (categorical/int8) →  Conect_Instalada
# tipo_conect (categorical/int8)  →  Tipo_Conect
# lat (float64)                   →  Latitud
# lng (float64)                   →  Longitud
# eq_total (int64)                →  Total de equipos de cómputo en la plaza
# eq_operan (int64)               →  Equipos de cómputo que operan
# tipos_eq (categorical/int16)    →  Tipos de equipos de cómputo
# imp_funcionan (int64)           →  Impresoras que funcionan
# imp_suministros (int64)         →  Impresoras con suministros (toner, hojas)
# srv_total (int64)               →  Total de servidores en la plaza
# srv_operan (int64)              →  Número de servidores que funcionan correctamente
# mesas (int64)                   →  Cuantas mesas funcionan
# sillas (int64)                  →  Cuantas sillas funcionan
# anaqueles (int64)               →  Cuantos Anaqueles funcionan
# mes (int64)                     →  (numérico auxiliar — se descarta después de Mes)
# anio (int64)                    →  Año
# estado_id (int64)               →  (índice entero de Estado — se mapea al nombre)
# situacion (int64)               →  (índice entero de Situación — se mapea al texto)
# ==============================================================================

import logging
from typing import Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tabla maestra: nuevo_nombre → nombre_legacy
# ---------------------------------------------------------------------------
_RENAME_MAP: dict[str, str] = {
    "Cve-mes":        "Cve-mes",           # ya coincide, pero lo normalizamos
    "Mes":            "Mes",
    "Clave_Edo":      "Clave_Edo",
    "zona":           "Coord. Zona",
    "municipio":      "Municipio",
    "localidad":      "Localidad",
    "colonia":        "Colonia",
    "cp":             "Cod_Post",
    "calle":          "Calle",
    "num":            "Num",
    "clave":          "Clave_Plaza",
    "nombre":         "Nombre_PC",
    "inc_inicial":    "Inc_Inicial",
    "inc_prim":       "Inc_Prim",
    "inc_sec":        "Inc_Sec",
    "inc_total":      "Inc_Total",
    "aten_inicial":   "Aten_Inicial",
    "aten_prim":      "Aten_Prim",
    "aten_sec":       "Aten_Sec",
    "aten_total":     "Aten_Total",
    "examenes":       "Exámenes aplicados",
    "cn_inicial":     "CN_Inicial_Acum",
    "cn_prim":        "CN_Prim_Acum",
    "cn_sec":         "CN_Sec_Acum",
    "cn_total":       "CN_Tot_Acum",
    "certificados":   "Cert_Emitidos",
    "tec_doc":        "Tec_Doc",
    "pvs1":           "Nom_PVS_1",
    "pvs2":           "Nom_PVS_2",
    "tipo_local":     "Tipo_local",
    "inst_aliada":    "Inst_aliada",
    "arq_discap":     "Arq_Discap.",
    "conectividad":   "Conect_Instalada",
    "tipo_conect":    "Tipo_Conect",
    "lat":            "Latitud",
    "lng":            "Longitud",
    "eq_total":       "Total de equipos de cómputo en la plaza",
    "eq_operan":      "Equipos de cómputo que operan",
    "tipos_eq":       "Tipos de equipos de cómputo",
    "imp_funcionan":  "Impresoras que funcionan",
    "imp_suministros":"Impresoras con suministros (toner, hojas)",
    "srv_total":      "Total de servidores en la plaza",
    "srv_operan":     "Número de servidores que funcionan correctamente",
    "mesas":          "Cuantas mesas funcionan",
    "sillas":         "Cuantas sillas funcionan",
    "anaqueles":      "Cuantos Anaqueles funcionan",
    "anio":           "Año",
    # columnas auxiliares que se manejan por separado:
    # "mes"       → descartada (ya tenemos Mes/Cve-mes)
    # "estado_id" → se expande al nombre del estado → "Estado"
    # "situacion" → se expande al texto de la situación → "Situación"
}

# ---------------------------------------------------------------------------
# Mapas de enteros → strings
# (deben coincidir con los catálogos usados al generar el parquet)
# ---------------------------------------------------------------------------

# Clave numérica de estado → nombre oficial
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

# Entero → texto de situación
_SITUACION_MAP: dict[int, str] = {

    0: "SUSPENSIÓN TEMPORAL",
    1: "EN OPERACIÓN",
    2: "EN PROCESO DE APERTURA",
    3: "BAJA DEFINITIVA",
    4: "REUBICACIÓN",
    # agrega más si tu catálogo tiene más valores
}


class ColumnTranslator:
    """
    Traduce un DataFrame del nuevo parquet optimizado al esquema de nombres
    que espera la aplicación legacy (app.py / Config).

    Uso:
        translator = ColumnTranslator()
        df_legacy  = translator.translate(df_nuevo)

    El DataFrame resultante contiene exactamente las mismas columnas que
    producía el antiguo excel/parquet, por lo que app.py no requiere ningún
    cambio.
    """

    def __init__(
        self,
        estado_map: Optional[dict] = None,
        situacion_map: Optional[dict] = None,
    ):
        self.estado_map    = estado_map    or _ESTADO_MAP
        self.situacion_map = situacion_map or _SITUACION_MAP
        # Mapa inverso legacy → nuevo (útil para debugging)
        self.reverse_map: dict[str, str] = {v: k for k, v in _RENAME_MAP.items()}

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def translate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma el DataFrame nuevo al esquema legacy.
        Pasos:
          1. Renombrar columnas cortas → nombres largos
          2. Expandir estado_id → columna "Estado" con nombre textual
          3. Expandir situacion (int) → columna "Situación" con texto
          4. Convertir categóricos almacenados como int a string
          5. Descartar columnas auxiliares (mes, estado_id, situacion raw)
        """
        if df.empty:
            logger.warning("ColumnTranslator: DataFrame vacío, se devuelve sin cambios.")
            return df

        df = df.copy()

        # ── Paso 1: Renombrar ──────────────────────────────────────────
        cols_to_rename = {c: _RENAME_MAP[c] for c in df.columns if c in _RENAME_MAP}
        df = df.rename(columns=cols_to_rename)
        logger.debug(f"ColumnTranslator: renombradas {len(cols_to_rename)} columnas.")

        # ── Paso 2: Expandir estado_id → "Estado" ─────────────────────
        if "estado_id" in df.columns:
            df["Estado"] = (
                df["estado_id"]
                .apply(lambda x: self.estado_map.get(int(x), f"Estado_{x}") if pd.notna(x) else "")
            )
            df = df.drop(columns=["estado_id"])
            logger.debug("ColumnTranslator: estado_id → Estado.")

        # ── Paso 3: Expandir situacion (int) → "Situación" ────────────
        if "situacion" in df.columns:
            df["Situación"] = (
                df["situacion"]
                .apply(lambda x: self.situacion_map.get(int(x), f"Situación_{x}") if pd.notna(x) else "")
            )
            df = df.drop(columns=["situacion"])
            logger.debug("ColumnTranslator: situacion → Situación.")

        # ── Paso 4: Convertir categóricos int → string ─────────────────
        # Los dtype 'category' con valores int se leen como CategoricalDtype;
        # pandas ya expone el valor textual cuando pyarrow decodifica el parquet.
        # Sin embargo, si por alguna razón quedaron como int, los convertimos.
        for col in df.columns:
            if df[col].dtype.name == "category":
                try:
                    df[col] = df[col].astype(str).replace({"nan": "", "<NA>": ""})
                except Exception as exc:
                    logger.warning(f"ColumnTranslator: no se pudo convertir '{col}': {exc}")

        # ── Paso 5: Descartar columna auxiliar 'mes' si existe ─────────
        # (ya tenemos Cve-mes con el valor numérico y Mes con el nombre)
        if "mes" in df.columns:
            df = df.drop(columns=["mes"])

        logger.info(
            f"ColumnTranslator: traducción completada. "
            f"Columnas finales ({len(df.columns)}): {list(df.columns)}"
        )
        return df

    # ------------------------------------------------------------------
    # Utilidades de diagnóstico
    # ------------------------------------------------------------------

    def check_schema(self, df: pd.DataFrame) -> dict:
        """
        Verifica qué columnas del nuevo parquet se reconocen y cuáles no.
        Devuelve un reporte dict para logging/debugging.
        """
        known     = [c for c in df.columns if c in _RENAME_MAP or c in ("estado_id", "situacion", "mes")]
        unknown   = [c for c in df.columns if c not in known]
        expected  = list(_RENAME_MAP.keys()) + ["estado_id", "situacion", "mes"]
        missing   = [c for c in expected if c not in df.columns]

        return {
            "columnas_reconocidas":      known,
            "columnas_desconocidas":     unknown,
            "columnas_esperadas_ausentes": missing,
            "total_df":                  len(df.columns),
        }

    def get_rename_map(self) -> dict:
        """Devuelve una copia del mapa de renombrado (solo lectura)."""
        return dict(_RENAME_MAP)

    def get_reverse_map(self) -> dict:
        """Devuelve el mapa inverso legacy → nuevo."""
        return dict(self.reverse_map)


# ---------------------------------------------------------------------------
# Instancia global lista para importar desde app.py
# ---------------------------------------------------------------------------
translator = ColumnTranslator()


def traducir_json_coordenadas(archivo: str = 'coordenadasplazas.json') -> int:
    """
    Traduce en el lugar los campos 'estado' y 'situacion' numéricos
    del JSON de coordenadas usando los mapas de translator.py.

    Retorna el número de registros traducidos.
    Uso:
        from translator import traducir_json_coordenadas
        traducir_json_coordenadas()
    """
    import json, os

    if not os.path.exists(archivo):
        logger.warning(f"traducir_json_coordenadas: {archivo} no encontrado")
        return 0

    with open(archivo, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Mapas con claves string (el JSON guarda "15", no 15)
    est_map = {str(k): v for k, v in _ESTADO_MAP.items()}
    sit_map = {str(k): v for k, v in _SITUACION_MAP.items()}

    count = 0
    for plaza in data:
        changed = False
        est = str(plaza.get('estado', '')).strip()
        if est in est_map:
            plaza['estado'] = est_map[est]
            changed = True
        sit = str(plaza.get('situacion', '')).strip()
        if sit in sit_map:
            plaza['situacion'] = sit_map[sit]
            changed = True
        if changed:
            count += 1

    try:
        import orjson
        with open(archivo, 'wb') as f:
            f.write(orjson.dumps(data))
    except ImportError:
        with open(archivo, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    logger.info(f"✅ traducir_json_coordenadas: {count}/{len(data)} registros traducidos en {archivo}")
    return count