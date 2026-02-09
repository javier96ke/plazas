import os
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, jsonify, send_from_directory, url_for, redirect, Response
import json
from unidecode import unidecode
from datetime import datetime
import traceback
import math
import pickle
from functools import lru_cache
import orjson
import threading
import logging

# ==============================================================================
# CORRECCIÓN: Manejar importaciones condicionales
# ==============================================================================
try:
    from drive_excel_reader import drive_excel_reader_readonly
    from drive_excel_reader import drive_excel_comparator
    from drive_excel_reader import (
        safe_json_serialize,
        obtener_años_desde_arbol_json,
        obtener_nombre_mes
    )
    DRIVE_MODULES_AVAILABLE = True
    logging.info("✅ Módulos de drive_excel_reader cargados correctamente")
except ImportError as e:
    logging.warning(f"⚠️ Módulos de drive_excel_reader no disponibles: {e}")
    DRIVE_MODULES_AVAILABLE = False
    
    # Crear placeholders para evitar errores
    class DummyDriveReader:
        def get_available_years(self):
            return []
        
        def get_available_months(self, year):
            return []
        
        def query_excel_data_readonly(self, *args, **kwargs):
            return {'status': 'error', 'message': 'Módulo no disponible'}
        
        def get_stats(self):
            return {
                'total_requests': 0,
                'cache_hits': 0,
                'drive_downloads': 0,
                'cache_hit_ratio': 0,
                'currently_loaded_files': 0,
                'tree_loaded': False
            }
        
        def load_excel_strict(self, year, month):
            return None, {'error': 'Módulo no disponible'}
        
        def get_excel_info(self, *args, **kwargs):
            return None
        
        def clear_all_cache(self):
            pass
        
        def get_loaded_files_count(self):
            return 0
        
        def load_tree(self):
            return False
    
    class DummyComparator:
        def comparar_periodos_avanzado(self, *args, **kwargs):
            return {'status': 'error', 'message': 'Módulo no disponible'}
        
        def comparar_periodos_avanzado_con_años_diferentes(self, *args, **kwargs):
            return {'status': 'error', 'message': 'Módulo no disponible'}
        
        def obtener_estados_disponibles(self, *args, **kwargs):
            return []
        
        def obtener_metricas_disponibles(self, *args, **kwargs):
            return []
    
    # Crear instancias dummy
    drive_excel_reader_readonly = DummyDriveReader()
    drive_excel_comparator = DummyComparator()
    
    # Funciones dummy
    def safe_json_serialize(obj):
        # Esta función es clave para evitar el error int64
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj) if not np.isnan(obj) else None
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict('records')
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        if isinstance(obj, datetime):
            return obj.isoformat()
        if pd.isna(obj):
            return None
        return obj
    
    def obtener_años_desde_arbol_json():
        return [], {}
    
    def obtener_nombre_mes(numero_mes: str) -> str:
        meses = {
            '01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril',
            '05': 'Mayo', '06': 'Junio', '07': 'Julio', '08': 'Agosto',
            '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'
        }
        return meses.get(numero_mes, f'Mes {numero_mes}')

# ==============================================================================
# 1. CONFIGURACIÓN Y LOGGING
# ==============================================================================
IS_PRODUCTION = os.environ.get('RENDER') is not None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8'
)
handler = RotatingFileHandler('app.log', maxBytes=1024000, backupCount=5, encoding='utf-8')
logging.getLogger().addHandler(handler)

app = Flask(__name__)

# ==============================================================================
# HELPER PARA ORJSON (optimización clave)
# ==============================================================================
def json_response(data, status=200):
    """
     REEMPLAZO ULTRARRÁPIDO DE jsonify para endpoints pesados.
    Usa orjson para serialización 10x más rápida.
    """
    return Response(
        orjson.dumps(
            data,
            option=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY
        ),
        status=status,
        mimetype="application/json"
    )

# ==============================================================================
# GLOBAL DATAFRAME CACHE - ÚNICA INSTANCIA EN MEMORIA
# ==============================================================================
class DataframeCache:
    """Clase para gestionar el DataFrame en memoria con cache"""
    
    def __init__(self):
        self._df_plazas = None
        self._df_ultimo_mes = None
        self._cache_timestamp = None
        self._mapeo_columnas = None
        self._estados_cache = None
        self._zonas_cache = {}
        self._municipios_cache = {}
        self._localidades_cache = {}
        
    def cargar_dataframe(self, force_reload=False):
        """Carga el DataFrame desde parquet o excel (solo una vez)"""
        if self._df_plazas is not None and not force_reload:
            logging.info("✅ Usando DataFrame cargado en memoria")
            return self._df_plazas
            
        try:
            # Intentar cargar desde parquet primero
            parquet_path = 'datos_plazas.parquet'
            excel_path = 'datos_plazas.xlsx'
            
            if os.path.exists(parquet_path):
                logging.info(f"📂 Cargando desde parquet: {parquet_path}")
                self._df_plazas = pd.read_parquet(parquet_path)
                logging.info(f"✅ Parquet cargado: {len(self._df_plazas)} registros")
                
            elif os.path.exists(excel_path):
                logging.warning(f"⚠️ Parquet no encontrado, cargando desde excel: {excel_path}")
                self._df_plazas = pd.read_excel(excel_path)
                logging.info(f"✅ Excel cargado (modo emergencia): {len(self._df_plazas)} registros")
                
                # Guardar como parquet para futuras cargas rápidas
                try:
                    self._df_plazas.to_parquet(parquet_path)
                    logging.info(f"💾 Excel convertido a parquet: {parquet_path}")
                except Exception as e:
                    logging.error(f"❌ Error guardando parquet: {e}")
            else:
                logging.critical("❌ No se encontró ni parquet ni excel")
                self._df_plazas = pd.DataFrame()
                return self._df_plazas
            
            # Procesar el DataFrame (solo una vez)
            self._df_plazas = self._preparar_dataframe(self._df_plazas)
            self._cache_timestamp = datetime.now()
            
            # Pre-cachear mapeo de columnas
            self._mapeo_columnas = inicializar_mapeo_columnas(self._df_plazas)
            
            # Limpiar caches secundarios
            self._clear_secondary_caches()
            
            logging.info(f"🎉 DataFrame cargado en memoria: {len(self._df_plazas)} registros")
            return self._df_plazas
            
        except Exception as e:
            logging.error(f"❌ Error crítico cargando DataFrame: {e}")
            self._df_plazas = pd.DataFrame()
            return self._df_plazas
    
    #  MODIFICACIÓN 1: Método _preparar_dataframe optimizado con CATEGORY
    def _preparar_dataframe(self, df):
        """
        Prepara el DataFrame creando columnas normalizadas basadas en Alias.
        INCLUYE OPTIMIZACIÓN 'CATEGORY' PARA MENOR USO DE RAM Y MAYOR VELOCIDAD.
        """
        if df.empty:
            return df
            
        # Crear copia para evitar modificaciones al original
        df = df.copy()
        
        # 1. Normalizar nombres de columnas (eliminar espacios y saltos de línea)
        df.columns = [str(col).strip() for col in df.columns]
        
        # 2. Inicializar el mapeo para saber qué columnas reales tenemos
        # Esto conecta tus Alias (ESTADO) con la columna real (Entidad, Edo, etc.)
        mapeo = inicializar_mapeo_columnas(df)
        
        # 3. Crear columnas normalizadas (Indispensable para búsqueda rápida y cascada)
        mapa_normalizacion = {
            'ESTADO': 'normalized_estado',
            'COORD_ZONA': 'normalized_zona',
            'MUNICIPIO': 'normalized_municipio',
            'LOCALIDAD': 'normalized_localidad',
            'CLAVE_PLAZA': 'normalized_clave'
        }

        for clave_alias, nombre_col_optimizada in mapa_normalizacion.items():
            # Buscamos el nombre real de la columna en el DataFrame usando el mapeo
            nombre_real = mapeo.get(clave_alias)
            
            if nombre_real and nombre_real in df.columns:
                try:
                    # A) Normalizar texto (Mayúsculas, sin acentos)
                    df[nombre_col_optimizada] = df[nombre_real].fillna('').astype(str).apply(normalizar_texto)
                    
                    # B)  GRAN OPTIMIZACIÓN: Convertir a 'category'
                    # Esto reduce el uso de RAM y hace los filtros 10x más rápidos
                    df[nombre_col_optimizada] = df[nombre_col_optimizada].astype('category')
                    
                except Exception as e:
                    logging.error(f"❌ Error optimizando {nombre_real}: {e}")
            else:
                # Si no existe la columna, llenamos con vacíos para evitar crashes, pero avisamos.
                logging.warning(f"⚠️ No se pudo crear optimización para {clave_alias}: Columna no encontrada.")
                df[nombre_col_optimizada] = ""
                # Convertimos a categoría incluso si está vacía para mantener consistencia de tipos
                df[nombre_col_optimizada] = df[nombre_col_optimizada].astype('category')

        # 4. Asegurar tipos numéricos para coordenadas
        for col_name in ['Latitud', 'Longitud']:
            real_col = mapeo.get(col_name.upper()) or col_name
            if real_col in df.columns:
                df[real_col] = pd.to_numeric(df[real_col], errors='coerce')
        
        return df
    
    def get_dataframe(self):
        """Obtiene el DataFrame (lo carga si no está en memoria)"""
        return self.cargar_dataframe()
    
    def get_ultimo_mes(self):
        """Obtiene DataFrame del último mes (con cache)"""
        if self._df_ultimo_mes is not None:
            return self._df_ultimo_mes
            
        df = self.get_dataframe()
        self._df_ultimo_mes = obtener_df_ultimo_mes(df)
        return self._df_ultimo_mes
    
    def get_mapeo_columnas(self):
        """Obtiene el mapeo de columnas"""
        if self._mapeo_columnas is None:
            df = self.get_dataframe()
            self._mapeo_columnas = inicializar_mapeo_columnas(df)
        return self._mapeo_columnas
    
    def get_estados_cache(self):
        """Obtiene estados con cache"""
        if self._estados_cache is None:
            df = self.get_ultimo_mes()
            if Config.COLUMNA_ESTADO in df.columns:
                estado_counts = df.groupby(Config.COLUMNA_ESTADO)[Config.COLUMNA_CLAVE].nunique()
                self._estados_cache = []
                for estado, count in estado_counts.items():
                    self._estados_cache.append({
                        'nombre': str(estado),
                        'cantidad': int(count)
                    })
                self._estados_cache.sort(key=lambda x: x['cantidad'], reverse=True)
            else:
                self._estados_cache = []
        return self._estados_cache
    
    def get_zonas_cache(self, estado):
        """Obtiene zonas para un estado con cache"""
        key = normalizar_texto(estado)
        if key not in self._zonas_cache:
            df = self.get_ultimo_mes()
            col_estado_norm = f"normalized_{Config.COLUMNA_ESTADO.lower()}"
            if col_estado_norm in df.columns:
                df_filtrado = df[df[col_estado_norm] == key]
                zonas = obtener_opciones_unicas(df_filtrado, Config.COLUMNA_COORD_ZONA)
                self._zonas_cache[key] = zonas
            else:
                self._zonas_cache[key] = []
        return self._zonas_cache[key]
    
    def _clear_secondary_caches(self):
        """Limpia caches secundarios"""
        self._df_ultimo_mes = None
        self._estados_cache = None
        self._zonas_cache.clear()
        self._municipios_cache.clear()
        self._localidades_cache.clear()
    
    def refresh_cache(self):
        """Fuerza recarga del DataFrame"""
        logging.info("🔄 Refrescando cache del DataFrame")
        self._df_plazas = None
        self._clear_secondary_caches()
        return self.cargar_dataframe(force_reload=True)

# Instancia global del cache
dataframe_cache = DataframeCache()

# ==============================================================================
# FUNCION PARA CONVERTIR TIPOS DE PANDAS A TIPOS STANDARD (EVITA ERROR INT64)
# ==============================================================================
def convertir_a_serializable(obj):
    """
    Convierte recursivamente objetos de numpy/pandas a tipos nativos de Python.
    Esto soluciona el error 'Object of type int64 is not JSON serializable'.
    """
    if isinstance(obj, dict):
        return {k: convertir_a_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convertir_a_serializable(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj) if not np.isnan(obj) else None
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    else:
        return obj

class Config:
    """Clase para almacenar todas las variables de configuración con sistema de alias."""
    EXCEL_PATH = 'datos_plazas.xlsx'
    PARQUET_PATH = 'datos_plazas.parquet'
    IMAGES_BASE_PATH = 'fotos_de_plazas'
    ARCHIVO_COORDENADAS = 'coordenadasplazas.json'

    # Constantes necesarias para cargar_y_preparar_excel
    COLUMNA_CLAVE = 'Clave_Plaza'
    COLUMNA_ESTADO = 'Estado'
    COLUMNA_MUNICIPIO = 'Municipio'
    COLUMNA_LOCALIDAD = 'Localidad'
    COLUMNA_COORD_ZONA = 'Coord. Zona'
    COLUMNA_LATITUD = 'Latitud'
    COLUMNA_LONGITUD = 'Longitud'
    COLUMNA_CVE_MES = 'Cve-mes'
    COLUMNA_SITUACION = 'Situación'
    COLUMNA_CONECT_INSTALADA = 'Conect_Instalada'
    COLUMNA_TOTAL_EQUIPOS_COMPUTO = 'Total de equipos de cómputo en la plaza'
    COLUMNA_EQUIPOS_COMPUTO_OPERAN = 'Equipos de cómputo que operan'
    COLUMNA_TOTAL_SERVIDORES = 'Total de servidores en la plaza'
    COLUMNA_SERVIDORES_FUNCIONAN = 'Número de servidores que funcionan correctamente'
    COLUMNA_MESAS_FUNCIONAN = 'Cuantas mesas funcionan'
    COLUMNA_SILLAS_FUNCIONAN = 'Cuantas sillas funcionan'
    COLUMNA_ANAQUELES_FUNCIONAN = 'Cuantos Anaqueles funcionan'
    COLUMNA_CN_INICIAL_ACUM = 'CN_Inicial_Acum'
    COLUMNA_CN_PRIM_ACUM = 'CN_Prim_Acum'
    COLUMNA_CN_SEC_ACUM = 'CN_Sec_Acum'
    COLUMNA_CN_TOT_ACUM = 'CN_Tot_Acum'
    COLUMNA_CERT_EMITIDOS = 'Cert_Emitidos'
    
    # Listas de columnas para endpoints
    TODAS_COLUMNAS = [
        COLUMNA_CLAVE, COLUMNA_ESTADO, COLUMNA_MUNICIPIO, COLUMNA_LOCALIDAD, 
        COLUMNA_LATITUD, COLUMNA_LONGITUD, COLUMNA_SITUACION
    ]
    
    COLUMNA_NOMBRE_PC = 'Nombre_PC'
    COLUMNA_TIPO_LOCAL = 'Tipo_local'
    COLUMNA_INST_ALIADA = 'Inst_aliada'
    COLUMNA_ARQ_DISCAP = 'Arq_Discap.'
    COLUMNA_TIPO_CONECT = 'Tipo_Conect'
    COLUMNA_CLAVE_EDO = 'Clave_Edo'
    COLUMNA_COLONIA = 'Colonia'
    COLUMNA_CALLE = 'Calle'
    COLUMNA_NUM = 'Num'
    COLUMNA_COD_POST = 'Cod_Post'
    COLUMNA_ANO = 'Año'
    COLUMNA_MES = 'Mes'
    COLUMNA_INC_INICIAL = 'Inc_Inicial'
    COLUMNA_INC_PRIM = 'Inc_Prim'
    COLUMNA_INC_SEC = 'Inc_Sec'
    COLUMNA_INC_TOTAL = 'Inc_Total'
    COLUMNA_ATEN_INICIAL = 'Aten_Inicial'
    COLUMNA_ATEN_PRIM = 'Aten_Prim'
    COLUMNA_ATEN_SEC = 'Aten_Sec'
    COLUMNA_ATEN_TOTAL = 'Aten_Total'
    COLUMNA_EXAMENES_APLICADOS = 'Exámenes aplicados'
    COLUMNA_TEC_DOC = 'Tec_Doc'
    COLUMNA_NOM_PVS_1 = 'Nom_PVS_1'
    COLUMNA_NOM_PVS_2 = 'Nom_PVS_2'
    COLUMNA_TIPO_LOCAL = 'Tipo_local'
    COLUMNA_INST_ALIADA = 'Inst_aliada'
    COLUMNA_ARQ_DISCAP = 'Arq_Discap.'
    COLUMNA_CONECT_INSTALADA = 'Conect_Instalada'
    COLUMNA_TIPO_CONECT = 'Tipo_Conect'
    COLUMNA_TOTAL_EQUIPOS_COMPUTO = 'Total de equipos de cómputo en la plaza'
    COLUMNA_EQUIPOS_COMPUTO_OPERAN = 'Equipos de cómputo que operan'
    COLUMNA_TIPOS_EQUIPOS_COMPUTO = 'Tipos de equipos de cómputo'
    COLUMNA_IMPRESORAS_FUNCIONAN = 'Impresoras que funcionan'
    COLUMNA_IMPRESORAS_SUMINISTROS = 'Impresoras con suministros (toner, hojas)'
    COLUMNA_TOTAL_SERVIDORES = 'Total de servidores en la plaza'
    COLUMNA_SERVIDORES_FUNCIONAN = 'Número de servidores que funcionan correctamente'
    COLUMNA_MESAS_FUNCIONAN = 'Cuantas mesas funcionan'
    COLUMNA_SILLAS_FUNCIONAN = 'Cuantas sillas funcionan'
    COLUMNA_ANAQUELES_FUNCIONAN = 'Cuantos Anaqueles funcionan'
    
    # ==================== SISTEMA DE ALIAS ====================
    COLUMNAS_CON_ALIAS = {
        'CLAVE_PLAZA': {
            'nombres_posibles': ['Clave_Plaza', 'Clave Plaza', 'CLAVE', 'Clave', 'CCT', 'ID', 'Clave de Plaza'], 
            'nombre_estandar': 'Clave_Plaza'
        },
        'ESTADO': {'nombres_posibles': ['Estado', 'ESTADO', 'Entidad'], 'nombre_estandar': 'Estado'},
        'MUNICIPIO': {'nombres_posibles': ['Municipio', 'MUNICIPIO', 'Municipio/Del'], 'nombre_estandar': 'Municipio'},
        'LOCALIDAD': {'nombres_posibles': ['Localidad', 'LOCALIDAD'], 'nombre_estandar': 'Localidad'},
        'LATITUD': {'nombres_posibles': ['Latitud', 'LATITUD', 'Lat', 'Latitude'], 'nombre_estandar': 'Latitud'},
        'LONGITUD': {'nombres_posibles': ['Longitud', 'LONGITUD', 'Lon', 'Longitude'], 'nombre_estandar': 'Longitud'},
        'COLONIA': {'nombres_posibles': ['Colonia', 'COLONIA'], 'nombre_estandar': 'Colonia'},
        'CALLE': {'nombres_posibles': ['Calle', 'CALLE', 'Calle/Avenida'], 'nombre_estandar': 'Calle'},
        'NUM': {'nombres_posibles': ['Num', 'NUM', 'Número', 'Número exterior'], 'nombre_estandar': 'Num'},
        'COD_POST': {'nombres_posibles': ['Cod_Post', 'Código Postal', 'CP', 'Zip Code'], 'nombre_estandar': 'Cod_Post'},
        'ANO': {'nombres_posibles': ['Año', 'AÑO', 'Year', 'Anio'], 'nombre_estandar': 'Año'},
        'CVE_MES': {'nombres_posibles': ['Cve-mes', 'CVE_MES', 'Clave Mes', 'Mes_Clave'], 'nombre_estandar': 'Cve-mes'},
        'MES': {'nombres_posibles': ['Mes', 'MES', 'Month'], 'nombre_estandar': 'Mes'},
        'CLAVE_EDO': {'nombres_posibles': ['Clave_Edo', 'CLAVE_EDO', 'Cve_Edo', 'Clave Estado'], 'nombre_estandar': 'Clave_Edo'},
        'NOMBRE_PC': {'nombres_posibles': ['Nombre_PC', 'Nombre PC', 'Nombre', 'Nombre de Plaza'], 'nombre_estandar': 'Nombre_PC'},
        'SITUACION': {'nombres_posibles': ['Situación', 'SITUACION', 'Status', 'Estado_Plaza'], 'nombre_estandar': 'Situación'},
        'INC_INICIAL': {'nombres_posibles': ['Inc_Inicial', 'Inscripciones Inicial', 'Inc. Inicial'], 'nombre_estandar': 'Inc_Inicial'},
        'INC_PRIM': {'nombres_posibles': ['Inc_Prim', 'Inscripciones Primaria', 'Inc. Prim'], 'nombre_estandar': 'Inc_Prim'},
        'INC_SEC': {'nombres_posibles': ['Inc_Sec', 'Inscripciones Secundaria', 'Inc. Sec'], 'nombre_estandar': 'Inc_Sec'},
        'INC_TOTAL': {'nombres_posibles': ['Inc_Total', 'Inscripciones Total', 'Total Inscripciones'], 'nombre_estandar': 'Inc_Total'},
        'ATEN_INICIAL': {'nombres_posibles': ['Aten_Inicial', 'Atenciones Inicial', 'Aten. Inicial'], 'nombre_estandar': 'Aten_Inicial'},
        'ATEN_PRIM': {'nombres_posibles': ['Aten_Prim', 'Atenciones Primaria', 'Aten. Prim'], 'nombre_estandar': 'Aten_Prim'},
        'ATEN_SEC': {'nombres_posibles': ['Aten_Sec', 'Atenciones Secundaria', 'Aten. Sec'], 'nombre_estandar': 'Aten_Sec'},
        'ATEN_TOTAL': {'nombres_posibles': ['Aten_Total', 'Atenciones Total', 'Total Atenciones'], 'nombre_estandar': 'Aten_Total'},
        'EXAMENES_APLICADOS': {'nombres_posibles': ['Exámenes aplicados', 'Examenes aplicados', 'Exámenes'], 'nombre_estandar': 'Exámenes aplicados'},
        'CN_INICIAL_ACUM': {'nombres_posibles': ['CN_Inicial_Acum', 'CN Inicial Acum', 'Certificados Inicial'], 'nombre_estandar': 'CN_Inicial_Acum'},
        'CN_PRIM_ACUM': {'nombres_posibles': ['CN_Prim_Acum', 'CN Prim Acum', 'Certificados Primaria'], 'nombre_estandar': 'CN_Prim_Acum'},
        'CN_SEC_ACUM': {'nombres_posibles': ['CN_Sec_Acum', 'CN Sec Acum', 'Certificados Secundaria'], 'nombre_estandar': 'CN_Sec_Acum'},
        'CN_TOT_ACUM': {'nombres_posibles': ['CN_Tot_Acum', 'CN Total Acum', 'Total Certificados'], 'nombre_estandar': 'CN_Tot_Acum'},
        'CERT_EMITIDOS': {'nombres_posibles': ['Cert_ Emitidos', '"Cert_ Emitidos"', 'Cert_Emitidos', 'Certificados Emitidos', 'Total Certificados Emitidos', 'CERT_EMITIDOS'], 'nombre_estandar': 'Cert_Emitidos'},
        'TEC_DOC': {'nombres_posibles': ['Tec_Doc', 'Tec. Doc', 'Técnico Docente'], 'nombre_estandar': 'Tec_Doc'},
        'NOM_PVS_1': {'nombres_posibles': ['Nom_PVS_1', 'PVS 1', 'Personal 1'], 'nombre_estandar': 'Nom_PVS_1'},
        'NOM_PVS_2': {'nombres_posibles': ['Nom_PVS_2', 'PVS 2', 'Personal 2'], 'nombre_estandar': 'Nom_PVS_2'},
        'TIPO_LOCAL': {'nombres_posibles': ['Tipo_local', 'Tipo Local', 'Tipo de Local'], 'nombre_estandar': 'Tipo_local'},
        'INST_ALIADA': {'nombres_posibles': ['Inst_aliada', 'Institución Aliada', 'Inst Aliada'], 'nombre_estandar': 'Inst_aliada'},
        'ARQ_DISCAP': {'nombres_posibles': ['Arq_Discap.', 'Arquitectura Discapacidad', 'Accesibilidad'], 'nombre_estandar': 'Arq_Discap.'},
        'CONECT_INSTALADA': {'nombres_posibles': ['Conect_Instalada', 'Conectividad Instalada', 'Internet'], 'nombre_estandar': 'Conect_Instalada'},
        'TIPO_CONECT': {'nombres_posibles': ['Tipo_Conect', 'Tipo Conectividad', 'Tipo de Conexión'], 'nombre_estandar': 'Tipo_Conect'},
        'TOTAL_EQUIPOS_COMPUTO': {'nombres_posibles': ['Total de equipos de cómputo en la plaza', 'Total equipos cómputo', 'Equipos de cómputo total', 'Total Computadoras'], 'nombre_estandar': 'Total de equipos de cómputo en la plaza'},
        'EQUIPOS_COMPUTO_OPERAN': {'nombres_posibles': ['Equipos de cómputo que operan', 'Equipos operativos', 'Computadoras que funcionan', 'Equipos funcionando'], 'nombre_estandar': 'Equipos de cómputo que operan'},
        'TIPOS_EQUIPOS_COMPUTO': {'nombres_posibles': ['Tipos de equipos de cómputo', 'Tipos de computadoras', 'Variedad equipos', 'Tipos equipo'], 'nombre_estandar': 'Tipos de equipos de cómputo'},
        'IMPRESORAS_FUNCIONAN': {'nombres_posibles': ['Impresoras que funcionan', 'Impresoras operativas', 'Impresoras en funcionamiento'], 'nombre_estandar': 'Impresoras que funcionan'},
        'IMPRESORAS_SUMINISTROS': {'nombres_posibles': ['Impresoras con suministros (toner, hojas)', 'Impresoras con suministros', 'Impresoras con insumos'], 'nombre_estandar': 'Impresoras con suministros (toner, hojas)'},
        'TOTAL_SERVIDORES': {'nombres_posibles': ['Total de servidores en la plaza', 'Servidores total', 'Cantidad servidores'], 'nombre_estandar': 'Total de servidores en la plaza'},
        'SERVIDORES_FUNCIONAN': {'nombres_posibles': ['Número de servidores que funcionan correctamente', 'Servidores operativos', 'Servidores funcionando'], 'nombre_estandar': 'Número de servidores que funcionan correctamente'},
        'COORD_ZONA': { 'nombres_posibles': [ 'Coord. Zona','COORD. ZONA', 'Coordinación de Zona', 'Zona', 'Coord_Zona', 'C. Zona' ], 'nombre_estandar': 'Coord. Zona' },
        'MESAS_FUNCIONAN': {'nombres_posibles': ['Cuantas mesas funcionan', 'Mesas operativas', 'Mesas en buen estado'], 'nombre_estandar': 'Cuantas mesas funcionan'},
        'SILLAS_FUNCIONAN': {'nombres_posibles': ['Cuantas sillas funcionan', 'Sillas operativas', 'Sillas en buen estado'], 'nombre_estandar': 'Cuantas sillas funcionan'},
        'ANAQUELES_FUNCIONAN': {'nombres_posibles': ['Cuantos Anaqueles funcionan', 'Anaquel operativo', 'Estantes funcionando'], 'nombre_estandar': 'Cuantos Anaqueles funcionan'},
    }
    
    # ==================== MÉTODOS DE ACCESO ====================
    @classmethod
    def obtener_nombre_columna(cls, clave: str, df: pd.DataFrame = None) -> str:
        """
        Obtiene el nombre real de la columna en el DataFrame.
        Primero busca coincidencia exacta, luego busca por alias.
        """
        if clave not in cls.COLUMNAS_CON_ALIAS:
            return None
            
        alias_info = cls.COLUMNAS_CON_ALIAS[clave]
        
        # Si tenemos DataFrame, buscar coincidencia exacta primero
        if df is not None:
            for nombre_posible in alias_info['nombres_posibles']:
                if nombre_posible in df.columns:
                    return nombre_posible
        
        # Si no encontramos o no hay DataFrame, devolver el estándar
        return alias_info.get('nombre_estandar')
    
    @classmethod
    def obtener_todas_columnas_estandar(cls) -> list:
        """Devuelve todos los nombres estándar de columnas."""
        return [info['nombre_estandar'] for info in cls.COLUMNAS_CON_ALIAS.values()]
    
    @classmethod
    def obtener_clave_por_nombre_columna(cls, nombre_columna: str) -> str:
        """Obtiene la clave interna para un nombre de columna."""
        for clave, info in cls.COLUMNAS_CON_ALIAS.items():
            if nombre_columna in info['nombres_posibles']:
                return clave
        return None
    
    @classmethod
    def verificar_coincidencias(cls, df: pd.DataFrame) -> dict:
        """Verifica qué columnas del Excel coinciden con nuestros alias."""
        resultados = {
            'coincidencias_exactas': [],
            'coincidencias_por_alias': [],
            'no_encontradas': [],
            'columnas_excel_sin_alias': []
        }
        
        # Verificar cada una de nuestras columnas
        for clave, info in cls.COLUMNAS_CON_ALIAS.items():
            encontrada = False
            
            for nombre_posible in info['nombres_posibles']:
                if nombre_posible in df.columns:
                    resultados['coincidencias_por_alias'].append({
                        'clave': clave,
                        'nombre_encontrado': nombre_posible,
                        'nombre_estandar': info['nombre_estandar']
                    })
                    encontrada = True
                    break
            
            if not encontrada:
                resultados['no_encontradas'].append({
                    'clave': clave,
                    'nombre_estandar': info['nombre_estandar'],
                    'alias_buscados': info['nombres_posibles']
                })
        
        # Verificar columnas del Excel que no tenemos en nuestro sistema
        for col_excel in df.columns:
            encontrada = False
            for info in cls.COLUMNAS_CON_ALIAS.values():
                if col_excel in info['nombres_posibles']:
                    encontrada = True
                    break
            
            if not encontrada:
                resultados['columnas_excel_sin_alias'].append(col_excel)
        
        return resultados

# ==============================================================================
# Funciones auxiliares (fuera de la clase Config para evitar problemas de indentación)
# ==============================================================================
def inicializar_mapeo_columnas(df: pd.DataFrame) -> dict:
    mapeo = {}
    for clave in Config.COLUMNAS_CON_ALIAS.keys():
        nombre_real = Config.obtener_nombre_columna(clave, df)
        if nombre_real:
            mapeo[clave] = nombre_real
            # logging.info(f"✅ Mapeo: {clave} -> '{nombre_real}'")
        else:
            logging.warning(f"⚠️ Columna no encontrada: {clave}")
            mapeo[clave] = None
    return mapeo

def obtener_valor_seguro(df_fila: pd.Series, clave: str, mapeo_columnas: dict, default=None):
    """
    Obtiene el valor de una columna de forma segura usando el mapeo.
    """
    nombre_columna = mapeo_columnas.get(clave)
    
    if nombre_columna and nombre_columna in df_fila.index:
        valor = df_fila[nombre_columna]
        # Devolver None si es NaN para compatibilidad JSON
        return None if pd.isna(valor) else valor
    
    return default

def obtener_nombre_columna_seguro(alias, mapeo, df):
    """Helper para obtener nombre seguro de columna."""
    if alias in df.columns: 
        return alias
    # Buscar en el mapeo inverso si es necesario, o usar la lógica de Config
    # Por simplicidad, aquí confiamos en que el alias coincida o esté en el mapeo
    k = Config.obtener_clave_por_nombre_columna(alias)
    if k and k in mapeo: 
        return mapeo[k]
    return alias  # Fallback

def normalizar_texto(texto: str) -> str:
    """Convierte texto a minúsculas, sin acentos ni espacios extra, y lo pone en mayúsculas para la comparación."""
    if not isinstance(texto, str):
        return ""
    return unidecode(texto).strip().upper()

# ==============================================================================
#  MODIFICACIÓN 2: Función de filtrado inteligente (Helper)
# ==============================================================================
def filtrar_df_cascada(df, filtros):
    """
    Filtra el DataFrame aplicando condiciones AND de forma segura y optimizada.
    
    Args:
        df: DataFrame a filtrar (usualmente el del último mes).
        filtros: dict {'ALIAS_COLUMNA': 'Valor Buscado'} 
                 Ej: {'ESTADO': 'Jalisco', 'MUNICIPIO': 'Guadalajara'}
    """
    if df.empty: return df
    
    df_res = df.copy()
    mapeo = dataframe_cache.get_mapeo_columnas()
    
    # Mapa de claves de alias a nombres de columnas optimizadas 
    cols_optimizadas = {
        'ESTADO': 'normalized_estado',
        'COORD_ZONA': 'normalized_zona',
        'MUNICIPIO': 'normalized_municipio',
        'LOCALIDAD': 'normalized_localidad',
        'CLAVE_PLAZA': 'normalized_clave'
    }

    for clave_alias, valor in filtros.items():
        if not valor: continue # Si el filtro viene vacío, ignorar
        
        val_norm = normalizar_texto(valor)
        col_opt = cols_optimizadas.get(clave_alias)
        col_real = mapeo.get(clave_alias)

        # ESTRATEGIA 1: Búsqueda Rápida (Columna Normalizada + Categoría)
        if col_opt and col_opt in df_res.columns:
            # Pandas es extremadamente rápido filtrando categorías
            df_res = df_res[df_res[col_opt] == val_norm]
            
        # ESTRATEGIA 2: Búsqueda Lenta pero Segura (Fallback si falla la optimización)
        elif col_real and col_real in df_res.columns:
            # logging.warning(f"⚠️ Usando filtro lento (fallback) para {clave_alias}")
            df_res = df_res[df_res[col_real].fillna('').astype(str).apply(normalizar_texto) == val_norm]
            
        # CASO DE ERROR: La columna no existe en el archivo
        else:
            logging.error(f"❌ Imposible filtrar por {clave_alias}: La columna no se encuentra.")
            return pd.DataFrame() # Devolver vacío para no filtrar mal y mostrar todo

    return df_res

# ==============================================================================
# 3. FUNCIÓN AUXILIAR FALTANTE - AGREGADA (optimizada con cache)
# ==============================================================================
@lru_cache(maxsize=1)
def obtener_df_ultimo_mes_cached():
    """
    Filtra el DataFrame para obtener solo los datos del último mes disponible.
    Con cache LRU para evitar reprocesamiento.
    """
    df = dataframe_cache.get_dataframe()
    
    try:
        # Verificar si existe la columna de mes
        if Config.COLUMNA_CVE_MES not in df.columns:
            return df.copy()
        
        # Convertir a numérico para ordenar correctamente
        df_temp = df.copy()
        df_temp['__temp_cve_mes'] = pd.to_numeric(df_temp[Config.COLUMNA_CVE_MES], errors='coerce')
        
        # Encontrar el mes más reciente
        max_mes = df_temp['__temp_cve_mes'].max()
        
        if pd.isna(max_mes):
            return df.copy()
        
        # Filtrar por el mes más reciente
        df_filtrado = df_temp[df_temp['__temp_cve_mes'] == max_mes].copy()
        
        # Eliminar columna temporal
        if '__temp_cve_mes' in df_filtrado.columns:
            df_filtrado = df_filtrado.drop('__temp_cve_mes', axis=1)
        
        return df_filtrado
        
    except Exception as e:
        logging.error(f"Error al filtrar por último mes: {e}")
        return df.copy()

# Función wrapper para compatibilidad
def obtener_df_ultimo_mes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Versión wrapper para mantener compatibilidad con código existente.
    """
    return obtener_df_ultimo_mes_cached()

# ==============================================================================
# 4. RUTAS DE LA API (ENDPOINTS) - OPTIMIZADAS
# ==============================================================================
@app.route('/')
def home():
    """Renderiza la página principal."""
    return render_template('index.html')

def obtener_opciones_unicas(df: pd.DataFrame, columna: str) -> list:
    """Obtiene valores únicos, sin nulos/vacíos y ordenados de una columna."""
    if df is None or columna not in df.columns:
        return []
    
    # Usar categorías si está disponible para mayor velocidad
    if df[columna].dtype.name == 'category':
        opciones = df[columna].cat.categories.tolist()
    else:
        opciones = df[columna].dropna().unique()
    
    # Usar convertir_a_serializable para que no falle al devolver JSON
    opciones_limpias = [convertir_a_serializable(opc) for opc in opciones if str(opc).strip()]
    
    # Filtrar None y ordenar como string para evitar errores de tipo mezclado
    opciones_finales = sorted([x for x in opciones_limpias if x is not None], key=str)
    return opciones_finales

@app.route('/api/estados')
def get_estados():
    df = dataframe_cache.get_dataframe()
    estados = obtener_opciones_unicas(df, Config.COLUMNA_ESTADO)
    if not estados:
        return jsonify({'error': 'La información de estados no está disponible.'}), 500
    return jsonify(estados)

@app.route('/api/estados_con_conteo')
def get_estados_con_conteo():
    """Devuelve los estados con el conteo de plazas DEL ÚLTIMO MES."""
    try:
        estados_con_conteo = dataframe_cache.get_estados_cache()
        
        if not estados_con_conteo:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        #  USAR json_response en lugar de jsonify para mayor velocidad
        return json_response(estados_con_conteo)
        
    except Exception as e:
        logging.error(f"Error obteniendo estados con conteo: {e}")
        return jsonify({'error': 'Error al obtener estados con conteo'}), 500

#  MODIFICACIÓN 3: Endpoints de cascada optimizados
@app.route('/api/zonas')
def get_zonas_por_estado():
    """Devuelve las zonas para un estado (Optimizado)."""
    estado = request.args.get('estado', '')
    if not estado:
        return jsonify([]) # Retornar lista vacía si no hay estado

    # 1. Obtener datos (último mes)
    df_actual = dataframe_cache.get_ultimo_mes()
    
    # 2. Filtrar usando la función inteligente
    df_filtrado = filtrar_df_cascada(df_actual, {'ESTADO': estado})
    
    # 3. Obtener columna real para extraer los nombres originales
    col_zona = dataframe_cache.get_mapeo_columnas().get('COORD_ZONA')
    
    if not col_zona: return jsonify([])

    zonas = obtener_opciones_unicas(df_filtrado, col_zona)
    return jsonify(zonas)

@app.route('/api/municipios')
def get_municipios_por_zona():
    """Devuelve los municipios para un estado y zona (Optimizado)."""
    estado = request.args.get('estado', '')
    zona = request.args.get('zona', '')
    
    # Validación estricta: si falta alguno, devuelve vacío para no romper la cascada
    if not estado or not zona:
        return jsonify([])

    df_actual = dataframe_cache.get_ultimo_mes()
    
    # Filtro cascada seguro
    df_filtrado = filtrar_df_cascada(df_actual, {
        'ESTADO': estado,
        'COORD_ZONA': zona
    })
    
    col_mun = dataframe_cache.get_mapeo_columnas().get('MUNICIPIO')
    if not col_mun: return jsonify([])

    municipios = obtener_opciones_unicas(df_filtrado, col_mun)
    return jsonify(municipios)

@app.route('/api/localidades')
def get_localidades_por_municipio():
    """Devuelve las localidades filtradas (Optimizado)."""
    estado = request.args.get('estado', '')
    zona = request.args.get('zona', '')
    municipio = request.args.get('municipio', '')
    
    if not all([estado, zona, municipio]):
        return jsonify([])

    df_actual = dataframe_cache.get_ultimo_mes()
    
    # Filtro cascada seguro
    df_filtrado = filtrar_df_cascada(df_actual, {
        'ESTADO': estado,
        'COORD_ZONA': zona,
        'MUNICIPIO': municipio
    })
    
    col_loc = dataframe_cache.get_mapeo_columnas().get('LOCALIDAD')
    if not col_loc: return jsonify([])

    localidades = obtener_opciones_unicas(df_filtrado, col_loc)
    return jsonify(localidades)

@app.route('/api/claves_plaza')
def get_claves_por_localidad():
    """Devuelve las claves filtradas hasta localidad (Optimizado)."""
    estado = request.args.get('estado', '')
    zona = request.args.get('zona', '')
    municipio = request.args.get('municipio', '')
    localidad = request.args.get('localidad', '')
    
    if not all([estado, zona, municipio, localidad]):
        return jsonify([])

    df_actual = dataframe_cache.get_ultimo_mes()
    
    # Filtro cascada seguro
    df_filtrado = filtrar_df_cascada(df_actual, {
        'ESTADO': estado,
        'COORD_ZONA': zona,
        'MUNICIPIO': municipio,
        'LOCALIDAD': localidad
    })
    
    col_clave = dataframe_cache.get_mapeo_columnas().get('CLAVE_PLAZA')
    if not col_clave: return jsonify([])

    claves = obtener_opciones_unicas(df_filtrado, col_clave)
    return jsonify(claves)

# ==============================================================================
# ENDPOINT DE BÚSQUEDA BLINDADO Y CORREGIDO - OPTIMIZADO
# ==============================================================================
@app.route('/api/search')
def api_search_plaza():
    """Busca una plaza por clave (Versión Ultra Blindada + Campos de Dirección Individuales)."""
    try:
        # 1. Obtener la clave del navegador
        clave_busqueda = request.args.get('clave', '').strip().upper()
        print(f"🔎 Buscando clave: '{clave_busqueda}'") 

        if not clave_busqueda:
            return jsonify({'error': 'Proporciona una clave.'}), 400
        
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_actual.empty:
            return jsonify({'error': 'Base de datos no cargada.'}), 503

        # 2. Asegurar que usamos el nombre correcto de la columna
        columna_clave = 'Clave_Plaza'
        if columna_clave not in df_actual.columns:
            mapeo = dataframe_cache.get_mapeo_columnas()
            columna_clave = mapeo.get('CLAVE_PLAZA')
            if not columna_clave:
                return jsonify({'error': f'No se encuentra la columna Clave_Plaza en el Excel.'}), 500

        # 3. Búsqueda ROBUSTA
        mask = df_actual[columna_clave].astype(str).str.strip().str.upper() == clave_busqueda
        plaza_data = df_actual[mask]

        if plaza_data.empty:
            return jsonify({'error': f'No se encontraron resultados para: {clave_busqueda}'}), 404

        # 4. Obtener la primera coincidencia
        fila = plaza_data.iloc[0]
        plaza_dict_raw = fila.to_dict()
        plaza_dict_clean = convertir_a_serializable(plaza_dict_raw)

        # 5. Preparar el mapeo para extraer datos seguros
        mapeo_cols = dataframe_cache.get_mapeo_columnas()
        
        def get_val(key):
            raw_val = obtener_valor_seguro(fila, key, mapeo_cols)
            return convertir_a_serializable(raw_val)

        # 6. Construir dirección y coordenadas
        partes_dir = [
            str(get_val('COLONIA') or ''),
            str(get_val('CALLE') or ''),
            str(get_val('NUM') or ''),
            str(get_val('COD_POST') or '')
        ]
        direccion = ', '.join([p for p in partes_dir if p.strip()])
        
        lat = get_val('LATITUD')
        lon = get_val('LONGITUD')
        maps_url = None
        if lat and lon:
            try:
                maps_url = f"https://www.google.com/maps/search/?api=1&query={float(lat)},{float(lon)}"
            except:
                pass

        # 7. Buscar imágenes
        images = []
        try:
            images = find_image_urls(clave_busqueda)
        except Exception as e:
            print(f"⚠️ Error buscando imágenes: {e}")

        # 8. Estructurar respuesta
        datos_organizados = {
            'informacion_general': {
                'Clave_Plaza': get_val('CLAVE_PLAZA'),
                'Nombre_PC': get_val('NOMBRE_PC'),
                'Situación': get_val('SITUACION'),
                'Tipo_local': get_val('TIPO_LOCAL'),
                'Inst_aliada': get_val('INST_ALIADA'),
                'Arq_Discap.': get_val('ARQ_DISCAP'),
                'Conect_Instalada': get_val('CONECT_INSTALADA'),
                'Tipo_Conect': get_val('TIPO_CONECT')
            },
            'ubicacion': {
                'Estado': get_val('ESTADO'),
                'Clave_Edo': get_val('CLAVE_EDO'),
                'Coord. Zona': get_val('COORD_ZONA'),
                'Municipio': get_val('MUNICIPIO'),
                'Localidad': get_val('LOCALIDAD'),
                'Colonia': get_val('COLONIA'),
                'Calle': get_val('CALLE'),
                'Num': get_val('NUM'),
                'Cod_Post': get_val('COD_POST'),
                'Direccion_Completa': direccion,
                'Latitud': lat,
                'Longitud': lon
            },
            'fecha_periodo': {
                'Año': get_val('ANO'),
                'Cve-mes': get_val('CVE_MES'),
                'Mes': get_val('MES')
            },
            'incripciones': {
                'Inc_Inicial': get_val('INC_INICIAL'),
                'Inc_Prim': get_val('INC_PRIM'),
                'Inc_Sec': get_val('INC_SEC'),
                'Inc_Total': get_val('INC_TOTAL')
            },
            'atenciones': {
                'Aten_Inicial': get_val('ATEN_INICIAL'),
                'Aten_Prim': get_val('ATEN_PRIM'),
                'Aten_Sec': get_val('ATEN_SEC'),
                'Aten_Total': get_val('ATEN_TOTAL'),
                'Exámenes aplicados': get_val('EXAMENES_APLICADOS')
            },
            'certificaciones': {
                'CN_Inicial_Acum': get_val('CN_INICIAL_ACUM'),
                'CN_Prim_Acum': get_val('CN_PRIM_ACUM'),
                'CN_Sec_Acum': get_val('CN_SEC_ACUM'),
                'CN_Tot_Acum': get_val('CN_TOT_ACUM'),
                'Cert_Emitidos': get_val('CERT_EMITIDOS')
            },
            'personal': {
                'Tec_Doc': get_val('TEC_DOC'),
                'Nom_PVS_1': get_val('NOM_PVS_1'),
                'Nom_PVS_2': get_val('NOM_PVS_2')
            },
            'equipamiento': {
                'Total de equipos de cómputo en la plaza': get_val('TOTAL_EQUIPOS_COMPUTO'),
                'Equipos de cómputo que operan': get_val('EQUIPOS_COMPUTO_OPERAN'),
                'Tipos de equipos de cómputo': get_val('TIPOS_EQUIPOS_COMPUTO'),
                'Impresoras que funcionan': get_val('IMPRESORAS_FUNCIONAN'),
                'Impresoras con suministros (toner, hojas)': get_val('IMPRESORAS_SUMINISTROS'),
                'Total de servidores en la plaza': get_val('TOTAL_SERVIDORES'),
                'Número de servidores que funcionan correctamente': get_val('SERVIDORES_FUNCIONAN')
            },
            'mobiliario': {
                'Cuantas mesas funcionan': get_val('MESAS_FUNCIONAN'),
                'Cuantas sillas funcionan': get_val('SILLAS_FUNCIONAN'),
                'Cuantos Anaqueles funcionan': get_val('ANAQUELES_FUNCIONAN')
            }
        }

        # 9. Limpieza final de la respuesta JSON
        todos_los_datos = {
            k: v 
            for k, v in plaza_dict_clean.items() 
            if not str(k).startswith('normalized_')
        }

        print("✅ Respuesta generada exitosamente")
        return jsonify({
            'datos_organizados': datos_organizados,
            'direccion_completa': direccion,
            'images': images,
            'google_maps_url': maps_url,
            'todos_los_datos': todos_los_datos,
            'excel_info': todos_los_datos 
        })

    except Exception as e:
        import traceback
        print(f"❌ ERROR 500 REAL: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

# ==============================================================================
# 5. FUNCIONES AUXILIARES Y SERVIDOR DE ARCHIVOS
# ==============================================================================
DRIVE_TREE_FILE = 'drive_tree.json'

def find_image_urls(clave_original: str) -> list:
    """Busca y devuelve las URLs de las imágenes para una clave de plaza."""
    try:
        if not os.path.exists(DRIVE_TREE_FILE):
            logging.warning("Archivo drive_tree.json no encontrado")
            return []
        
        with open(DRIVE_TREE_FILE, 'r', encoding='utf-8') as f:
            drive_data = json.load(f)
        
        clave_lower = clave_original.strip().lower()
        image_list = []
        
        def search_images_in_tree(tree, target_folder):
            if tree.get('type') == 'folder' and tree.get('name', '').lower() == target_folder:
                # Encontramos la carpeta, buscar imágenes
                for child in tree.get('children', []):
                    if child.get('type') == 'file' and child.get('mimeType', '').startswith('image/'):
                        # USAR URL DIRECTA DE GOOGLE DRIVE - webContentLink
                        image_url = child.get('webContentLink')
                        if image_url:
                            # Convertir a URL de vista directa (remover parámetros de descarga)
                            direct_url = image_url.replace('&export=download', '').replace('?usp=drivesdk', '')
                            image_list.append(direct_url)
                        else:
                            pass
                return True
            
            for child in tree.get('children', []):
                if search_images_in_tree(child, target_folder):
                    return True
            return False
        
        # Buscar en el árbol
        found = search_images_in_tree(drive_data['structure'], clave_lower)
        
        if not found:
            pass
        elif not image_list:
            pass
        else:
            logging.info(f"🎉 Encontradas {len(image_list)} imágenes para '{clave_lower}'")
            
        return image_list
        
    except Exception as e:
        logging.error(f"❌ Error buscando imágenes en Drive para '{clave_original}': {e}")
        return []

@app.route('/imagenes/<path:filename>')
def serve_image(filename: str):
    """Sirve archivos de imagen de forma segura."""
    return send_from_directory(Config.IMAGES_BASE_PATH, filename)

# ==============================================================================
# 6. ENDPOINT DE ESTADÍSTICAS CON NUEVAS COLUMNAS - OPTIMIZADO
# ==============================================================================
@app.route('/api/estadisticas')
def get_estadisticas():
    """Devuelve estadísticas generales basadas en el ÚLTIMO MES REPORTADO."""
    try:
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_actual.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        # ============================================================
        # 0️⃣ FILTRADO POR ÚLTIMO MES (Lógica Solicitada)
        # ============================================================
        mes_usado = "Todos (Histórico)"

        if Config.COLUMNA_CVE_MES in df_actual.columns:
            # Obtener el mes más reciente
            df_temp = df_actual.copy()
            df_temp['__temp_cve_mes'] = pd.to_numeric(df_temp[Config.COLUMNA_CVE_MES], errors='coerce')
            max_mes = df_temp['__temp_cve_mes'].max()
            
            if pd.notna(max_mes):
                mes_usado = str(int(max_mes))
                logging.info(f"Calculando estadísticas sobre el mes más reciente: {mes_usado}")

        # ============================================================
        # 1️⃣ Estadísticas generales básicas
        # ============================================================
        total_plazas = df_actual[Config.COLUMNA_CLAVE].nunique()
        
        # Contar plazas en operación
        if Config.COLUMNA_SITUACION in df_actual.columns:
            df_operacion = df_actual[
                df_actual[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
            ]
            plazas_operacion = df_operacion[Config.COLUMNA_CLAVE].nunique()
        else:
            plazas_operacion = 0
        
        # ============================================================
        # 2️⃣ Estado con más y menos plazas
        # ============================================================
        total_estados = 0
        estado_mas_plazas = {'nombre': 'N/A', 'cantidad': 0}
        estado_menos_plazas = {'nombre': 'N/A', 'cantidad': 0}

        if Config.COLUMNA_ESTADO in df_actual.columns:
            estado_counts = df_actual.groupby(Config.COLUMNA_ESTADO)[Config.COLUMNA_CLAVE].nunique().sort_values(ascending=False)
            total_estados = len(estado_counts)

            if not estado_counts.empty:
                estado_mas_plazas = {
                    'nombre': str(estado_counts.index[0]),
                    'cantidad': int(estado_counts.iloc[0])
                }
                estado_menos_plazas = {
                    'nombre': str(estado_counts.index[-1]),
                    'cantidad': int(estado_counts.iloc[-1])
                }

        # ============================================================
        # 3️⃣ Estado con mayor conectividad
        # ============================================================
        estado_mayor_conectividad = {'nombre': 'N/A', 'porcentaje': 0}
        
        if Config.COLUMNA_CONECT_INSTALADA in df_actual.columns:
            df_conect = df_actual.copy()
            df_conect['conectiva'] = df_conect[Config.COLUMNA_CONECT_INSTALADA].apply(
                lambda v: 1 if pd.notna(v) and str(v).strip().lower() not in ['', 'nan', 'na', 'none', 'null', '0'] else 0
            )

            conect_por_estado = (
                df_conect.groupby(Config.COLUMNA_ESTADO)['conectiva']
                .mean()
                .sort_values(ascending=False)
            )

            if not conect_por_estado.empty:
                estado_mayor_conectividad = {
                    'nombre': str(conect_por_estado.index[0]),
                    'porcentaje': round(conect_por_estado.iloc[0] * 100, 2)
                }

        # ============================================================
        # 4️⃣ Estado con mayor porcentaje de Operación/Suspensión
        # ============================================================
        estado_mas_operacion = {'nombre': 'N/A', 'porcentaje': 0}
        estado_mas_suspension = {'nombre': 'N/A', 'porcentaje': 0}

        if Config.COLUMNA_SITUACION in df_actual.columns:
            df_sit = df_actual.copy()
            df_sit['Situacion_Norm'] = df_sit[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper()

            conteo_por_estado = df_sit.groupby([Config.COLUMNA_ESTADO, 'Situacion_Norm'])[Config.COLUMNA_CLAVE].nunique().unstack(fill_value=0)
            
            conteo_por_estado['Total_Estado'] = conteo_por_estado.sum(axis=1)
            
            if 'EN OPERACIÓN' in conteo_por_estado.columns:
                conteo_por_estado['Pct_Operacion'] = conteo_por_estado['EN OPERACIÓN'] / conteo_por_estado['Total_Estado']
                top_op = conteo_por_estado['Pct_Operacion'].idxmax()
                estado_mas_operacion = {
                    'nombre': str(top_op),
                    'porcentaje': round(conteo_por_estado.loc[top_op, 'Pct_Operacion'] * 100, 2)
                }

            if 'SUSPENSIÓN TEMPORAL' in conteo_por_estado.columns:
                conteo_por_estado['Pct_Suspension'] = conteo_por_estado['SUSPENSIÓN TEMPORAL'] / conteo_por_estado['Total_Estado']
                top_susp = conteo_por_estado['Pct_Suspension'].idxmax()
                estado_mas_suspension = {
                    'nombre': str(top_susp),
                    'porcentaje': round(conteo_por_estado.loc[top_susp, 'Pct_Suspension'] * 100, 2)
                }

        # ============================================================
        # 5️⃣ Estadísticas de equipamiento
        # ============================================================
        estadisticas_equipamiento = {}
        
        if Config.COLUMNA_TOTAL_EQUIPOS_COMPUTO in df_actual.columns:
            equipos_operando = pd.to_numeric(df_actual[Config.COLUMNA_EQUIPOS_COMPUTO_OPERAN], errors='coerce').sum()
            equipos_totales = pd.to_numeric(df_actual[Config.COLUMNA_TOTAL_EQUIPOS_COMPUTO], errors='coerce').sum()
            porcentaje_operativos = (equipos_operando / equipos_totales * 100) if equipos_totales > 0 else 0
            
            estadisticas_equipamiento['equipos_computo'] = {
                'total': float(equipos_totales),
                'operando': float(equipos_operando),
                'porcentaje_operativos': round(porcentaje_operativos, 2)
            }
        
        if Config.COLUMNA_TOTAL_SERVIDORES in df_actual.columns:
            servidores_operando = pd.to_numeric(df_actual[Config.COLUMNA_SERVIDORES_FUNCIONAN], errors='coerce').sum()
            servidores_totales = pd.to_numeric(df_actual[Config.COLUMNA_TOTAL_SERVIDORES], errors='coerce').sum()
            porcentaje_servidores = (servidores_operando / servidores_totales * 100) if servidores_totales > 0 else 0
            
            estadisticas_equipamiento['servidores'] = {
                'total': float(servidores_totales),
                'operando': float(servidores_operando),
                'porcentaje_operativos': round(porcentaje_servidores, 2)
            }

        # ============================================================
        # 6️⃣ Estadísticas de mobiliario
        # ============================================================
        estadisticas_mobiliario = {}
        
        mobiliario_columns = {
            'mesas': Config.COLUMNA_MESAS_FUNCIONAN,
            'sillas': Config.COLUMNA_SILLAS_FUNCIONAN,
            'anaqueles': Config.COLUMNA_ANAQUELES_FUNCIONAN
        }
        
        for item, columna in mobiliario_columns.items():
            if columna in df_actual.columns:
                total = pd.to_numeric(df_actual[columna], errors='coerce').sum()
                estadisticas_mobiliario[item] = float(total)

        # ============================================================
        # 7️⃣ Estadísticas de certificaciones
        # ============================================================
        estadisticas_certificaciones = {}
        
        cn_columns = [
            Config.COLUMNA_CN_INICIAL_ACUM,
            Config.COLUMNA_CN_PRIM_ACUM,
            Config.COLUMNA_CN_SEC_ACUM,
            Config.COLUMNA_CN_TOT_ACUM,
            Config.COLUMNA_CERT_EMITIDOS
        ]
        
        for col in cn_columns:
            if col in df_actual.columns:
                total = pd.to_numeric(df_actual[col], errors='coerce').sum()
                estadisticas_certificaciones[col] = float(total)

        # ============================================================
        # 🔹 Respuesta final
        # ============================================================
        #  USAR json_response en lugar de jsonify para mayor velocidad
        return json_response({
            'meta': {'mes_reportado': mes_usado},
            'totalPlazas': int(total_plazas),
            'plazasOperacion': int(plazas_operacion),
            'totalEstados': int(total_estados),
            'estadoMasPlazas': estado_mas_plazas,
            'estadoMenosPlazas': estado_menos_plazas,
            'estadoMayorConectividad': estado_mayor_conectividad,
            'estadoMasOperacion': estado_mas_operacion,
            'estadoMasSuspension': estado_mas_suspension,
            'estadisticasEquipamiento': estadisticas_equipamiento,
            'estadisticasMobiliario': estadisticas_mobiliario,
            'estadisticasCertificaciones': estadisticas_certificaciones
        })
        
    except Exception as e:
        logging.error(f"Error generando estadísticas: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return jsonify({'error': 'Error al generar estadísticas'}), 500

# ==============================================================================
# ENDPOINT PARA REFRESCAR CACHE
# ==============================================================================
@app.route('/api/refresh-cache', methods=['POST'])
def refresh_cache():
    """Endpoint para forzar refresco del cache del DataFrame."""
    try:
        dataframe_cache.refresh_cache()
        return jsonify({
            'status': 'success',
            'message': 'Cache refrescado exitosamente',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logging.error(f"Error refrescando cache: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# NOTA: Mantengo el resto de los endpoints (8-20) como estaban, 
# pero ahora usan dataframe_cache en lugar de df_plazas global
# ==============================================================================

# En los endpoints restantes, reemplaza todas las referencias a:
# df_plazas -> dataframe_cache.get_dataframe()
# df_actual (cuando se refiere al último mes) -> dataframe_cache.get_ultimo_mes()

# Por ejemplo, en el endpoint /api/columnas-disponibles:
@app.route('/api/columnas-disponibles')
def get_columnas_disponibles():
    """Devuelve la lista de columnas disponibles en el dataset."""
    try:
        df = dataframe_cache.get_dataframe()
        
        if df.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        columnas = list(df.columns)
        
        # Filtrar columnas normalizadas
        columnas_reales = [col for col in columnas if not col.startswith('normalized_')]
        
        return jsonify({
            'total_columnas': len(columnas_reales),
            'columnas': columnas_reales,
            'columnas_esperadas': Config.TODAS_COLUMNAS,
            'columnas_faltantes': [col for col in Config.TODAS_COLUMNAS if col not in columnas_reales]
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo columnas disponibles: {e}")
        return jsonify({'error': 'Error al obtener columnas disponibles'}), 500

# Continuar con el resto de los endpoints, aplicando el mismo patrón...

# ==============================================================================
# 8. ENDPOINT PARA OBTENER DATOS DETALLADOS DE UNA PLAZA (VERSIÓN COMPLETA)
# ==============================================================================
@app.route('/api/plaza-detallada/<clave>')
def get_plaza_detallada(clave):
    """Devuelve TODOS los datos de una plaza específica."""
    try:
        df = dataframe_cache.get_dataframe()
        
        if df.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        clave_busqueda = clave.strip().upper()
        plaza_data = df[df[Config.COLUMNA_CLAVE] == clave_busqueda]
        
        if plaza_data.empty:
            return jsonify({'error': f'No se encontró la plaza con clave: {clave}'}), 404
        
        plaza_dict = plaza_data.iloc[0].replace({np.nan: None}).to_dict()
        
        # Crear un diccionario organizado con todas las columnas
        datos_completos = {}
        
        # Agrupar por categorías para mejor organización
        categorias = {
            'informacion_general': [
                Config.COLUMNA_CLAVE, Config.COLUMNA_NOMBRE_PC, Config.COLUMNA_SITUACION,
                Config.COLUMNA_TIPO_LOCAL, Config.COLUMNA_INST_ALIADA, Config.COLUMNA_ARQ_DISCAP
            ],
            'conectividad': [
                Config.COLUMNA_CONECT_INSTALADA, Config.COLUMNA_TIPO_CONECT
            ],
            'ubicacion': [
                Config.COLUMNA_ESTADO, Config.COLUMNA_CLAVE_EDO, Config.COLUMNA_COORD_ZONA,
                Config.COLUMNA_MUNICIPIO, Config.COLUMNA_LOCALIDAD, Config.COLUMNA_COLONIA,
                Config.COLUMNA_CALLE, Config.COLUMNA_NUM, Config.COLUMNA_COD_POST,
                Config.COLUMNA_LATITUD, Config.COLUMNA_LONGITUD
            ],
            'fecha_periodo': [
                Config.COLUMNA_ANO, Config.COLUMNA_CVE_MES, Config.COLUMNA_MES
            ],
            'incripciones': [
                Config.COLUMNA_INC_INICIAL, Config.COLUMNA_INC_PRIM,
                Config.COLUMNA_INC_SEC, Config.COLUMNA_INC_TOTAL
            ],
            'atenciones': [
                Config.COLUMNA_ATEN_INICIAL, Config.COLUMNA_ATEN_PRIM,
                Config.COLUMNA_ATEN_SEC, Config.COLUMNA_ATEN_TOTAL,
                Config.COLUMNA_EXAMENES_APLICADOS
            ],
            'certificaciones': [
                Config.COLUMNA_CN_INICIAL_ACUM, Config.COLUMNA_CN_PRIM_ACUM,
                Config.COLUMNA_CN_SEC_ACUM, Config.COLUMNA_CN_TOT_ACUM,
                Config.COLUMNA_CERT_EMITIDOS
            ],
            'personal': [
                Config.COLUMNA_TEC_DOC, Config.COLUMNA_NOM_PVS_1,
                Config.COLUMNA_NOM_PVS_2
            ],
            'equipamiento': [
                Config.COLUMNA_TOTAL_EQUIPOS_COMPUTO, Config.COLUMNA_EQUIPOS_COMPUTO_OPERAN,
                Config.COLUMNA_TIPOS_EQUIPOS_COMPUTO, Config.COLUMNA_IMPRESORAS_FUNCIONAN,
                Config.COLUMNA_IMPRESORAS_SUMINISTROS, Config.COLUMNA_TOTAL_SERVIDORES,
                Config.COLUMNA_SERVIDORES_FUNCIONAN
            ],
            'mobiliario': [
                Config.COLUMNA_MESAS_FUNCIONAN, Config.COLUMNA_SILLAS_FUNCIONAN,
                Config.COLUMNA_ANAQUELES_FUNCIONAN
            ]
        }
        
        # Llenar datos por categoría
        for categoria, columnas in categorias.items():
            datos_categoria = {}
            for columna in columnas:
                if columna in plaza_dict:
                    # Usar el convertidor serializable
                    datos_categoria[columna] = convertir_a_serializable(plaza_dict[columna])
            datos_completos[categoria] = datos_categoria
        
        # Agregar imágenes
        image_urls = find_image_urls(clave)
        
        return jsonify({
            'status': 'success',
            'clave': clave_busqueda,
            'datos': datos_completos,
            'images': image_urls,
            'datos_completos': {k: convertir_a_serializable(v) for k, v in plaza_dict.items() if not k.startswith('normalized_')}
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo datos detallados para plaza {clave}: {e}")
        return jsonify({'error': 'Error al obtener datos de la plaza'}), 500

@app.route('/api/estados_populares')
def get_estados_populares():
    """Devuelve los estados con más plazas para las pestañas rápidas."""
    try:
        df = dataframe_cache.get_ultimo_mes()
        
        if df.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        estado_counts = df[Config.COLUMNA_ESTADO].value_counts()
        estados_populares = []
        
        for estado, count in estado_counts.head(8).items():
            estados_populares.append({
                'nombre': str(estado),
                'cantidad': int(count)
            })
        
        return jsonify(estados_populares)
    except Exception as e:
        logging.error(f"Error obteniendo estados populares: {e}")
        return jsonify({'error': 'Error al obtener estados populares'}), 500

@app.route('/api/plazas_por_estado/<estado>')
def get_plazas_por_estado(estado):
    """Devuelve las plazas de un estado específico (SOLO ÚLTIMO MES)."""
    try:
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_actual.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        df_filtrado = df_actual[df_actual[Config.COLUMNA_ESTADO] == estado]
        
        if df_filtrado.empty:
            return jsonify([])
        
        plazas = []
        for _, plaza in df_filtrado.iterrows():
            partes_direccion = [
                str(plaza.get(Config.COLUMNA_COLONIA, '') or '').strip(),
                str(plaza.get(Config.COLUMNA_CALLE, '') or '').strip(),
                str(plaza.get(Config.COLUMNA_NUM, '') or '').strip(),
                str(plaza.get(Config.COLUMNA_COD_POST, '') or '').strip()
            ]
            direccion_completa = ', '.join(filter(None, partes_direccion))
            
            plazas.append({
                'clave': plaza[Config.COLUMNA_CLAVE],
                'direccion': direccion_completa,
                'municipio': plaza.get(Config.COLUMNA_MUNICIPIO, ''),
                'localidad': plaza.get(Config.COLUMNA_LOCALIDAD, '')
            })
        
        return jsonify(plazas)
    except Exception as e:
        logging.error(f"Error obteniendo plazas por estado {estado}: {e}")
        return jsonify({'error': 'Error al obtener plazas'}), 500

@app.route('/api/busqueda_global')
def busqueda_global():
    """Búsqueda global en todas las columnas relevantes (SOLO ÚLTIMO MES)."""
    try:
        query = request.args.get('q', '').strip().lower()
        if not query or len(query) < 2:
            return jsonify([])
        
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_actual.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        resultados = []
        columnas_busqueda = [
            Config.COLUMNA_CLAVE, Config.COLUMNA_ESTADO, Config.COLUMNA_MUNICIPIO,
            Config.COLUMNA_LOCALIDAD, Config.COLUMNA_COLONIA, Config.COLUMNA_CALLE
        ]
        
        # Iteramos sobre el DataFrame filtrado
        for _, plaza in df_actual.iterrows():
            for columna in columnas_busqueda:
                valor = str(plaza.get(columna, '')).lower()
                if query in valor:
                    partes_direccion = [
                        str(plaza.get(Config.COLUMNA_COLONIA, '') or '').strip(),
                        str(plaza.get(Config.COLUMNA_CALLE, '') or '').strip(),
                        str(plaza.get(Config.COLUMNA_NUM, '') or '').strip(),
                        str(plaza.get(Config.COLUMNA_COD_POST, '') or '').strip()
                    ]
                    direccion_completa = ', '.join(filter(None, partes_direccion))
                    
                    resultados.append({
                        'tipo': 'Plaza',
                        'clave': plaza[Config.COLUMNA_CLAVE],
                        'estado': plaza.get(Config.COLUMNA_ESTADO, ''),
                        'municipio': plaza.get(Config.COLUMNA_MUNICIPIO, ''),
                        'direccion': direccion_completa,
                        'columna_encontrada': columna
                    })
                    break 
        
        resultados_unicos = []
        claves_vistas = set()
        for resultado in resultados:
            if resultado['clave'] not in claves_vistas:
                resultados_unicos.append(resultado)
                claves_vistas.add(resultado['clave'])
        
        return jsonify(resultados_unicos[:20])
    except Exception as e:
        logging.error(f"Error en búsqueda global: {e}")
        return jsonify({'error': 'Error en búsqueda global'}), 500

@app.route('/api/cn_resumen')
def cn_resumen():
    """
    Resumen nacional de CN. CORREGIDO para usar Alias de columnas.
    """
    try:
        df_completo = dataframe_cache.get_dataframe()
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_completo.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        # 1. OBTENER NOMBRES REALES USANDO EL MAPEO (Alias)
        mapeo = dataframe_cache.get_mapeo_columnas()
        
        # Diccionario: Clave Interna -> Nombre Real en el Excel
        cols_map = {
            'CN_Inicial_Acum': mapeo.get('CN_INICIAL_ACUM'),
            'CN_Prim_Acum':    mapeo.get('CN_PRIM_ACUM'),
            'CN_Sec_Acum':     mapeo.get('CN_SEC_ACUM')
        }

        # 2. DataFrame COMPLETO (para las sumatorias) - Usamos copia para no afectar cache
        df_tmp = df_completo.copy()
        
        # 3. Crear máscara de operación SOLO en el DataFrame ACTUAL
        col_situacion = mapeo.get('SITUACION')
        if col_situacion and col_situacion in df_actual.columns:
            mask_operacion_actual = df_actual[col_situacion].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
        else:
            mask_operacion_actual = pd.Series([False] * len(df_actual), index=df_actual.index)

        total_registros = len(df_tmp)
        resumen_nacional = {}
        cn_total_nacional = 0
        
        for clave_interna, nombre_real in cols_map.items():
            colnum = f'__{clave_interna}_num'
            
            # Si la columna no existe en el Excel, llenamos con 0 para no romper el sistema
            if not nombre_real or nombre_real not in df_tmp.columns:
                df_tmp[colnum] = 0
                df_actual[colnum] = 0
                n_nulos = total_registros
                suma = 0.0
            else:
                # Convertir a numérico de forma segura
                df_tmp[colnum] = pd.to_numeric(df_tmp[nombre_real], errors='coerce')
                # También en el actual para contar plazas operativas
                if nombre_real in df_actual.columns:
                    df_actual[colnum] = pd.to_numeric(df_actual[nombre_real], errors='coerce')
                else:
                    df_actual[colnum] = 0

                n_nulos = df_tmp[colnum].isna().sum()
                suma = float(df_tmp[colnum].fillna(0).sum())

            cn_total_nacional += suma
            
            # CÁLCULO DE PLAZAS EN OPERACIÓN (Solo mes actual)
            if mask_operacion_actual.any():
                plazas_operacion_cat = len(df_actual[
                    mask_operacion_actual & 
                    (df_actual[colnum].fillna(0) > 0)
                ])
            else:
                plazas_operacion_cat = 0
            
            resumen_nacional[clave_interna] = {
                'total_registros': int(total_registros),
                'nulos': int(n_nulos),
                'pct_nulos': round(n_nulos / total_registros * 100, 2) if total_registros > 0 else 0.0,
                'suma': round(suma, 2),
                'plazasOperacion': int(plazas_operacion_cat) 
            }

        # Total de plazas en operación (solo del mes actual)
        plazas_operacion_total = int(mask_operacion_actual.sum())
        
        resumen_nacional['CN_Total'] = {
            'total_registros': int(total_registros),
            'nulos': 0,
            'pct_nulos': 0.0,
            'suma': round(cn_total_nacional, 2),
            'plazasOperacion': plazas_operacion_total 
        }

        # Top 5 se mantiene igual (sobre el histórico)
        col_estado = mapeo.get('ESTADO')
        top5 = []
        
        if col_estado and col_estado in df_tmp.columns:
            df_tmp['__CN_Total_num'] = (
                df_tmp[f'__{"CN_Inicial_Acum"}_num'].fillna(0) + 
                df_tmp[f'__{"CN_Prim_Acum"}_num'].fillna(0) + 
                df_tmp[f'__{"CN_Sec_Acum"}_num'].fillna(0)
            )
            
            grp = df_tmp.groupby(col_estado)['__CN_Total_num'].sum().sort_values(ascending=False)
            top5 = [{'estado': str(idx), 'suma_CN_Total': float(v)} for idx,v in grp.head(5).items()]

        return json_response({
            'resumen_nacional': resumen_nacional,
            'top5_estados_por_CN_Total': top5
        })
    except Exception as e:
        logging.error(f"Error en /api/cn_resumen: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return jsonify({'error': f'Error interno: {str(e)}'}), 500
    
@app.route('/api/cn_por_estado')
def cn_por_estado():
    """
    CORREGIDO: Elimina el error 'tuple index out of range' convirtiendo Series a Dicts
    antes de acceder a ellas y manejando DataFrames vacíos.
    """
    try:
        # 1. Cargar datos
        df_historico = dataframe_cache.get_dataframe().copy()
        df_actual = dataframe_cache.get_ultimo_mes().copy()
        
        if df_historico.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        # 2. Obtener nombres reales de columnas (Mapeo seguro)
        mapeo = dataframe_cache.get_mapeo_columnas()
        col_estado = mapeo.get('ESTADO', 'Estado')
        col_situacion = mapeo.get('SITUACION', 'Situación')
        col_conect = mapeo.get('CONECT_INSTALADA', 'Conect_Instalada')

        # Validación crítica
        if col_estado not in df_actual.columns:
            return jsonify({'error': f'Columna {col_estado} no encontrada'}), 500

        # 3. PRE-CALCULAR DICCIONARIOS (Evita error de índice en bucles)
        # Convertimos .size() (que devuelve Series) a .to_dict() inmediatamente
        
        # A) Total plazas actuales por estado
        conteo_plazas_actuales = df_actual.groupby(col_estado).size().to_dict()
        
        # B) Plazas en operación
        plazas_operacion_dict = {}
        if col_situacion in df_actual.columns:
            # Normalizar para filtrar
            sit_norm = df_actual[col_situacion].astype(str).str.strip().str.upper()
            mask_op = sit_norm == 'EN OPERACIÓN'
            plazas_operacion_dict = df_actual[mask_op].groupby(col_estado).size().to_dict()

        # C) Conectividad
        conectividad_dict = {}
        if col_conect in df_actual.columns and col_situacion in df_actual.columns:
            # Lógica: En operación AND (Conectividad != vacio/0/false)
            mask_op = df_actual[col_situacion].astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
            df_op = df_actual[mask_op].copy()
            
            # Limpieza básica de conectividad
            def es_valido(val):
                s = str(val).lower().strip()
                return s not in ['nan', 'none', '0', 'false', 'no', '']
            
            mask_conect = df_op[col_conect].apply(es_valido)
            conectividad_dict = df_op[mask_conect].groupby(col_estado).size().to_dict()

        # 4. CALCULAR MÉTRICAS HISTÓRICAS (CN)
        # Definir columnas de métricas numéricas
        cols_cn = {
            'CN_Inicial_Acum': mapeo.get('CN_INICIAL_ACUM', 'CN_Inicial_Acum'),
            'CN_Prim_Acum': mapeo.get('CN_PRIM_ACUM', 'CN_Prim_Acum'),
            'CN_Sec_Acum': mapeo.get('CN_SEC_ACUM', 'CN_Sec_Acum')
        }

        # Asegurar tipos numéricos en el histórico (rellenar NaN con 0)
        cn_total_nacional = 0
        for key, col_name in cols_cn.items():
            temp_col = f'__{key}_num'
            if col_name in df_historico.columns:
                df_historico[temp_col] = pd.to_numeric(df_historico[col_name], errors='coerce').fillna(0)
            else:
                df_historico[temp_col] = 0
            cn_total_nacional += df_historico[temp_col].sum()

        # 5. CONSTRUIR RESPUESTA ITERANDO GRUPOS
        estados_summary = []
        
        # Agrupar histórico por estado
        grouped = df_historico.groupby(col_estado)

        for estado, grupo in grouped:
            # IMPORTANTE: Manejar si 'estado' es una tupla (pasa a veces con groupby complejos)
            estado_key = estado[0] if isinstance(estado, tuple) else estado
            estado_str = str(estado_key).strip()

            # Obtener datos de los diccionarios pre-calculados (Búsqueda O(1) segura)
            total_plazas = int(conteo_plazas_actuales.get(estado_key, 0))
            plazas_operacion = int(plazas_operacion_dict.get(estado_key, 0))
            conectados = int(conectividad_dict.get(estado_key, 0))

            # Calcular porcentaje conectividad
            pct_conect = 0.0
            if plazas_operacion > 0:
                pct_conect = round((conectados / plazas_operacion) * 100, 1)

            # Sumar métricas del grupo histórico
            suma_inicial = float(grupo['__CN_Inicial_Acum_num'].sum())
            suma_prim = float(grupo['__CN_Prim_Acum_num'].sum())
            suma_sec = float(grupo['__CN_Sec_Acum_num'].sum())
            suma_total = suma_inicial + suma_prim + suma_sec

            # Porcentaje sobre nacional
            pct_nacional = 0.0
            if cn_total_nacional > 0:
                pct_nacional = round((suma_total / cn_total_nacional) * 100, 2)

            estados_summary.append({
                'estado': estado_str,
                'total_plazas': total_plazas,
                'plazas_operacion': plazas_operacion,
                'conectados_actual': conectados,
                'pct_conectividad': pct_conect,
                'suma_CN_Inicial_Acum': int(suma_inicial),
                'suma_CN_Prim_Acum': int(suma_prim),
                'suma_CN_Sec_Acum': int(suma_sec),
                'suma_CN_Total': int(suma_total),
                'pct_sobre_nacional': pct_nacional
            })

        # Ordenar por CN Total descendente
        estados_summary.sort(key=lambda x: x['suma_CN_Total'], reverse=True)

        return json_response({
            'status': 'success',
            'estados': estados_summary,
            'metadata': {
                'cn_total_nacional': int(cn_total_nacional),
                'total_estados_procesados': len(estados_summary)
            }
        })

    except Exception as e:
        import traceback
        logging.error(f"❌ Error CRÍTICO en /api/cn_por_estado: {str(e)}")
        logging.error(traceback.format_exc())
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@app.route('/api/cn_top_estados')
def cn_top_estados():
    """
    Top N estados por métrica (metric param: inicial | prim | sec). default N=5
    query params: ?metric=inicial|prim|sec&n=5
    """
    try:
        metric = request.args.get('metric', 'inicial').lower()
        n = int(request.args.get('n', 5))
        col_map = {
            'inicial': '__CN_Inicial_Acum_num',
            'prim': '__CN_Prim_Acum_num',
            'sec': '__CN_Sec_Acum_num'
        }
        col_key = {'inicial': 'CN_Inicial_Acum', 'prim': 'CN_Prim_Acum', 'sec': 'CN_Sec_Acum'}[metric]

        df_tmp = dataframe_cache.get_dataframe()
        
        if col_key not in df_tmp.columns:
            return jsonify({'error': f'No existe la columna {col_key}'}), 400

        for c in ['CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum']:
            df_tmp[f'__{c}_num'] = pd.to_numeric(df_tmp[c], errors='coerce').fillna(0)

        grp = df_tmp.groupby(Config.COLUMNA_ESTADO)[col_map[metric]].sum().sort_values(ascending=False)
        topn = [{'estado': str(idx), 'valor': float(v)} for idx,v in grp.head(n).items()]
        return jsonify({'metric': metric, 'top': topn})
    except Exception as e:
        logging.error(f"Error en /api/cn_top_estados: {e}")
        return jsonify({'error': 'Error generando top CN estados'}), 500

@app.route('/api/cn_estados_destacados')
def cn_estados_destacados():
    """Devuelve los estados #1 en cada categoría de cn"""
    try:
        df_tmp = dataframe_cache.get_dataframe()
        
        if df_tmp.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        cols = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']
        missing_cols = [c for c in cols if c not in df_tmp.columns]
        if missing_cols:
            return jsonify({'error': 'Faltan columnas', 'faltantes': missing_cols}), 400

        for c in cols:
            df_tmp[f'__{c}_num'] = pd.to_numeric(df_tmp[c], errors='coerce').fillna(0)

        estados_destacados = {}
        
        for c in cols:
            colnum = f'__{c}_num'
            if Config.COLUMNA_ESTADO in df_tmp.columns:
                estado_top = df_tmp.groupby(Config.COLUMNA_ESTADO)[colnum].sum().idxmax()
                valor_top = df_tmp.groupby(Config.COLUMNA_ESTADO)[colnum].sum().max()
                estados_destacados[c] = {
                    'estado': str(estado_top),
                    'valor': float(valor_top)
                }

        return jsonify(estados_destacados)
        
    except Exception as e:
        logging.error(f"Error en /api/cn_estados_destacados: {e}")
        return jsonify({'error': 'Error generando estados destacados CN'}), 500

@app.route('/api/cn_top5_todos')
def cn_top5_todos():
    """Devuelve Top 5 estados para todas las categorías CN"""
    try:
        df_tmp = dataframe_cache.get_dataframe()
        
        if df_tmp.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        for c in ['CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum']:
            df_tmp[f'__{c}_num'] = pd.to_numeric(df_tmp[c], errors='coerce').fillna(0)

        resultados = {}
        
        for categoria, columna in [
            ('inicial', '__CN_Inicial_Acum_num'),
            ('primaria', '__CN_Prim_Acum_num'), 
            ('secundaria', '__CN_Sec_Acum_num')
        ]:
            if Config.COLUMNA_ESTADO in df_tmp.columns:
                grp = df_tmp.groupby(Config.COLUMNA_ESTADO)[columna].sum().sort_values(ascending=False)
                top5 = [{'estado': str(idx), 'valor': float(v)} for idx,v in grp.head(5).items()]
                resultados[categoria] = top5

        return jsonify(resultados)
        
    except Exception as e:
        logging.error(f"Error en /api/cn_top5_todos: {e}")
        return jsonify({'error': 'Error generando top5 todas las categorías'}), 500

# ==============================================================================
# 9. ENDPOINT PARA FECHA DE ACTUALIZACIÓN
# ==============================================================================
@app.route('/api/excel/last-update')
def get_last_update():
    """Devuelve la fecha de última modificación del Excel, ajustada un mes atrás (salvo Diciembre)."""
    try:
        # Primero verificar parquet
        archivo = Config.PARQUET_PATH if os.path.exists(Config.PARQUET_PATH) else Config.EXCEL_PATH
        
        if os.path.exists(archivo):
            timestamp = os.path.getmtime(archivo)
            fecha_real = datetime.fromtimestamp(timestamp)

            # Obtener año y mes de la fecha real
            año = fecha_real.year
            mes = fecha_real.month

            
            if mes == 12:
                # Si es diciembre, NO tocamos nada. El código salta todo el bloque 'elif/else'
                pass 
            elif mes == 1:  
                # Si es enero, restamos un año y pasamos a diciembre
                año -= 1
                mes = 12
            else:
                # Cualquier otro mes (2-11), restamos 1
                mes -= 1
            # ------------------------

            # Crear la fecha ajustada (manteniendo el mismo día si es posible)
            try:
                fecha_ajustada = datetime(año, mes, fecha_real.day)
            except ValueError:
                # Lógica para manejar días que no existen (ej: 31 en un mes de 30 días)
                if mes == 2:  # Febrero
                    if (año % 4 == 0 and año % 100 != 0) or (año % 400 == 0):
                        ultimo_dia = 29
                    else:
                        ultimo_dia = 28
                elif mes in [4, 6, 9, 11]: 
                    ultimo_dia = 30
                else: 
                    ultimo_dia = 31
                fecha_ajustada = datetime(año, mes, ultimo_dia)

            return jsonify({
                "status": "success",
                "mensaje": "Última actualización procesada",
                "last_modified_real": fecha_real.isoformat(),
                "last_modified": fecha_ajustada.isoformat(),
                "formatted": fecha_ajustada.strftime('%d/%m/%Y'),
                "archivo_fuente": os.path.basename(archivo)
            }), 200

        return jsonify({
            "status": "archivo_no_encontrado",
            "mensaje": "No se encontró el archivo Excel.",
            "last_modified": None
        }), 404

    except Exception as e:
        logging.error(f"Error obteniendo fecha de Excel: {e}")
        return jsonify({
            "status": "error",
            "mensaje": f"Ocurrió un error al procesar la fecha: {str(e)}",
            "last_modified": None
        }), 500

# ==============================================================================
# 10. ENDPOINTS PARA GOOGLE DRIVE
# ==============================================================================
@app.route('/api/drive-tree')
def get_drive_tree():
    """Sirve el árbol de Google Drive generado"""
    try:
        if not os.path.exists(DRIVE_TREE_FILE):
            return jsonify({'error': 'El árbol de Drive no está disponible', 'status': 'not_generated'}), 503
        
        with open(DRIVE_TREE_FILE, 'r', encoding='utf-8') as f:
            drive_data = json.load(f)
        
        generated_at = datetime.fromisoformat(drive_data['generated_at'])
        if (datetime.now() - generated_at).days > 500:
            return jsonify({
                'error': 'El árbol de Drive está desactualizado',
                'last_update': drive_data['generated_at'],
                'status': 'stale'
            }), 503
        
        return jsonify(drive_data)
        
    except Exception as e:
        logging.error(f"Error cargando árbol de Drive: {e}")
        return jsonify({'error': 'Error al cargar el árbol de Drive'}), 500

@app.route('/api/drive-image/<path:image_path>')
def serve_drive_image(image_path):
    """Redirige a imágenes de Google Drive"""
    try:
        if not os.path.exists(DRIVE_TREE_FILE):
            return jsonify({'error': 'Árbol de Drive no disponible'}), 503
        
        with open(DRIVE_TREE_FILE, 'r', encoding='utf-8') as f:
            drive_data = json.load(f)
        
        def find_file_in_tree(tree, target_path):
            if tree.get('type') == 'file' and tree.get('path') == target_path:
                return tree
            for child in tree.get('children', []):
                result = find_file_in_tree(child, target_path)
                if result:
                    return result
            return None
        
        file_info = find_file_in_tree(drive_data['structure'], image_path)
        
        if not file_info:
            return jsonify({'error': 'Imagen no encontrada en Drive'}), 404
        
        file_id = file_info.get('id')
        if file_id:
            download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
            return redirect(download_url)
        else:
            return jsonify({'error': 'URL de imagen no disponible'}), 404
        
    except Exception as e:
        logging.error(f"Error sirviendo imagen de Drive: {e}")
        return jsonify({'error': 'Error al cargar imagen'}), 500

# ==============================================================================
# 11. ENDPOINTS PARA EXCEL DESDE GOOGLE DRIVE (SOLO LECTURA - BAJO DEMANDA)
# ==============================================================================
@app.route('/api/drive-excel/years')
def get_drive_excel_years():
    """Obtiene años disponibles desde Google Drive - SOLO METADATOS"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        years = drive_excel_reader_readonly.get_available_years()
        return jsonify({
            'status': 'success',
            'data_type': 'metadata_only',
            'years': years
        })
    except Exception as e:
        logging.error(f"Error obteniendo años desde Drive: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-excel/years/<year>/months')
def get_drive_excel_months(year):
    """Obtiene meses disponibles para un año desde Drive - SOLO METADATOS"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        months = drive_excel_reader_readonly.get_available_months(year)
        return jsonify({
            'status': 'success', 
            'data_type': 'metadata_only',
            'year': year,
            'months': months
        })
    except Exception as e:
        logging.error(f"Error obteniendo meses para {year} desde Drive: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-excel/years/<year>/months/<month>/files')
def get_drive_excel_files(year, month):
    """Obtiene archivos Excel para fecha específica - SOLO METADATOS"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        files = drive_excel_reader_readonly.get_excel_files_by_date(year, month)
        return jsonify({
            'status': 'success',
            'data_type': 'metadata_only', 
            'year': year,
            'month': month,
            'files': files,
            'count': len(files)
        })
    except Exception as e:
        logging.error(f"Error obteniendo archivos para {year}/{month} desde Drive: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-excel/query/<year>/<month>')
def query_drive_excel_data(year, month):
    """CONSULTA DE SOLO LECTURA - Carga bajo demanda estricta desde Drive"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        filename = request.args.get('filename')
        query_type = request.args.get('query', 'basic_stats')
        
        result = drive_excel_reader_readonly.query_excel_data_readonly(
            year, month, filename, query_type
        )
        
        # Siempre devolver información de la fuente
        result['data_source'] = 'google_drive_readonly'
        result['requested_file'] = f"{year}/{month}" + (f"/{filename}" if filename else "")
        
        return jsonify(result)
            
    except Exception as e:
        logging.error(f"Error en consulta Drive para {year}/{month}: {e}")
        return jsonify({
            'status': 'error', 
            'message': str(e),
            'data_source': 'google_drive_readonly'
        }), 500

@app.route('/api/drive-excel/info/<year>/<month>')
def get_drive_excel_info(year, month):
    """Obtiene información de archivo Excel - SIN cargarlo"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        filename = request.args.get('filename')
        file_info = drive_excel_reader_readonly.get_excel_info(year, month, filename)
        
        if file_info:
            return jsonify({
                'status': 'success',
                'data_type': 'metadata_only',
                'file_info': file_info
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Archivo no encontrado en Drive'
            }), 404
            
    except Exception as e:
        logging.error(f"Error obteniendo info Drive para {year}/{month}: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-excel/stats')
def get_drive_excel_stats():
    """Estadísticas de uso del sistema Drive - SIN datos sensibles"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        stats = drive_excel_reader_readonly.get_stats()
        return jsonify({
            'status': 'success',
            'data_type': 'usage_stats',
            'stats': stats
        })
    except Exception as e:
        logging.error(f"Error obteniendo stats Drive: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-excel/cleanup')
def cleanup_drive_cache():
    """Limpia cache de Drive manualmente - Solo para mantenimiento"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        before_count = drive_excel_reader_readonly.get_loaded_files_count()
        drive_excel_reader_readonly.clear_all_cache()
        after_count = drive_excel_reader_readonly.get_loaded_files_count()
        
        return jsonify({
            'status': 'success',
            'message': f'Cache Drive limpiado: {before_count} -> {after_count} archivos',
            'cleaned_files': before_count - after_count
        })
    except Exception as e:
        logging.error(f"Error limpiando cache Drive: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# 12. ENDPOINTS PARA COMPARATIVAS DE PERÍODOS ACUMULATIVOS - CORREGIDOS
# ==============================================================================
@app.route('/api/drive-comparativas/periodos')
def get_comparativa_periodos():
    """Obtiene años y meses disponibles para comparativas - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        # Usar la instancia directa del reader
        years, meses_disponibles = obtener_años_desde_arbol_json()
        return jsonify({
            'status': 'success',
            'years': years,
            'meses_por_anio': meses_disponibles
        })
    except Exception as e:
        logging.error(f"Error obteniendo períodos para comparativas: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/comparar')
def comparar_periodos_acumulativos():
    """Compara dos períodos acumulativos desde Drive - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo1 = request.args.get('periodo1', '')
        periodo2 = request.args.get('periodo2', '')
        
        if not all([year, periodo1, periodo2]):
            return jsonify({
                'status': 'error', 
                'message': 'Se requieren year, periodo1 y periodo2'
            }), 400
        
        # Usar el método avanzado en lugar del básico
        resultado = drive_excel_comparator.comparar_periodos_avanzado(
            year, periodo1, periodo2, 'Todos', 
            ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum']
        )
        
        if resultado.get('status') == 'success':
            return jsonify(resultado)
        else:
            return jsonify(resultado), 400
            
    except Exception as e:
        logging.error(f"Error en comparativa de períodos: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/cn-resumen-comparativo')
def get_cn_resumen_comparativo():
    """Resumen CN comparativo entre períodos - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo1 = request.args.get('periodo1', '')
        periodo2 = request.args.get('periodo2', '')
        
        if not all([year, periodo1, periodo2]):
            return jsonify({
                'status': 'error', 
                'message': 'Se requieren year, periodo1 y periodo2'
            }), 400
        
        # Usar comparación avanzada para obtener datos completos
        resultado = drive_excel_comparator.comparar_periodos_avanzado(
            year, periodo1, periodo2, 'Todos',
            ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum']
        )
        
        if resultado.get('status') == 'success':
            # Extraer solo la información de CN para el resumen
            comparacion = resultado.get('comparacion', {})
            metricas_globales = comparacion.get('metricas_globales', {})
            metricas_principales = resultado.get('metricas_principales', {})
            
            resumen_cn = {
                'comparacion_general': {
                    'periodo1': periodo1,
                    'periodo2': periodo2,
                    'plazas_nuevas': metricas_principales.get('plazas_nuevas', 0),
                    'plazas_eliminadas': metricas_principales.get('plazas_eliminadas', 0),
                    'incremento_cn_total': metricas_principales.get('incremento_cn_total', 0)
                },
                'metricas_detalladas': metricas_globales,
                'resumen_cambios': metricas_principales.get('resumen_cambios', '')
            }
            
            return jsonify({
                'status': 'success',
                'year': year,
                'periodo1': periodo1,
                'periodo2': periodo2,
                'resumen_comparativo': resumen_cn
            })
        else:
            return jsonify({
                'status': 'error',
                'message': resultado.get('error', 'Error en la comparación')
            }), 400
            
    except Exception as e:
        logging.error(f"Error en resumen CN comparativo: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/top-estados-comparativo')
def get_top_estados_comparativo():
    """Top estados comparativo por métrica CN entre períodos - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo1 = request.args.get('periodo1', '')
        periodo2 = request.args.get('periodo2', '')
        metric = request.args.get('metric', 'CN_Tot_Acum')
        n = int(request.args.get('n', 5))
        
        if not all([year, periodo1, periodo2]):
            return jsonify({
                'status': 'error', 
                'message': 'Se requieren year, periodo1 y periodo2'
            }), 400
        
        # Usar comparación avanzada
        resultado = drive_excel_comparator.comparar_periodos_avanzado(
            year, periodo1, periodo2, 'Todos', [metric]
        )
        
        if resultado.get('status') == 'success':
            comparacion = resultado.get('comparacion', {})
            analisis_por_estado = comparacion.get('analisis_por_estado', {})
            
            # Calcular top estados por la métrica específica
            estados_con_metricas = []
            for estado, datos in analisis_por_estado.items():
                metricas_estado = datos.get('metricas', {})
                if metric in metricas_estado:
                    metrica_data = metricas_estado[metric]
                    estados_con_metricas.append({
                        'estado': estado,
                        'periodo1': metrica_data.get('periodo1', 0),
                        'periodo2': metrica_data.get('periodo2', 0),
                        'cambio': metrica_data.get('cambio', 0),
                        'porcentaje_cambio': metrica_data.get('porcentaje_cambio', 0)
                    })
            
            # Ordenar por cambio absoluto (mayor primero)
            estados_con_metricas.sort(key=lambda x: abs(x['cambio']), reverse=True)
            top_estados = estados_con_metricas[:n]
            
            return jsonify({
                'status': 'success',
                'year': year,
                'periodo1': periodo1,
                'periodo2': periodo2,
                'metric': metric,
                'top_comparativo': top_estados
            })
        else:
            return jsonify({
                'status': 'error',
                'message': resultado.get('error', 'Error en la comparación')
            }), 400
            
    except Exception as e:
        logging.error(f"Error en top estados comparativo: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/estadisticas-comparativas')
def get_estadisticas_comparativas():
    """Estadísticas comparativas generales entre períodos - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo1 = request.args.get('periodo1', '')
        periodo2 = request.args.get('periodo2', '')
        
        if not all([year, periodo1, periodo2]):
            return jsonify({
                'status': 'error', 
                'message': 'Se requieren year, periodo1 y periodo2'
            }), 400
        
        # Usar comparación avanzada
        resultado = drive_excel_comparator.comparar_periodos_avanzado(
            year, periodo1, periodo2, 'Todos'
        )
        
        if resultado.get('status') == 'success':
            comparacion = resultado.get('comparacion', {})
            metricas_principales = resultado.get('metricas_principales', {})
            
            estadisticas_comparativas = {
                'general': comparacion.get('resumen_general', {}),
                'analisis_plazas': comparacion.get('analisis_plazas', {}),
                'metricas_globales': comparacion.get('metricas_globales', {}),
                'analisis_por_estado': comparacion.get('analisis_por_estado', {}),
                'resumen_cambios': metricas_principales
            }
            
            return jsonify({
                'status': 'success',
                'year': year,
                'periodo1': periodo1,
                'periodo2': periodo2,
                'estadisticas_comparativas': estadisticas_comparativas
            })
        else:
            return jsonify({
                'status': 'error',
                'message': resultado.get('error', 'Error en la comparación')
            }), 400
            
    except Exception as e:
        logging.error(f"Error en estadísticas comparativas: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/analisis-tendencia')
def get_analisis_tendencia():
    """Análisis de tendencia entre múltiples períodos - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo_inicio = request.args.get('periodo_inicio', '01')
        periodo_fin = request.args.get('periodo_fin', '12')
        
        if not year:
            return jsonify({
                'status': 'error', 
                'message': 'Se requiere el parámetro year'
            }), 400
        
        # Obtener todos los meses disponibles para el año
        meses_disponibles = drive_excel_reader_readonly.get_available_months(year)
        if not meses_disponibles:
            return jsonify({
                'status': 'error',
                'message': f'No hay datos disponibles para el año {year}'
            }), 404
        
        # Filtrar meses en el rango solicitado
        meses_analisis = [mes for mes in meses_disponibles 
                          if periodo_inicio <= mes <= periodo_fin]
        
        if not meses_analisis:
            return jsonify({
                'status': 'error',
                'message': 'No hay meses en el rango especificado'
            }), 400
        
        # Analizar tendencia
        tendencia_data = []
        for mes in sorted(meses_analisis):
            df, info = drive_excel_reader_readonly.load_excel_strict(year, mes)
            if df is not None:
                total_plazas = len(df)
                
                # Calcular métricas CN
                metricas_cn = {}
                for col in ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum']:
                    if col in df.columns:
                        metricas_cn[col] = int(round(pd.to_numeric(df[col], errors='coerce').fillna(0).sum()))
                
                tendencia_data.append({
                    'mes': mes,
                    'nombre_mes': obtener_nombre_mes(mes),
                    'total_plazas': total_plazas,
                    'metricas_cn': metricas_cn,
                    'periodo': f"{obtener_nombre_mes(mes)} {year}"
                })
        
        return jsonify({
            'status': 'success',
            'year': year,
            'periodo_inicio': periodo_inicio,
            'periodo_fin': periodo_fin,
            'tendencia': tendencia_data
        })
            
    except Exception as e:
        logging.error(f"Error en análisis de tendencia: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# 13. ENDPOINTS ADICIONALES PARA CONSULTAS ESPECÍFICAS - CORREGIDOS
# ==============================================================================
@app.route('/api/drive-comparativas/consulta-plazas')
def consulta_plazas_especificas():
    """Consulta plazas específicas entre períodos - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo = request.args.get('periodo', '')
        clave_plaza = request.args.get('clave_plaza', '')
        
        if not all([year, periodo]):
            return jsonify({
                'status': 'error', 
                'message': 'Se requieren year y periodo'
            }), 400
        
        # Cargar el período solicitado
        df, info = drive_excel_reader_readonly.load_excel_strict(year, periodo)
        
        if df is None:
            return jsonify({
                'status': 'error',
                'message': f'No se pudo cargar el período {periodo}-{year}'
            }), 400
        
        # Filtrar por clave de plaza si se proporciona
        if clave_plaza and 'Clave_Plaza' in df.columns:
            resultado = df[df['Clave_Plaza'].astype(str).str.contains(clave_plaza, na=False)]
            datos = resultado.fillna('').to_dict('records')
        else:
            datos = df.head(100).fillna('').to_dict('records')  # Límite de 100 registros
        
        return jsonify({
            'status': 'success',
            'year': year,
            'periodo': periodo,
            'clave_plaza': clave_plaza,
            'total_resultados': len(datos),
            'datos': datos,
            'metadata': info.get('file_info', {})
        })
            
    except Exception as e:
        logging.error(f"Error en consulta de plazas: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/estadisticas-rapidas')
def get_estadisticas_rapidas():
    """Estadísticas rápidas de un período específico - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo = request.args.get('periodo', '')
        
        if not all([year, periodo]):
            return jsonify({
                'status': 'error', 
                'message': 'Se requieren year y periodo'
            }), 400
        
        # Usar la consulta de solo lectura para estadísticas básicas
        resultado = drive_excel_reader_readonly.query_excel_data_readonly(
            year, periodo, query_type='basic_stats'
        )
        
        if resultado.get('status') == 'success':
            return jsonify({
                'status': 'success',
                'year': year,
                'periodo': periodo,
                'estadisticas': {
                    'total_registros': resultado.get('total_rows', 0),
                    'total_columnas': resultado.get('total_columns', 0),
                    'columnas': resultado.get('column_names', []),
                    'muestra': resultado.get('sample_data', [])
                },
                'drive_file': resultado.get('drive_file', {})
            })
        else:
            return jsonify({
                'status': 'error',
                'message': resultado.get('error', 'Error desconocido')
            }), 400
            
    except Exception as e:
        logging.error(f"Error en estadísticas rápidas: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/status')
def get_status_comparativas():
    """Obtiene el estado del sistema de comparativas - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        stats_reader = drive_excel_reader_readonly.get_stats()
        
        # Obtener años disponibles
        years = drive_excel_reader_readonly.get_available_years()
        
        return jsonify({
            'status': 'success',
            'drive_reader': {
                'total_requests': stats_reader['total_requests'],
                'cache_hits': stats_reader['cache_hits'],
                'drive_downloads': stats_reader['drive_downloads'],
                'cache_hit_ratio': stats_reader['cache_hit_ratio'],
                'currently_loaded_files': stats_reader['currently_loaded_files'],
                'tree_loaded': stats_reader['tree_loaded']
            },
            'comparator': {
                'available': True,
                'description': 'DriveExcelComparator integrado y funcionando'
            },
            'datos_disponibles': {
                'total_años': len(years),
                'años': years
            },
            'system': {
                'timestamp': datetime.now().isoformat(),
                'status': 'operational'
            }
        })
            
    except Exception as e:
        logging.error(f"Error obteniendo status: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# 14. ENDPOINTS DE MANTENIMIENTO 
# ==============================================================================
@app.route('/api/drive-comparativas/limpiar-cache', methods=['POST'])
def limpiar_cache_comparativas():
    """Limpia el cache del sistema de comparativas - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        # Agregar método clear_all_cache si no existe
        if hasattr(drive_excel_reader_readonly, 'loaded_excels'):
            drive_excel_reader_readonly.loaded_excels.clear()
        
        return jsonify({
            'status': 'success',
            'message': 'Cache limpiado exitosamente',
            'timestamp': datetime.now().isoformat()
        })
            
    except Exception as e:
        logging.error(f"Error limpiando cache: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/recargar-arbol', methods=['POST'])
def recargar_arbol_comparativas():
    """Recarga el árbol de archivos desde el JSON - CORREGIDO"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        success = drive_excel_reader_readonly.load_tree()
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Árbol recargado exitosamente',
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Error al recargar el árbol'
            }), 500
            
    except Exception as e:
        logging.error(f"Error recargando árbol: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# 15. ENDPOINTS NUEVOS PARA COMPARATIVAS AVANZADAS
# ==============================================================================
@app.route('/api/drive-comparativas/comparar-avanzado')
def comparar_periodos_avanzado_unificado():
    """Compara dos períodos - ENDPOINT PRINCIPAL CORREGIDO"""
    try:                
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        # Obtener parámetros (soporta años iguales y diferentes)
        year1 = request.args.get('year1', '')
        year2 = request.args.get('year2', '')
        periodo1 = request.args.get('periodo1', '')
        periodo2 = request.args.get('periodo2', '')
        
        # Soporte para parámetros legacy (mismo año)
        if not year1:
            year1 = request.args.get('year', '')
        if not year2:
            year2 = year1  # Default al mismo año
        
        filtro_estado = request.args.get('filtro_estado', 'Todos')
        metricas = request.args.getlist('metricas')
        
        logging.info(f"📥 Parámetros recibidos: year1={year1}, periodo1={periodo1}, year2={year2}, periodo2={periodo2}")
        
        if not all([year1, periodo1, year2, periodo2]):
            return jsonify({
                'status': 'error', 
                'message': 'Se requieren year1, periodo1, year2 y periodo2'
            }), 400
        
        # Validar que los años existan
        years_disponibles = drive_excel_reader_readonly.get_available_years()
        
        if year1 not in years_disponibles:
            return jsonify({
                'status': 'error',
                'message': f'El año {year1} no está disponible',
                'available_years': years_disponibles
            }), 404
            
        if year2 not in years_disponibles:
            return jsonify({
                'status': 'error',
                'message': f'El año {year2} no está disponible', 
                'available_years': years_disponibles
            }), 404
        
        # Verificar meses disponibles
        meses_year1 = drive_excel_reader_readonly.get_available_months(year1)
        meses_year2 = drive_excel_reader_readonly.get_available_months(year2)
        
        if periodo1 not in meses_year1:
            return jsonify({
                'status': 'error',
                'message': f'El mes {periodo1} no está disponible para {year1}',
                'available_months': meses_year1
            }), 404
            
        if periodo2 not in meses_year2:
            return jsonify({
                'status': 'error',
                'message': f'El mes {periodo2} no está disponible para {year2}',
                'available_months': meses_year2
            }), 404
        
        logging.info(f"🔍 Iniciando comparativa unificada: {year1}-{periodo1} vs {year2}-{periodo2}")
        
        # Usar métricas por defecto si no se especifican
        if not metricas:
            metricas = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum', 'Situación']
        
        # Determinar si son años diferentes
        if year1 != year2:
            logging.info("🔄 Modo: Comparación entre años diferentes")
            resultado = drive_excel_comparator.comparar_periodos_avanzado_con_años_diferentes(
                year1, periodo1, year2, periodo2, filtro_estado, metricas
            )
        else:
            logging.info("🔄 Modo: Comparación en el mismo año")
            resultado = drive_excel_comparator.comparar_periodos_avanzado(
                year1, periodo1, periodo2, filtro_estado, metricas
            )
        
        if resultado.get('status') == 'success':
            logging.info("✅ Comparativa completada exitosamente")
            
            # Asegurar que los datos sean serializables - CORREGIDO
            try:
                # Primero intentar serialización directa
                json.dumps(resultado)
                resultado_serializable = resultado
                logging.info("✅ Serialización directa exitosa")
            except (TypeError, ValueError) as e:
                logging.warning(f"⚠️ Necesita serialización especial: {e}")
                # Usar serialización segura
                resultado_serializable = safe_json_serialize(resultado)
                logging.info("✅ Serialización segura completada")
            
            return jsonify(resultado_serializable)
        else:
            logging.error(f"❌ Error en comparativa: {resultado.get('error')}")
            return jsonify({
                'status': 'error',
                'message': resultado.get('error', 'Error desconocido en la comparación')
            }), 400
            
    except Exception as e:
        logging.error(f"❌ ERROR CRÍTICO en comparativa avanzada: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        
        return jsonify({
            'status': 'error', 
            'message': f'Error interno del servidor: {str(e)}'
        }), 500

@app.route('/api/drive-comparativas/estados-disponibles')
def estados_disponibles_comparativas():
    """Obtiene estados disponibles para un período específico"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo = request.args.get('periodo', '')
        
        if not year or not periodo:
            return jsonify({'error': 'Se requieren year y periodo'}), 400
        
        estados = drive_excel_comparator.obtener_estados_disponibles(year, periodo)
        return jsonify({'estados': estados})
        
    except Exception as e:
        logging.error(f"Error obteniendo estados disponibles: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/drive-comparativas/metricas-disponibles')
def metricas_disponibles_comparativas():
    """Obtiene métricas disponibles para un período específico"""
    try:
        if not DRIVE_MODULES_AVAILABLE:
            return jsonify({'error': 'Módulos de Drive no disponibles'}), 503
        
        year = request.args.get('year', '')
        periodo = request.args.get('periodo', '')
        
        if not year or not periodo:
            return jsonify({'error': 'Se requieren year y periodo'}), 400
        
        metricas = drive_excel_comparator.obtener_metricas_disponibles(year, periodo)
        return jsonify({'metricas': metricas})
        
    except Exception as e:
        logging.error(f"Error obteniendo métricas disponibles: {e}")
        return jsonify({'error': str(e)}), 500

# ==============================================================================
# 16. ENDPOINTS PARA SERIALIZACIÓN SEGURA
# ==============================================================================
@app.route('/api/safe-serialize-test')
def safe_serialize_test():
    """Endpoint de prueba para serialización segura"""
    try:
        # Datos de prueba con diferentes tipos
        test_data = {
            'string': 'Texto normal',
            'integer': 42,
            'float': 3.14159,
            'nan': float('nan'),
            'inf': float('inf'),
            'none': None,
            'list': [1, 2, 3, float('nan')],
            'dict': {'key': 'value', 'nan': float('nan')},
            'timestamp': datetime.now()
        }
        
        # Serializar usando la función del módulo
        serialized = safe_json_serialize(test_data)
        
        return jsonify({
            'status': 'success',
            'original': test_data,
            'serialized': serialized
        })
        
    except Exception as e:
        logging.error(f"Error en serialización segura: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# 17. ENDPOINTS PARA INFORMACIÓN DEL SISTEMA
# ==============================================================================
@app.route('/api/system/info')
def get_system_info():
    """Obtiene información completa del sistema"""
    try:
        # Información del árbol de Drive
        if DRIVE_MODULES_AVAILABLE:
            drive_stats = drive_excel_reader_readonly.get_stats()
            years = drive_excel_reader_readonly.get_available_years()
            total_months = sum(len(drive_excel_reader_readonly.get_available_months(year)) for year in years)
        else:
            drive_stats = {'tree_loaded': False, 'cache_hit_ratio': 0}
            years = []
            total_months = 0
        
        # Información del cache local
        df = dataframe_cache.get_dataframe()
        local_excel_info = {
            'exists': not df.empty,
            'total_plazas': len(df) if not df.empty else 0,
            'fuente_datos': 'parquet' if os.path.exists(Config.PARQUET_PATH) else 'excel_emergencia'
        }
        
        # Verificar archivos fuente
        archivos = {
            'parquet': os.path.exists(Config.PARQUET_PATH),
            'excel': os.path.exists(Config.EXCEL_PATH)
        }
        
        #  USAR json_response en lugar de jsonify para mayor velocidad
        return json_response({
            'status': 'success',
            'system': {
                'timestamp': datetime.now().isoformat(),
                'python_version': os.sys.version,
                'platform': os.sys.platform
            },
            'drive_system': {
                'available': DRIVE_MODULES_AVAILABLE,
                'tree_loaded': drive_stats['tree_loaded'],
                'total_years': len(years),
                'total_months': total_months,
                'cache_performance': {
                    'hit_ratio': drive_stats['cache_hit_ratio'],
                    'total_requests': drive_stats.get('total_requests', 0),
                    'cache_hits': drive_stats.get('cache_hits', 0),
                    'drive_downloads': drive_stats.get('drive_downloads', 0)
                }
            },
            'local_cache': {
                'dataframe_en_memoria': not df.empty,
                'registros': len(df),
                'columnas': len(df.columns) if not df.empty else 0,
                'ultimo_mes_registros': len(dataframe_cache.get_ultimo_mes()) if not df.empty else 0,
                'estados_cacheados': len(dataframe_cache.get_estados_cache()) if not df.empty else 0,
                'fuente_actual': local_excel_info['fuente_datos']
            },
            'archivos_disco': archivos,
            'available_years': years
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo información del sistema: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# 18. ENDPOINT PARA OBTENER TODOS LOS DATOS DISPONIBLES
# ==============================================================================
@app.route('/api/drive-comparativas/datos-completos')
def get_datos_completos():
    """Obtiene todos los datos disponibles del sistema"""
    try:
        if DRIVE_MODULES_AVAILABLE:
            años, meses_por_año = obtener_años_desde_arbol_json()
        else:
            años, meses_por_año = [], {}
        
        # Información del cache local
        df = dataframe_cache.get_dataframe()
        
        datos_completos = {
            'años_disponibles': años,
            'meses_por_año': meses_por_año,
            'drive_modules_available': DRIVE_MODULES_AVAILABLE,
            'cache_local': {
                'registros': len(df),
                'columnas': list(df.columns) if not df.empty else [],
                'muestra': df.head(5).fillna('').to_dict('records') if not df.empty else []
            },
            'ultima_actualizacion': datetime.now().isoformat()
        }
        
        if DRIVE_MODULES_AVAILABLE:
            datos_completos['estadisticas_sistema'] = drive_excel_reader_readonly.get_stats()
        
        return jsonify({
            'status': 'success',
            'datos': datos_completos
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo datos completos: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/drive-comparativas/buscar-estados')
def buscar_estados_comparativa():
    """Busca estados para la barra de búsqueda - SOLO ESTADOS"""
    try:
        query = request.args.get('q', '').strip().lower()
        
        if not query:
            return jsonify({
                'status': 'success',
                'query': query,
                'resultados': [],
                'total_resultados': 0
            })
        
        # Usar los datos locales de plazas para buscar estados
        df = dataframe_cache.get_dataframe()
        
        if df.empty:
            return jsonify({
                'status': 'error',
                'message': 'No hay datos disponibles'
            }), 503
        
        # Obtener estados únicos que coincidan con la búsqueda
        if Config.COLUMNA_ESTADO in df.columns:
            estados_unicos = df[Config.COLUMNA_ESTADO].dropna().unique()
            
            resultados = []
            for estado in estados_unicos:
                estado_str = str(estado).strip()
                estado_lower = estado_str.lower()
                
                if query in estado_lower:
                    match_type = 'exact' if query == estado_lower else 'partial'
                    resultados.append({
                        'nombre': estado_str,
                        'match_type': match_type
                    })
            
            # Ordenar: exact matches primero, luego alfabéticamente
            resultados.sort(key=lambda x: (x['match_type'] != 'exact', x['nombre']))
            
            return jsonify({
                'status': 'success',
                'query': query,
                'resultados': resultados,
                'total_resultados': len(resultados)
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Columna de estados no disponible'
            }), 400
            
    except Exception as e:
        logging.error(f"Error buscando estados: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
    
# ==============================================================================
# NUEVO ENDPOINT: HISTORIAL DE UNA PLAZA (Para tablas interactivas)
# ==============================================================================
@app.route('/api/plaza-historial')
def get_plaza_historial():
    """Devuelve el historial completo de una plaza para selectores de fecha."""
    try:
        clave_busqueda = request.args.get('clave', '').strip().upper()
        
        if not clave_busqueda:
            return jsonify([])

        df = dataframe_cache.get_dataframe()
        
        if df.empty:
            return jsonify([])

        # 1. Identificar columna clave
        columna_clave = 'Clave_Plaza'
        if columna_clave not in df.columns:
            mapeo = dataframe_cache.get_mapeo_columnas()
            columna_clave = mapeo.get('CLAVE_PLAZA')
            if not columna_clave:
                return jsonify([])

        # 2. Filtrar todas las filas de esa plaza (Histórico completo)
        mask = df[columna_clave].astype(str).str.strip().str.upper() == clave_busqueda
        df_historial = df[mask].copy()

        if df_historial.empty:
            return jsonify([])

        # 3. Ordenar por Año y Mes (Descendente: más reciente primero)
        if Config.COLUMNA_ANO in df_historial.columns and Config.COLUMNA_CVE_MES in df_historial.columns:
            df_historial['__sort_anio'] = pd.to_numeric(df_historial[Config.COLUMNA_ANO], errors='coerce').fillna(0)
            df_historial['__sort_mes'] = pd.to_numeric(df_historial[Config.COLUMNA_CVE_MES], errors='coerce').fillna(0)
            df_historial = df_historial.sort_values(by=['__sort_anio', '__sort_mes'], ascending=[False, False])

        # 4. Seleccionar columnas relevantes para la tabla de Atención/Productividad
        cols_interes = [
            'Año', 'Cve-mes', 'Mes',
            'Inc_Inicial', 'Inc_Prim', 'Inc_Sec', 'Inc_Total',
            'Aten_Inicial', 'Aten_Prim', 'Aten_Sec', 'Aten_Total',
            'Exámenes aplicados',
            'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum',
            'Cert_Emitidos',
            'Tec_Doc', 'Nom_PVS_1', 'Nom_PVS_2'
        ]
        
        # Mapear nombres reales del Excel si usan alias
        mapeo_cols = dataframe_cache.get_mapeo_columnas()
        cols_finales = []
        for c in cols_interes:
            real = obtener_nombre_columna_seguro(c, mapeo_cols, df)
            if real: 
                cols_finales.append(real)
            
        # 5. Convertir a lista de diccionarios limpia
        historial_data = []
        for _, row in df_historial.iterrows():
            row_dict = row.to_dict()
            clean_dict = convertir_a_serializable(row_dict)
            historial_data.append(clean_dict)

        return jsonify(historial_data)

    except Exception as e:
        logging.error(f"Error obteniendo historial: {e}")
        return jsonify([])

# ==============================================================================
# CORRECCIÓN EN APP.PY - Función get_metricas_por_estado (DEFINIDA UNA VEZ)
# ==============================================================================
@app.route('/api/metricas-por-estado/<estado>')
def get_metricas_por_estado(estado):
    """
    Devuelve las métricas de plazas y el acumulado por municipios.
    Estructura JSON: { "plazas": [...], "municipios": [...] }
    """
    try:
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_actual.empty:
            return jsonify({'plazas': [], 'municipios': []})

        # 1. Filtrar por Estado
        estado_busqueda = normalizar_texto(estado)
        col_estado_norm = f"normalized_{Config.COLUMNA_ESTADO.lower()}"
        
        if col_estado_norm in df_actual.columns:
            mask = df_actual[col_estado_norm] == estado_busqueda
        else:
            mask = df_actual[Config.COLUMNA_ESTADO].astype(str).apply(normalizar_texto) == estado_busqueda
            
        df_estado = df_actual[mask].copy()

        if df_estado.empty:
            return jsonify({'plazas': [], 'municipios': []})

        # 2. Definir columnas numéricas para métricas
        columnas_metricas = [
            'Aten_Inicial', 'Aten_Prim', 'Aten_Sec', 'Aten_Total', 
            'Exámenes aplicados',
            'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum',
            'Cert_Emitidos'
        ]
        
        # Columnas de identificación
        columnas_info = ['Clave_Plaza', 'Nombre_PC', 'Municipio']
        
        mapeo = dataframe_cache.get_mapeo_columnas()
        
        datos_plazas = []

        # 3. Procesar datos de PLAZAS (Iteración fila por fila)
        for _, row in df_estado.iterrows():
            fila_dict = {}
            
            # Procesar identificadores
            for col_js in columnas_info:
                col_excel = mapeo.get(Config.obtener_clave_por_nombre_columna(col_js)) or col_js
                val = row.get(col_excel)
                fila_dict[col_js] = str(val).strip() if pd.notna(val) else "N/D"

            # Procesar métricas numéricas
            for col_js in columnas_metricas:
                col_excel = mapeo.get(Config.obtener_clave_por_nombre_columna(col_js)) or col_js
                val = row.get(col_excel)
                try:
                    fila_dict[col_js] = int(float(val)) if pd.notna(val) else 0
                except (ValueError, TypeError):
                    fila_dict[col_js] = 0
            
            datos_plazas.append(fila_dict)

        # 4. Calcular Agrupación por MUNICIPIOS
        municipios_dict = {}

        for plaza in datos_plazas:
            nom_muni = plaza.get('Municipio', 'Sin Municipio')
            
            if nom_muni not in municipios_dict:
                municipios_dict[nom_muni] = {
                    'Municipio': nom_muni,
                    # Inicializar contadores en 0
                    **{k: 0 for k in columnas_metricas}
                }
            
            # Sumar métricas
            for k in columnas_metricas:
                municipios_dict[nom_muni][k] += plaza.get(k, 0)

        datos_municipios = list(municipios_dict.values())

        # 5. Retornar estructura correcta para el Frontend
        return jsonify({
            'plazas': datos_plazas,
            'municipios': datos_municipios
        })

    except Exception as e:
        logging.error(f"Error en métricas por estado: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return jsonify({'plazas': [], 'municipios': []}), 500

# ==============================================================================
# 🌟 NUEVA SECCIÓN: MAPA BLINDADO Y AUTOMATIZADO 🌟
# ==============================================================================

# 1. Ruta para ver el mapa (Carga HTML vacío, ahorra RAM)
@app.route('/ver-mapa')
def vista_mapa():
    """Renderiza la plantilla del mapa con cluster."""
    return render_template('mapa_cluster.html')

# 2. Endpoint "Ping" de Versión (Para caché automática)
@app.route('/api/version-coordenadas')
def check_version():
    """Devuelve la fecha de modificación del archivo JSON para invalidar caché."""
    archivo = Config.ARCHIVO_COORDENADAS
    
    if not os.path.exists(archivo):
        return jsonify({'version': None}), 404
        
    try:
        # Timestamp de la última modificación (float)
        timestamp_modificacion = os.path.getmtime(archivo)
        return jsonify({'version': timestamp_modificacion})
    except Exception as e:
        logging.error(f"Error obteniendo versión de coordenadas: {e}")
        return jsonify({'error': 'Error interno'}), 500

# 3. Endpoint "Lazy" de Datos (Carga pesada solo bajo demanda)
@app.route('/api/coordenadas-lazy')
def get_coordenadas_lazy():
    """Lee el JSON de disco solo cuando se solicita."""
    archivo = Config.ARCHIVO_COORDENADAS
    
    # Blindaje 1: Existencia
    if not os.path.exists(archivo):
        logging.warning(f"⚠️ Archivo de coordenadas no encontrado: {archivo}")
        return jsonify({'error': 'Archivo de coordenadas no disponible', 'datos': []}), 404

    try:
        # Blindaje 2: Lectura y Parseo
        with open(archivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Opcional: Validar que sea lista
        if not isinstance(data, list):
            raise ValueError("El JSON no es una lista de registros")
            
        return jsonify(data)

    except json.JSONDecodeError:
        logging.error(f"❌ JSON corrupto en {archivo}")
        return jsonify({'error': 'El archivo de coordenadas está dañado', 'datos': []}), 500
    except Exception as e:
        logging.error(f"❌ Error leyendo coordenadas: {e}")
        return jsonify({'error': str(e), 'datos': []}), 500

# ==============================================================================
# 19. ENDPOINT PARA MAPA SEGURO - OPTIMIZADO CON CACHE
# ==============================================================================
# Cache LRU para el mapa
@lru_cache(maxsize=5)
def get_coordenadas_cache():
    """Cache para coordenadas del mapa"""
    try:
        archivo = Config.ARCHIVO_COORDENADAS
        if not os.path.exists(archivo):
            return None
        
        with open(archivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Reducir precisión para reducir payload
        for item in data:
            if 'lat' in item:
                item['lat'] = round(float(item['lat']), 6)
            if 'lng' in item:
                item['lng'] = round(float(item['lng']), 6)
        
        return data[:1000]  # Limitar a 1000 registros para el mapa
        
    except Exception as e:
        logging.error(f"Error cargando coordenadas cache: {e}")
        return None

@app.route('/api/mapa/coordenadas-optimizadas')
def get_coordenadas_optimizadas():
    """Devuelve coordenadas optimizadas para el mapa con cache"""
    try:
        cached_data = get_coordenadas_cache()
        if cached_data is None:
            return jsonify({'error': 'Datos no disponibles', 'datos': []}), 404
        
        #  USAR json_response en lugar de jsonify para mayor velocidad
        return json_response({
            'status': 'success',
            'total_plazas': len(cached_data),
            'plazas': cached_data,
            'cached': True
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo coordenadas optimizadas: {e}")
        return jsonify({'error': str(e), 'datos': []}), 500

@app.route('/api/mapa/seguro')
def mapa_seguro_endpoint():
    """
    Endpoint seguro para operaciones del mapa.
    Mueve toda la lógica crítica del frontend al backend.
    """
    try:
        action = request.args.get('action')
        
        if action == 'cercanos':
            return handle_cercanos()
        elif action == 'calcular-distancia':
            return handle_calcular_distancia()
        elif action == 'ruta':
            return handle_ruta()
        elif action == 'buscar':
            return handle_buscar()
        elif action == 'filtro-estados':
            return handle_filtro_estados()
        else:
            return jsonify({
                'status': 'error',
                'message': 'Acción no válida'
            }), 400
            
    except Exception as e:
        logging.error(f"Error en endpoint mapa seguro: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

def handle_cercanos():
    """Calcula las plazas más cercanas a una ubicación."""
    lat = request.args.get('lat', type=float)
    lng = request.args.get('lng', type=float)
    distancia_max = request.args.get('distancia_max', 50, type=float)  # km por defecto
    limite = request.args.get('limite', 10, type=int)
    
    if lat is None or lng is None:
        return jsonify({
            'status': 'error',
            'message': 'Se requieren coordenadas (lat, lng)'
        }), 400
    
    df_actual = dataframe_cache.get_ultimo_mes()
    
    if df_actual.empty:
        return jsonify({
            'status': 'error',
            'message': 'No hay datos de plazas disponibles'
        }), 503
    
    # Inicializar mapeo de columnas
    mapeo = dataframe_cache.get_mapeo_columnas()
    
    # Calcular distancias
    resultados = []
    for _, plaza in df_actual.iterrows():
        try:
            # Obtener coordenadas de la plaza
            plaza_lat = obtener_valor_seguro(plaza, 'LATITUD', mapeo)
            plaza_lng = obtener_valor_seguro(plaza, 'LONGITUD', mapeo)
            
            if plaza_lat is None or plaza_lng is None:
                continue
                
            # Calcular distancia
            distancia = calcular_distancia_km(lat, lng, float(plaza_lat), float(plaza_lng))
            
            if distancia <= distancia_max:
                clave = obtener_valor_seguro(plaza, 'CLAVE_PLAZA', mapeo, '')
                nombre = obtener_valor_seguro(plaza, 'NOMBRE_PC', mapeo, '')
                estado = obtener_valor_seguro(plaza, 'ESTADO', mapeo, '')
                municipio = obtener_valor_seguro(plaza, 'MUNICIPIO', mapeo, '')
                
                resultados.append({
                    'clave': clave,
                    'nombre': nombre,
                    'estado': estado,
                    'municipio': municipio,
                    'lat': plaza_lat,
                    'lng': plaza_lng,
                    'distancia_km': round(distancia, 2),
                    'distancia_formateada': f"{distancia:.1f} km"
                })
        except Exception as e:
            logging.warning(f"Error procesando plaza: {e}")
            continue
    
    # Ordenar por distancia
    resultados.sort(key=lambda x: x['distancia_km'])
    
    # Limitar resultados
    resultados = resultados[:limite]
    
    return jsonify({
        'status': 'success',
        'ubicacion_usuario': {'lat': lat, 'lng': lng},
        'distancia_maxima_km': distancia_max,
        'total_encontradas': len(resultados),
        'plazas_cercanas': resultados
    })

def handle_calcular_distancia():
    """Calcula distancia entre dos puntos."""
    lat1 = request.args.get('lat1', type=float)
    lng1 = request.args.get('lng1', type=float)
    lat2 = request.args.get('lat2', type=float)
    lng2 = request.args.get('lng2', type=float)
    
    if None in [lat1, lng1, lat2, lng2]:
        return jsonify({
            'status': 'error',
            'message': 'Se requieren todas las coordenadas'
        }), 400
    
    distancia = calcular_distancia_km(lat1, lng1, lat2, lng2)
    
    return jsonify({
        'status': 'success',
        'punto1': {'lat': lat1, 'lng': lng1},
        'punto2': {'lat': lat2, 'lng': lng2},
        'distancia_km': round(distancia, 2),
        'distancia_metros': round(distancia * 1000),
        'distancia_formateada': f"{distancia:.1f} km"
    })

def handle_ruta():
    """Genera información para rutas de navegación."""
    origen_lat = request.args.get('origen_lat', type=float)
    origen_lng = request.args.get('origen_lng', type=float)
    destino_lat = request.args.get('destino_lat', type=float)
    destino_lng = request.args.get('destino_lng', type=float)
    destino_nombre = request.args.get('destino_nombre', 'Destino')
    
    if None in [origen_lat, origen_lng, destino_lat, destino_lng]:
        return jsonify({
            'status': 'error',
            'message': 'Se requieren coordenadas de origen y destino'
        }), 400
    
    # Calcular distancia
    distancia = calcular_distancia_km(origen_lat, origen_lng, destino_lat, destino_lng)
    
    # Generar URLs de navegación (sin exponer lógica en frontend)
    urls = {
        'google_maps': generar_url_google_maps(origen_lat, origen_lng, destino_lat, destino_lng, destino_nombre),
        'waze': generar_url_waze(destino_lat, destino_lng, destino_nombre),
        'google_maps_directo': f"https://www.google.com/maps/search/?api=1&query={destino_lat},{destino_lng}",
        'waze_directo': f"https://www.waze.com/ul?ll={destino_lat},{destino_lng}&navigate=yes"
    }
    
    return jsonify({
        'status': 'success',
        'origen': {'lat': origen_lat, 'lng': origen_lng},
        'destino': {
            'lat': destino_lat,
            'lng': destino_lng,
            'nombre': destino_nombre
        },
        'distancia': {
            'km': round(distancia, 2),
            'metros': round(distancia * 1000),
            'formateada': f"{distancia:.1f} km"
        },
        'urls_navegacion': urls,
        'estimacion_tiempo': estimar_tiempo_viaje(distancia)
    })

def handle_buscar():
    """Búsqueda avanzada de plazas."""
    query = request.args.get('q', '').strip()
    tipo = request.args.get('tipo', 'todas')  # 'exacta', 'parcial', 'todas'
    limite = request.args.get('limite', 20, type=int)
    
    if not query or len(query) < 2:
        return jsonify({
            'status': 'error',
            'message': 'Término de búsqueda demasiado corto'
        }), 400
    
    df_actual = dataframe_cache.get_ultimo_mes()
    
    if df_actual.empty:
        return jsonify({
            'status': 'error',
            'message': 'No hay datos disponibles'
        }), 503
    
    mapeo = dataframe_cache.get_mapeo_columnas()
    
    # Normalizar query
    query_normalizada = normalizar_texto(query)
    
    resultados = []
    for _, plaza in df_actual.iterrows():
        try:
            # Obtener valores
            clave = obtener_valor_seguro(plaza, 'CLAVE_PLAZA', mapeo, '')
            nombre = obtener_valor_seguro(plaza, 'NOMBRE_PC', mapeo, '')
            estado = obtener_valor_seguro(plaza, 'ESTADO', mapeo, '')
            municipio = obtener_valor_seguro(plaza, 'MUNICIPIO', mapeo, '')
            localidad = obtener_valor_seguro(plaza, 'LOCALIDAD', mapeo, '')
            lat = obtener_valor_seguro(plaza, 'LATITUD', mapeo)
            lng = obtener_valor_seguro(plaza, 'LONGITUD', mapeo)
            
            # Normalizar valores para búsqueda
            clave_norm = normalizar_texto(clave)
            estado_norm = normalizar_texto(estado)
            municipio_norm = normalizar_texto(municipio)
            localidad_norm = normalizar_texto(localidad)
            nombre_norm = normalizar_texto(nombre)
            
            # Determinar tipo de coincidencia
            coincidencia = None
            score = 0
            
            # Búsqueda exacta por clave
            if clave_norm == query_normalizada:
                coincidencia = 'exacta'
                score = 100
            # Búsqueda parcial por clave
            elif query_normalizada in clave_norm:
                coincidencia = 'clave_parcial'
                score = 90 - (len(clave_norm) - len(query_normalizada)) * 2
            # Búsqueda por estado
            elif query_normalizada in estado_norm:
                coincidencia = 'estado'
                score = 80
            # Búsqueda por municipio
            elif query_normalizada in municipio_norm:
                coincidencia = 'municipio'
                score = 70
            # Búsqueda por localidad
            elif query_normalizada in localidad_norm:
                coincidencia = 'localidad'
                score = 60
            # Búsqueda por nombre
            elif query_normalizada in nombre_norm:
                coincidencia = 'nombre'
                score = 50
            
            # Si encontramos coincidencia según el tipo solicitado
            if coincidencia and (tipo == 'todas' or tipo == coincidencia):
                resultados.append({
                    'clave': clave,
                    'nombre': nombre,
                    'estado': estado,
                    'municipio': municipio,
                    'localidad': localidad,
                    'lat': lat,
                    'lng': lng,
                    'tipo_coincidencia': coincidencia,
                    'score': score,
                    'highlight': generar_highlight(query, clave, nombre, estado, municipio)
                })
                
        except Exception as e:
            logging.warning(f"Error en búsqueda de plaza: {e}")
            continue
    
    # Ordenar por score
    resultados.sort(key=lambda x: x['score'], reverse=True)
    
    # Limitar resultados
    resultados = resultados[:limite]
    
    return jsonify({
        'status': 'success',
        'query': query,
        'tipo_busqueda': tipo,
        'total_encontradas': len(resultados),
        'resultados': resultados
    })

def handle_filtro_estados():
    """Obtiene estados disponibles para filtro."""
    df_actual = dataframe_cache.get_ultimo_mes()
    
    if df_actual.empty:
        return jsonify({
            'status': 'error',
            'message': 'No hay datos disponibles'
        }), 503
    
    # Obtener estados únicos con conteo
    if Config.COLUMNA_ESTADO in df_actual.columns:
        estados_conteo = df_actual[Config.COLUMNA_ESTADO].value_counts().to_dict()
        
        estados = []
        for estado, cantidad in estados_conteo.items():
            if pd.isna(estado):
                continue
                
            estados.append({
                'nombre': str(estado),
                'cantidad': int(cantidad),
                'codigo': normalizar_texto(str(estado))[:10]  # Para usar como ID
            })
        
        # Ordenar alfabéticamente
        estados.sort(key=lambda x: x['nombre'])
        
        return jsonify({
            'status': 'success',
            'total_estados': len(estados),
            'estados': estados
        })
    
    return jsonify({
        'status': 'error',
        'message': 'Columna de estados no disponible'
    }), 400

# ==============================================================================
# FUNCIONES AUXILIARES PARA EL ENDPOINT DEL MAPA
# ==============================================================================

def calcular_distancia_km(lat1, lng1, lat2, lng2):
    """Calcula distancia en kilómetros entre dos coordenadas."""
    # Radio de la Tierra en kilómetros
    R = 6371.0
    
    # Convertir grados a radianes
    lat1_rad = math.radians(lat1)
    lng1_rad = math.radians(lng1)
    lat2_rad = math.radians(lat2)
    lng2_rad = math.radians(lng2)
    
    # Diferencia de coordenadas
    dlat = lat2_rad - lat1_rad
    dlng = lng2_rad - lng1_rad
    
    # Fórmula de Haversine
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c

def generar_url_google_maps(origen_lat, origen_lng, destino_lat, destino_lng, destino_nombre):
    """Genera URL de Google Maps para ruta."""
    nombre_codificado = destino_nombre.replace(' ', '+')
    return f"https://www.google.com/maps/dir/?api=1&origin={origen_lat},{origen_lng}&destination={destino_lat},{destino_lng}&destination_place_id={nombre_codificado}&travelmode=driving"

def generar_url_waze(destino_lat, destino_lng, destino_nombre):
    """Genera URL de Waze para navegación."""
    nombre_codificado = destino_nombre.replace(' ', '+')
    return f"https://www.waze.com/ul?ll={destino_lat},{destino_lng}&navigate=yes&to={nombre_codificado}"

def estimar_tiempo_viaje(distancia_km):
    """Estima tiempo de viaje basado en distancia."""
    # Velocidad promedio en ciudad: 40 km/h
    tiempo_horas = distancia_km / 40
    horas = int(tiempo_horas)
    minutos = int((tiempo_horas - horas) * 60)
    
    if horas > 0:
        return f"{horas}h {minutos}min"
    else:
        return f"{minutos} min"

def generar_highlight(query, clave, nombre, estado, municipio):
    """Genera texto resaltado para resultados de búsqueda."""
    textos = []
    query_lower = query.lower()
    
    # Buscar coincidencias en cada campo
    if query_lower in clave.lower():
        textos.append(f"Clave: <strong>{clave}</strong>")
    else:
        textos.append(f"Clave: {clave}")
    
    if query_lower in nombre.lower():
        textos.append(f"Nombre: <strong>{nombre}</strong>")
    else:
        textos.append(f"Nombre: {nombre}")
    
    if query_lower in estado.lower():
        textos.append(f"Estado: <strong>{estado}</strong>")
    else:
        textos.append(f"Estado: {estado}")
    
    if query_lower in municipio.lower():
        textos.append(f"Municipio: <strong>{municipio}</strong>")
    else:
        textos.append(f"Municipio: {municipio}")
    
    return " | ".join(textos)

# ==============================================================================
# ENDPOINT PARA OBTENER TODAS LAS COORDENADAS (optimizado)
# ==============================================================================
@app.route('/api/mapa/coordenadas-completas')
def get_coordenadas_completas():
    """Devuelve solo datos JSON de plazas, sin HTML en popups."""
    try:
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_actual.empty:
            return jsonify({'datos': []}), 200
        
        mapeo = dataframe_cache.get_mapeo_columnas()
        
        plazas = []
        for _, plaza in df_actual.iterrows():
            try:
                lat = obtener_valor_seguro(plaza, 'LATITUD', mapeo)
                lng = obtener_valor_seguro(plaza, 'LONGITUD', mapeo)
                
                # Solo incluir plazas con coordenadas válidas
                if lat is not None and lng is not None:
                    plazas.append({
                        'clave': obtener_valor_seguro(plaza, 'CLAVE_PLAZA', mapeo, ''),
                        'nombre': obtener_valor_seguro(plaza, 'NOMBRE_PC', mapeo, ''),
                        'estado': obtener_valor_seguro(plaza, 'ESTADO', mapeo, ''),
                        'municipio': obtener_valor_seguro(plaza, 'MUNICIPIO', mapeo, ''),
                        'localidad': obtener_valor_seguro(plaza, 'LOCALIDAD', mapeo, ''),
                        'lat': float(lat),
                        'lng': float(lng),
                        'situacion': obtener_valor_seguro(plaza, 'SITUACION', mapeo, '')
                    })
            except Exception as e:
                logging.debug(f"Error procesando plaza para mapa: {e}")
                continue
        
        # Estadísticas simples
        estados = {}
        for plaza in plazas:
            estado = plaza['estado']
            if estado not in estados:
                estados[estado] = 0
            estados[estado] += 1
        
        #  USAR json_response en lugar de jsonify para mayor velocidad
        return json_response({
            'status': 'success',
            'total_plazas': len(plazas),
            'total_estados': len(estados),
            'estadisticas_estados': estados,
            'plazas': plazas  # Solo datos puros
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo coordenadas completas: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'plazas': []
        }), 500

# ==============================================================================
# ENDPOINTS ADICIONALES PARA FUNCIONALIDADES COMPLETAS
# ==============================================================================

@app.route('/api/mapa/ubicar-plaza-cercana')
def ubicar_plaza_cercana():
    """Encuentra la plaza más cercana a unas coordenadas."""
    try:
        lat = request.args.get('lat', type=float)
        lng = request.args.get('lng', type=float)
        
        if lat is None or lng is None:
            return jsonify({
                'status': 'error',
                'message': 'Se requieren coordenadas (lat, lng)'
            }), 400
        
        df_actual = dataframe_cache.get_ultimo_mes()
        
        if df_actual.empty:
            return jsonify({
                'status': 'error',
                'message': 'No hay datos de plazas disponibles'
            }), 503
        
        mapeo = dataframe_cache.get_mapeo_columnas()
        
        plaza_mas_cercana = None
        distancia_minima = float('inf')
        
        for _, plaza in df_actual.iterrows():
            try:
                plaza_lat = obtener_valor_seguro(plaza, 'LATITUD', mapeo)
                plaza_lng = obtener_valor_seguro(plaza, 'LONGITUD', mapeo)
                
                if plaza_lat is None or plaza_lng is None:
                    continue
                
                distancia = calcular_distancia_km(lat, lng, float(plaza_lat), float(plaza_lng))
                
                if distancia < distancia_minima:
                    distancia_minima = distancia
                    plaza_mas_cercana = {
                        'clave': obtener_valor_seguro(plaza, 'CLAVE_PLAZA', mapeo, ''),
                        'nombre': obtener_valor_seguro(plaza, 'NOMBRE_PC', mapeo, ''),
                        'estado': obtener_valor_seguro(plaza, 'ESTADO', mapeo, ''),
                        'municipio': obtener_valor_seguro(plaza, 'MUNICIPIO', mapeo, ''),
                        'lat': float(plaza_lat),
                        'lng': float(plaza_lng),
                        'distancia_km': round(distancia, 2),
                        'distancia_formateada': f"{distancia:.1f} km"
                    }
            except Exception:
                continue
        
        if plaza_mas_cercana is None:
            return jsonify({
                'status': 'error',
                'message': 'No se encontraron plazas cercanas'
            }), 404
        
        return jsonify({
            'status': 'success',
            'ubicacion_usuario': {'lat': lat, 'lng': lng},
            'plaza_mas_cercana': plaza_mas_cercana,
            'distancia_minima_km': distancia_minima
        })
        
    except Exception as e:
        logging.error(f"Error ubicando plaza cercana: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/mapa/generar-linea-ruta')
def generar_linea_ruta():
    """Genera coordenadas para línea de ruta entre dos puntos."""
    try:
        origen_lat = request.args.get('origen_lat', type=float)
        origen_lng = request.args.get('origen_lng', type=float)
        destino_lat = request.args.get('destino_lat', type=float)
        destino_lng = request.args.get('destino_lng', type=float)
        
        if None in [origen_lat, origen_lng, destino_lat, destino_lng]:
            return jsonify({
                'status': 'error',
                'message': 'Se requieren todas las coordenadas'
            }), 400
        
        # Calcular puntos intermedios para una línea más suave
        puntos = calcular_puntos_intermedios(
            origen_lat, origen_lng, destino_lat, destino_lng, num_puntos=10
        )
        
        distancia = calcular_distancia_km(origen_lat, origen_lng, destino_lat, destino_lng)
        
        return jsonify({
            'status': 'success',
            'origen': {'lat': origen_lat, 'lng': origen_lng},
            'destino': {'lat': destino_lat, 'lng': destino_lng},
            'puntos_ruta': puntos,
            'distancia_km': round(distancia, 2),
            'estilo_linea': {
                'color': '#007bff',
                'weight': 3,
                'opacity': 0.7,
                'dashArray': '10, 10'
            }
        })
        
    except Exception as e:
        logging.error(f"Error generando línea de ruta: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/mapa/opciones-navegacion')
def opciones_navegacion():
    """Genera URLs para todas las opciones de navegación."""
    try:
        destino_lat = request.args.get('destino_lat', type=float)
        destino_lng = request.args.get('destino_lng', type=float)
        destino_nombre = request.args.get('destino_nombre', 'Destino')
        origen_lat = request.args.get('origen_lat', type=float)
        origen_lng = request.args.get('origen_lng', type=float)
        
        if destino_lat is None or destino_lng is None:
            return jsonify({
                'status': 'error',
                'message': 'Se requieren coordenadas del destino'
            }), 400
        
        # Generar todas las URLs posibles
        opciones = {
            'ver_ubicacion': {
                'google_maps': f"https://www.google.com/maps/search/?api=1&query={destino_lat},{destino_lng}",
                'waze': f"https://www.waze.com/ul?ll={destino_lat},{destino_lng}&navigate=yes"
            }
        }
        
        # Si tenemos origen, generar rutas
        if origen_lat is not None and origen_lng is not None:
            nombre_codificado = destino_nombre.replace(' ', '+')
            
            opciones['crear_ruta'] = {
                'google_maps': f"https://www.google.com/maps/dir/?api=1&origin={origen_lat},{origen_lng}&destination={destino_lat},{destino_lng}&destination_place_id={nombre_codificado}&travelmode=driving",
                'waze': f"https://www.waze.com/ul?ll={destino_lat},{destino_lng}&navigate=yes&to={nombre_codificado}"
            }
            
            # Calcular distancia para mostrar información
            distancia = calcular_distancia_km(origen_lat, origen_lng, destino_lat, destino_lng)
            opciones['informacion'] = {
                'distancia_km': round(distancia, 2),
                'tiempo_estimado': estimar_tiempo_viaje(distancia)
            }
        
        return jsonify({
            'status': 'success',
            'destino': {
                'lat': destino_lat,
                'lng': destino_lng,
                'nombre': destino_nombre
            },
            'opciones': opciones
        })
        
    except Exception as e:
        logging.error(f"Error generando opciones de navegación: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# FUNCIONES AUXILIARES ADICIONALES
# ==============================================================================

def calcular_puntos_intermedios(lat1, lng1, lat2, lng2, num_puntos=10):
    """Calcula puntos intermedios entre dos coordenadas."""
    puntos = []
    
    for i in range(num_puntos + 1):
        factor = i / num_puntos
        lat = lat1 + (lat2 - lat1) * factor
        lng = lng1 + (lng2 - lng1) * factor
        puntos.append([lat, lng])
    
    return puntos

def normalizar_texto_para_busqueda(texto):
    """Normaliza texto para búsqueda (sin acentos, minúsculas)."""
    if not isinstance(texto, str):
        return ""
    
    # Convertir a minúsculas y quitar acentos
    texto = texto.lower()
    texto = unidecode(texto)
    
    # Eliminar caracteres especiales
    texto = ''.join(c for c in texto if c.isalnum() or c.isspace())
    
    return texto.strip()

# ==============================================================================
# 20. ENDPOINT PARA ESTADO DEL CACHE (NUEVO)
# ==============================================================================
@app.route('/api/cache/status')
def get_cache_status():
    """Devuelve información del estado del cache."""
    try:
        df = dataframe_cache.get_dataframe()
        
        return jsonify({
            'status': 'success',
            'cache_info': {
                'dataframe_en_memoria': not df.empty,
                'total_registros': len(df),
                'columnas_disponibles': list(df.columns) if not df.empty else [],
                'ultimo_mes_registros': len(dataframe_cache.get_ultimo_mes()) if not df.empty else 0,
                'estados_cacheados': len(dataframe_cache.get_estados_cache()) if not df.empty else 0,
                'zonas_cacheadas': len(dataframe_cache._zonas_cache),
                'timestamp': dataframe_cache._cache_timestamp.isoformat() if dataframe_cache._cache_timestamp else None
            },
            'archivos': {
                'parquet_existe': os.path.exists(Config.PARQUET_PATH),
                'excel_existe': os.path.exists(Config.EXCEL_PATH),
                'coordenadas_existe': os.path.exists(Config.ARCHIVO_COORDENADAS)
            }
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo estado del cache: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
#  OPTIMIZACIONES ADICIONALES: PRE-CÁLCULO EN BACKGROUND
# ==============================================================================

# Cache para estadísticas pre-calculadas
STATS_CACHE = None
COORDENADAS_CACHE = None

def precalcular_datos():
    """Pre-cálculo de datos pesados al iniciar el servidor."""
    global STATS_CACHE, COORDENADAS_CACHE
    
    try:
        logging.info("🔄 Iniciando pre-cálculo de datos pesados...")
        
        # Pre-cargar DataFrame
        df = dataframe_cache.cargar_dataframe()
        
        if not df.empty:
            # 1. Pre-calcular estadísticas
            logging.info("📊 Pre-calculando estadísticas...")
            STATS_CACHE = {
                'total_plazas': int(df[Config.COLUMNA_CLAVE].nunique()),
                'total_registros': len(df),
                'total_estados': int(df[Config.COLUMNA_ESTADO].nunique()) if Config.COLUMNA_ESTADO in df.columns else 0,
                'ultimo_mes_registros': len(dataframe_cache.get_ultimo_mes())
            }
            
            # 2. Pre-calcular coordenadas para el mapa
            logging.info("🗺️ Pre-calculando coordenadas para el mapa...")
            mapeo = dataframe_cache.get_mapeo_columnas()
            plazas_coordenadas = []
            
            for _, plaza in df.iterrows():
                try:
                    lat = obtener_valor_seguro(plaza, 'LATITUD', mapeo)
                    lng = obtener_valor_seguro(plaza, 'LONGITUD', mapeo)
                    
                    if lat is not None and lng is not None:
                        plazas_coordenadas.append({
                            'clave': obtener_valor_seguro(plaza, 'CLAVE_PLAZA', mapeo, ''),
                            'nombre': obtener_valor_seguro(plaza, 'NOMBRE_PC', mapeo, ''),
                            'estado': obtener_valor_seguro(plaza, 'ESTADO', mapeo, ''),
                            'municipio': obtener_valor_seguro(plaza, 'MUNICIPIO', mapeo, ''),
                            'lat': round(float(lat), 6),
                            'lng': round(float(lng), 6)
                        })
                except Exception:
                    continue
            
            COORDENADAS_CACHE = plazas_coordenadas[:2000]  # Limitar a 2000 para el mapa
            
            logging.info(f"✅ Pre-cálculo completado: {len(COORDENADAS_CACHE)} coordenadas, {STATS_CACHE['total_plazas']} plazas")
        else:
            logging.warning("⚠️ No se pudo realizar pre-cálculo: DataFrame vacío")
            
    except Exception as e:
        logging.error(f"❌ Error en pre-cálculo: {e}")
# ==============================================================================
# 21. INICIALIZACIÓN GLOBAL (SE EJECUTA SIEMPRE: LOCAL Y RENDER)
# ==============================================================================

# 1. Configurar Headers de Caché (Decorador global)
@app.after_request
def add_header(response):
    """Configura headers de caché para endpoints pesados."""
    # Cache por 1 hora para endpoints de mapas y estadísticas
    if request.path.startswith('/api/mapa/') or request.path.startswith('/api/estadisticas'):
        response.headers['Cache-Control'] = 'public, max-age=3600'
        response.headers['ETag'] = 'v1'
    return response

# 2. Iniciar Precarga y Hilos
# Esto asegura que los datos se carguen cuando Gunicorn importe la app
logging.info("🚀 Iniciando secuencia de arranque...")

# Iniciar hilo de precálculo en background
import threading
try:
    precalc_thread = threading.Thread(target=precalcular_datos, daemon=True)
    precalc_thread.start()
    logging.info("✅ Hilo de precálculo iniciado")
except Exception as e:
    logging.error(f"❌ Error iniciando hilo: {e}")

# 3. Cargar DataFrame en Memoria
df_inicial = dataframe_cache.cargar_dataframe()

if df_inicial.empty:
    logging.critical("⚠️ ADVERTENCIA: La aplicación inició sin datos. Se intentará recargar en la primera petición.")
else:
    logging.info(f"✅ Aplicación lista con {len(df_inicial)} registros")

# ==============================================================================
# 21. PUNTO DE ENTRADA
# ==============================================================================
if __name__ == '__main__':
    # Precargar el DataFrame al iniciar
    logging.info("🚀 Iniciando aplicación Flask...")
    
    #  Ejecutar pre-cálculo en background
    import threading
    precalc_thread = threading.Thread(target=precalcular_datos, daemon=True)
    precalc_thread.start()
    
    df = dataframe_cache.cargar_dataframe()
    
    if df.empty:
        logging.critical("❌ La aplicación no puede iniciar porque la carga del archivo falló.")
    else:
        logging.info(f"✅ Aplicación Flask iniciada correctamente con {len(df)} registros")
        
        #  Configurar headers de cache para endpoints estáticos
        @app.after_request
        def add_header(response):
            # Cache por 1 hora para endpoints pesados
            if request.path.startswith('/api/mapa/') or request.path.startswith('/api/estadisticas'):
                response.headers['Cache-Control'] = 'public, max-age=3600'
                response.headers['ETag'] = 'v1'
            return response
        
        app.run(host='0.0.0.0', port=5000, debug=True)