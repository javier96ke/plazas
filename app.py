import os
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, jsonify, send_from_directory, url_for, redirect
import json
from unidecode import unidecode
from datetime import datetime
import traceback

# ==============================================================================
#  Manejar importaciones condicionales
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
    IMAGES_BASE_PATH = 'fotos_de_plazas'
    DRIVE_TREE_FILE = 'drive_tree.json'
    EXCEL_TREE_FILE = 'excel_tree_real.json'

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
    COLUMNA_TIPOS_EQUIPOS_COMPUTO = 'Tipos de equipos de cómputo'
    COLUMNA_IMPRESORAS_FUNCIONAN = 'Impresoras que funcionan'
    COLUMNA_IMPRESORAS_SUMINISTROS = 'Impresoras con suministros (toner, hojas)'
    
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
        'MESAS_FUNCIONAN': {'nombres_posibles': ['Cuantas mesas funcionan', 'Mesas operativas', 'Mesas en buen estado'], 'nombre_estandar': 'Cuantas mesas funcionan'},
        'SILLAS_FUNCIONAN': {'nombres_posibles': ['Cuantas sillas funcionan', 'Sillas operativas', 'Sillas en buen estado'], 'nombre_estandar': 'Cuantas sillas funcionan'},
        'COORD_ZONA': { 'nombres_posibles': [ 'Coord. Zona','COORD. ZONA', 'Coordinación de Zona', 'Zona', 'Coord_Zona', 'C. Zona' ], 'nombre_estandar': 'Coord. Zona' },
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

# ==============================================================================
# 2. CARGA Y PREPARACIÓN DE DATOS
# ==============================================================================
def normalizar_texto(texto: str) -> str:
    """Convierte texto a minúsculas, sin acentos ni espacios extra, y lo pone en mayúsculas para la comparación."""
    if not isinstance(texto, str):
        return ""
    return unidecode(texto).strip().upper()

def cargar_y_preparar_excel(config: Config) -> pd.DataFrame | None:
    """Carga y limpia el archivo Excel de manera robusta."""
    try:
        logging.info(f"Iniciando carga del archivo Excel: {config.EXCEL_PATH}")
        if not os.path.exists(config.EXCEL_PATH):
            raise FileNotFoundError(f"El archivo '{config.EXCEL_PATH}' no existe.")

        # Leer con dtype especificado para la clave para evitar conversiones automáticas
        # Si la columna se llama 'Clave_Plaza' en el Excel, esto funcionará directo.
        # Si no, pandas podría no aplicar el dtype si el nombre difiere, pero lo manejamos luego.
        df = pd.read_excel(config.EXCEL_PATH)

        if df.empty:
            logging.warning("El archivo Excel está vacío.")
            return pd.DataFrame()

        df.columns = [str(col).strip() for col in df.columns]

        columnas_a_normalizar = [
            config.COLUMNA_ESTADO, config.COLUMNA_COORD_ZONA,
            config.COLUMNA_MUNICIPIO, config.COLUMNA_LOCALIDAD
        ]
        
        for col_name in columnas_a_normalizar:
            if col_name in df.columns:
                df[f"normalized_{col_name.lower()}"] = df[col_name].fillna('').astype(str).apply(normalizar_texto)
            else:
                logging.warning(f"ADVERTENCIA: La columna de filtro '{col_name}' no se encontró.")
        
        # Encontrar la columna de clave usando los alias
        col_clave_real = Config.obtener_nombre_columna('CLAVE_PLAZA', df)
        
        if col_clave_real:
            # Normalizar la clave para búsquedas
            df[config.COLUMNA_CLAVE] = df[col_clave_real].fillna('').astype(str).str.strip().str.upper()
        else:
            raise ValueError(f"CRÍTICO: La columna clave '{config.COLUMNA_CLAVE}' no se encontró.")

        for col_name in [config.COLUMNA_LATITUD, config.COLUMNA_LONGITUD]:
            if col_name in df.columns:
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')

        logging.info("Archivo Excel cargado y procesado exitosamente.")
        return df

    except FileNotFoundError as e:
        logging.error(f"Error de archivo no encontrado: {e}")
    except Exception as e:
        logging.error(f"Error inesperado al leer el Excel. Detalle: {e}")
    return None

df_plazas = cargar_y_preparar_excel(Config)

if not os.path.isdir(Config.IMAGES_BASE_PATH):
    logging.warning(f"La carpeta de imágenes '{Config.IMAGES_BASE_PATH}' no fue encontrada.")

# ==============================================================================
# 3. FUNCIÓN AUXILIAR 
# ==============================================================================
def obtener_df_ultimo_mes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra el DataFrame para obtener solo los datos del último mes disponible.
    """
    try:
        # Verificar si existe la columna de mes
        if Config.COLUMNA_CVE_MES not in df.columns:
            # logging.warning("Columna Cve-mes no encontrada, devolviendo DataFrame completo")
            return df.copy()
        
        # Convertir a numérico para ordenar correctamente
        df_temp = df.copy()
        df_temp['__temp_cve_mes'] = pd.to_numeric(df_temp[Config.COLUMNA_CVE_MES], errors='coerce')
        
        # Encontrar el mes más reciente
        max_mes = df_temp['__temp_cve_mes'].max()
        
        if pd.isna(max_mes):
            # logging.warning("No se encontraron valores numéricos válidos en Cve-mes")
            return df.copy()
        
        # Filtrar por el mes más reciente
        df_filtrado = df_temp[df_temp['__temp_cve_mes'] == max_mes].copy()
        
        # Eliminar columna temporal
        if '__temp_cve_mes' in df_filtrado.columns:
            df_filtrado = df_filtrado.drop('__temp_cve_mes', axis=1)
        
        # logging.info(f"Filtrado para último mes ({int(max_mes)}): {len(df_filtrado)} registros de {len(df)} totales")
        return df_filtrado
        
    except Exception as e:
        logging.error(f"Error al filtrar por último mes: {e}")
        return df.copy()

# ==============================================================================
# 4. RUTAS DE LA API (ENDPOINTS)
# ==============================================================================
@app.route('/')
def home():
    """Renderiza la página principal."""
    return render_template('index.html')

def obtener_opciones_unicas(df: pd.DataFrame, columna: str) -> list:
    """Obtiene valores únicos, sin nulos/vacíos y ordenados de una columna."""
    if df is None or columna not in df.columns:
        return []
    opciones = df[columna].dropna().unique()
    # Usar convertir_a_serializable para que no falle al devolver JSON
    opciones_limpias = [convertir_a_serializable(opc) for opc in opciones if str(opc).strip()]
    
    # Filtrar None y ordenar como string para evitar errores de tipo mezclado
    opciones_finales = sorted([x for x in opciones_limpias if x is not None], key=str)
    return opciones_finales

@app.route('/api/estados')
def get_estados():
    estados = obtener_opciones_unicas(df_plazas, Config.COLUMNA_ESTADO)
    if not estados:
        return jsonify({'error': 'La información de estados no está disponible.'}), 500
    return jsonify(estados)

@app.route('/api/estados_con_conteo')
def get_estados_con_conteo():
    """Devuelve los estados con el conteo de plazas DEL ÚLTIMO MES."""
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        # --- CAMBIO: Usar solo el último mes ---
        df_actual = obtener_df_ultimo_mes(df_plazas)
        # ---------------------------------------

        # Contar plazas por estado (usando claves únicas por si acaso)
        estado_counts = df_actual.groupby(Config.COLUMNA_ESTADO)[Config.COLUMNA_CLAVE].nunique()
        
        estados_con_conteo = []
        for estado, count in estado_counts.items():
            estados_con_conteo.append({
                'nombre': str(estado),
                'cantidad': int(count)
            })
        
        # Ordenar por cantidad descendente
        estados_con_conteo.sort(key=lambda x: x['cantidad'], reverse=True)
        
        return jsonify(estados_con_conteo)
        
    except Exception as e:
        logging.error(f"Error obteniendo estados con conteo: {e}")
        return jsonify({'error': 'Error al obtener estados con conteo'}), 500

@app.route('/api/zonas')
def get_zonas_por_estado():
    """Devuelve las 'Coord. Zona' para un estado dado."""
    estado = request.args.get('estado', '')
    if not estado:
        return jsonify({'error': 'Se requiere un estado.'}), 400
    
    col_estado_norm = f"normalized_{Config.COLUMNA_ESTADO.lower()}"
    df_filtrado = df_plazas[df_plazas[col_estado_norm] == normalizar_texto(estado)]
    
    zonas = obtener_opciones_unicas(df_filtrado, Config.COLUMNA_COORD_ZONA)
    return jsonify(zonas)

@app.route('/api/municipios')
def get_municipios_por_zona():
    """Devuelve los municipios para un estado y zona dados."""
    estado = request.args.get('estado', '')
    zona = request.args.get('zona', '')
    if not estado or not zona:
        return jsonify({'error': 'Se requieren estado y zona.'}), 400

    col_estado_norm = f"normalized_{Config.COLUMNA_ESTADO.lower()}"
    col_zona_norm = f"normalized_{Config.COLUMNA_COORD_ZONA.lower()}"

    df_filtrado = df_plazas[
        (df_plazas[col_estado_norm] == normalizar_texto(estado)) &
        (df_plazas[col_zona_norm] == normalizar_texto(zona))
    ]
    municipios = obtener_opciones_unicas(df_filtrado, Config.COLUMNA_MUNICIPIO)
    return jsonify(municipios)

@app.route('/api/localidades')
def get_localidades_por_municipio():
    """Devuelve las localidades para un estado, zona y municipio dados."""
    estado = request.args.get('estado', '')
    zona = request.args.get('zona', '')
    municipio = request.args.get('municipio', '')
    if not all([estado, zona, municipio]):
        return jsonify({'error': 'Se requieren estado, zona y municipio.'}), 400

    col_estado_norm = f"normalized_{Config.COLUMNA_ESTADO.lower()}"
    col_zona_norm = f"normalized_{Config.COLUMNA_COORD_ZONA.lower()}"
    col_municipio_norm = f"normalized_{Config.COLUMNA_MUNICIPIO.lower()}"
    
    df_filtrado = df_plazas[
        (df_plazas[col_estado_norm] == normalizar_texto(estado)) &
        (df_plazas[col_zona_norm] == normalizar_texto(zona)) &
        (df_plazas[col_municipio_norm] == normalizar_texto(municipio))
    ]
    localidades = obtener_opciones_unicas(df_filtrado, Config.COLUMNA_LOCALIDAD)
    return jsonify(localidades)

@app.route('/api/claves_plaza')
def get_claves_por_localidad():
    """Devuelve las claves de plaza finales basadas en todos los filtros."""
    estado = request.args.get('estado', '')
    zona = request.args.get('zona', '')
    municipio = request.args.get('municipio', '')
    localidad = request.args.get('localidad', '')
    if not all([estado, zona, municipio, localidad]):
        return jsonify({'error': 'Se requieren todos los filtros.'}), 400

    df_filtrado = df_plazas[
        (df_plazas[f"normalized_{Config.COLUMNA_ESTADO.lower()}"] == normalizar_texto(estado)) &
        (df_plazas[f"normalized_{Config.COLUMNA_COORD_ZONA.lower()}"] == normalizar_texto(zona)) &
        (df_plazas[f"normalized_{Config.COLUMNA_MUNICIPIO.lower()}"] == normalizar_texto(municipio)) &
        (df_plazas[f"normalized_{Config.COLUMNA_LOCALIDAD.lower()}"] == normalizar_texto(localidad))
    ]
    claves = obtener_opciones_unicas(df_filtrado, Config.COLUMNA_CLAVE)
    return jsonify(claves)
# ==============================================================================
# ENDPOINT DE BÚSQUEDA  (Agregados Colonia, Calle, Num, CP)
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
        
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'Base de datos no cargada.'}), 503

        # 2. Asegurar que usamos el nombre correcto de la columna
        columna_clave = 'Clave_Plaza'
        if columna_clave not in df_plazas.columns:
            mapeo = inicializar_mapeo_columnas(df_plazas)
            columna_clave = mapeo.get('CLAVE_PLAZA')
            if not columna_clave:
                return jsonify({'error': f'No se encuentra la columna Clave_Plaza en el Excel.'}), 500

        # 3. Filtrar usando el ÚLTIMO MES disponible
        df_actual = obtener_df_ultimo_mes(df_plazas)

        # 4. Búsqueda ROBUSTA
        mask = df_actual[columna_clave].astype(str).str.strip().str.upper() == clave_busqueda
        plaza_data = df_actual[mask]

        if plaza_data.empty:
            return jsonify({'error': f'No se encontraron resultados para: {clave_busqueda}'}), 404

        # 5. Obtener la primera coincidencia
        fila = plaza_data.iloc[0]
        # Convertimos la fila a diccionario
        plaza_dict_raw = fila.to_dict()
        
        # APLICAR EL LIMPIADOR RECURSIVO
        plaza_dict_clean = convertir_a_serializable(plaza_dict_raw)

        # 6. Preparar el mapeo para extraer datos seguros
        mapeo_cols = inicializar_mapeo_columnas(df_plazas)
        
        def get_val(key):
            # Obtener y limpiar inmediatamente
            raw_val = obtener_valor_seguro(fila, key, mapeo_cols)
            return convertir_a_serializable(raw_val)

        # 7. Construir dirección y coordenadas
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

        # 8. Buscar imágenes
        images = []
        try:
            images = find_image_urls(clave_busqueda)
        except Exception as e:
            print(f"⚠️ Error buscando imágenes: {e}")

        # 9. Estructurar respuesta 
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

        # 10. Limpieza final de la respuesta JSON
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
                            # logging.info(f"✅ Imagen encontrada: {child.get('name')} -> {direct_url}")
                        else:
                            pass
                            # logging.warning(f"⚠️ Imagen sin URL: {child.get('name')}")
                return True
            
            for child in tree.get('children', []):
                if search_images_in_tree(child, target_folder):
                    return True
            return False
        
        # Buscar en el árbol
        found = search_images_in_tree(drive_data['structure'], clave_lower)
        
        if not found:
            # logging.warning(f"❌ Carpeta '{clave_lower}' no encontrada en Drive")
            pass
        elif not image_list:
            # logging.warning(f"⚠️ Carpeta '{clave_lower}' encontrada pero sin imágenes")
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
# 6. ESTADÍSTICAS CON NUEVAS COLUMNAS 
# ==============================================================================
@app.route('/api/estadisticas')
def get_estadisticas():
    """Devuelve estadísticas generales basadas en el ÚLTIMO MES REPORTADO."""
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        # ============================================================
        # 0️⃣ FILTRADO POR ÚLTIMO MES (Lógica Solicitada)
        # ============================================================
        # Trabajaremos con un DataFrame filtrado solo para los conteos de "Situación actual"
        df_actual = df_plazas.copy()
        mes_usado = "Todos (Histórico)"

        if Config.COLUMNA_CVE_MES in df_actual.columns:
            # Convertir a numérico para asegurar orden correcto (evitar que "10" sea menor que "2" en string)
            # Usamos una columna temporal para no afectar el df original global si no se desea
            col_temp = '__temp_cve_mes'
            df_actual[col_temp] = pd.to_numeric(df_actual[Config.COLUMNA_CVE_MES], errors='coerce')
            
            # Obtener el máximo valor (el mes más reciente)
            max_mes = df_actual[col_temp].max()
            
            if pd.notna(max_mes):
                # Filtrar el DataFrame para mantener solo los registros del último mes
                df_actual = df_actual[df_actual[col_temp] == max_mes]
                mes_usado = str(int(max_mes))
                logging.info(f"Calculando estadísticas sobre el mes más reciente: {mes_usado}")
            else:
                logging.warning("La columna Cve-mes existe pero no tiene valores numéricos válidos.")

        # ============================================================
        # 1️⃣ Estadísticas generales básicas (Usando df_actual filtrado)
        # ============================================================
        # Usamos nunique() en la Clave_Plaza para asegurar que sean plazas únicas
        total_plazas = df_actual[Config.COLUMNA_CLAVE].nunique()
        
        # Contar plazas en operación en el mes actual
        if Config.COLUMNA_SITUACION in df_actual.columns:
            # Filtramos por situación y luego contamos claves únicas
            df_operacion = df_actual[
                df_actual[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
            ]
            plazas_operacion = df_operacion[Config.COLUMNA_CLAVE].nunique()
        else:
            plazas_operacion = 0
        
        # ============================================================
        # 2️⃣ Estado con más y menos plazas (Usando df_actual)
        # ============================================================
        # Es importante usar df_actual aquí también, de lo contrario un estado 
        # parecería tener el doble de plazas si hay 2 meses cargados.
        
        total_estados = 0
        estado_mas_plazas = {'nombre': 'N/A', 'cantidad': 0}
        estado_menos_plazas = {'nombre': 'N/A', 'cantidad': 0}

        if Config.COLUMNA_ESTADO in df_actual.columns:
            # Contamos claves únicas por estado
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
        # 3️⃣ Estado con mayor conectividad (Usando df_actual)
        # ============================================================
        estado_mayor_conectividad = {'nombre': 'N/A', 'porcentaje': 0}
        
        if Config.COLUMNA_CONECT_INSTALADA in df_actual.columns:
            df_conect = df_actual.copy()

            # Normalizar columna: contar como 1 si hay algo distinto de vacío, NA, None, etc.
            df_conect['conectiva'] = df_conect[Config.COLUMNA_CONECT_INSTALADA].apply(
                lambda v: 1 if pd.notna(v) and str(v).strip().lower() not in ['', 'nan', 'na', 'none', 'null', '0'] else 0
            )

            # Promedio de conectividad por estado
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
        # 4️⃣ Estado con mayor porcentaje de Operación/Suspensión (Usando df_actual)
        # ============================================================
        estado_mas_operacion = {'nombre': 'N/A', 'porcentaje': 0}
        estado_mas_suspension = {'nombre': 'N/A', 'porcentaje': 0}

        if Config.COLUMNA_SITUACION in df_actual.columns:
            df_sit = df_actual.copy()
            df_sit['Situacion_Norm'] = df_sit[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper()

            # Calculamos totales por estado
            conteo_por_estado = df_sit.groupby([Config.COLUMNA_ESTADO, 'Situacion_Norm'])[Config.COLUMNA_CLAVE].nunique().unstack(fill_value=0)
            
            # Añadir columna total si no existe para evitar división por cero
            conteo_por_estado['Total_Estado'] = conteo_por_estado.sum(axis=1)
            
            # Calcular porcentajes
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
        # 5️⃣ Estadísticas de equipamiento (Sumatoria del mes actual)
        # ============================================================
        estadisticas_equipamiento = {}
        
        # Nota: Aquí usamos sum() sobre df_actual. Si una plaza aparece una vez en df_actual, 
        # sus equipos se suman una sola vez, lo cual es correcto.
        
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
        # 6️⃣ Estadísticas de mobiliario (Sumatoria del mes actual)
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
        # 7️⃣ Estadísticas de CN (Acumuladas al mes actual)
        # ============================================================
        # Como las columnas dicen "Acum" (ej. CN_Inicial_Acum), tomar el dato del último mes
        # es la forma correcta de ver el acumulado del año hasta la fecha.
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
        return jsonify({
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
# 7. ENDPOINT PARA OBTENER COLUMNAS DISPONIBLES
# ==============================================================================
@app.route('/api/columnas-disponibles')
def get_columnas_disponibles():
    """Devuelve la lista de columnas disponibles en el dataset."""
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        columnas = list(df_plazas.columns)
        
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

# ==============================================================================
# 8. ENDPOINT PARA OBTENER DATOS DETALLADOS DE UNA PLAZA 
# ==============================================================================
@app.route('/api/plaza-detallada/<clave>')
def get_plaza_detallada(clave):
    """Devuelve TODOS los datos de una plaza específica."""
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        clave_busqueda = clave.strip().upper()
        plaza_data = df_plazas[df_plazas[Config.COLUMNA_CLAVE] == clave_busqueda]
        
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
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        estado_counts = df_plazas[Config.COLUMNA_ESTADO].value_counts()
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
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        # --- CAMBIO: Usar solo el último mes ---
        df_actual = obtener_df_ultimo_mes(df_plazas)
        # ---------------------------------------

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
        
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        # --- CAMBIO: Usar solo el último mes ---
        df_actual = obtener_df_ultimo_mes(df_plazas)
        # ---------------------------------------

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
    Resumen nacional de CN_Inicial_Acum, CN_Prim_Acum, CN_Sec_Acum:
    - Sumas: Calculadas sobre TODO el dataset (Histórico).
    - Plazas en operación: Calculadas SOLO sobre el ÚLTIMO MES.
    """
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        cols = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']
        missing_cols = [c for c in cols if c not in df_plazas.columns]
        if missing_cols:
            return jsonify({'error': 'Faltan columnas', 'faltantes': missing_cols}), 400

        # 1. DataFrame COMPLETO (para las sumatorias)
        df_tmp = df_plazas.copy()
        
        # 2. DataFrame FILTRADO (para conteo de plazas actuales)
        df_actual = obtener_df_ultimo_mes(df_plazas) 

        # Convertir a numérico en AMBOS dataframes
        for c in cols:
            col_key = f'__{c}_num'
            df_tmp[col_key] = pd.to_numeric(df_tmp[c], errors='coerce')
            df_actual[col_key] = pd.to_numeric(df_actual[c], errors='coerce')

        # 3. Crear máscara de operación SOLO en el DataFrame ACTUAL
        if Config.COLUMNA_SITUACION in df_actual.columns:
            mask_operacion_actual = df_actual[Config.COLUMNA_SITUACION].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
        else:
            mask_operacion_actual = pd.Series([False] * len(df_actual), index=df_actual.index)

        total_registros = len(df_tmp)
        resumen_nacional = {}
        cn_total_nacional = 0
        
        for c in cols:
            colnum = f'__{c}_num'
            
            # --- A. CÁLCULO DE SUMAS (Usando df_tmp - Histórico) ---
            n_nulos = df_tmp[colnum].isna().sum()
            suma = float(df_tmp[colnum].fillna(0).sum())
            cn_total_nacional += suma
            
            # --- B. CÁLCULO DE PLAZAS (Usando df_actual - Último Mes) ---
            # Contamos cuántas plazas del mes actual tienen este indicador > 0 y están en operación
            if mask_operacion_actual.any():
                plazas_operacion_cat = len(df_actual[
                    mask_operacion_actual & 
                    (df_actual[colnum].fillna(0) > 0)
                ])
            else:
                plazas_operacion_cat = 0
            
            resumen_nacional[c] = {
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

        if Config.COLUMNA_ESTADO in df_tmp.columns:
            df_tmp['__CN_Total_num'] = (
                df_tmp['__CN_Inicial_Acum_num'].fillna(0) + 
                df_tmp['__CN_Prim_Acum_num'].fillna(0) + 
                df_tmp['__CN_Sec_Acum_num'].fillna(0)
            )
            
            grp = df_tmp.groupby(Config.COLUMNA_ESTADO)['__CN_Total_num'].sum().sort_values(ascending=False)
            top5 = [{'estado': str(idx), 'suma_CN_Total': float(v)} for idx,v in grp.head(5).items()]
        else:
            top5 = []

        return jsonify({
            'resumen_nacional': resumen_nacional,
            'top5_estados_por_CN_Total': top5
        })
    except Exception as e:
        logging.error(f"Error en /api/cn_resumen: {e}")
        return jsonify({'error': 'Error generando resumen CN'}), 500

@app.route('/api/cn_por_estado')
def cn_por_estado():
    """
    Agregados por estado: 
    - Sumas (CN): Histórico completo.
    - Total Plazas: SOLO ÚLTIMO MES.
    """
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        cols = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']
        missing_cols = [c for c in cols if c not in df_plazas.columns]
        if missing_cols:
            return jsonify({'error': 'Faltan columnas', 'faltantes': missing_cols}), 400

        # 1. DataFrame HISTÓRICO (para las sumas de certificaciones)
        df_historico = df_plazas.copy()
        
        # 2. DataFrame ACTUAL (para contar las plazas activas del último mes)
        df_actual = obtener_df_ultimo_mes(df_plazas)

        # Convertir columnas numéricas en ambos
        for c in cols:
            col_key = f'__{c}_num'
            df_historico[col_key] = pd.to_numeric(df_historico[c], errors='coerce')
            # No necesitamos convertir en df_actual si solo vamos a contar filas, 
            # pero lo hacemos por si acaso.

        # Calcular totales nacionales para porcentajes (usando histórico)
        nacional_totales = {
            c: float(df_historico[f'__{c}_num'].fillna(0).sum())
            for c in cols
        }
        cn_total_nacional = sum(nacional_totales.values())

        # Agrupar datos históricos
        grouped_historico = df_historico.groupby(Config.COLUMNA_ESTADO)
        
        # Pre-calcular conteos de plazas por estado del MES ACTUAL
        # Esto nos da una Series con el conteo real de plazas hoy
        conteo_plazas_actuales = df_actual.groupby(Config.COLUMNA_ESTADO).size()

        estados_summary = []
        
        # Iteramos sobre los grupos históricos para obtener las sumas
        for estado, g in grouped_historico:
        
            # Obtenemos el total de plazas del último mes para este estado.
            # Si no hay datos en el mes actual para este estado, es 0.
            total_plazas_actual = int(conteo_plazas_actuales.get(estado, 0))
            
            # Sumas históricas (se mantienen igual)
            s_inicial = float(g['__CN_Inicial_Acum_num'].fillna(0).sum())
            s_prim = float(g['__CN_Prim_Acum_num'].fillna(0).sum())
            s_sec = float(g['__CN_Sec_Acum_num'].fillna(0).sum())
            s_total = s_inicial + s_prim + s_sec
            
            # Promedio (opcional, sobre histórico)
            mean_inicial = float(g['__CN_Inicial_Acum_num'].dropna().mean()) if g['__CN_Inicial_Acum_num'].dropna().shape[0]>0 else 0.0

            pct_sobre_nacional = round((s_total / cn_total_nacional * 100), 2) if cn_total_nacional > 0 else 0.0

            estados_summary.append({
                'estado': str(estado),
                'total_plazas': total_plazas_actual, # <--- DATO CORREGIDO
                'suma_CN_Inicial_Acum': round(s_inicial, 2),
                'suma_CN_Prim_Acum': round(s_prim, 2),
                'suma_CN_Sec_Acum': round(s_sec, 2),
                'suma_CN_Total': round(s_total, 2),
                'promedio_CN_Inicial_Acum': round(mean_inicial, 2),
                'pct_sobre_nacional': pct_sobre_nacional
            })

        estados_sorted = sorted(estados_summary, key=lambda x: x['suma_CN_Inicial_Acum'], reverse=True)
        
        return jsonify({
            'nacional_totales': {k: round(v, 2) for k,v in nacional_totales.items()},
            'cn_total_nacional': round(cn_total_nacional, 2),
            'estados': estados_sorted
        })
        
    except Exception as e:
        logging.error(f"Error en /api/cn_por_estado: {e}")
        return jsonify({'error': 'Error generando CN por estado'}), 500

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

        if col_key not in df_plazas.columns:
            return jsonify({'error': f'No existe la columna {col_key}'}), 400

        df_tmp = df_plazas.copy()
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
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        cols = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']
        missing_cols = [c for c in cols if c not in df_plazas.columns]
        if missing_cols:
            return jsonify({'error': 'Faltan columnas', 'faltantes': missing_cols}), 400

        df_tmp = df_plazas.copy()
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
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        df_tmp = df_plazas.copy()
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
def get_excel_last_update():
    """Devuelve la fecha de última modificación del Excel, ajustada un mes atrás (salvo diciembre)."""
    try:
        if os.path.exists(Config.EXCEL_PATH):
            timestamp = os.path.getmtime(Config.EXCEL_PATH)
            fecha_real = datetime.fromtimestamp(timestamp)

            # Obtener año y mes de la fecha real
            año = fecha_real.year
            mes = fecha_real.month

            # Ajuste de mes
            if mes == 12:
                # Si es diciembre, no se modifica
                pass
            elif mes == 1:
                # Enero → diciembre del año anterior
                año -= 1
                mes = 12
            else:
                # Meses 2–11 → restar uno
                mes -= 1

            # Crear la fecha ajustada (manteniendo el día si es posible)
            try:
                fecha_ajustada = datetime(año, mes, fecha_real.day)
            except ValueError:
                # Manejo de días inválidos (ej. 31 en meses de 30 días o febrero)
                if mes == 2:
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
                "formatted": fecha_ajustada.strftime('%d/%m/%Y')
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
        
        # Información del Excel local
        local_excel_info = {
            'exists': os.path.exists(Config.EXCEL_PATH),
            'last_modified': None,
            'total_plazas': len(df_plazas) if df_plazas is not None else 0
        }
        
        if local_excel_info['exists']:
            timestamp = os.path.getmtime(Config.EXCEL_PATH)
            local_excel_info['last_modified'] = datetime.fromtimestamp(timestamp).isoformat()
        
        return jsonify({
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
            'local_data': local_excel_info,
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
        
        datos_completos = {
            'años_disponibles': años,
            'meses_por_año': meses_por_año,
            'drive_modules_available': DRIVE_MODULES_AVAILABLE,
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
        if df_plazas is None or df_plazas.empty:
            return jsonify({
                'status': 'error',
                'message': 'No hay datos disponibles'
            }), 503
        
        # Obtener estados únicos que coincidan con la búsqueda
        if Config.COLUMNA_ESTADO in df_plazas.columns:
            estados_unicos = df_plazas[Config.COLUMNA_ESTADO].dropna().unique()
            
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
# HISTORIAL DE UNA PLAZA (Para tablas interactivas)
# ==============================================================================
@app.route('/api/plaza-historial')
def get_plaza_historial():
    """Devuelve el historial completo de una plaza para selectores de fecha."""
    try:
        clave_busqueda = request.args.get('clave', '').strip().upper()
        
        if not clave_busqueda or df_plazas is None:
            return jsonify([])

        # 1. Identificar columna clave
        columna_clave = 'Clave_Plaza'
        if columna_clave not in df_plazas.columns:
            mapeo = inicializar_mapeo_columnas(df_plazas)
            columna_clave = mapeo.get('CLAVE_PLAZA')
            if not columna_clave:
                return jsonify([])

        # 2. Filtrar todas las filas de esa plaza (Histórico completo)
        # Convertimos a string para asegurar el match
        mask = df_plazas[columna_clave].astype(str).str.strip().str.upper() == clave_busqueda
        df_historial = df_plazas[mask].copy()

        if df_historial.empty:
            return jsonify([])

        # 3. Ordenar por Año y Mes (Descendente: más reciente primero)
        # Aseguramos que sean numéricos para ordenar bien
        if Config.COLUMNA_ANO in df_historial.columns and Config.COLUMNA_CVE_MES in df_historial.columns:
            df_historial['__sort_anio'] = pd.to_numeric(df_historial[Config.COLUMNA_ANO], errors='coerce').fillna(0)
            df_historial['__sort_mes'] = pd.to_numeric(df_historial[Config.COLUMNA_CVE_MES], errors='coerce').fillna(0)
            df_historial = df_historial.sort_values(by=['__sort_anio', '__sort_mes'], ascending=[False, False])

        # 4. Seleccionar columnas relevantes para la tabla de Atención/Productividad
        # (Incluimos las fechas para el selector)
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
        mapeo_cols = inicializar_mapeo_columnas(df_plazas)
        cols_finales = []
        for c in cols_interes:
            real = obtener_nombre_columna_seguro(c, mapeo_cols, df_plazas) # Helper simple o búsqueda directa
            if real: cols_finales.append(real)
            
        # 5. Convertir a lista de diccionarios limpia
        historial_data = []
        for _, row in df_historial.iterrows():
            row_dict = row.to_dict()
            # Usamos el limpiador 'convertir_a_serializable' que ya definimos antes
            clean_dict = convertir_a_serializable(row_dict)
            historial_data.append(clean_dict)

        return jsonify(historial_data)

    except Exception as e:
        logging.error(f"Error obteniendo historial: {e}")
        return jsonify([])

# Helper rápido para nombres de columnas (puedes ponerlo junto a los otros helpers)
def obtener_nombre_columna_seguro(alias, mapeo, df):
    if alias in df.columns: return alias
    # Buscar en el mapeo inverso si es necesario, o usar la lógica de Config
    # Por simplicidad, aquí confiamos en que el alias coincida o esté en el mapeo
    k = Config.obtener_clave_por_nombre_columna(alias)
    if k and k in mapeo: return mapeo[k]
    return alias # Fallback
# ==============================================================================
# NUEVO ENDPOINT: METRICAS DETALLADAS POR ESTADO (BLINDADO PARA SUMAS)
# ==============================================================================
@app.route('/api/metricas-por-estado/<estado>')
def get_metricas_por_estado(estado):
    """
    Devuelve las métricas de atención y certificación para todas las plazas de un estado.
    - Filtra por el ÚLTIMO MES.
    - Asegura que los números sean números (0 si es nulo) para que el JS pueda sumar.
    - INCLUYE DATOS AGRUPADOS POR MUNICIPIO en el mismo JSON.
    """
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({
                "plazas": [],
                "municipios": []
            })

        # 1. Filtrar por último mes disponible (Usando la función auxiliar existente)
        df_actual = obtener_df_ultimo_mes(df_plazas)
        
        # 2. Filtrar por Estado (Normalizando texto para evitar errores de acentos/mayúsculas)
        estado_busqueda = normalizar_texto(estado)
        
        # Intentamos buscar en la columna normalizada si existe, si no, calculamos al vuelo
        col_estado_norm = f"normalized_{Config.COLUMNA_ESTADO.lower()}"
        
        if col_estado_norm in df_actual.columns:
            mask = df_actual[col_estado_norm] == estado_busqueda
        else:
            # Fallback: normalizar al vuelo
            mask = df_actual[Config.COLUMNA_ESTADO].astype(str).apply(normalizar_texto) == estado_busqueda
            
        df_estado = df_actual[mask].copy()

        if df_estado.empty:
            return jsonify({
                "plazas": [],
                "municipios": []
            })

        # 3. Definir las columnas que el JS espera (Claves exactas del frontend)
        columnas_exportar = [
            # Identificadores
            'Clave_Plaza', 'Nombre_PC', 'Municipio', 
            # Métricas numéricas
            'Aten_Inicial', 'Aten_Prim', 'Aten_Sec', 'Aten_Total', 
            'Exámenes aplicados',
            'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum',
            'Cert_Emitidos'
        ]
        
        # Columnas que son métricas numéricas (para la agrupación)
        metricas_numericas = [
            'Aten_Inicial', 'Aten_Prim', 'Aten_Sec', 'Aten_Total', 
            'Exámenes aplicados',
            'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum',
            'Cert_Emitidos'
        ]
        
        # Inicializar mapeo para encontrar los nombres reales en el Excel (por si usan alias)
        mapeo = inicializar_mapeo_columnas(df_plazas)
        
        datos_plazas = []
        # Diccionario temporal para acumular datos por municipio
        acumulador_municipios = {}

        for _, row in df_estado.iterrows():
            fila_dict = {}
            
            for col_js in columnas_exportar:
                # Buscar nombre real de la columna en el Excel usando el sistema de alias
                # Si es una métrica directa del Excel, buscamos su alias. Si no, usamos el nombre tal cual.
                col_excel = mapeo.get(Config.obtener_clave_por_nombre_columna(col_js)) or col_js
                
                # Obtener valor seguro
                val = row.get(col_excel)
                
                # Regla de limpieza:
                # Si es Texto (Identificadores) -> Dejar como string
                # Si es Métrica -> Convertir a número (0 si es nulo/texto inválido)
                if col_js in ['Clave_Plaza', 'Nombre_PC', 'Municipio']:
                    fila_dict[col_js] = str(val).strip() if pd.notna(val) else "N/D"
                else:
                    # Es métrica numérica
                    try:
                        # Convertir a float y luego int para quitar decimales .0
                        num_val = int(float(val)) if pd.notna(val) else 0
                        fila_dict[col_js] = num_val
                    except (ValueError, TypeError):
                        fila_dict[col_js] = 0
            
            datos_plazas.append(fila_dict)
            
            # --- PROCESAMIENTO PARA AGRUPACIÓN POR MUNICIPIO ---
            municipio = fila_dict.get('Municipio', 'SIN MUNICIPIO')
            
            # Si el municipio no existe en el acumulador, inicializarlo
            if municipio not in acumulador_municipios:
                acumulador_municipios[municipio] = {
                    'Municipio': municipio
                }
                # Inicializar todas las métricas en 0
                for metrica in metricas_numericas:
                    acumulador_municipios[municipio][metrica] = 0
            
            # Sumar las métricas al municipio correspondiente
            for metrica in metricas_numericas:
                acumulador_municipios[municipio][metrica] += fila_dict.get(metrica, 0)

        # Convertir el diccionario de municipios a lista
        datos_municipios = list(acumulador_municipios.values())
        
        # Ordenar municipios alfabéticamente
        datos_municipios.sort(key=lambda x: x['Municipio'])

        return jsonify({
            "plazas": datos_plazas,
            "municipios": datos_municipios
        })

    except Exception as e:
        logging.error(f"Error en métricas por estado: {e}")
        return jsonify({
            "plazas": [],
            "municipios": []
        })
# ==============================================================================
# ENDPOINT: ANALISIS CN COMPLETO CON PLAZAS ÚNICAS POR CATEGORÍA
# ==============================================================================
@app.route('/api/analisis-cn-script')
def analisis_cn_script():
    """
    Replica la lógica del script 'AnalizadorCN' mejorado:
    1. Totales Globales.
    2. Totales por Mes.
    3. Detalle por Estado con plazas únicas por categoría y porcentajes exclusivos.
    """
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'Base de datos no cargada'}), 503

        # --- 1. CONFIGURACIÓN ---
        mapa_columnas = {
            'cn_inicial': Config.COLUMNA_CN_INICIAL_ACUM,
            'cn_primaria': Config.COLUMNA_CN_PRIM_ACUM,
            'cn_secundaria': Config.COLUMNA_CN_SEC_ACUM,
            'cn_total': Config.COLUMNA_CN_TOT_ACUM
        }
        
        col_clave = Config.COLUMNA_CLAVE
        col_mes = Config.COLUMNA_CVE_MES
        col_estado = Config.COLUMNA_ESTADO

        if col_clave not in df_plazas.columns:
            return jsonify({'error': f'Columna clave {col_clave} no encontrada'}), 500

        # --- 2. PREPARACIÓN DE DATOS ---
        # Definir columnas necesarias
        cols_necesarias = [col_clave] + list(mapa_columnas.values())
        if col_mes in df_plazas.columns: cols_necesarias.append(col_mes)
        if col_estado in df_plazas.columns: cols_necesarias.append(col_estado)
            
        # Filtrar solo columnas que existen en el DF
        cols_existentes = [c for c in cols_necesarias if c in df_plazas.columns]
        df_work = df_plazas[cols_existentes].copy()

        # Convertir métricas a numérico y rellenar nulos con 0
        for key, col_name in mapa_columnas.items():
            if col_name in df_work.columns:
                df_work[f'{key}_num'] = pd.to_numeric(df_work[col_name], errors='coerce').fillna(0)

        # --- 3. CÁLCULOS GLOBALES CON PLAZAS ÚNICAS ---
        resultados_globales = {}
        df_work['tiene_alguna_cn'] = False # Bandera
        df_work['tiene_inicial'] = False
        df_work['tiene_primaria'] = False
        df_work['tiene_secundaria'] = False

        total_plazas_unicas = df_work[col_clave].nunique()

        for key, col_name in mapa_columnas.items():
            if col_name in df_work.columns:
                col_num = f'{key}_num'
                
                # Suma Total
                suma_total = float(df_work[col_num].sum())

                # PLAZAS ÚNICAS CON ACTIVIDAD EN ESTA CATEGORÍA
                # Agrupar por clave y verificar si alguna fila tiene valor > 0
                if key == 'cn_inicial':
                    df_work['tiene_inicial'] = df_work[col_num] > 0
                elif key == 'cn_primaria':
                    df_work['tiene_primaria'] = df_work[col_num] > 0
                elif key == 'cn_secundaria':
                    df_work['tiene_secundaria'] = df_work[col_num] > 0
                
                # Plazas Activas (>0) - Contando claves únicas
                plazas_activas_por_categoria = df_work[df_work[col_num] > 0][col_clave].nunique()
                
                pct = round((plazas_activas_por_categoria / total_plazas_unicas * 100), 1) if total_plazas_unicas > 0 else 0

                resultados_globales[key] = {
                    'suma_total': suma_total,
                    'plazas_con_actividad': plazas_activas_por_categoria,  # PLAZAS ÚNICAS
                    'porcentaje_plazas': pct,
                    'plazas_unicas_con_actividad': plazas_activas_por_categoria  # Clarificar que son únicas
                }

                # Lógica combinada: si tiene Inicial, Primaria o Secundaria
                if key != 'cn_total':
                    df_work['tiene_alguna_cn'] = df_work['tiene_alguna_cn'] | (df_work[col_num] > 0)

        # Total Combinado "Alguna CN" - SIN DUPLICADOS
        plazas_con_alguna = df_work.groupby(col_clave)['tiene_alguna_cn'].any().sum()
        
        resultados_globales['combinado_alguna_cn'] = {
            'plazas_unicas_con_actividad': int(plazas_con_alguna),
            'porcentaje_total': round((plazas_con_alguna / total_plazas_unicas * 100), 1) if total_plazas_unicas > 0 else 0
        }

        # --- 4. DESGLOSE POR ESTADO CON PLAZAS ÚNICAS POR CATEGORÍA ---
        desglose_estados = []
        
        if col_estado in df_work.columns:
            # A) Universo total por estado (Plazas Únicas)
            total_por_estado = df_work.groupby(col_estado)[col_clave].nunique()
            
            # B) Plazas que tienen actividad en ALGUNA categoría (sin duplicados)
            df_activas = df_work[df_work['tiene_alguna_cn']]
            plazas_activas_por_estado = df_activas.groupby(col_estado)[col_clave].nunique()
            
            # C) PLAZAS ÚNICAS POR CATEGORÍA ESPECÍFICA
            # Inicial
            df_inicial = df_work[df_work['tiene_inicial']]
            plazas_inicial_por_estado = df_inicial.groupby(col_estado)[col_clave].nunique()
            
            # Primaria
            df_primaria = df_work[df_work['tiene_primaria']]
            plazas_primaria_por_estado = df_primaria.groupby(col_estado)[col_clave].nunique()
            
            # Secundaria
            df_secundaria = df_work[df_work['tiene_secundaria']]
            plazas_secundaria_por_estado = df_secundaria.groupby(col_estado)[col_clave].nunique()
            
            # D) Sumas de los valores numéricos por estado
            cols_suma = ['cn_inicial_num', 'cn_primaria_num', 'cn_secundaria_num']
            cols_suma_existentes = [c for c in cols_suma if c in df_work.columns]
            sumas_por_estado = df_work.groupby(col_estado)[cols_suma_existentes].sum()

            # E) Construir la lista final con TODA la información necesaria
            for estado in total_por_estado.index:
                
                # 1. Totales y conteos básicos
                total = int(total_por_estado.get(estado, 0))
                con_actividad = int(plazas_activas_por_estado.get(estado, 0))
                pct_actividad = round((con_actividad / total * 100), 1) if total > 0 else 0

                # 2. Plazas únicas por categoría específica
                plazas_inicial = int(plazas_inicial_por_estado.get(estado, 0))
                plazas_primaria = int(plazas_primaria_por_estado.get(estado, 0))
                plazas_secundaria = int(plazas_secundaria_por_estado.get(estado, 0))
                
                # 3. Porcentajes por categoría (sobre total de plazas del estado)
                pct_inicial = round((plazas_inicial / total * 100), 1) if total > 0 else 0
                pct_primaria = round((plazas_primaria / total * 100), 1) if total > 0 else 0
                pct_secundaria = round((plazas_secundaria / total * 100), 1) if total > 0 else 0
                
                # 4. Porcentajes por categoría (sobre plazas con actividad del estado)
                pct_inicial_activas = round((plazas_inicial / con_actividad * 100), 1) if con_actividad > 0 else 0
                pct_primaria_activas = round((plazas_primaria / con_actividad * 100), 1) if con_actividad > 0 else 0
                pct_secundaria_activas = round((plazas_secundaria / con_actividad * 100), 1) if con_actividad > 0 else 0

                # 5. Obtener las sumas específicas
                s_inicial = 0.0
                s_primaria = 0.0
                s_secundaria = 0.0

                if estado in sumas_por_estado.index:
                    if 'cn_inicial_num' in sumas_por_estado.columns:
                        s_inicial = float(sumas_por_estado.loc[estado, 'cn_inicial_num'])
                    if 'cn_primaria_num' in sumas_por_estado.columns:
                        s_primaria = float(sumas_por_estado.loc[estado, 'cn_primaria_num'])
                    if 'cn_secundaria_num' in sumas_por_estado.columns:
                        s_secundaria = float(sumas_por_estado.loc[estado, 'cn_secundaria_num'])

                # 6. Agregar al objeto CON TODA LA INFORMACIÓN
                desglose_estados.append({
                    'estado': str(estado),
                    'total_plazas': total,
                    'plazas_con_actividad': con_actividad,
                    'porcentaje': pct_actividad,
                    
                    # PLAZAS ÚNICAS POR CATEGORÍA
                    'plazas_inicial': plazas_inicial,
                    'plazas_primaria': plazas_primaria,
                    'plazas_secundaria': plazas_secundaria,
                    
                    # PORCENTAJES POR CATEGORÍA (sobre total estado)
                    'pct_inicial': pct_inicial,
                    'pct_primaria': pct_primaria,
                    'pct_secundaria': pct_secundaria,
                    
                    # PORCENTAJES POR CATEGORÍA (sobre plazas activas)
                    'pct_inicial_activas': pct_inicial_activas,
                    'pct_primaria_activas': pct_primaria_activas,
                    'pct_secundaria_activas': pct_secundaria_activas,
                    
                    # Sumas de valores (para mantener compatibilidad)
                    'cn_inicial': s_inicial,
                    'cn_primaria': s_primaria,
                    'cn_secundaria': s_secundaria,
                    'cn_total': s_inicial + s_primaria + s_secundaria 
                })

            # Ordenar por defecto: mayor cantidad de plazas con actividad arriba
            desglose_estados.sort(key=lambda x: x['plazas_con_actividad'], reverse=True)

        # --- 5. DESGLOSE POR MES (opcional, mantener igual) ---
        desglose_mensual = []
        if col_mes in df_work.columns:
            df_work['mes_sort'] = pd.to_numeric(df_work[col_mes], errors='coerce').fillna(0)
            grp_mes = df_work.groupby('mes_sort')
            
            for mes_num, grupo in grp_mes:
                if mes_num == 0: continue
                mes_str = str(int(mes_num)).zfill(2)
                
                datos_mes = {
                    'mes': obtener_nombre_mes(mes_str),
                    'numero_mes': mes_str,
                    'total_registros_mes': int(len(grupo))
                }
                
                for key in mapa_columnas.keys():
                    c_num = f'{key}_num'
                    if c_num in grupo.columns:
                        datos_mes[key] = float(grupo[c_num].sum())
                
                desglose_mensual.append(datos_mes)

        # RETORNO FINAL CON ESTRUCTURA MEJORADA
        return jsonify({
            'status': 'success',
            'info_general': {
                'total_plazas_base': int(total_plazas_unicas)
            },
            'analisis_global': resultados_globales,
            'desglose_estados': desglose_estados,
            'desglose_mensual': desglose_mensual,
            'metadatos': {
                'tipo_conteo': 'plazas_unicas',
                'descripcion': 'Cada plaza cuenta solo una vez por categoría',
                'version': '2.0'
            }
        })

    except Exception as e:
        logging.error(f"Error en analisis script: {e}")
        print(f"ERROR API CN: {e}") 
        return jsonify({'status': 'error', 'message': str(e)}), 500
# ==============================================================================
# 19. PUNTO DE ENTRADA
# ==============================================================================
if __name__ == '__main__':
    if df_plazas is None:
        logging.critical("La aplicación no puede iniciar porque la carga del archivo Excel falló.")
    else:
        logging.info(f"✅ Aplicación Flask iniciada correctamente con {len(df_plazas)} registros")
        app.run(host='0.0.0.0', port=5000, debug=True)