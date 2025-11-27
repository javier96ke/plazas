import json
import os
import logging
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import io

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================================================
# FUNCIONES DE SERIALIZACIÓN SEGURA PARA FLASK - CORREGIDAS
# ==============================================================================
def safe_json_serialize(obj):
    """
    Convierte objetos de pandas/numpy a tipos nativos de Python para JSON
    COMPLETAMENTE CORREGIDA para manejar arrays de pandas/numpy
    """
    try:
        # Manejar arrays de numpy/pandas PRIMERO - CORRECCIÓN CRÍTICA
        if hasattr(obj, '__array__'):
            # Es un array de numpy o pandas
            try:
                arr = np.array(obj)
                if arr.size == 0:
                    return []
                elif arr.size == 1:
                    # Array con un solo elemento
                    return safe_json_serialize(arr.item())
                else:
                    # Array con múltiples elementos - usar tolist() para conversión segura
                    return [safe_json_serialize(item) for item in arr.tolist()]
            except Exception as e:
                logger.warning(f"⚠️ Error procesando array: {e}")
                return []
        
        # Manejar Series de pandas
        elif isinstance(obj, pd.Series):
            return safe_json_serialize(obj.tolist())
        
        # Manejar DataFrames de pandas
        elif isinstance(obj, pd.DataFrame):
            return safe_json_serialize(obj.to_dict('records'))
        
        # Luego manejar valores NaN/None - CORREGIDO para evitar el error con arrays
        elif obj is None:
            return None
        elif hasattr(obj, 'dtype') and pd.isna(obj):
            return None
        
        # Manejar tipos numéricos de numpy
        elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
            try:
                float_val = float(obj)
                if np.isnan(float_val) or np.isinf(float_val):
                    return 0.0
                # Redondear a 4 decimales para evitar notación científica
                return round(float_val, 4)
            except (ValueError, TypeError):
                return 0.0
        elif isinstance(obj, np.bool_):
            return bool(obj)
        
        # Manejar datetime
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        
        # Manejar listas y tuplas
        elif isinstance(obj, (list, tuple)):
            return [safe_json_serialize(item) for item in obj]
        
        # Manejar diccionarios
        elif isinstance(obj, dict):
            return {key: safe_json_serialize(value) for key, value in obj.items()}
        
        # Manejar objetos con método to_dict
        elif hasattr(obj, 'to_dict'):
            return safe_json_serialize(obj.to_dict())
        
        # Para otros tipos, intentar serialización estándar
        else:
            try:
                # Verificar si es serializable directamente
                json.dumps(obj)
                return obj
            except (TypeError, ValueError):
                # Si falla, convertir a string
                return str(obj)
                
    except Exception as e:
        # En caso de error crítico, devolver representación segura
        logger.warning(f"⚠️ Error en serialización segura: {e}, tipo: {type(obj)}")
        try:
            return str(obj)
        except:
            return "UNSERIALIZABLE_VALUE"
# ==============================================================================
# CLASE DRIVE EXCEL READER
# ==============================================================================

class DriveExcelReader:
    """
    Clase para lectura STRICTA bajo demanda de archivos Excel desde Google Drive
    Adaptada para la estructura REAL del JSON
    """
    
    def __init__(self, tree_file: str = 'excel_tree_real.json'):
        self.tree_file = tree_file
        self.tree_data = None
        self.loaded_excels = {}  # Cache temporal: {(año, mes, filename): (DataFrame, timestamp)}
        self.cache_timeout = 3000  # 5 minutos en cache máximo
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'drive_downloads': 0,
            'errors': 0
        }
        
        # Cargar el árbol al inicializar
        self.load_tree()
    
    def load_tree(self) -> bool:
        """Carga la estructura del árbol desde el JSON - SOLO METADATOS"""
        try:
            if not os.path.exists(self.tree_file):
                logger.error(f"❌ Archivo de árbol no encontrado: {self.tree_file}")
                return False
            
            with open(self.tree_file, 'r', encoding='utf-8') as f:
                self.tree_data = json.load(f)
                self.index = self.tree_data.get("index", {})
            
            logger.info("✅ Árbol de Excel cargado (solo metadatos)")
            
            # Mostrar información de diagnóstico
            stats = self.tree_data.get('statistics', {})
            logger.info(f"📊 Archivos disponibles: {stats.get('total_excel_files', 0)}")
            logger.info(f"📅 Años: {stats.get('unique_years', [])}")
            logger.info(f"📆 Meses: {stats.get('unique_months', [])}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error cargando árbol: {e}")
            return False
    
    def get_available_years(self) -> List[str]:
        """Obtiene los años disponibles - SIN cargar archivos"""
        if not self.tree_data:
            return []
        
        # Usar las estadísticas del JSON
        stats = self.tree_data.get('statistics', {})
        years = stats.get('unique_years', [])
        
        # También buscar en el índice por si acaso
        index = self.tree_data.get('index', {})
        for key in index.keys():
            year = key.split('-')[0]
            if year not in years:
                years.append(year)
        
        return sorted(years, reverse=True)
    
    def get_available_months(self, year: str) -> List[str]:
        """Obtiene los meses disponibles para un año - SIN cargar archivos"""
        if not self.tree_data:
            return []
        
        months = set()
        
        # Buscar en el índice
        index = self.tree_data.get('index', {})
        for key, file_info in index.items():
            if key.startswith(f"{year}-"):
                month = key.split('-')[1]
                months.add(month)
        
        # También buscar en las estadísticas
        stats = self.tree_data.get('statistics', {})
        if year in stats.get('unique_years', []):
            all_months = stats.get('unique_months', [])
            months.update(all_months)
        
        return sorted(list(months))
    
    def find_excel_file(self, year: str, month: str, filename: str = None) -> Optional[Dict]:
        """Busca un archivo Excel específico - SOLO METADATOS"""
        if not self.tree_data:
            return None
        
        # Primero buscar en el índice (más rápido)
        index = self.tree_data.get('index', {})
        key = f"{year}-{month}"
        if key in index:
            file_info = index[key]
            # Si no se especifica filename o coincide
            if filename is None or filename.lower() in file_info.get('name', '').lower():
                return file_info
        
        # Si no se encuentra en el índice, buscar en el árbol
        def search_in_tree(node):
            if isinstance(node, dict):
                # Verificar si es archivo que coincide
                if (node.get('type') == 'file' and
                    node.get('year') == year and
                    node.get('month') == month):
                    
                    # Si no se especifica filename, devolver el primero
                    if filename is None:
                        return node
                    # Si se especifica filename, verificar coincidencia
                    elif filename.lower() in node.get('name', '').lower():
                        return node
                
                # Buscar en hijos
                children = node.get('children', [])
                for child in children:
                    result = search_in_tree(child)
                    if result:
                        return result
            return None
        
        tree = self.tree_data.get('tree', {})
        return search_in_tree(tree)
    
    def get_excel_files_by_date(self, year: str, month: str) -> List[Dict]:
        """Obtiene todos los archivos Excel para fecha - SOLO METADATOS"""
        if not self.tree_data:
            return []
        
        files = []
        
        # Buscar en el índice
        index = self.tree_data.get('index', {})
        key = f"{year}-{month}"
        if key in index:
            file_info = index[key]
            files.append({
                'name': file_info.get('name'),
                'year': year,
                'month': month,
                'size': file_info.get('size'),
                'modifiedTime': file_info.get('modifiedTime'),
                'download_url': file_info.get('download_url'),
                'view_url': file_info.get('view_url'),
                'id': file_info.get('id')
            })
        
        # También buscar en el árbol por si hay múltiples archivos
        def collect_from_tree(node):
            if isinstance(node, dict):
                if (node.get('type') == 'file' and
                    node.get('year') == year and
                    node.get('month') == month):
                    
                    file_meta = {
                        'name': node.get('name'),
                        'year': node.get('year'),
                        'month': node.get('month'),
                        'size': node.get('size'),
                        'modifiedTime': node.get('modifiedTime'),
                        'download_url': node.get('download_url'),
                        'view_url': node.get('view_url'),
                        'id': node.get('id')
                    }
                    # Evitar duplicados
                    if not any(f['id'] == file_meta['id'] for f in files):
                        files.append(file_meta)
                
                # Buscar en hijos
                children = node.get('children', [])
                for child in children:
                    collect_from_tree(child)
        
        tree = self.tree_data.get('tree', {})
        collect_from_tree(tree)
        
        return files

    def _download_excel_from_drive(self, file_info: Dict) -> Optional[pd.DataFrame]:
        """DESCARGA DIRECTA desde Google Drive - SOLO cuando se solicita"""
        try:
            file_id = file_info.get('id')
            if not file_id:
                logger.error("❌ No hay ID de archivo para descargar")
                return None
            
            # URL de descarga directa
            download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
            
            logger.info(f"⬇️ Descargando Excel bajo demanda: {file_info.get('name')}")
            
            # Configurar headers para evitar problemas
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Descargar el archivo
            session = requests.Session()
            response = session.get(download_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Cargar en pandas
            excel_data = pd.read_excel(io.BytesIO(response.content), dtype={'Clave_Plaza': str})
            
            self.stats['drive_downloads'] += 1
            logger.info(f"✅ Excel descargado: {file_info.get('name')} - {excel_data.shape}")
            return excel_data
            
        except Exception as e:
            logger.error(f"❌ Error descargando Excel {file_info.get('name')}: {e}")
            return None
    
    def _clean_old_cache(self):
        """Limpia cache antiguo automáticamente"""
        current_time = datetime.now().timestamp()
        keys_to_remove = []
        
        for key, (df, timestamp) in self.loaded_excels.items():
            if current_time - timestamp > self.cache_timeout:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.loaded_excels[key]
            logger.info(f"🧹 Cache limpiado: {key}")
    
    def load_excel_strict(self, year: str, month: str, filename: str = None) -> Tuple[Optional[pd.DataFrame], Dict]:
        """
        CARGA ESTRICTA bajo demanda - SOLO cuando se solicita explícitamente
        """
        self.stats['total_requests'] += 1
        self._clean_old_cache()
        
        try:
            # Buscar el archivo en el árbol (solo metadatos)
            file_info = self.find_excel_file(year, month, filename)
            if not file_info:
                error_msg = f"Archivo no encontrado: año={year}, mes={month}"
                if filename:
                    error_msg += f", archivo={filename}"
                logger.warning(f"❌ {error_msg}")
                return None, {'error': error_msg, 'status': 'not_found'}
            
            # Clave para el cache
            cache_key = (year, month, file_info['name'])
            current_time = datetime.now().timestamp()
            
            # Verificar cache (con timeout)
            if cache_key in self.loaded_excels:
                df, timestamp = self.loaded_excels[cache_key]
                if current_time - timestamp <= self.cache_timeout:
                    self.stats['cache_hits'] += 1
                    logger.info(f"✅ Excel desde cache: {file_info['name']}")
                    return df, {'status': 'from_cache', 'file_info': file_info}
                else:
                    # Cache expirado, remover
                    del self.loaded_excels[cache_key]
            
            # DESCARGAR bajo demanda
            self.stats['cache_misses'] += 1
            logger.info(f"🔍 Descargando Excel: {file_info['name']}")
            df = self._download_excel_from_drive(file_info)
            
            if df is not None:
                # Guardar en cache temporal
                self.loaded_excels[cache_key] = (df, current_time)
                return df, {'status': 'loaded_from_drive', 'file_info': file_info}
            else:
                self.stats['errors'] += 1
                return None, {'error': 'Error al descargar el Excel', 'status': 'download_error'}
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Error en carga estricta: {e}")
            return None, {'error': str(e), 'status': 'exception'}
    
    def get_excel_info(self, year: str, month: str, filename: str = None) -> Optional[Dict]:
        """Obtiene información de un Excel SIN cargarlo - SOLO METADATOS"""
        file_info = self.find_excel_file(year, month, filename)
        if file_info:
            return {
                'name': file_info.get('name'),
                'year': file_info.get('year'),
                'month': file_info.get('month'),
                'size': file_info.get('size'),
                'modified_time': file_info.get('modifiedTime'),
                'download_url': file_info.get('download_url'),
                'view_url': file_info.get('view_url'),
                'id': file_info.get('id'),
                'available': True
            }
        return None
    
    def query_excel_data_readonly(self, year: str, month: str, filename: str = None, 
                                  query_type: str = 'basic_stats') -> Dict:
        """
        CONSULTA DE SOLO LECTURA - Carga bajo demanda estricta
        """
        # Carga ESTRICTA bajo demanda
        df, load_info = self.load_excel_strict(year, month, filename)
        
        if df is None:
            return {
                'error': load_info.get('error', 'Error desconocido'), 
                'status': 'load_failed',
                'drive_file': load_info.get('file_info', {})
            }
        
        try:
            result = {
                'status': 'success', 
                'query_type': query_type,
                'load_source': load_info.get('status'),
                'drive_file': load_info.get('file_info', {})
            }
            
            if query_type == 'basic_stats':
                result.update({
                    'total_rows': len(df),
                    'total_columns': len(df.columns),
                    'column_names': list(df.columns),
                    'sample_data': df.head(5).fillna('').to_dict('records')
                })
                
            elif query_type == 'claves':
                if 'Clave_Plaza' in df.columns:
                    claves = df['Clave_Plaza'].dropna().unique().tolist()
                    result.update({'claves': claves[:50]})
                else:
                    result.update({'error': 'Columna Clave_Plaza no encontrada'})
                    
            elif query_type == 'estadisticas':
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    result.update({
                        'estadisticas_numericas': df[numeric_cols].describe().to_dict(),
                        'total_registros': len(df),
                        'columnas_numericas': list(numeric_cols)
                    })
                else:
                    result.update({'error': 'No hay columnas numéricas'})
                    
            else:
                result.update({
                    'error': 'Tipo de consulta no válido',
                    'available_queries': ['basic_stats', 'claves', 'estadisticas']
                })
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error en consulta readonly: {e}")
            return {
                'error': str(e), 
                'status': 'query_error',
                'drive_file': load_info.get('file_info', {})
            }
    
    def get_stats(self) -> Dict:
        """Obtiene estadísticas de uso - SIN exponer datos"""
        return {
            'total_requests': self.stats['total_requests'],
            'cache_hits': self.stats['cache_hits'],
            'drive_downloads': self.stats['drive_downloads'],
            'cache_hit_ratio': round(self.stats['cache_hits'] / max(self.stats['total_requests'], 1) * 100, 2),
            'errors': self.stats['errors'],
            'currently_loaded_files': len(self.loaded_excels),
            'cache_timeout_seconds': self.cache_timeout,
            'tree_loaded': self.tree_data is not None
        }

# Instancia global STRICTA de solo lectura
drive_excel_reader_readonly = DriveExcelReader()

# ==============================================================================
# CLASE DRIVE EXCEL COMPARATOR COMPLETAMENTE CORREGIDA
# ==============================================================================

class DriveExcelComparator:
    """
    Módulo completo para comparar períodos acumulativos de archivos Excel de Drive
    """

    def __init__(self, drive_reader=None):
        self.drive_reader = drive_reader or drive_excel_reader_readonly
        logger.info("✅ DriveExcelComparator inicializado con todos los métodos")
        
        # Métricas específicas
        self.METRICAS_CN = [
            'Situación', 'Aten_Ult_mes', 'CN_Inicial_Acum', 
            'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum'
        ]
    
    # ========== MÉTODO NUEVO PARA COMPARACIÓN CON AÑOS DIFERENTES ==========
    
    def comparar_periodos_avanzado_con_años_diferentes(self, year1: str, periodo1: str, year2: str, 
                                                     periodo2: str, filtro_estado: str = None, 
                                                     metricas: List[str] = None) -> Dict:
        """
        Compara períodos de AÑOS DIFERENTES - MÉTODO NUEVO CORREGIDO
        """
        try:
            logger.info(f"🔍 Comparando años diferentes: {year1}-{periodo1} vs {year2}-{periodo2}")
            
            # Cargar ambos períodos de años diferentes
            df1, info1 = self.drive_reader.load_excel_strict(year1, periodo1)
            df2, info2 = self.drive_reader.load_excel_strict(year2, periodo2)
            
            if df1 is None or df2 is None:
                error_msg = f"No se pudieron cargar los períodos: {year1}-{periodo1} o {year2}-{periodo2}"
                if df1 is None:
                    error_msg += f" | Periodo1: {info1.get('error')}"
                if df2 is None:
                    error_msg += f" | Periodo2: {info2.get('error')}"
                return {'status': 'error', 'error': error_msg}
            
            # Aplicar filtro de estado si se especifica
            if filtro_estado and filtro_estado != 'Todos':
                if 'Estado' in df1.columns:
                    df1 = df1[df1['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
                if 'Estado' in df2.columns:
                    df2 = df2[df2['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
            
            # Usar métricas por defecto si no se especifican
            if metricas is None:
                metricas = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum', 'Situación']
            
            # Realizar comparación
            comparacion = self._realizar_comparacion_avanzada(df1, df2, periodo1, periodo2, metricas, filtro_estado)
            
            # Extraer métricas principales
            metricas_principales = self._extraer_metricas_principales(comparacion)
            
            return {
                'status': 'success',
                'year1': year1,
                'year2': year2,
                'periodo1': periodo1,
                'periodo2': periodo2,
                'filtro_estado': filtro_estado,
                'metricas_analizadas': metricas,
                'metricas_principales': metricas_principales,
                'comparacion': comparacion,
                'metadata': {
                    'periodo1_info': info1.get('file_info', {}),
                    'periodo2_info': info2.get('file_info', {})
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error en comparar_periodos_avanzado_con_años_diferentes: {e}")
            return {'status': 'error', 'error': str(e)}
    
    # ========== MÉTODOS ORIGINALES (PRESERVADOS) ==========
    
    def comparar_periodos(self, year: str, periodo1: str, periodo2: str) -> Dict:
        """Compara dos períodos acumulativos (MÉTODO ORIGINAL)"""
        try:
            logger.info(f"🔍 Comparando períodos: {year} - {periodo1} vs {periodo2}")
            
            # Cargar ambos períodos
            df1, info1 = self.drive_reader.load_excel_strict(year, periodo1)
            df2, info2 = self.drive_reader.load_excel_strict(year, periodo2)
            
            if df1 is None or df2 is None:
                error_msg = f"No se pudieron cargar los períodos: {periodo1} o {periodo2}"
                if df1 is None:
                    error_msg += f" | Periodo1: {info1.get('error')}"
                if df2 is None:
                    error_msg += f" | Periodo2: {info2.get('error')}"
                return {'status': 'error', 'error': error_msg}
            
            # Realizar comparación básica
            comparacion = self._realizar_comparacion_basica(df1, df2, periodo1, periodo2)
            
            return {
                'status': 'success',
                'year': year,
                'periodo1': periodo1,
                'periodo2': periodo2,
                'comparacion': comparacion,
                'metadata': {
                    'periodo1_info': info1.get('file_info', {}),
                    'periodo2_info': info2.get('file_info', {})
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error en comparar_periodos: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _realizar_comparacion_basica(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                   nombre1: str, nombre2: str) -> Dict:
        """Realiza comparación básica entre dos DataFrames (MÉTODO ORIGINAL)"""
        try:
            # Estadísticas básicas
            stats_df1 = {
                'total_registros': len(df1),
                'total_columnas': len(df1.columns),
                'columnas': list(df1.columns),
                'registros_unicos': len(df1.drop_duplicates())
            }
            
            stats_df2 = {
                'total_registros': len(df2),
                'total_columnas': len(df2.columns),
                'columnas': list(df2.columns),
                'registros_unicos': len(df2.drop_duplicates())
            }
            
            # Diferencias
            diferencias = {
                'diferencia_registros': len(df2) - len(df1),
                'porcentaje_cambio_registros': round(((len(df2) - len(df1)) / len(df1)) * 100, 2) if len(df1) > 0 else 0,
                'columnas_nuevas': list(set(df2.columns) - set(df1.columns)),
                'columnas_eliminadas': list(set(df1.columns) - set(df2.columns))
            }
            
            # Análisis de claves comunes
            analisis_claves = {}
            if 'Clave_Plaza' in df1.columns and 'Clave_Plaza' in df2.columns:
                claves_df1 = set(df1['Clave_Plaza'].dropna().astype(str))
                claves_df2 = set(df2['Clave_Plaza'].dropna().astype(str))
                
                analisis_claves = {
                    'claves_comunes': len(claves_df1.intersection(claves_df2)),
                    'claves_solo_periodo1': len(claves_df1 - claves_df2),
                    'claves_solo_periodo2': len(claves_df2 - claves_df1),
                    'total_claves_unicas': len(claves_df1.union(claves_df2))
                }
            
            return {
                'estadisticas_periodo1': stats_df1,
                'estadisticas_periodo2': stats_df2,
                'diferencias': diferencias,
                'analisis_claves': analisis_claves,
                'resumen': f"Comparación: {nombre1} ({len(df1)} reg) vs {nombre2} ({len(df2)} reg)"
            }
            
        except Exception as e:
            logger.error(f"❌ Error en comparación básica: {e}")
            return {'error': str(e)}
    
    def generar_cn_resumen_comparativo(self, year: str, periodo1: str, periodo2: str) -> Dict:
        """Genera resumen comparativo de CN (MÉTODO ORIGINAL)"""
        try:
            resultado = self.comparar_periodos(year, periodo1, periodo2)
            if resultado.get('status') == 'error':
                return {'error': resultado.get('error')}
            
            comparacion = resultado.get('comparacion', {})
            
            # Enriquecer con análisis específico de CN
            resumen_cn = {
                'comparacion_general': {
                    'periodo1': periodo1,
                    'periodo2': periodo2,
                    'cambio_total_registros': comparacion.get('diferencias', {}).get('diferencia_registros', 0),
                    'porcentaje_cambio': comparacion.get('diferencias', {}).get('porcentaje_cambio_registros', 0)
                },
                'estadisticas_avanzadas': comparacion.get('analisis_claves', {}),
                'metadata': resultado.get('metadata', {})
            }
            
            return resumen_cn
            
        except Exception as e:
            logger.error(f"❌ Error en resumen CN comparativo: {e}")
            return {'error': str(e)}
    
    def top_estados_cn_comparativo(self, year: str, periodo1: str, periodo2: str, 
                                 metric: str = 'inicial', n: int = 5) -> Dict:
        """Top estados comparativo por métrica CN (MÉTODO ORIGINAL)"""
        try:
            # Cargar datos
            df1, _ = self.drive_reader.load_excel_strict(year, periodo1)
            df2, _ = self.drive_reader.load_excel_strict(year, periodo2)
            
            if df1 is None or df2 is None:
                return {'error': 'No se pudieron cargar los períodos para análisis de estados'}
            
            # Análisis básico por estado
            top_estados = {}
            if 'Estado' in df1.columns and 'Estado' in df2.columns:
                conteo_estados1 = df1['Estado'].value_counts().head(n).to_dict()
                conteo_estados2 = df2['Estado'].value_counts().head(n).to_dict()
                
                top_estados = {
                    f'top_{n}_periodo1': {str(k): int(v) for k, v in conteo_estados1.items()},
                    f'top_{n}_periodo2': {str(k): int(v) for k, v in conteo_estados2.items()},
                    'metric': metric,
                    'n': n
                }
            
            return top_estados
            
        except Exception as e:
            logger.error(f"❌ Error en top estados comparativo: {e}")
            return {'error': str(e)}
    
    def cargar_periodo_acumulado(self, year: str, periodo: str) -> Optional[pd.DataFrame]:
        """Carga un período acumulado específico (MÉTODO ORIGINAL)"""
        try:
            df, info = self.drive_reader.load_excel_strict(year, periodo)
            return df
        except Exception as e:
            logger.error(f"❌ Error cargando período {periodo}: {e}")
            return None
    
    def _comparar_estadisticas_generales(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Compara estadísticas generales entre dos DataFrames (MÉTODO ORIGINAL)"""
        try:
            return {
                'cambios_dimensiones': {
                    'filas_periodo1': len(df1),
                    'filas_periodo2': len(df2),
                    'diferencia_filas': len(df2) - len(df1),
                    'columnas_periodo1': len(df1.columns),
                    'columnas_periodo2': len(df2.columns),
                    'columnas_comunes': len(set(df1.columns).intersection(set(df2.columns)))
                }
            }
        except Exception as e:
            logger.error(f"❌ Error comparando estadísticas: {e}")
            return {}
    
    def _comparar_por_estado(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Compara datos por estado entre dos DataFrames (MÉTODO ORIGINAL)"""
        try:
            comparacion_estados = {}
            
            if 'Estado' in df1.columns and 'Estado' in df2.columns:
                estados1 = df1['Estado'].value_counts().to_dict()
                estados2 = df2['Estado'].value_counts().to_dict()
                
                todos_estados = set(estados1.keys()).union(set(estados2.keys()))
                
                comparacion_estados = {
                    'estados_comunes': {},
                    'estados_nuevos': list(set(estados2.keys()) - set(estados1.keys())),
                    'estados_eliminados': list(set(estados1.keys()) - set(estados2.keys()))
                }
                
                for estado in todos_estados:
                    if estado in estados1 and estado in estados2:
                        cambio = estados2[estado] - estados1[estado]
                        porcentaje = (cambio / estados1[estado] * 100) if estados1[estado] > 0 else 100
                        comparacion_estados['estados_comunes'][str(estado)] = {
                            'periodo1': int(estados1[estado]),
                            'periodo2': int(estados2[estado]),
                            'cambio': int(cambio),
                            'porcentaje_cambio': float(round(porcentaje, 2))
                        }
            
            return comparacion_estados
            
        except Exception as e:
            logger.error(f"❌ Error comparando por estado: {e}")
            return {}
    
    def _calcular_resumen_cambios(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Calcula resumen de cambios entre períodos (MÉTODO ORIGINAL)"""
        try:
            cambios_positivos = len(df2) - len(df1) if len(df2) > len(df1) else 0
            cambios_negativos = len(df1) - len(df2) if len(df1) > len(df2) else 0
            
            return {
                'cambios_positivos': int(cambios_positivos),
                'cambios_negativos': int(cambios_negativos),
                'cambio_neto': int(len(df2) - len(df1)),
                'tasa_cambio': float(round(((len(df2) - len(df1)) / len(df1)) * 100, 2)) if len(df1) > 0 else 0.0
            }
        except Exception as e:
            logger.error(f"❌ Error calculando resumen cambios: {e}")
            return {}

    # ========== MÉTODOS CORREGIDOS PARA EXTRACCIÓN DE MÉTRICAS ESPECÍFICAS ==========
    
    def _analizar_cambios_plazas(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Analiza cambios específicos en plazas entre períodos - CORREGIDO"""
        try:
            # Verificar que tenemos la columna Clave_Plaza
            if 'Clave_Plaza' not in df1.columns or 'Clave_Plaza' not in df2.columns:
                return {
                    'plazas_nuevas': 0,
                    'plazas_eliminadas': 0,
                    'error': 'Columna Clave_Plaza no encontrada'
                }
            
            # Obtener conjuntos de claves (convertir a string y limpiar)
            claves_periodo1 = set(df1['Clave_Plaza'].dropna().astype(str).str.strip().str.upper())
            claves_periodo2 = set(df2['Clave_Plaza'].dropna().astype(str).str.strip().str.upper())
            
            # Calcular cambios
            plazas_nuevas = len(claves_periodo2 - claves_periodo1)
            plazas_eliminadas = len(claves_periodo1 - claves_periodo2)
            plazas_comunes = len(claves_periodo1.intersection(claves_periodo2))
            
            return {
                'plazas_nuevas': int(plazas_nuevas),
                'plazas_eliminadas': int(plazas_eliminadas),
                'plazas_comunes': int(plazas_comunes),
                'total_plazas_periodo1': int(len(claves_periodo1)),
                'total_plazas_periodo2': int(len(claves_periodo2)),
                'cambio_neto_plazas': int(len(claves_periodo2) - len(claves_periodo1))
            }
            
        except Exception as e:
            logger.error(f"❌ Error analizando cambios de plazas: {e}")
            return {
                'plazas_nuevas': 0,
                'plazas_eliminadas': 0,
                'error': str(e)
            }

    def _analizar_metricas_cn(self, df1: pd.DataFrame, df2: pd.DataFrame, metricas: List[str]) -> Dict:
        """Analiza métricas CN específicas - CON ENTEROS PARA VALORES"""
        try:
            resultados = {}
            
            for metrica in metricas:
                if metrica in df1.columns and metrica in df2.columns:
                    if pd.api.types.is_numeric_dtype(df1[metrica]) or pd.api.types.is_numeric_dtype(df2[metrica]):
                        # Métricas numéricas (CN_Total, etc.) - USAR ENTEROS
                        total_periodo1 = pd.to_numeric(df1[metrica], errors='coerce').fillna(0).sum()
                        total_periodo2 = pd.to_numeric(df2[metrica], errors='coerce').fillna(0).sum()
                        incremento = total_periodo2 - total_periodo1
                        
                        # CONVERTIR A ENTEROS (redondear hacia arriba para valores positivos)
                        resultados[metrica] = {
                            'periodo1': int(round(total_periodo1)),  # ENTERO
                            'periodo2': int(round(total_periodo2)),  # ENTERO
                            'incremento': int(round(incremento)),    # ENTERO
                            'porcentaje_cambio': float(round((incremento / total_periodo1 * 100), 2)) if total_periodo1 > 0 else 0.0,  # FLOAT para porcentaje
                            'tipo': 'numerica'
                        }
                    else:
                        # Métricas categóricas (Situación, etc.)
                        distribucion1 = df1[metrica].fillna('SIN DATO').astype(str).value_counts().to_dict()
                        distribucion2 = df2[metrica].fillna('SIN DATO').astype(str).value_counts().to_dict()
                        
                        # CONVERTIR a tipos nativos
                        resultados[metrica] = {
                            'distribucion_periodo1': {str(k): int(v) for k, v in distribucion1.items()},
                            'distribucion_periodo2': {str(k): int(v) for k, v in distribucion2.items()},
                            'tipo': 'categorica'
                        }
        
            return resultados
            
        except Exception as e:
            logger.error(f"❌ Error analizando métricas CN: {e}")
            return {'error': str(e)}

    def _calcular_cn_total(self, df: pd.DataFrame) -> int:
        """Calcula CN Total sumando las tres categorías si no existe la columna - DEVUELVE ENTERO"""
        try:
            # Si existe la columna CN_Tot_Acum, usarla
            if 'CN_Tot_Acum' in df.columns:
                return int(round(pd.to_numeric(df['CN_Tot_Acum'], errors='coerce').fillna(0).sum()))
            
            # Si no existe, calcular sumando las tres categorías
            cn_total = 0.0
            for col in ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']:
                if col in df.columns:
                    cn_total += pd.to_numeric(df[col], errors='coerce').fillna(0).sum()
            
            return int(round(cn_total))  # ENTERO
        except Exception as e:
            logger.error(f"❌ Error calculando CN Total: {e}")
            return 0

    def _extraer_metricas_principales(self, comparacion: Dict) -> Dict:
        """Extrae las métricas principales para mostrar en la interfaz - CON ENTEROS"""
        try:
            analisis_plazas = comparacion.get('analisis_plazas', {})
            metricas_globales = comparacion.get('metricas_globales', {})
            
            # Calcular incremento CN Total
            incremento_cn_total = 0
            
            # Intentar obtener de CN_Tot_Acum si existe
            if 'CN_Tot_Acum' in metricas_globales:
                incremento_cn_total = int(round(metricas_globales['CN_Tot_Acum'].get('incremento', 0)))
            else:
                # Calcular sumando las tres categorías
                for col in ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']:
                    if col in metricas_globales:
                        incremento_cn_total += int(round(metricas_globales[col].get('incremento', 0)))
            
            return {
                'plazas_nuevas': analisis_plazas.get('plazas_nuevas', 0),
                'plazas_eliminadas': analisis_plazas.get('plazas_eliminadas', 0),
                'incremento_cn_total': incremento_cn_total,  # ENTERO
                'resumen_cambios': self._generar_resumen_cambios(analisis_plazas, metricas_globales)
            }
        except Exception as e:
            logger.error(f"❌ Error extrayendo métricas principales: {e}")
            return {
                'plazas_nuevas': 0,
                'plazas_eliminadas': 0,
                'incremento_cn_total': 0,
                'resumen_cambios': 'Error calculando métricas'
            }

    def _generar_resumen_cambios(self, analisis_plazas: Dict, metricas_globales: Dict) -> str:
        """Genera resumen textual de cambios - CON ENTEROS EN EL TEXTO"""
        try:
            plazas_nuevas = analisis_plazas.get('plazas_nuevas', 0)
            plazas_eliminadas = analisis_plazas.get('plazas_eliminadas', 0)
            cambio_neto = analisis_plazas.get('cambio_neto_plazas', 0)
            
            partes = []
            
            if plazas_nuevas > 0:
                partes.append(f"{plazas_nuevas} plazas nuevas")
            if plazas_eliminadas > 0:
                partes.append(f"{plazas_eliminadas} plazas eliminadas")
            
            # Información de CN - USAR ENTEROS EN EL TEXTO
            cn_info = []
            for col in ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum']:
                if col in metricas_globales:
                    incremento = metricas_globales[col].get('incremento', 0)
                    if incremento != 0:
                        signo = "+" if incremento > 0 else ""
                        # Mostrar como entero en el texto
                        cn_info.append(f"{col}: {signo}{int(round(incremento))}")
            
            if cn_info:
                partes.append("Cambios CN: " + ", ".join(cn_info))
            
            if not partes:
                return "Sin cambios significativos entre períodos"
            
            return ". ".join(partes) + "."
            
        except Exception as e:
            logger.error(f"❌ Error generando resumen: {e}")
            return "Resumen no disponible"

    def comparar_periodos_avanzado(self, year: str, periodo1: str, periodo2: str, 
                         filtro_estado: str = None, metricas: List[str] = None) -> Dict:
        """Compara dos períodos acumulativos con métricas específicas - CORREGIDO"""
        try:
            logger.info(f"🔍 Comparando períodos avanzado: {year} - {periodo1} vs {periodo2}")
            
            # Cargar ambos períodos
            df1, info1 = self.drive_reader.load_excel_strict(year, periodo1)
            df2, info2 = self.drive_reader.load_excel_strict(year, periodo2)
            
            if df1 is None or df2 is None:
                error_msg = f"No se pudieron cargar los períodos: {periodo1} o {periodo2}"
                if df1 is None:
                    error_msg += f" | Periodo1: {info1.get('error')}"
                if df2 is None:
                    error_msg += f" | Periodo2: {info2.get('error')}"
                return {'status': 'error', 'error': error_msg}
            
            # Aplicar filtro de estado si se especifica
            if filtro_estado and filtro_estado != 'Todos':
                if 'Estado' in df1.columns:
                    df1 = df1[df1['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
                if 'Estado' in df2.columns:
                    df2 = df2[df2['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
            
            # Usar métricas por defecto si no se especifican (CN + Situación)
            if metricas is None:
                metricas = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum', 'Situación']
            
            # Realizar comparación con métricas específicas
            comparacion = self._realizar_comparacion_avanzada(df1, df2, periodo1, periodo2, metricas, filtro_estado)
            
            # EXTRAER MÉTRICAS PRINCIPALES PARA LA INTERFAZ
            metricas_principales = self._extraer_metricas_principales(comparacion)
            
            return {
                'status': 'success',
                'year': year,
                'periodo1': periodo1,
                'periodo2': periodo2,
                'filtro_estado': filtro_estado,
                'metricas_analizadas': metricas,
                'metricas_principales': metricas_principales,  # ¡NUEVO! Datos para la UI
                'comparacion': comparacion,
                'metadata': {
                    'periodo1_info': info1.get('file_info', {}),
                    'periodo2_info': info2.get('file_info', {})
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error en comparar_periodos_avanzado: {e}")
            return {'status': 'error', 'error': str(e)}

    def _realizar_comparacion_avanzada(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                     nombre1: str, nombre2: str, metricas: List[str], 
                                     filtro_estado: str) -> Dict:
        """Realiza comparación avanzada con métricas específicas - CORREGIDO"""
        try:
            # 1. ANÁLISIS DE CLAVES DE PLAZAS (Para Plazas Nuevas/Eliminadas)
            analisis_plazas = self._analizar_cambios_plazas(df1, df2)
            
            # ✅ CORRECCIÓN: Agregar contador de plazas en operación
            analisis_plazas['plazas_operacion_periodo1'] = self._contar_plazas_operacion(df1)
            analisis_plazas['plazas_operacion_periodo2'] = self._contar_plazas_operacion(df2)
            
            # 2. ANÁLISIS DE MÉTRICAS CN (Para Incremento CN Total)
            # Asegurar que tenemos las métricas CN básicas
            metricas_cn = [m for m in ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum'] 
                          if m in df1.columns or m in df2.columns]
            
            # Agregar métricas específicas si se solicitaron
            for metrica in metricas:
                if metrica not in metricas_cn:
                    metricas_cn.append(metrica)
                    
            analisis_cn = self._analizar_metricas_cn(df1, df2, metricas_cn)
            
            # 3. RESUMEN GENERAL
            resumen_general = self._comparar_resumen_general_avanzado(df1, df2)
            
            # 4. Combinar todos los resultados
            resultados = {
                'resumen_general': resumen_general,
                'analisis_plazas': analisis_plazas, 
                'metricas_globales': analisis_cn,
                'analisis_por_estado': self._comparar_por_estado_detallado(df1, df2, metricas_cn),
                'top_cambios': self._analizar_top_cambios(df1, df2, metricas_cn)
            }
            
            # Si hay filtro de estado, agregar análisis específico
            if filtro_estado and filtro_estado != 'Todos':
                resultados['analisis_estado_filtrado'] = self._analizar_estado_especifico(
                    df1, df2, filtro_estado, metricas_cn
                )
            
            return resultados
            
        except Exception as e:
            logger.error(f"❌ Error en comparación avanzada: {e}")
            return {'error': str(e)}
        
    def _contar_plazas_operacion(self, df: pd.DataFrame) -> int:
        """Cuenta las plazas en operación basado en la columna Situación"""
        try:
            if 'Situación' in df.columns:
                # Filtrar plazas con situación "EN OPERACIÓN"
                plazas_operacion = len(df[
                    df['Situación'].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
                ])
                return int(plazas_operacion)
            else:
                # Si no existe la columna, devolver total como fallback
                logger.warning("⚠️ Columna 'Situación' no encontrada, usando total de plazas")
                return int(len(df))
        except Exception as e:
            logger.error(f"❌ Error contando plazas en operación: {e}")
            return int(len(df))  # Fallback seguro

    def _contar_plazas_operacion_estado(self, df_estado: pd.DataFrame) -> int:
        """Cuenta plazas en operación para un estado específico"""
        try:
            if 'Situación' in df_estado.columns:
                plazas_operacion = len(df_estado[
                    df_estado['Situación'].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
                ])
                return int(plazas_operacion)
            else:
                return int(len(df_estado))  # Fallback
        except Exception as e:
            logger.error(f"❌ Error contando plazas operación por estado: {e}")
            return int(len(df_estado))

    def _comparar_resumen_general_avanzado(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Compara resumen general entre períodos - CORREGIDO"""
        return {
            'total_registros_periodo1': int(len(df1)),
            'total_registros_periodo2': int(len(df2)),
            'diferencia_registros': int(len(df2) - len(df1)),
            'porcentaje_cambio_registros': float(round(((len(df2) - len(df1)) / len(df1)) * 100, 2)) if len(df1) > 0 else 0.0,
            'estados_unicos_periodo1': int(df1['Estado'].nunique()) if 'Estado' in df1.columns else 0,
            'estados_unicos_periodo2': int(df2['Estado'].nunique()) if 'Estado' in df2.columns else 0
        }

    def _comparar_por_estado_detallado(self, df1: pd.DataFrame, df2: pd.DataFrame, metricas: List[str]) -> Dict:
        """Compara métricas detalladas por estado - CON PLACAS EN OPERACIÓN"""
        if 'Estado' not in df1.columns or 'Estado' not in df2.columns:
            return {}
        
        estados_comunes = set(df1['Estado'].unique()).intersection(set(df2['Estado'].unique()))
        resultados_estados = {}
        
        for estado in estados_comunes:
            df1_estado = df1[df1['Estado'] == estado]
            df2_estado = df2[df2['Estado'] == estado]
            
            # ✅Contar plazas en operación por estado
            plazas_operacion_p1 = self._contar_plazas_operacion_estado(df1_estado)
            plazas_operacion_p2 = self._contar_plazas_operacion_estado(df2_estado)
            
            metricas_estado = {}
            for metrica in metricas:
                if metrica in df1_estado.columns and metrica in df2_estado.columns:
                    if pd.api.types.is_numeric_dtype(df1_estado[metrica]):
                        total1 = pd.to_numeric(df1_estado[metrica], errors='coerce').fillna(0).sum()
                        total2 = pd.to_numeric(df2_estado[metrica], errors='coerce').fillna(0).sum()
                        cambio = total2 - total1
                        
                        # USAR ENTEROS para valores, FLOAT para porcentajes
                        metricas_estado[metrica] = {
                            'periodo1': int(round(total1)),  # ENTERO
                            'periodo2': int(round(total2)),  # ENTERO
                            'cambio': int(round(cambio)),    # ENTERO
                            'porcentaje_cambio': float(round(((total2 - total1) / total1) * 100, 2)) if total1 > 0 else 0.0  # FLOAT
                        }
    
            #  Incluir plazas en operación en los resultados por estado
            resultados_estados[str(estado)] = {
                'total_plazas_periodo1': int(len(df1_estado)),
                'total_plazas_periodo2': int(len(df2_estado)),
                'plazas_operacion_periodo1': plazas_operacion_p1,  # NUEVO
                'plazas_operacion_periodo2': plazas_operacion_p2,  # NUEVO
                'metricas': metricas_estado
            }
        
        return resultados_estados

    def _analizar_top_cambios(self, df1: pd.DataFrame, df2: pd.DataFrame, metricas: List[str]) -> Dict:
        """Analiza los mayores cambios por estado - CON ENTEROS PARA VALORES"""
        if 'Estado' not in df1.columns or 'Estado' not in df2.columns:
            return {}
        
        cambios_por_estado = {}
        estados_comunes = set(df1['Estado'].unique()).intersection(set(df2['Estado'].unique()))
        
        for estado in estados_comunes:
            df1_estado = df1[df1['Estado'] == estado]
            df2_estado = df2[df2['Estado'] == estado]
            
            cambios_estado = {}
            for metrica in metricas:
                if (metrica in df1_estado.columns and metrica in df2_estado.columns and
                    pd.api.types.is_numeric_dtype(df1_estado[metrica])):
                    
                    total1 = pd.to_numeric(df1_estado[metrica], errors='coerce').fillna(0).sum()
                    total2 = pd.to_numeric(df2_estado[metrica], errors='coerce').fillna(0).sum()
                    cambio = total2 - total1
                    porcentaje = (cambio / total1 * 100) if total1 > 0 else (100 if total2 > 0 else 0)
                    
                    # USAR ENTEROS para valores absolutos, FLOAT para porcentajes
                    cambios_estado[metrica] = {
                        'cambio_absoluto': int(round(cambio)),  # ENTERO
                        'porcentaje_cambio': float(round(porcentaje, 2))  # FLOAT
                    }
            
            # Calcular cambio total ponderado
            cambio_total = sum(abs(cambio['cambio_absoluto']) for cambio in cambios_estado.values())
            cambios_por_estado[str(estado)] = {
                'cambios_metricas': cambios_estado,
                'cambio_total_absoluto': int(round(cambio_total))  # ENTERO
            }
        
        # Ordenar por cambio total absoluto (mayores cambios primero)
        top_cambios = dict(sorted(
            cambios_por_estado.items(), 
            key=lambda x: x[1]['cambio_total_absoluto'], 
            reverse=True
        )[:10])  # Top 10
        
        return top_cambios

    def _analizar_estado_especifico(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                  estado: str, metricas: List[str]) -> Dict:
        """Análisis detallado para un estado específico - CON ENTEROS PARA VALORES"""
        df1_estado = df1[df1['Estado'] == estado]
        df2_estado = df2[df2['Estado'] == estado]
        
        analisis = {
            'estado': estado,
            'resumen_plazas': {
                'periodo1': int(len(df1_estado)),
                'periodo2': int(len(df2_estado)),
                'cambio': int(len(df2_estado) - len(df1_estado))
            },
            'metricas_detalladas': {}
        }
        
        for metrica in metricas:
            if metrica in df1_estado.columns and metrica in df2_estado.columns:
                if pd.api.types.is_numeric_dtype(df1_estado[metrica]):
                    total1 = pd.to_numeric(df1_estado[metrica], errors='coerce').fillna(0).sum()
                    total2 = pd.to_numeric(df2_estado[metrica], errors='coerce').fillna(0).sum()
                    cambio = total2 - total1
                    
                    # USAR ENTEROS para valores, FLOAT para porcentajes
                    analisis['metricas_detalladas'][metrica] = {
                        'periodo1': int(round(total1)),  
                        'periodo2': int(round(total2)),  
                        'cambio': int(round(cambio)),    
                        'porcentaje_cambio': float(round(((total2 - total1) / total1) * 100, 2)) if total1 > 0 else 0.0  # FLOAT
                    }
                else:
                    # Análisis de distribución para métricas categóricas
                    distribucion1 = {str(k): int(v) for k, v in df1_estado[metrica].value_counts().to_dict().items()}
                    distribucion2 = {str(k): int(v) for k, v in df2_estado[metrica].value_counts().to_dict().items()}
                    analisis['metricas_detalladas'][metrica] = {
                        'distribucion_periodo1': distribucion1,
                        'distribucion_periodo2': distribucion2
                    }
        
        return analisis

    def obtener_estados_disponibles(self, year: str, periodo: str) -> List[str]:
        """Obtiene lista de estados disponibles para un período - CORREGIDO"""
        try:
            df, _ = self.drive_reader.load_excel_strict(year, periodo)
            if df is not None and 'Estado' in df.columns:
                estados = sorted([str(e) for e in df['Estado'].dropna().unique().tolist()])
                return ['Todos'] + estados  # Agregar opción "Todos"
            return ['Todos']
        except Exception as e:
            logger.error(f"Error obteniendo estados: {e}")
            return ['Todos']

    def obtener_metricas_disponibles(self, year: str, periodo: str) -> List[str]:
        """Obtiene métricas disponibles para un período - CORREGIDO"""
        try:
            df, _ = self.drive_reader.load_excel_strict(year, periodo)
            if df is not None:
                # Filtrar solo las métricas de interés que existan en el DataFrame
                metricas_encontradas = [m for m in self.METRICAS_CN if m in df.columns]
                return metricas_encontradas
            return self.METRICAS_CN
        except Exception as e:
            logger.error(f"Error obteniendo métricas: {e}")
            return self.METRICAS_CN

# Instancia global del comparador
drive_excel_comparator = DriveExcelComparator(drive_excel_reader_readonly)

# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

def obtener_años_desde_arbol_json() -> Tuple[List[str], Dict[str, List[str]]]:
    """Obtiene años y meses disponibles desde el árbol JSON"""
    try:
        reader = drive_excel_reader_readonly

        años_set = set()
        meses_por_año: Dict[str, set] = {}

        # Usar INDEX como verdad absoluta (clave: 'YYYY-MM')
        if hasattr(reader, "index") and isinstance(reader.index, dict):
            for clave in reader.index.keys():
                partes = str(clave).split("-")
                if len(partes) < 2:
                    continue

                año, mes = partes[0], partes[1]

                años_set.add(año)

                if año not in meses_por_año:
                    meses_por_año[año] = set()

                meses_por_año[año].add(mes)

        # Convertir sets → listas ordenadas
        años = sorted(años_set)
        meses_por_año = {
            año: sorted(list(meses))
            for año, meses in meses_por_año.items()
        }

        return años, meses_por_año

    except Exception as e:
        logger.error(f"Error obteniendo años desde árbol: {e}")
        return [], {}

def obtener_nombre_mes(numero_mes: str) -> str:
    """Convierte número de mes a nombre"""
    meses = {
        '01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril',
        '05': 'Mayo', '06': 'Junio', '07': 'Julio', '08': 'Agosto',
        '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'
    }
    return meses.get(numero_mes, f"Mes {numero_mes}")

def get_loaded_files_count(self) -> int:
    """Obtiene el número de archivos actualmente en cache"""
    return len(self.loaded_excels)

def clear_all_cache(self):
    """Limpia todo el cache de archivos cargados"""
    self.loaded_excels.clear()
    logger.info("🧹 Cache completamente limpiado")
    
logger.info("✅ Módulo drive_excel_reader completo CORREGIDO - SIN RUTAS FLASK")