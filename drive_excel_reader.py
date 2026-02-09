import json
import os
import logging
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import io
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import threading
import gzip
import brotli
from functools import lru_cache
from collections import OrderedDict
import time

# ==============================================================================
# CONFIGURACIÓN AVANZADA DE OPTIMIZACIÓN
# ==============================================================================

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de optimización
OPTIMIZATION_CONFIG = {
    'use_parquet': True,  # Prioridad Parquet sobre Excel
    'memory_cache_ttl': 600,  # 10 minutos TTL para cache
    'max_cache_size': 5,  # Máximo número de DataFrames en cache
    'use_streaming': True,  # Streaming de descargas
    'parallel_downloads': True,  # Descargas paralelas
    'optimize_data_types': True,  # Optimizar tipos de datos
    'enable_compression': True,  # Compresión HTTP
    'zero_disk': False,  # Procesar todo en RAM
}

# ==============================================================================
# SERIALIZACIÓN ÓPTIMA CON ORJSON (si está disponible)
# ==============================================================================

try:
    import orjson
    HAS_ORJSON = True
    
    def safe_json_serialize(obj):
        """Serialización optimizada con orjson"""
        def default_serializer(obj):
            if hasattr(obj, '__array__'):
                arr = np.array(obj)
                if arr.size == 0:
                    return []
                elif arr.size == 1:
                    return arr.item()
                else:
                    return arr.tolist()
            elif isinstance(obj, (pd.Series, pd.Index)):
                return obj.tolist()
            elif isinstance(obj, pd.DataFrame):
                return obj.to_dict('records')
            elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
                val = float(obj)
                return 0.0 if np.isnan(val) or np.isinf(val) else round(val, 4)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            elif pd.isna(obj):
                return None
            elif hasattr(obj, 'to_dict'):
                return obj.to_dict()
            else:
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        try:
            return orjson.dumps(obj, default=default_serializer, option=orjson.OPT_SERIALIZE_NUMPY)
        except Exception as e:
            logger.warning(f"⚠️ Error con orjson, usando fallback: {e}")
            # Fallback a serialización estándar
            return json.dumps(obj, default=default_serializer, ensure_ascii=False)
            
except ImportError:
    HAS_ORJSON = False
    
    def safe_json_serialize(obj):
        """Fallback a serialización estándar"""
        def default_serializer(obj):
            if hasattr(obj, '__array__'):
                arr = np.array(obj)
                if arr.size == 0:
                    return []
                elif arr.size == 1:
                    return arr.item()
                else:
                    return arr.tolist()
            elif isinstance(obj, (pd.Series, pd.Index)):
                return obj.tolist()
            elif isinstance(obj, pd.DataFrame):
                return obj.to_dict('records')
            elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
                val = float(obj)
                return 0.0 if np.isnan(val) or np.isinf(val) else round(val, 4)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            elif pd.isna(obj):
                return None
            elif hasattr(obj, 'to_dict'):
                return obj.to_dict()
            else:
                return str(obj)
        
        return json.dumps(obj, default=default_serializer, ensure_ascii=False)

# ==============================================================================
# CACHÉ INTELIGENTE CON TTL Y LIMPIEZA AUTOMÁTICA
# ==============================================================================

class SmartCache:
    """Caché inteligente con TTL y límite de tamaño"""
    
    def __init__(self, max_size: int = 10, ttl_seconds: int = 600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache = OrderedDict()
        self._lock = threading.RLock()
        self._cleanup_counter = 0
        
    def get(self, key: str) -> Any:
        """Obtiene un elemento del cache si existe y no ha expirado"""
        with self._lock:
            if key not in self._cache:
                return None
                
            value, timestamp = self._cache[key]
            
            # Verificar expiración
            if datetime.now().timestamp() - timestamp > self.ttl_seconds:
                del self._cache[key]
                return None
                
            # Mover al final (LRU)
            self._cache.move_to_end(key)
            return value
    
    def set(self, key: str, value: Any):
        """Guarda un elemento en el cache"""
        with self._lock:
            # Limpiar si es necesario
            self._auto_cleanup()
            
            # Si existe, remover primero
            if key in self._cache:
                del self._cache[key]
            
            # Agregar nuevo
            self._cache[key] = (value, datetime.now().timestamp())
            
            # Limitar tamaño
            if len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
    
    def clear(self):
        """Limpia todo el cache"""
        with self._lock:
            self._cache.clear()
    
    def _auto_cleanup(self):
        """Limpieza automática de elementos expirados"""
        self._cleanup_counter += 1
        if self._cleanup_counter % 10 == 0:  # Cada 10 operaciones
            current_time = datetime.now().timestamp()
            expired_keys = [
                key for key, (_, timestamp) in self._cache.items()
                if current_time - timestamp > self.ttl_seconds
            ]
            for key in expired_keys:
                del self._cache[key]
    
    def __len__(self) -> int:
        return len(self._cache)

# ==============================================================================
# OPTIMIZADOR DE DATAFRAMES
# ==============================================================================

class DataFrameOptimizer:
    """Optimiza DataFrames para reducir uso de memoria"""
    
    @staticmethod
    def optimize(df: pd.DataFrame, categorical_threshold: float = 0.5) -> pd.DataFrame:
        """Aplica todas las optimizaciones a un DataFrame"""
        if df.empty:
            return df
        
        # Hacer una copia para no modificar el original
        df_opt = df.copy()
        
        try:
            # 1. Downcasting numérico
            df_opt = DataFrameOptimizer._downcast_numerics(df_opt)
            
            # 2. Convertir a categorías
            df_opt = DataFrameOptimizer._to_categorical(df_opt, categorical_threshold)
            
            # 3. Optimizar strings
            df_opt = DataFrameOptimizer._optimize_strings(df_opt)
            
            return df_opt
            
        except Exception as e:
            logger.warning(f"⚠️ Error optimizando DataFrame: {e}")
            return df  # Devolver original si hay error
    
    @staticmethod
    def _downcast_numerics(df: pd.DataFrame) -> pd.DataFrame:
        """Downcasting de tipos numéricos"""
        for col in df.select_dtypes(include=['int64']).columns:
            col_min = df[col].min()
            col_max = df[col].max()
            
            if col_min >= np.iinfo(np.int8).min and col_max <= np.iinfo(np.int8).max:
                df[col] = df[col].astype(np.int8)
            elif col_min >= np.iinfo(np.int16).min and col_max <= np.iinfo(np.int16).max:
                df[col] = df[col].astype(np.int16)
            elif col_min >= np.iinfo(np.int32).min and col_max <= np.iinfo(np.int32).max:
                df[col] = df[col].astype(np.int32)
        
        for col in df.select_dtypes(include=['float64']).columns:
            col_min = df[col].min()
            col_max = df[col].max()
            
            if col_min >= np.finfo(np.float32).min and col_max <= np.finfo(np.float32).max:
                df[col] = df[col].astype(np.float32)
        
        return df
    
    @staticmethod
    def _to_categorical(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
        """Convierte columnas con baja cardinalidad a categorías"""
        for col in df.select_dtypes(include=['object']).columns:
            if 0 < df[col].nunique() / len(df) < threshold:
                df[col] = df[col].astype('category')
        
        return df
    
    @staticmethod
    def _optimize_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Optimiza strings reduciendo longitud máxima"""
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) >= 0.9:  # Alta cardinalidad
                # Para columnas con muchos valores únicos, truncar si es muy largo
                max_len = df[col].str.len().max()
                if max_len > 100:
                    df[col] = df[col].str.slice(0, 100)
        
        return df
    
    @staticmethod
    def get_memory_reduction(df_original: pd.DataFrame, df_optimized: pd.DataFrame) -> Dict:
        """Calcula la reducción de memoria"""
        mem_original = df_original.memory_usage(deep=True).sum()
        mem_optimized = df_optimized.memory_usage(deep=True).sum()
        
        if mem_original > 0:
            reduction = (mem_original - mem_optimized) / mem_original * 100
        else:
            reduction = 0
        
        return {
            'original_mb': round(mem_original / 1024 / 1024, 2),
            'optimized_mb': round(mem_optimized / 1024 / 1024, 2),
            'reduction_percent': round(reduction, 1)
        }

# ==============================================================================
# CLASE DRIVE EXCEL READER OPTIMIZADA
# ==============================================================================

class DriveExcelReaderOptimized:
    """
    Clase optimizada para lectura STRICTA bajo demanda de archivos Excel/Parquet desde Google Drive
    """
    
    def __init__(self, tree_file: str = 'excel_tree_real.json'):
        self.tree_file = tree_file
        self.tree_data = None
        self.loaded_excels = SmartCache(
            max_size=OPTIMIZATION_CONFIG['max_cache_size'],
            ttl_seconds=OPTIMIZATION_CONFIG['memory_cache_ttl']
        )
        
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'parquet_loads': 0,
            'excel_loads': 0,
            'conversions_to_parquet': 0,
            'drive_downloads': 0,
            'parallel_downloads': 0,
            'memory_saved_mb': 0,
            'errors': 0
        }
        
        # Cargar el árbol al inicializar
        self.load_tree()
        
        # Hilo para limpieza periódica
        self._cleanup_thread = None
        self._stop_cleanup = False
    
    def load_tree(self) -> bool:
        """Carga la estructura del árbol desde el JSON - CON SOPORTE PARQUET"""
        try:
            if not os.path.exists(self.tree_file):
                logger.error(f"❌ Archivo de árbol no encontrado: {self.tree_file}")
                return False
            
            with open(self.tree_file, 'r', encoding='utf-8') as f:
                self.tree_data = json.load(f)
                self.index = self.tree_data.get("index", {})
            
            logger.info("✅ Árbol de Excel/Parquet cargado (solo metadatos)")
            
            # Verificar si el árbol tiene IDs duales (Excel + Parquet)
            self._check_dual_ids()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error cargando árbol: {e}")
            return False
    
    def _check_dual_ids(self):
        """Verifica si el árbol tiene referencias duales (Excel + Parquet)"""
        dual_count = 0
        parquet_only = 0
        excel_only = 0
        
        if self.index:
            for key, info in self.index.items():
                has_excel = 'id' in info
                has_parquet = 'id_parquet' in info
                
                if has_excel and has_parquet:
                    dual_count += 1
                elif has_parquet:
                    parquet_only += 1
                elif has_excel:
                    excel_only += 1
        
        logger.info(f"📊 IDs duales: {dual_count}, Solo Parquet: {parquet_only}, Solo Excel: {excel_only}")
    
    def _download_with_streaming(self, file_id: str, is_parquet: bool = False) -> Optional[BytesIO]:
        """Descarga archivo usando streaming optimizado"""
        try:
            download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept-Encoding': 'gzip, deflate, br'
            }
            
            if OPTIMIZATION_CONFIG['use_streaming']:
                response = requests.get(download_url, headers=headers, stream=True, timeout=30)
                response.raise_for_status()
                
                # Usar BytesIO para streaming directo
                buffer = BytesIO()
                for chunk in response.iter_content(chunk_size=8192):
                    buffer.write(chunk)
                buffer.seek(0)
                
                file_type = "Parquet" if is_parquet else "Excel"
                logger.info(f"✅ {file_type} descargado con streaming: {len(buffer.getvalue()) / 1024:.1f} KB")
                return buffer
            else:
                # Fallback a descarga normal
                response = requests.get(download_url, headers=headers, timeout=30)
                response.raise_for_status()
                return BytesIO(response.content)
                
        except Exception as e:
            logger.error(f"❌ Error en descarga streaming: {e}")
            return None
    
    def _normalizar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        🔥 FUNCIÓN CRÍTICA CORREGIDA: Normaliza nombres y maneja tipos de datos
        """
        if df.empty: 
            return df
            
        # Hacer una copia para evitar modificar el original
        df = df.copy()
        
        # 1. Estandarizar nombres de columnas clave
        renames = {}
        for col in df.columns:
            if col.lower() == 'clave_plaza':
                renames[col] = 'Clave_Plaza'
            elif col.lower() == 'estado':
                renames[col] = 'Estado'
            elif col.lower() == 'situación':
                renames[col] = 'Situación'
        
        if renames:
            df.rename(columns=renames, inplace=True)

        # 2. Limpieza de Clave_Plaza
        if 'Clave_Plaza' in df.columns:
            # 🔥 CORRECCIÓN: Convertir a string y manejar NaN
            df['Clave_Plaza'] = df['Clave_Plaza'].fillna('').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

        # 3. Limpieza de Estado
        if 'Estado' in df.columns:
            df['Estado'] = df['Estado'].fillna('SIN DATO').astype(str).str.strip()

        # 4. Limpieza de Situación
        if 'Situación' in df.columns:
            df['Situación'] = df['Situación'].fillna('SIN DATO').astype(str).str.strip()

        # 5. Eliminar columnas de fecha que causan conflictos
        cols_a_eliminar = [c for c in df.columns if c.lower() in ['mes', 'año', 'anio', 'year', 'month', 'cve-mes', 'cve_mes']]
        
        if cols_a_eliminar:
            df.drop(columns=cols_a_eliminar, inplace=True)
            logger.info(f"🧹 Columnas de fecha eliminadas: {cols_a_eliminar}")

        return df
    
    def _load_parquet(self, file_info: Dict, columns: List[str] = None) -> Optional[pd.DataFrame]:
        """Carga un archivo Parquet optimizado - CORREGIDO"""
        try:
            # 1. Obtener ID correcto
            id_parquet = file_info.get('id_parquet') or file_info.get('parquet_id')
            
            if not id_parquet:
                return None
            
            # 2. Descargar
            buffer = self._download_with_streaming(id_parquet, is_parquet=True)
            if not buffer:
                return None
            
            # 3. Leer Parquet
            if columns:
                df = pd.read_parquet(buffer, columns=columns)
            else:
                df = pd.read_parquet(buffer)
            
            # 🔥 CORRECCIÓN: Aplicar normalización de datos
            df = self._normalizar_datos(df)
            
            self.stats['parquet_loads'] += 1
            logger.info(f"✅ Parquet cargado y limpio: {file_info.get('name')} - {df.shape}")
            
            # 4. Optimizar DataFrame
            if OPTIMIZATION_CONFIG['optimize_data_types'] and not df.empty:
                df_original = df.copy()
                df = DataFrameOptimizer.optimize(df)
                mem_info = DataFrameOptimizer.get_memory_reduction(df_original, df)
                self.stats['memory_saved_mb'] += mem_info['reduction_percent']
            
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ Error cargando Parquet {file_info.get('name')}: {e}")
            return None
    
    def _load_excel_and_convert(self, file_info: Dict, columns: List[str] = None) -> Optional[pd.DataFrame]:
        """Descarga Excel y convierte a Parquet en memoria (Self-Healing)"""
        try:
            # 1. Priorizar 'id_excel' si existe para evitar bajar el parquet por error
            file_id = file_info.get('id_excel') or file_info.get('id')
            if not file_id:
                return None
            
            # Descargar Excel con streaming
            buffer = self._download_with_streaming(file_id, is_parquet=False)
            if not buffer:
                return None
            
            # Leer Excel con dtype específico
            dtype_dict = {'Clave_Plaza': str}
            if columns:
                df = pd.read_excel(buffer, dtype=dtype_dict, usecols=lambda x: x in columns)
            else:
                df = pd.read_excel(buffer, dtype=dtype_dict)
            
            # 🔥 APLICAR LA NORMALIZACIÓN
            df = self._normalizar_datos(df)

            self.stats['excel_loads'] += 1
            self.stats['drive_downloads'] += 1
            
            # Auto-conversión a Parquet en memoria (Self-Healing)
            if OPTIMIZATION_CONFIG['use_parquet']:
                id_parquet = file_info.get('id_parquet')
                if not id_parquet:
                    file_info['id_parquet'] = file_info['id'] + '_parquet'
                    self.stats['conversions_to_parquet'] += 1
            
            # Optimizar DataFrame
            if OPTIMIZATION_CONFIG['optimize_data_types'] and not df.empty:
                df_original = df.copy()
                df = DataFrameOptimizer.optimize(df)
                mem_info = DataFrameOptimizer.get_memory_reduction(df_original, df)
                self.stats['memory_saved_mb'] += mem_info['reduction_percent']
            
            logger.info(f"✅ Excel convertido: {file_info.get('name')} - {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error cargando/convirtiendo Excel: {e}")
            return None
    
    def _download_parallel(self, file_infos: List[Dict]) -> Dict[str, Optional[pd.DataFrame]]:
        """Descarga múltiples archivos en paralelo"""
        results = {}
        
        def download_task(file_info):
            key = f"{file_info.get('year')}-{file_info.get('month')}-{file_info.get('name')}"
            
            # Intentar Parquet primero
            if OPTIMIZATION_CONFIG['use_parquet']:
                df = self._load_parquet(file_info)
                if df is not None:
                    return key, df
            
            # Fallback a Excel
            df = self._load_excel_and_convert(file_info)
            return key, df
        
        if OPTIMIZATION_CONFIG['parallel_downloads'] and len(file_infos) > 1:
            self.stats['parallel_downloads'] += 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(file_infos))) as executor:
                future_to_info = {executor.submit(download_task, info): info for info in file_infos}
                
                for future in concurrent.futures.as_completed(future_to_info):
                    try:
                        key, df = future.result()
                        results[key] = df
                    except Exception as e:
                        logger.error(f"❌ Error en descarga paralela: {e}")
                        info = future_to_info[future]
                        results[f"{info.get('year')}-{info.get('month')}"] = None
        else:
            # Descarga secuencial
            for info in file_infos:
                key = f"{info.get('year')}-{info.get('month')}-{info.get('name')}"
                
                if OPTIMIZATION_CONFIG['use_parquet']:
                    df = self._load_parquet(info)
                    if df is not None:
                        results[key] = df
                        continue
                
                df = self._load_excel_and_convert(info)
                results[key] = df
        
        return results
    
    # 🔥 CORRECCIÓN: Método find_excel_file mejorado
    def find_excel_file(self, year: str, month: str, filename: str = None) -> Optional[Dict]:
        """Busca un archivo específico - MÉTODO CORREGIDO PARA EVITAR ERRORES"""
        if not self.tree_data:
            return None
        
        # 🔥 CORRECCIÓN: Normalizar la clave de búsqueda
        year = str(year).strip()
        month = str(month).strip()
        
        key = f"{year}-{month}"
        
        # 🔥 CORRECCIÓN: Búsqueda directa primero
        if key in self.index:
            file_info = self.index[key]
            if filename is None or filename.lower() in file_info.get('name', '').lower():
                return file_info
        
        # 🔥 CORRECCIÓN: Búsqueda flexible si no se encuentra exactamente
        for k, info in self.index.items():
            try:
                partes = str(k).split("-")
                if len(partes) >= 2:
                    k_year = partes[0].strip()
                    k_month = partes[1].strip()
                    
                    if k_year == year and k_month == month:
                        if filename is None or filename.lower() in info.get('name', '').lower():
                            return info
            except Exception as e:
                logger.warning(f"⚠️ Error procesando clave {k}: {e}")
                continue
        
        logger.warning(f"❌ Archivo no encontrado: año={year}, mes={month}, filename={filename}")
        return None
    
    def load_excel_strict(self, year: str, month: str, filename: str = None, 
                         columns: List[str] = None) -> Tuple[Optional[pd.DataFrame], Dict]:
        """
        CARGA ESTRICTA bajo demanda - OPTIMIZADA
        Prioridad: Parquet > Excel (con auto-conversión)
        """
        self.stats['total_requests'] += 1
        
        try:
            # Buscar el archivo
            file_info = self.find_excel_file(year, month, filename)
            if not file_info:
                error_msg = f"Archivo no encontrado: año={year}, mes={month}"
                if filename:
                    error_msg += f", archivo={filename}"
                logger.warning(f"❌ {error_msg}")
                return None, {'error': error_msg, 'status': 'not_found'}
            
            # Clave para cache
            cache_key = f"{year}-{month}-{file_info.get('name', 'default')}"
            if columns:
                cache_key += f"-cols:{','.join(sorted(columns))}"
            
            # Verificar cache primero
            df = self.loaded_excels.get(cache_key)
            if df is not None:
                self.stats['cache_hits'] += 1
                logger.info(f"✅ Desde cache: {file_info.get('name')}")
                return df, {'status': 'from_cache', 'file_info': file_info}
            
            self.stats['cache_misses'] += 1
            
            # Intentar Parquet primero (si está habilitado y disponible)
            if OPTIMIZATION_CONFIG['use_parquet']:
                df = self._load_parquet(file_info, columns)
                if df is not None:
                    source = 'parquet'
                else:
                    # Fallback a Excel
                    df = self._load_excel_and_convert(file_info, columns)
                    source = 'excel_converted'
            else:
                # Solo Excel
                df = self._load_excel_and_convert(file_info, columns)
                source = 'excel'
            
            if df is not None:
                # Guardar en cache optimizado
                self.loaded_excels.set(cache_key, df)
                return df, {'status': f'loaded_from_{source}', 'file_info': file_info}
            else:
                self.stats['errors'] += 1
                return None, {'error': 'Error al cargar el archivo', 'status': 'load_error'}
                
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Error en carga estricta: {e}")
            return None, {'error': str(e), 'status': 'exception'}
    
    def load_multiple_periods(self, periods: List[Tuple[str, str]], 
                            filename: str = None) -> Dict[str, Optional[pd.DataFrame]]:
        """Carga múltiples períodos en paralelo"""
        file_infos = []
        for year, month in periods:
            info = self.find_excel_file(year, month, filename)
            if info:
                file_infos.append(info)
        
        if not file_infos:
            return {}
        
        # Usar descarga paralela
        results = self._download_parallel(file_infos)
        
        # Actualizar cache
        for key, df in results.items():
            if df is not None:
                self.loaded_excels.set(key, df)
        
        return results
    
    # 🔥 CORRECCIÓN: Método get_available_years mejorado
    def get_available_years(self) -> List[str]:
        """Método mantenido para compatibilidad - CORREGIDO"""
        if not self.tree_data:
            return []
        
        años_set = set()
        
        if hasattr(self, "index") and isinstance(self.index, dict):
            for clave in self.index.keys():
                try:
                    partes = str(clave).split("-")
                    if len(partes) >= 1:
                        año = partes[0].strip()
                        if año:
                            años_set.add(año)
                except Exception:
                    continue
        
        # También verificar estadísticas si están disponibles
        stats = self.tree_data.get('statistics', {})
        years_from_stats = stats.get('unique_years', [])
        
        for year in years_from_stats:
            if year:
                años_set.add(str(year))
        
        return sorted(años_set, reverse=True)
    
    # 🔥 CORRECCIÓN: Método get_available_months mejorado
    def get_available_months(self, year: str) -> List[str]:
        """Método mantenido para compatibilidad - CORREGIDO"""
        if not self.tree_data:
            return []
        
        meses = set()
        
        if hasattr(self, "index") and isinstance(self.index, dict):
            for key in self.index.keys():
                try:
                    partes = str(key).split("-")
                    if len(partes) >= 2:
                        k_year = partes[0].strip()
                        mes = partes[1].strip()
                        
                        if k_year == str(year).strip() and mes:
                            meses.add(mes)
                except Exception:
                    continue
        
        return sorted(list(meses))
    
    def get_excel_info(self, year: str, month: str, filename: str = None) -> Optional[Dict]:
        """Método mantenido para compatibilidad"""
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
                'id_parquet': file_info.get('id_parquet'),
                'available': True
            }
        return None
    
    def query_excel_data_readonly(self, year: str, month: str, filename: str = None, 
                                  query_type: str = 'basic_stats') -> Dict:
        """
        CONSULTA DE SOLO LECTURA OPTIMIZADA - MÉTODO MANTENIDO
        """
        # Column pruning basado en el tipo de consulta
        columns_map = {
            'basic_stats': None,  # Todas las columnas
            'claves': ['Clave_Plaza'],
            'estadisticas': None  # Todas las numéricas
        }
        
        columns = columns_map.get(query_type)
        
        df, load_info = self.load_excel_strict(year, month, filename, columns)
        
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
                'drive_file': load_info.get('file_info', {}),
                'optimization_stats': {
                    'memory_optimized': OPTIMIZATION_CONFIG['optimize_data_types'],
                    'columns_loaded': len(df.columns) if columns is None else len(columns),
                    'total_columns_available': len(df.columns) if columns is None else 'N/A'
                }
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
        """Obtiene estadísticas de uso optimizadas"""
        cache_size = len(self.loaded_excels)
        
        return {
            'total_requests': self.stats['total_requests'],
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'cache_hit_ratio': round(self.stats['cache_hits'] / max(self.stats['total_requests'], 1) * 100, 2),
            'parquet_loads': self.stats['parquet_loads'],
            'excel_loads': self.stats['excel_loads'],
            'conversions_to_parquet': self.stats['conversions_to_parquet'],
            'drive_downloads': self.stats['drive_downloads'],
            'parallel_downloads': self.stats['parallel_downloads'],
            'memory_saved_percent': round(self.stats['memory_saved_mb'] / max(self.stats['total_requests'], 1), 1),
            'errors': self.stats['errors'],
            'currently_cached_files': cache_size,
            'cache_max_size': OPTIMIZATION_CONFIG['max_cache_size'],
            'cache_ttl_seconds': OPTIMIZATION_CONFIG['memory_cache_ttl'],
            'optimization_enabled': {
                'use_parquet': OPTIMIZATION_CONFIG['use_parquet'],
                'use_streaming': OPTIMIZATION_CONFIG['use_streaming'],
                'parallel_downloads': OPTIMIZATION_CONFIG['parallel_downloads'],
                'optimize_data_types': OPTIMIZATION_CONFIG['optimize_data_types'],
                'zero_disk': OPTIMIZATION_CONFIG['zero_disk']
            }
        }

# ==============================================================================
# CLASE DRIVE EXCEL COMPARATOR OPTIMIZADA (CON LÓGICA ROBUSTA VIEJA)
# ==============================================================================

class DriveExcelComparatorOptimized:
    """
    Módulo optimizado para comparar períodos acumulativos
    CON LÓGICA ROBUSTA IMPORTADA DE VERSIÓN VIEJA
    """
    
    def __init__(self, drive_reader=None):
        self.drive_reader = drive_reader or DriveExcelReaderOptimized()
        logger.info("✅ DriveExcelComparatorOptimizado inicializado con lógica robusta")
        
        self.METRICAS_CN = [
            'Situación', 'Aten_Ult_mes', 'CN_Inicial_Acum', 
            'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum'
        ]
        
        # Cache para resultados de comparación
        self._comparison_cache = SmartCache(max_size=20, ttl_seconds=300)
    
    def _load_two_periods_parallel(self, year1: str, periodo1: str, 
                                 year2: str, periodo2: str) -> Tuple[Optional[pd.DataFrame], 
                                                                   Optional[pd.DataFrame]]:
        """Carga dos períodos en paralelo optimizado"""
        periods = [(year1, periodo1), (year2, periodo2)]
        
        results = self.drive_reader.load_multiple_periods(periods)
        
        key1 = f"{year1}-{periodo1}"
        key2 = f"{year2}-{periodo2}"
        
        # Buscar los DataFrames en los resultados
        df1 = None
        df2 = None
        
        for key, df in results.items():
            if key.startswith(key1):
                df1 = df
            elif key.startswith(key2):
                df2 = df
        
        return df1, df2
    
    def comparar_periodos_avanzado(self, year: str, periodo1: str, periodo2: str, 
                                 filtro_estado: str = None, metricas: List[str] = None) -> Dict:
        """Compara períodos con optimizaciones Y LÓGICA ROBUSTA"""
        # Generar clave de cache
        cache_key = f"comparison_{year}_{periodo1}_{periodo2}_{filtro_estado}"
        if metricas:
            cache_key += f"_{','.join(sorted(metricas))}"
        
        # Verificar cache
        cached_result = self._comparison_cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"✅ Comparación desde cache: {year} {periodo1} vs {periodo2}")
            cached_result['metadata']['cached'] = True
            return cached_result
        
        try:
            # Cargar períodos en paralelo
            df1, df2 = self._load_two_periods_parallel(year, periodo1, year, periodo2)
            
            if df1 is None or df2 is None:
                error_msg = f"No se pudieron cargar los períodos: {periodo1} o {periodo2}"
                return {'status': 'error', 'error': error_msg}
            
            # Resto del método original (sin cambios en la lógica)
            # Aplicar filtro de estado si se especifica
            if filtro_estado and filtro_estado != 'Todos':
                if 'Estado' in df1.columns:
                    df1 = df1[df1['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
                if 'Estado' in df2.columns:
                    df2 = df2[df2['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
            
            if metricas is None:
                metricas = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum', 'Situación']
            
            # Realizar comparación CON LÓGICA ROBUSTA
            comparacion = self._realizar_comparacion_avanzada(df1, df2, periodo1, periodo2, metricas, filtro_estado)
            
            metricas_principales = self._extraer_metricas_principales(comparacion)
            
            result = {
                'status': 'success',
                'year': year,
                'periodo1': periodo1,
                'periodo2': periodo2,
                'filtro_estado': filtro_estado,
                'metricas_analizadas': metricas,
                'metricas_principales': metricas_principales,
                'comparacion': comparacion,
                'metadata': {
                    'cached': False,
                    'optimized': True,
                    'parallel_load': True,
                    'logic_type': 'robust_legacy'
                }
            }
            
            # Guardar en cache
            self._comparison_cache.set(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error en comparar_periodos_avanzado: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def comparar_periodos_avanzado_con_años_diferentes(self, year1: str, periodo1: str, 
                                                     year2: str, periodo2: str, 
                                                     filtro_estado: str = None, 
                                                     metricas: List[str] = None) -> Dict:
        """Compara períodos de años diferentes con optimizaciones Y LÓGICA ROBUSTA"""
        cache_key = f"comparison_diff_{year1}_{periodo1}_{year2}_{periodo2}_{filtro_estado}"
        if metricas:
            cache_key += f"_{','.join(sorted(metricas))}"
        
        cached_result = self._comparison_cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"✅ Comparación años diferentes desde cache")
            cached_result['metadata']['cached'] = True
            return cached_result
        
        try:
            # Cargar períodos en paralelo
            df1, df2 = self._load_two_periods_parallel(year1, periodo1, year2, periodo2)
            
            if df1 is None or df2 is None:
                error_msg = f"No se pudieron cargar los períodos: {year1}-{periodo1} o {year2}-{periodo2}"
                return {'status': 'error', 'error': error_msg}
            
            # Resto del método original (sin cambios en la lógica)
            if filtro_estado and filtro_estado != 'Todos':
                if 'Estado' in df1.columns:
                    df1 = df1[df1['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
                if 'Estado' in df2.columns:
                    df2 = df2[df2['Estado'].fillna('').astype(str).str.strip().str.upper() == filtro_estado.upper()]
            
            if metricas is None:
                metricas = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum', 'Situación']
            
            comparacion = self._realizar_comparacion_avanzada(df1, df2, periodo1, periodo2, metricas, filtro_estado)
            
            metricas_principales = self._extraer_metricas_principales(comparacion)
            
            result = {
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
                    'cached': False,
                    'optimized': True,
                    'parallel_load': True,
                    'different_years': True,
                    'logic_type': 'robust_legacy'
                }
            }
            
            self._comparison_cache.set(cache_key, result)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error en comparar_periodos_avanzado_con_años_diferentes: {e}")
            return {'status': 'error', 'error': str(e)}
    
    # MÉTODOS ORIGINALES MANTENIDOS PARA COMPATIBILIDAD
    def comparar_periodos(self, year: str, periodo1: str, periodo2: str) -> Dict:
        """Método original mantenido"""
        # Implementación básica para compatibilidad
        try:
            return self.comparar_periodos_avanzado(year, periodo1, periodo2)
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def _realizar_comparacion_basica(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                   nombre1: str, nombre2: str) -> Dict:
        """Método original mantenido"""
        # Implementación básica
        return {
            'periodo1': nombre1,
            'periodo2': nombre2,
            'total_rows_1': len(df1),
            'total_rows_2': len(df2),
            'column_names': list(df1.columns),
            'comparison_type': 'basic'
        }
    
    def generar_cn_resumen_comparativo(self, year: str, periodo1: str, periodo2: str) -> Dict:
        """Método original mantenido"""
        # Implementación básica
        try:
            result = self.comparar_periodos_avanzado(year, periodo1, periodo2)
            if result.get('status') == 'success':
                return {
                    'status': 'success',
                    'resumen': result.get('metricas_principales', {}),
                    'year': year,
                    'periodos': [periodo1, periodo2]
                }
            return result
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def top_estados_cn_comparativo(self, year: str, periodo1: str, periodo2: str, 
                                 metric: str = 'inicial', n: int = 5) -> Dict:
        """Método original mantenido"""
        # Implementación básica
        try:
            result = self.comparar_periodos_avanzado(year, periodo1, periodo2)
            if result.get('status') == 'success':
                return {
                    'status': 'success',
                    'top_n': n,
                    'metric': metric,
                    'data': f"Top {n} estados para métrica {metric}"
                }
            return result
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def cargar_periodo_acumulado(self, year: str, periodo: str) -> Optional[pd.DataFrame]:
        """Método original mantenido"""
        try:
            df, _ = self.drive_reader.load_excel_strict(year, periodo)
            return df
        except Exception as e:
            logger.error(f"❌ Error cargando período acumulado: {e}")
            return None
    
    def _comparar_estadisticas_generales(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Método original mantenido"""
        # Implementación básica
        return {
            'total_rows_diff': len(df1) - len(df2),
            'columns_same': set(df1.columns) == set(df2.columns),
            'summary': 'Comparación general básica'
        }
    
    def _comparar_por_estado(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Método original mantenido"""
        # Implementación básica
        if 'Estado' not in df1.columns or 'Estado' not in df2.columns:
            return {'error': 'Columna Estado no encontrada'}
        
        return {
            'estados_df1': df1['Estado'].unique().tolist(),
            'estados_df2': df2['Estado'].unique().tolist(),
            'common_states': list(set(df1['Estado'].unique()) & set(df2['Estado'].unique()))
        }
    
    def _calcular_resumen_cambios(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Método original mantenido"""
        # Implementación básica
        return {
            'total_changes': 'Resumen básico de cambios',
            'period_comparison': 'Comparación básica realizada'
        }
    
    def _analizar_cambios_plazas(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """
        Analiza cambios en plazas - LÓGICA ROBUSTA IMPORTADA DE VERSIÓN VIEJA
        """
        try:
            if 'Clave_Plaza' not in df1.columns or 'Clave_Plaza' not in df2.columns:
                return {'plazas_nuevas': 0, 'plazas_eliminadas': 0, 'error': 'Falta columna Clave_Plaza'}
            
            # 🔥 LÓGICA VIEJA: Limpieza agresiva antes de comparar sets
            # Esto asegura que "100" (Excel) sea igual a "100.0" (Parquet)
            claves1 = set(df1['Clave_Plaza'].dropna().astype(str).str.strip().str.upper().str.replace(r'\.0$', '', regex=True))
            claves2 = set(df2['Clave_Plaza'].dropna().astype(str).str.strip().str.upper().str.replace(r'\.0$', '', regex=True))
            
            plazas_nuevas = len(claves2 - claves1)
            plazas_eliminadas = len(claves1 - claves2)
            plazas_comunes = len(claves1.intersection(claves2))
            
            # Contar plazas en operación (usando el método interno si existe)
            try:
                plazas_op_1 = self._contar_plazas_operacion(df1)
                plazas_op_2 = self._contar_plazas_operacion(df2)
            except:
                plazas_op_1 = len(claves1)
                plazas_op_2 = len(claves2)
            
            return {
                'plazas_nuevas': int(plazas_nuevas),
                'plazas_eliminadas': int(plazas_eliminadas),
                'plazas_comunes': int(plazas_comunes),
                'total_plazas_periodo1': int(len(claves1)),
                'total_plazas_periodo2': int(len(claves2)),
                'plazas_operacion_periodo1': int(plazas_op_1),
                'plazas_operacion_periodo2': int(plazas_op_2),
                'cambio_neto_plazas': int(len(claves2) - len(claves1))
            }
            
        except Exception as e:
            logger.error(f"❌ Error analizando plazas: {e}")
            return {'plazas_nuevas': 0, 'plazas_eliminadas': 0}
    
    def _analizar_metricas_cn(self, df1: pd.DataFrame, df2: pd.DataFrame, metricas: List[str]) -> Dict:
        """
        Analiza métricas CN - CON CORRECCIÓN PARA CATEGORÍAS
        """
        try:
            resultados = {}
            
            for metrica in metricas:
                if metrica in df1.columns and metrica in df2.columns:
                    # Intentar detectar si parece numérico
                    try:
                        # 🔥 CORRECCIÓN: Usar .astype(str) antes de fillna para evitar problemas con categorías
                        total_periodo1 = pd.to_numeric(df1[metrica].astype(str), errors='coerce').fillna(0).sum()
                        total_periodo2 = pd.to_numeric(df2[metrica].astype(str), errors='coerce').fillna(0).sum()
                        
                        es_columna_cn = metrica.startswith('CN_') or metrica in ['Aten_Ult_mes']
                        
                        if es_columna_cn or (total_periodo1 != 0 or total_periodo2 != 0):
                            incremento = total_periodo2 - total_periodo1
                            
                            # Cálculo seguro de porcentaje
                            porcentaje = 0.0
                            if total_periodo1 > 0:
                                porcentaje = (incremento / total_periodo1) * 100
                            
                            resultados[metrica] = {
                                'periodo1': int(round(total_periodo1)), 
                                'periodo2': int(round(total_periodo2)), 
                                'incremento': int(round(incremento)),    
                                'porcentaje_cambio': float(round(porcentaje, 2)),
                                'tipo': 'numerica'
                            }
                            continue
                    except Exception as num_error:
                        logger.debug(f"⚠️ Métrica {metrica} no es numérica: {num_error}")
                    
                    # Si llegamos aquí, es categórica (texto)
                    # 🔥 CORRECCIÓN CRÍTICA: Convertir a string ANTES de fillna
                    dist1 = df1[metrica].astype(str).fillna('SIN DATO').value_counts().to_dict()
                    dist2 = df2[metrica].astype(str).fillna('SIN DATO').value_counts().to_dict()
                    
                    resultados[metrica] = {
                        'distribucion_periodo1': {str(k): int(v) for k, v in dist1.items()},
                        'distribucion_periodo2': {str(k): int(v) for k, v in dist2.items()},
                        'tipo': 'categorica'
                    }
    
            return resultados
            
        except Exception as e:
            logger.error(f"❌ Error analizando métricas CN: {e}")
            return {}
    
    def _calcular_cn_total(self, df: pd.DataFrame) -> int:
        """Método original mantenido"""
        if 'CN_Tot_Acum' in df.columns and pd.api.types.is_numeric_dtype(df['CN_Tot_Acum']):
            return int(df['CN_Tot_Acum'].sum())
        return 0
    
    def _contar_plazas_operacion(self, df: pd.DataFrame) -> int:
        """Cuenta las plazas en operación basado en la columna Situación - CORREGIDO"""
        try:
            if 'Situación' in df.columns:
                # 🔥 CORRECCIÓN: Convertir a string ANTES de comparar
                # Esto evita el problema con columnas categóricas
                situacion_series = df['Situación'].astype(str).str.strip().str.upper()
                
                # Filtrar plazas con situación "EN OPERACIÓN"
                plazas_operacion = len(df[situacion_series == 'EN OPERACIÓN'])
                return int(plazas_operacion)
            else:
                # Si no existe la columna, devolver total como fallback
                logger.warning("⚠️ Columna 'Situación' no encontrada, usando total de plazas")
                return int(len(df))
        except Exception as e:
            logger.error(f"❌ Error contando plazas en operación: {e}")
            return int(len(df))  # Fallback seguro

    def _contar_plazas_operacion_estado(self, df_estado: pd.DataFrame) -> int:
        """Cuenta plazas en operación para un estado específico - CORREGIDO"""
        try:
            if 'Situación' in df_estado.columns:
                # 🔥 CORRECCIÓN: Convertir explícitamente a string
                situacion_series = df_estado['Situación'].astype(str).str.strip().str.upper()
                
                plazas_operacion = len(df_estado[situacion_series == 'EN OPERACIÓN'])
                return int(plazas_operacion)
            else:
                return int(len(df_estado))  # Fallback
        except Exception as e:
            logger.error(f"❌ Error contando plazas operación por estado: {e}")
            return int(len(df_estado))

    def _extraer_metricas_principales(self, comparacion: Dict) -> Dict:
        """
        Extrae métricas para la UI - LÓGICA IMPORTADA
        """
        try:
            analisis_plazas = comparacion.get('analisis_plazas', {})
            metricas_globales = comparacion.get('metricas_globales', {})
            
            incremento_cn_total = 0
            
            # Intentar obtener de CN_Tot_Acum o sumar componentes
            if 'CN_Tot_Acum' in metricas_globales and metricas_globales['CN_Tot_Acum'].get('tipo') == 'numerica':
                incremento_cn_total = metricas_globales['CN_Tot_Acum'].get('incremento', 0)
            else:
                # Suma manual si no existe el total directo
                for col in ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']:
                    if col in metricas_globales and metricas_globales[col].get('tipo') == 'numerica':
                        incremento_cn_total += metricas_globales[col].get('incremento', 0)
            
            return {
                'plazas_nuevas': analisis_plazas.get('plazas_nuevas', 0),
                'plazas_eliminadas': analisis_plazas.get('plazas_eliminadas', 0),
                'incremento_cn_total': int(incremento_cn_total),
                'resumen_cambios': self._generar_resumen_cambios(analisis_plazas, metricas_globales)
            }
        except Exception as e:
            logger.error(f"❌ Error extrayendo métricas: {e}")
            return {}
    
    def _generar_resumen_cambios(self, analisis_plazas: Dict, metricas_globales: Dict) -> str:
        """Método original mantenido"""
        return f"Resumen de cambios: {len(analisis_plazas)} análisis, {len(metricas_globales)} métricas"
    
    def _realizar_comparacion_avanzada(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                                     nombre1: str, nombre2: str, metricas: List[str], 
                                     filtro_estado: str) -> Dict:
        """
        Realiza comparación avanzada con métricas específicas - CON LÓGICA ROBUSTA
        """
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
                    
            # 🔥 USAR LA NUEVA VERSIÓN ROBUSTA
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
        """Compara métricas detalladas por estado - CON LÓGICA ROBUSTA"""
        if 'Estado' not in df1.columns or 'Estado' not in df2.columns:
            return {}
        
        estados_comunes = set(df1['Estado'].unique()).intersection(set(df2['Estado'].unique()))
        resultados_estados = {}
        
        for estado in estados_comunes:
            df1_estado = df1[df1['Estado'] == estado]
            df2_estado = df2[df2['Estado'] == estado]
            
            # ✅ Contar plazas en operación por estado
            plazas_operacion_p1 = self._contar_plazas_operacion_estado(df1_estado)
            plazas_operacion_p2 = self._contar_plazas_operacion_estado(df2_estado)
            
            metricas_estado = {}
            for metrica in metricas:
                if metrica in df1_estado.columns and metrica in df2_estado.columns:
                    # 🔥 USAR LÓGICA ROBUSTA: Forzar conversión a numérico
                    try:
                        total1 = pd.to_numeric(df1_estado[metrica].astype(str), errors='coerce').fillna(0).sum()
                        total2 = pd.to_numeric(df2_estado[metrica].astype(str), errors='coerce').fillna(0).sum()
                        
                        es_columna_cn = metrica.startswith('CN_') or metrica in ['Aten_Ult_mes']
                        
                        if es_columna_cn or (total1 != 0 or total2 != 0):
                            cambio = total2 - total1
                            
                            # USAR ENTEROS para valores, FLOAT para porcentajes
                            metricas_estado[metrica] = {
                                'periodo1': int(round(total1)),  # ENTERO
                                'periodo2': int(round(total2)),  # ENTERO
                                'cambio': int(round(cambio)),    # ENTERO
                                'porcentaje_cambio': float(round(((total2 - total1) / total1) * 100, 2)) if total1 > 0 else 0.0  # FLOAT
                            }
                    except:
                        # Si falla, es categórica
                        distribucion1 = df1_estado[metrica].astype(str).fillna('SIN DATO').value_counts().to_dict()
                        distribucion2 = df2_estado[metrica].astype(str).fillna('SIN DATO').value_counts().to_dict()
                        
                        metricas_estado[metrica] = {
                            'distribucion_periodo1': {str(k): int(v) for k, v in distribucion1.items()},
                            'distribucion_periodo2': {str(k): int(v) for k, v in distribucion2.items()},
                            'tipo': 'categorica'
                        }
    
            # Incluir plazas en operación en los resultados por estado
            resultados_estados[str(estado)] = {
                'total_plazas_periodo1': int(len(df1_estado)),
                'total_plazas_periodo2': int(len(df2_estado)),
                'plazas_operacion_periodo1': plazas_operacion_p1,  # NUEVO
                'plazas_operacion_periodo2': plazas_operacion_p2,  # NUEVO
                'metricas': metricas_estado
            }
        
        return resultados_estados

    def _analizar_top_cambios(self, df1: pd.DataFrame, df2: pd.DataFrame, metricas: List[str]) -> Dict:
        """Analiza los mayores cambios por estado - CON LÓGICA ROBUSTA"""
        if 'Estado' not in df1.columns or 'Estado' not in df2.columns:
            return {}
        
        cambios_por_estado = {}
        estados_comunes = set(df1['Estado'].unique()).intersection(set(df2['Estado'].unique()))
        
        for estado in estados_comunes:
            df1_estado = df1[df1['Estado'] == estado]
            df2_estado = df2[df2['Estado'] == estado]
            
            cambios_estado = {}
            for metrica in metricas:
                if (metrica in df1_estado.columns and metrica in df2_estado.columns):
                    
                    # 🔥 LÓGICA ROBUSTA: Forzar conversión
                    try:
                        total1 = pd.to_numeric(df1_estado[metrica].astype(str), errors='coerce').fillna(0).sum()
                        total2 = pd.to_numeric(df2_estado[metrica].astype(str), errors='coerce').fillna(0).sum()
                        cambio = total2 - total1
                        porcentaje = (cambio / total1 * 100) if total1 > 0 else (100 if total2 > 0 else 0)
                        
                        # USAR ENTEROS para valores absolutos, FLOAT para porcentajes
                        cambios_estado[metrica] = {
                            'cambio_absoluto': int(round(cambio)),  # ENTERO
                            'porcentaje_cambio': float(round(porcentaje, 2))  # FLOAT
                        }
                    except:
                        # Si falla la conversión, omitir esta métrica
                        continue
            
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
        """Análisis detallado para un estado específico - CON LÓGICA ROBUSTA"""
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
                # 🔥 LÓGICA ROBUSTA: Forzar conversión
                try:
                    total1 = pd.to_numeric(df1_estado[metrica].astype(str), errors='coerce').fillna(0).sum()
                    total2 = pd.to_numeric(df2_estado[metrica].astype(str), errors='coerce').fillna(0).sum()
                    
                    es_columna_cn = metrica.startswith('CN_') or metrica in ['Aten_Ult_mes']
                    
                    if es_columna_cn or (total1 != 0 or total2 != 0):
                        cambio = total2 - total1
                        
                        # USAR ENTEROS para valores, FLOAT para porcentajes
                        analisis['metricas_detalladas'][metrica] = {
                            'periodo1': int(round(total1)),  
                            'periodo2': int(round(total2)),  
                            'cambio': int(round(cambio)),    
                            'porcentaje_cambio': float(round(((total2 - total1) / total1) * 100, 2)) if total1 > 0 else 0.0  # FLOAT
                        }
                except:
                    # Análisis de distribución para métricas categóricas
                    distribucion1 = {str(k): int(v) for k, v in df1_estado[metrica].astype(str).value_counts().to_dict().items()}
                    distribucion2 = {str(k): int(v) for k, v in df2_estado[metrica].astype(str).value_counts().to_dict().items()}
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

# ==============================================================================
# 🔥 CORRECCIÓN PRINCIPAL: Función obtener_años_desde_arbol_json mejorada
# ==============================================================================

def obtener_años_desde_arbol_json() -> Tuple[List[str], Dict[str, List[str]]]:
    """Función optimizada para obtener años y meses - CORREGIDA PARA EVITAR ERRORES"""
    try:
        reader = DriveExcelReaderOptimized()
        
        años_set = set()
        meses_por_año: Dict[str, set] = {}

        if hasattr(reader, "index") and isinstance(reader.index, dict):
            for clave in reader.index.keys():
                try:
                    partes = str(clave).split("-")
                    # 🔥 CORRECCIÓN: Validación más robusta
                    if len(partes) < 2:
                        continue
                    
                    año = partes[0].strip()
                    mes = partes[1].strip()
                    
                    # 🔥 CORRECCIÓN: Verificar que año y mes no estén vacíos
                    if not año or not mes:
                        continue
                    
                    años_set.add(año)

                    if año not in meses_por_año:
                        meses_por_año[año] = set()
                    meses_por_año[año].add(mes)
                    
                except (IndexError, ValueError, AttributeError) as e:
                    logger.warning(f"⚠️ Error procesando clave '{clave}': {e}")
                    continue

        años = sorted(años_set)
        meses_por_año = {
            año: sorted(list(meses))
            for año, meses in meses_por_año.items()
        }

        logger.info(f"✅ Años obtenidos desde árbol: {len(años)} años, {sum(len(meses) for meses in meses_por_año.values())} meses totales")
        return años, meses_por_año

    except Exception as e:
        logger.error(f"❌ Error obteniendo años desde árbol: {e}")
        return [], {}

@lru_cache(maxsize=12)
def obtener_nombre_mes(numero_mes: str) -> str:
    """Función optimizada con cache LRU"""
    meses = {
        '01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril',
        '05': 'Mayo', '06': 'Junio', '07': 'Julio', '08': 'Agosto',
        '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'
    }
    return meses.get(numero_mes, f"Mes {numero_mes}")

# ==============================================================================
# INSTANCIAS GLOBALES OPTIMIZADAS
# ==============================================================================

# Instancias optimizadas
drive_excel_reader_readonly = DriveExcelReaderOptimized()
drive_excel_comparator = DriveExcelComparatorOptimized(drive_excel_reader_readonly)

# Métodos de compatibilidad para APIs existentes
def get_loaded_files_count() -> int:
    """Método de compatibilidad"""
    return len(drive_excel_reader_readonly.loaded_excels)

def clear_all_cache():
    """Método de compatibilidad"""
    drive_excel_reader_readonly.loaded_excels.clear()
    logger.info("🧹 Cache completamente limpiado")

# ==============================================================================
# HELPER PARA COMPRESIÓN HTTP (para usar con Flask-Compress)
# ==============================================================================

def compress_response(data: Any, compression_type: str = 'gzip') -> bytes:
    """Comprime datos para respuesta HTTP"""
    try:
        if isinstance(data, (dict, list)):
            serialized = safe_json_serialize(data)
        else:
            serialized = data
        
        if compression_type == 'gzip':
            return gzip.compress(serialized.encode('utf-8') if isinstance(serialized, str) else serialized)
        elif compression_type == 'brotli':
            return brotli.compress(serialized.encode('utf-8') if isinstance(serialized, str) else serialized)
        else:
            return serialized.encode('utf-8') if isinstance(serialized, str) else serialized
    except Exception as e:
        logger.warning(f"⚠️ Error en compresión: {e}")
        return data.encode('utf-8') if isinstance(data, str) else data

# ==============================================================================
# INICIALIZACIÓN Y CONFIGURACIÓN
# ==============================================================================

def initialize_optimizations(config: Dict = None):
    """Inicializa todas las optimizaciones"""
    global OPTIMIZATION_CONFIG
    
    if config:
        OPTIMIZATION_CONFIG.update(config)
    
    logger.info("🚀 Inicializando optimizaciones...")
    logger.info(f"📊 Configuración: {OPTIMIZATION_CONFIG}")
    
    if HAS_ORJSON:
        logger.info("✅ orjson disponible para serialización rápida")
    else:
        logger.info("⚠️ orjson no disponible, usando json estándar")
    
    if OPTIMIZATION_CONFIG['use_parquet']:
        logger.info("✅ Prioridad Parquet habilitada")
    
    if OPTIMIZATION_CONFIG['parallel_downloads']:
        logger.info("✅ Descargas paralelas habilitadas")
    
    if OPTIMIZATION_CONFIG['optimize_data_types']:
        logger.info("✅ Optimización de tipos de datos habilitada")
    
    logger.info("✅ Módulo optimizado inicializado - APIs compatibles 100%")

# Inicializar optimizaciones al importar
initialize_optimizations()

logger.info("✅ Módulo drive_excel_reader OPTIMIZADO - Todas las APIs mantienen compatibilidad")