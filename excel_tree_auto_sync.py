import json
import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import re
import pandas as pd
import numpy as np

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIALS_FILE = 'credentials.json'
EXCEL_TREE_FILE = 'excel_tree_real.json'
DRIVE_FOLDER_ID = '1PcAReUcZVNwQGsgUzOieVVraTLRPLCdq'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==============================================================================
# FUNCIONES DE SERIALIZACIÓN SEGURA
# ==============================================================================

def safe_json_serializer(obj):
    """
    Función segura para serializar objetos a JSON que maneja tipos de pandas/numpy
    """
    if pd.isna(obj) or obj is None:
        return None
    elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj) if not np.isnan(obj) else None
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif isinstance(obj, (list, np.ndarray)):
        return [safe_json_serializer(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: safe_json_serializer(value) for key, value in obj.items()}
    elif isinstance(obj, pd.Series):
        return safe_json_serializer(obj.to_list())
    elif hasattr(obj, 'to_dict'):
        return safe_json_serializer(obj.to_dict())
    else:
        try:
            # Intentar serialización normal
            return json.JSONEncoder().default(obj)
        except TypeError:
            # Como último recurso, convertir a string
            return str(obj)

def safe_json_dump(data, filename, **kwargs):
    """
    Versión segura de json.dump que maneja tipos de pandas/numpy
    """
    def default_handler(obj):
        try:
            return safe_json_serializer(obj)
        except Exception as e:
            logging.warning(f"Error serializando objeto {type(obj)}: {e}")
            return str(obj)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=default_handler, **kwargs)

# ==============================================================================
# FUNCIONES DE LIMPIEZA DE DATOS
# ==============================================================================

def clean_data_structure(data):
    """
    Limpia recursivamente una estructura de datos para hacerla JSON serializable
    """
    if isinstance(data, dict):
        return {key: clean_data_structure(value) for key, value in data.items()}
    elif isinstance(data, (list, tuple)):
        return [clean_data_structure(item) for item in data]
    elif isinstance(data, (np.integer, np.int64, np.int32)):
        return int(data)
    elif isinstance(data, (np.floating, np.float64, np.float32)):
        return float(data) if not np.isnan(data) else None
    elif isinstance(data, np.bool_):
        return bool(data)
    elif pd.isna(data) or data is None:
        return None
    elif isinstance(data, (pd.Timestamp, datetime)):
        return data.isoformat()
    else:
        return data

# ==============================================================================
# FUNCIONES DE SERVICIO (sin cambios)
# ==============================================================================

def get_drive_service():
    try:
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        logging.error(f"Error servicio: {e}")
        return None

def list_folder_contents(service, folder_id):
    """Lista TODO el contenido de una carpeta"""
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            pageSize=1000,
            fields="files(id, name, mimeType, size, modifiedTime, webViewLink, webContentLink)"
        ).execute()
        return results.get('files', [])
    except Exception as e:
        logging.error(f"Error listando carpeta {folder_id}: {e}")
        return []

# ==============================================================================
# FUNCIONES DE EXTRACCIÓN DE DATOS (sin cambios)
# ==============================================================================

def extract_month(filename):
    """Extrae el mes del nombre del archivo"""
    filename_lower = filename.lower()
    
    month_map = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12',
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }
    
    for month_name, month_num in month_map.items():
        if month_name in filename_lower:
            return month_num
    
    # Buscar patrones numéricos
    month_match = re.search(r'\b(0?[1-9]|1[0-2])\b', filename_lower)
    if month_match:
        return month_match.group(1).zfill(2)
    
    return None

def extract_year_from_path(path):
    """Extrae el año desde el path o nombre de carpeta"""
    match = re.search(r'(?:Año[_\s]?|)(20\d{2})', path)
    if match:
        return match.group(1)
    return None

# ==============================================================================
# CONSTRUCCIÓN DEL ÁRBOL (sin cambios)
# ==============================================================================

def build_tree_structure(service, folder_id, path="", depth=0):
    """Construye la estructura del árbol de forma recursiva"""
    logging.info(f"{'  ' * depth}📂 Procesando carpeta (profundidad {depth})")
    
    items = list_folder_contents(service, folder_id)
    structure = {
        'name': os.path.basename(path) if path else 'carpeta_abuelo',
        'path': path,
        'type': 'folder',
        'children': []
    }
    
    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            # Es una carpeta - procesar recursivamente
            new_path = os.path.join(path, item['name']).replace('\\', '/')
            child_folder = build_tree_structure(service, item['id'], new_path, depth + 1)
            structure['children'].append(child_folder)
            
        else:
            # Es un archivo - verificar si es Excel
            name_lower = item['name'].lower()
            is_excel = any(name_lower.endswith(ext) for ext in ['.xlsx', '.xls', '.csv'])
            
            if is_excel:
                year = extract_year_from_path(path)
                month = extract_month(item['name'])
                
                file_info = {
                    'name': item['name'],
                    'path': os.path.join(path, item['name']).replace('\\', '/'),
                    'type': 'file',
                    'mimeType': item['mimeType'],
                    'id': item['id'],
                    'size': item.get('size', 0),
                    'modifiedTime': item.get('modifiedTime', ''),
                    'year': year,
                    'month': month,
                    'download_url': f"https://drive.google.com/uc?id={item['id']}&export=download",
                    'view_url': f"https://drive.google.com/file/d/{item['id']}/view"
                }
                
                structure['children'].append(file_info)
                logging.info(f"{'  ' * (depth + 1)}✅ Excel: {item['name']} (año: {year}, mes: {month})")
    
    return structure

# ==============================================================================
# CREACIÓN DE ÍNDICE PLANO (sin cambios)
# ==============================================================================

def flatten_tree_for_index(node, index=None):
    """Convierte el árbol en un índice plano {año-mes: info}"""
    if index is None:
        index = {}
    if node['type'] == 'file' and node.get('year') and node.get('month'):
        key = f"{node['year']}-{node['month']}"
        index[key] = {
            'name': node['name'],
            'id': node['id'],
            'download_url': node['download_url'],
            'view_url': node['view_url']
        }
    elif node['type'] == 'folder':
        for child in node['children']:
            flatten_tree_for_index(child, index)
    return index

# ==============================================================================
# GENERADOR PRINCIPAL (MODIFICADO)
# ==============================================================================

def generate_excel_tree():
    """Función principal que genera el árbol"""
    print("🚀 INICIANDO GENERACIÓN DE ÁRBOL EXCEL...")
    
    service = get_drive_service()
    if not service:
        print("❌ No se pudo conectar a Google Drive")
        return False
    
    try:
        # Verificar que la carpeta existe
        folder_info = service.files().get(fileId=DRIVE_FOLDER_ID).execute()
        print(f"✅ Carpeta encontrada: {folder_info['name']}")
        
        # Construir el árbol
        tree_structure = build_tree_structure(service, DRIVE_FOLDER_ID)
        
        # Contar estadísticas
        def count_stats(node):
            if node['type'] == 'file':
                return 1, [node['year']] if node['year'] else [], [node['month']] if node['month'] else []
            else:
                total_files = 0
                years = []
                months = []
                for child in node['children']:
                    child_files, child_years, child_months = count_stats(child)
                    total_files += child_files
                    years.extend(child_years)
                    months.extend(child_months)
                return total_files, years, months
        
        total_files, all_years, all_months = count_stats(tree_structure)
        
        # Crear metadata - LIMPIAR LOS DATOS ANTES DE SERIALIZAR
        metadata = {
            'generated_at': datetime.now().isoformat(),
            'source_folder_id': DRIVE_FOLDER_ID,
            'source_folder_name': folder_info['name'],
            'statistics': {
                'total_excel_files': total_files,
                'unique_years': sorted(list(set(all_years))),
                'unique_months': sorted(list(set(all_months)))
            },
            'tree': clean_data_structure(tree_structure)  # LIMPIAR ESTRUCTURA
        }

        # Crear índice consultable
        index = flatten_tree_for_index(tree_structure)
        metadata['index'] = clean_data_structure(index)  # LIMPIAR ÍNDICE
        
        # Guardar JSON USANDO LA FUNCIÓN SEGURA
        safe_json_dump(metadata, EXCEL_TREE_FILE)
        
        print(f"✅ Árbol generado exitosamente: {EXCEL_TREE_FILE}")
        print(f"📊 Estadísticas:")
        print(f"   - Archivos Excel: {total_files}")
        print(f"   - Años: {metadata['statistics']['unique_years']}")
        print(f"   - Meses: {metadata['statistics']['unique_months']}")
        print(f"   - Índices generados: {len(index)}")
        
        # Verificación adicional
        print(f"🔍 Verificando serialización...")
        try:
            with open(EXCEL_TREE_FILE, 'r', encoding='utf-8') as f:
                test_data = json.load(f)
            print("✅ JSON verificado y cargado correctamente")
        except Exception as e:
            print(f"❌ Error verificando JSON: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generando árbol: {e}")
        logging.exception("Error detallado:")
        return False

# ==============================================================================
# EJECUCIÓN
# ==============================================================================

if __name__ == "__main__":
    generate_excel_tree()