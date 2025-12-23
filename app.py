import os
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, jsonify, send_from_directory, url_for
import json
from flask import redirect 
from unidecode import unidecode
from datetime import datetime
from drive_excel_reader import drive_excel_reader_readonly  
from drive_excel_reader import drive_excel_comparator  
from drive_excel_reader import (
    safe_json_serialize,  
    obtener_años_desde_arbol_json,
    obtener_nombre_mes
)

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

class Config:
    """Clase para almacenar todas las variables de configuración."""
    EXCEL_PATH = 'datos_plazas.xlsx'
    IMAGES_BASE_PATH = 'fotos_de_plazas'
    
    COLUMNA_CLAVE = 'Clave_Plaza'
    COLUMNA_ESTADO = 'Estado'
    COLUMNA_COORD_ZONA = 'Coord. Zona'
    COLUMNA_MUNICIPIO = 'Municipio'
    COLUMNA_LOCALIDAD = 'Localidad'
    COLUMNA_LATITUD = 'Latitud'
    COLUMNA_LONGITUD = 'Longitud'
    
    COLUMNA_COLONIA = 'Colonia'
    COLUMNA_CALLE = 'Calle'
    COLUMNA_NUM = 'Num'
    COLUMNA_COD_POST = 'Cod_Post'

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

        df = pd.read_excel(config.EXCEL_PATH, dtype={config.COLUMNA_CLAVE: str})

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
        
        if config.COLUMNA_CLAVE in df.columns:
            df[config.COLUMNA_CLAVE] = df[config.COLUMNA_CLAVE].fillna('').astype(str).str.strip().str.upper()
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
# 3. RUTAS DE LA API (ENDPOINTS)
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
    opciones_limpias = [opc for opc in opciones if str(opc).strip()]
    return sorted(opciones_limpias)

@app.route('/api/estados')
def get_estados():
    estados = obtener_opciones_unicas(df_plazas, Config.COLUMNA_ESTADO)
    if not estados:
        return jsonify({'error': 'La información de estados no está disponible.'}), 500
    return jsonify(estados)

@app.route('/api/estados_con_conteo')
def get_estados_con_conteo():
    """Devuelve los estados con el conteo de plazas para la vista de estados."""
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        # Contar plazas por estado
        estado_counts = df_plazas[Config.COLUMNA_ESTADO].value_counts()
        
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

@app.route('/api/search')
def api_search_plaza():
    clave_busqueda = request.args.get('clave', '').strip().upper()
    if not clave_busqueda:
        return jsonify({'error': 'Proporciona una clave de plaza.'}), 400
    if df_plazas is None:
        return jsonify({'error': 'La base de datos no está disponible.'}), 503
    try:
        plaza_data = df_plazas[df_plazas[Config.COLUMNA_CLAVE] == clave_busqueda]
        if plaza_data.empty:
            return jsonify({'error': f'No se encontraron resultados para la clave: "{request.args.get("clave", "")}".'}), 404
        
        plaza_dict = plaza_data.iloc[0].replace({np.nan: None}).to_dict()
        partes_direccion = [
            str(plaza_dict.get(Config.COLUMNA_COLONIA, '') or '').strip(),
            str(plaza_dict.get(Config.COLUMNA_CALLE, '') or '').strip(),
            str(plaza_dict.get(Config.COLUMNA_NUM, '') or '').strip(),
            str(plaza_dict.get(Config.COLUMNA_COD_POST, '') or '').strip()
        ]
        direccion_completa = ', '.join(filter(None, partes_direccion))
        lat = plaza_dict.get(Config.COLUMNA_LATITUD)
        lon = plaza_dict.get(Config.COLUMNA_LONGITUD)
        maps_url = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}" if lat and lon else None
        image_urls = find_image_urls(request.args.get('clave', '').strip())
        excel_data_limpio = {k: v for k, v in plaza_dict.items() if not k.startswith('normalized_')}
        
        return jsonify({
            'excel_info': excel_data_limpio,
            'direccion_completa': direccion_completa,
            'images': image_urls,
            'google_maps_url': maps_url
        })
    except Exception as e:
        logging.error(f"Error en /api/search para clave '{clave_busqueda}': {e}")
        return jsonify({'error': 'Ocurrió un error en el servidor.'}), 500

# ==============================================================================
# 4. FUNCIONES AUXILIARES Y SERVIDOR DE ARCHIVOS
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
                            logging.info(f"✅ Imagen encontrada: {child.get('name')} -> {direct_url}")
                        else:
                            logging.warning(f"⚠️ Imagen sin URL: {child.get('name')}")
                return True
            
            for child in tree.get('children', []):
                if search_images_in_tree(child, target_folder):
                    return True
            return False
        
        # Buscar en el árbol
        found = search_images_in_tree(drive_data['structure'], clave_lower)
        
        if not found:
            logging.warning(f"❌ Carpeta '{clave_lower}' no encontrada en Drive")
        elif not image_list:
            logging.warning(f"⚠️ Carpeta '{clave_lower}' encontrada pero sin imágenes")
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
# 5. ENDPOINT DE ESTADÍSTICAS 
# ==============================================================================
@app.route('/api/estadisticas')
def get_estadisticas():
    """Devuelve estadísticas generales del sistema, incluyendo conectividad y situación."""
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        # ============================================================
        # 1️⃣ Estadísticas generales básicas
        # ============================================================
        total_plazas = len(df_plazas)
        
        # NUEVO: Contar plazas en operación
        if 'Situación' in df_plazas.columns:
            plazas_operacion = len(df_plazas[
                df_plazas['Situación'].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
            ])
        else:
            plazas_operacion = 0
        
        total_estados = 0
        estado_mas_plazas = {'nombre': 'N/A', 'cantidad': 0}
        estado_menos_plazas = {'nombre': 'N/A', 'cantidad': 0}
        estado_mayor_conectividad = {'nombre': 'N/A', 'porcentaje': 0}
        estado_mas_operacion = {'nombre': 'N/A', 'porcentaje': 0}
        estado_mas_suspension = {'nombre': 'N/A', 'porcentaje': 0}

        # ============================================================
        # 2️⃣ Estado con más y menos plazas
        # ============================================================
        if Config.COLUMNA_ESTADO in df_plazas.columns:
            estado_counts = df_plazas[Config.COLUMNA_ESTADO].value_counts()
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
        # 3️⃣ Estado con mayor conectividad (basado en todo el Excel)
        # ============================================================
        if 'Conect_Instalada' in df_plazas.columns:
            df_conect = df_plazas.copy()

            # Normalizar columna: contar como 1 si hay algo distinto de vacío, NA, None, etc.
            df_conect['conectiva'] = df_conect['Conect_Instalada'].apply(
                lambda v: 1 if pd.notna(v) and str(v).strip().lower() not in ['', 'nan', 'na', 'none', 'null'] else 0
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
        # 4️⃣ Estado con mayor porcentaje de "EN OPERACIÓN" y "SUSPENSIÓN TEMPORAL"
        # ============================================================
        if 'Situación' in df_plazas.columns:
            df_sit = df_plazas.copy()
            df_sit['Situacion_Norm'] = df_sit['Situación'].fillna('').astype(str).str.strip().str.upper()

            def calc_ratio(df, estado, valor):
                subset = df[df[Config.COLUMNA_ESTADO] == estado]
                total = len(subset)
                if total == 0:
                    return 0
                coincidencias = (subset['Situacion_Norm'] == valor).sum()
                return coincidencias / total

            ratios_operacion = {
                estado: calc_ratio(df_sit, estado, 'EN OPERACIÓN')
                for estado in df_sit[Config.COLUMNA_ESTADO].dropna().unique()
            }

            ratios_suspension = {
                estado: calc_ratio(df_sit, estado, 'SUSPENSIÓN TEMPORAL')
                for estado in df_sit[Config.COLUMNA_ESTADO].dropna().unique()
            }

            if ratios_operacion:
                estado_mas_operacion = {
                    'nombre': max(ratios_operacion, key=ratios_operacion.get),
                    'porcentaje': round(max(ratios_operacion.values()) * 100, 2)
                }

            if ratios_suspension:
                estado_mas_suspension = {
                    'nombre': max(ratios_suspension, key=ratios_suspension.get),
                    'porcentaje': round(max(ratios_suspension.values()) * 100, 2)
                }

        # ============================================================
        # 🔹 Respuesta final con todos los datos
        # ============================================================
        return jsonify({
            'totalPlazas': total_plazas,
            'plazasOperacion': plazas_operacion,
            'totalEstados': total_estados,
            'estadoMasPlazas': estado_mas_plazas,
            'estadoMenosPlazas': estado_menos_plazas,
            'estadoMayorConectividad': estado_mayor_conectividad,
            'estadoMasOperacion': estado_mas_operacion,
            'estadoMasSuspension': estado_mas_suspension
        })
        
    except Exception as e:
        logging.error(f"Error generando estadísticas: {e}")
        return jsonify({'error': 'Error al generar estadísticas'}), 500

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
    """Devuelve todas las plazas de un estado específico."""
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        df_filtrado = df_plazas[df_plazas[Config.COLUMNA_ESTADO] == estado]
        
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
    """Búsqueda global en todas las columnas relevantes."""
    try:
        query = request.args.get('q', '').strip().lower()
        if not query or len(query) < 2:
            return jsonify([])
        
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503
        
        resultados = []
        columnas_busqueda = [
            Config.COLUMNA_CLAVE, Config.COLUMNA_ESTADO, Config.COLUMNA_MUNICIPIO,
            Config.COLUMNA_LOCALIDAD, Config.COLUMNA_COLONIA, Config.COLUMNA_CALLE
        ]
        
        for _, plaza in df_plazas.iterrows():
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
    - totales, CN_Total (suma de las 3 categorías)
    - top 5 estados por CN_Total
    - plazas en operación por categoría
    """
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        cols = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']
        missing_cols = [c for c in cols if c not in df_plazas.columns]
        if missing_cols:
            return jsonify({'error': 'Faltan columnas', 'faltantes': missing_cols}), 400

        df_tmp = df_plazas.copy()
        for c in cols:
            df_tmp[f'__{c}_num'] = pd.to_numeric(df_tmp[c], errors='coerce')

        total_registros = len(df_tmp)
        resumen_nacional = {}
        
        cn_total_nacional = 0
        
        if 'Situación' in df_tmp.columns:
            mask_operacion = df_tmp['Situación'].fillna('').astype(str).str.strip().str.upper() == 'EN OPERACIÓN'
        else:
            mask_operacion = pd.Series([False] * len(df_tmp))
        
        for c in cols:
            colnum = f'__{c}_num'
            n_nulos = df_tmp[colnum].isna().sum()
            suma = float(df_tmp[colnum].fillna(0).sum())
            cn_total_nacional += suma
            
            if mask_operacion.any():
                plazas_operacion_cat = len(df_tmp[
                    mask_operacion & 
                    (df_tmp[colnum].fillna(0) > 0)
                ])
            else:
                plazas_operacion_cat = 0
            
            resumen_nacional[c] = {
                'total_registros': int(total_registros),
                'nulos': int(n_nulos),
                'pct_nulos': round(n_nulos / total_registros * 100, 2) if total_registros>0 else 0.0,
                'suma': round(suma, 2),
                'plazasOperacion': int(plazas_operacion_cat)
            }

        plazas_operacion_total = int(mask_operacion.sum()) if 'Situación' in df_tmp.columns else 0
        
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
    Agregados por estado: suma, promedio, % sobre nacional basado en CN_TOTAL
    Devuelve lista por estado ordenada por suma de CN_Inicial_Acum descendente.
    """
    try:
        if df_plazas is None or df_plazas.empty:
            return jsonify({'error': 'No hay datos disponibles'}), 503

        cols = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum']
        missing_cols = [c for c in cols if c not in df_plazas.columns]
        if missing_cols:
            return jsonify({'error': 'Faltan columnas', 'faltantes': missing_cols}), 400

        df_tmp = df_plazas.copy()
        for c in cols:
            df_tmp[f'__{c}_num'] = pd.to_numeric(df_tmp[c], errors='coerce')

        nacional_totales = {
            c: float(df_tmp[f'__{c}_num'].fillna(0).sum())
            for c in cols
        }
        
        cn_total_nacional = sum(nacional_totales.values())

        estados_summary = []
        grouped = df_tmp.groupby(Config.COLUMNA_ESTADO)
        for estado, g in grouped:
            total_registros_estado = len(g)
            s_inicial = float(g['__CN_Inicial_Acum_num'].fillna(0).sum())
            s_prim = float(g['__CN_Prim_Acum_num'].fillna(0).sum())
            s_sec = float(g['__CN_Sec_Acum_num'].fillna(0).sum())
            s_total = s_inicial + s_prim + s_sec
            mean_inicial = float(g['__CN_Inicial_Acum_num'].dropna().mean()) if g['__CN_Inicial_Acum_num'].dropna().shape[0]>0 else 0.0

            pct_sobre_nacional = round((s_total / cn_total_nacional * 100), 2) if cn_total_nacional > 0 else 0.0

            estados_summary.append({
                'estado': str(estado),
                'total_plazas': int(total_registros_estado),
                'suma_CN_Inicial_Acum': round(s_inicial,2),
                'suma_CN_Prim_Acum': round(s_prim,2),
                'suma_CN_Sec_Acum': round(s_sec,2),
                'suma_CN_Total': round(s_total,2),
                'promedio_CN_Inicial_Acum': round(mean_inicial,2),
                'pct_sobre_nacional': pct_sobre_nacional
            })

        estados_sorted = sorted(estados_summary, key=lambda x: x['suma_CN_Inicial_Acum'], reverse=True)
        return jsonify({
            'nacional_totales': {k: round(v,2) for k,v in nacional_totales.items()},
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
# 6. ENDPOINT PARA FECHA DE ACTUALIZACIÓN
# ==============================================================================
@app.route('/api/excel/last-update')
def get_last_update():
    """Devuelve la fecha de última modificación del Excel, ajustada un mes atrás (salvo Diciembre)."""
    try:
        if os.path.exists(Config.EXCEL_PATH):
            timestamp = os.path.getmtime(Config.EXCEL_PATH)
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
# 7. ENDPOINTS PARA GOOGLE DRIVE
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
# 8. ENDPOINTS PARA EXCEL DESDE GOOGLE DRIVE (SOLO LECTURA - BAJO DEMANDA)
# ==============================================================================
@app.route('/api/drive-excel/years')
def get_drive_excel_years():
    """Obtiene años disponibles desde Google Drive - SOLO METADATOS"""
    try:
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
# 9. ENDPOINTS PARA COMPARATIVAS DE PERÍODOS ACUMULATIVOS - CORREGIDOS
# ==============================================================================

@app.route('/api/drive-comparativas/periodos')
def get_comparativa_periodos():
    """Obtiene años y meses disponibles para comparativas - CORREGIDO"""
    try:
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
# 10. ENDPOINTS ADICIONALES PARA CONSULTAS ESPECÍFICAS - CORREGIDOS
# ==============================================================================

@app.route('/api/drive-comparativas/consulta-plazas')
def consulta_plazas_especificas():
    """Consulta plazas específicas entre períodos - CORREGIDO"""
    try:
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
# 11. ENDPOINTS DE MANTENIMIENTO 
# ==============================================================================

@app.route('/api/drive-comparativas/limpiar-cache', methods=['POST'])
def limpiar_cache_comparativas():
    """Limpia el cache del sistema de comparativas - CORREGIDO"""
    try:
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
# 12. ENDPOINTS NUEVOS PARA COMPARATIVAS AVANZADAS
# ==============================================================================

@app.route('/api/drive-comparativas/comparar-avanzado')
def comparar_periodos_avanzado_unificado():
    """Compara dos períodos - ENDPOINT PRINCIPAL CORREGIDO"""
    try:                
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
# 13. ENDPOINTS PARA SERIALIZACIÓN SEGURA
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
# 14. ENDPOINTS PARA INFORMACIÓN DEL SISTEMA
# ==============================================================================

@app.route('/api/system/info')
def get_system_info():
    """Obtiene información completa del sistema"""
    try:
        # Información del árbol de Drive
        drive_stats = drive_excel_reader_readonly.get_stats()
        
        # Años y meses disponibles
        years = drive_excel_reader_readonly.get_available_years()
        total_months = sum(len(drive_excel_reader_readonly.get_available_months(year)) for year in years)
        
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
                'tree_loaded': drive_stats['tree_loaded'],
                'total_years': len(years),
                'total_months': total_months,
                'cache_performance': {
                    'hit_ratio': drive_stats['cache_hit_ratio'],
                    'total_requests': drive_stats['total_requests'],
                    'cache_hits': drive_stats['cache_hits'],
                    'drive_downloads': drive_stats['drive_downloads']
                }
            },
            'local_data': local_excel_info,
            'available_years': years
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo información del sistema: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# 15. FUNCIÓN AUXILIAR PARA OBTENER NOMBRE DEL MES
# ==============================================================================

def obtener_nombre_mes(numero_mes: str) -> str:
    """Convierte número de mes a nombre"""
    meses = {
        '01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril',
        '05': 'Mayo', '06': 'Junio', '07': 'Julio', '08': 'Agosto',
        '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'
    }
    return meses.get(numero_mes, f'Mes {numero_mes}')

# ==============================================================================
# 16. ENDPOINT PARA OBTENER TODOS LOS DATOS DISPONIBLES
# ==============================================================================

@app.route('/api/drive-comparativas/datos-completos')
def get_datos_completos():
    """Obtiene todos los datos disponibles del sistema"""
    try:
        años, meses_por_año = obtener_años_desde_arbol_json()
        
        datos_completos = {
            'años_disponibles': años,
            'meses_por_año': meses_por_año,
            'estadisticas_sistema': drive_excel_reader_readonly.get_stats(),
            'ultima_actualizacion': datetime.now().isoformat()
        }
        
        return jsonify({
            'status': 'success',
            'datos': datos_completos
        })
        
    except Exception as e:
        logging.error(f"Error obteniendo datos completos: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

logging.info("✅ Todos los endpoints de comparativas han sido corregidos y están listos")

# ==============================================================================
# 17. PUNTO DE ENTRADA
# ==============================================================================
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
# 19. PUNTO DE ENTRADA
# ==============================================================================
if __name__ == '__main__':
    if df_plazas is None:
        logging.critical("La aplicación no puede iniciar porque la carga del archivo Excel falló.")
    else:
        app.run(host='0.0.0.0', port=5000, debug=True)
