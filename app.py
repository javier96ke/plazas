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
    """Devuelve la fecha de última modificación del Excel original."""
    try:
        if os.path.exists(Config.EXCEL_PATH):
            timestamp = os.path.getmtime(Config.EXCEL_PATH)
            last_modified = datetime.fromtimestamp(timestamp)
            
            return jsonify({
                'last_modified': last_modified.isoformat(),
                'formatted': last_modified.strftime('%d/%m/%Y'),
                'status': 'success'
            })
        else:
            return jsonify({
                'last_modified': None,
                'status': 'archivo_no_encontrado'
            }), 404
            
    except Exception as e:
        logging.error(f"Error obteniendo fecha de Excel: {e}")
        return jsonify({
            'last_modified': None,
            'status': 'error'
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
        if (datetime.now() - generated_at).days > 16:
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
# 8. PUNTO DE ENTRADA
# ==============================================================================
if __name__ == '__main__':
    if df_plazas is None:
        logging.critical("La aplicación no puede iniciar porque la carga del archivo Excel falló.")
    else:
        app.run(host='0.0.0.0', port=5000, debug=True)
