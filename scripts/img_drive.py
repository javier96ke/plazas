"""
drive_tree_generator.py — Generador del árbol de imágenes de Google Drive
=========================================================================
Cambios respecto a la versión anterior:
  1. Cada carpeta de plaza se guarda con su clave YA NORMALIZADA ("k")
     en lugar del nombre original ("name"). El cliente ya no necesita
     normalizar nada — la búsqueda es siempre un hit exacto O(1).
  2. Se agrega campo "alias" con el nombre original cuando difiere de
     la clave (útil para carpetas de Sinaloa/Puebla sin clave estructural).
  3. El alias del estado (nombre canónico del translator.py) se guarda
     en cada nodo de estado para facilitar filtrado futuro.
  4. Se eliminan las rutas compuestas — el cliente indexa sólo por "k".

Estructura del JSON generado:
  {
    "generated_at": "...",
    "source": "google_drive",
    "root_folder_id": "...",
    "total_images": 8730,
    "structure": {
      "name": "fotos_de_plazas",
      "type": "folder",
      "children": [
        {
          "id": 15,                   ← id numérico del estado
          "estado": "México",         ← nombre canónico (del ColumnTranslator)
          "type": "folder",
          "children": [
            {
              "k": "i-15-001-09",     ← clave normalizada (nueva)
              "alias": "...",         ← nombre original SI difiere (opcional)
              "type": "folder",
              "children": [
                { "i": "driveId", "n": "foto.jpg", "s": 144015, "m": "2025-10-29" }
              ]
            }
          ]
        }
      ]
    }
  }
"""

import json
import os
import logging
import sys
import time
import re
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import schedule

# ===================== CONFIGURACIÓN =====================
SCOPES           = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIALS_FILE = 'credentials.json'
DRIVE_TREE_FILE  = 'drive_tree.json'
DRIVE_FOLDER_ID  = '1hRfCJZfET8h1BX7rX6G5I8kwmhGz8C0Q'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ===================== MAPAS DE ESTADO =====================
# id numérico → nombre canónico (debe coincidir con ColumnTranslator._ESTADO_MAP)
ESTADO_IDS: dict[str, int] = {
    'Aguascalientes':     1,  'Baja California':      2,
    'Baja California Sur':3,  'Campeche':              4,
    'Coahuila':           5,  'Colima':                6,
    'Chiapas':            7,  'Chihuahua':             8,
    'CDMX':               9,  'Ciudad de México':      9,   # alias
    'Durango':           10,  'Guanajuato':           11,
    'Guerrero':          12,  'Hidalgo':              13,
    'Jalisco':           14,  'Estado de México':     15,
    'México':            15,  'Estado Mexico':        15,   # alias
    'Michoacán':         16,  'Morelos':              17,
    'Nayarit':           18,  'Nuevo León':           19,
    'Nuevo Leon':        19,  # alias sin tilde
    'Oaxaca':            20,  'Puebla':               21,
    'Querétaro':         22,  'Queretaro':            22,   # alias sin tilde
    'Quintana Roo':      23,  'San Luis Potosí':      24,
    'San Luis Potosi':   24,  # alias sin tilde
    'Sinaloa':           25,  'Sonora':               26,
    'Tabasco':           27,  'Tamaulipas':           28,
    'Tlaxcala':          29,  'Veracruz':             30,
    'Yucatán':           31,  'Yucatan':              31,   # alias sin tilde
    'Zacatecas':         32,
}

# id → nombre canónico (para el campo "estado" en el JSON)
ID_A_ESTADO: dict[int, str] = {
    1:  'Aguascalientes',    2:  'Baja California',
    3:  'Baja California Sur', 4: 'Campeche',
    5:  'Coahuila',          6:  'Colima',
    7:  'Chiapas',           8:  'Chihuahua',
    9:  'Ciudad de México',  10: 'Durango',
    11: 'Guanajuato',        12: 'Guerrero',
    13: 'Hidalgo',           14: 'Jalisco',
    15: 'México',            16: 'Michoacán',
    17: 'Morelos',           18: 'Nayarit',
    19: 'Nuevo León',        20: 'Oaxaca',
    21: 'Puebla',            22: 'Querétaro',
    23: 'Quintana Roo',      24: 'San Luis Potosí',
    25: 'Sinaloa',           26: 'Sonora',
    27: 'Tabasco',           28: 'Tamaulipas',
    29: 'Tlaxcala',          30: 'Veracruz',
    31: 'Yucatán',           32: 'Zacatecas',
}

# ===================== NORMALIZACIÓN DE CLAVES =====================
# Patrón: letra_tipo - NN - NNN - NN  (con tolerancia a variantes)
_PATRON_CLAVE = re.compile(r'([A-Za-z1l])-\s*(\d{2,3})-(\d{2,3})-(\d{2,3})')

def normalizar_clave(nombre_carpeta: str) -> tuple[str, str | None]:
    """
    Extrae y normaliza la clave estructural de un nombre de carpeta de Drive.

    Normaliza:
      - Prefijos:   'MEXICO_I-15-086-02'      → 'i-15-086-02'
      - Espacios:   'I- 07-011-02'            → 'i-07-011-02'
      - Minúsculas: 'i-21-061-02'             → 'i-21-061-02'  (ya correcto)
      - 'l'/'1':    'l-11-007-02', '1-07-001' → 'i-11-007-02', 'i-07-001'
      - Sufijos:    'I-32-009-03_PEDREGOSO'   → 'i-32-009-03'
      - Notas:      'I-19-001-17 (1)'         → 'i-19-001-17'
      - Num cortos: 'I-07-49-02'              → 'i-07-049-02'
      - Cero extra: 'I-021-001-16'            → 'i-021-001-16'  (se conserva)

    Devuelve:
      (clave_normalizada, alias_original | None)
      alias es None cuando el nombre original ya era la clave normalizada.
      alias es el nombre original cuando el nombre tenía decoración extra.
      alias es el nombre original cuando NO hay clave extraíble (carpetas sin
      clave estructural como 'PLAZA CROC MAZATLAN').
    """
    nombre_original = nombre_carpeta.strip()

    m = _PATRON_CLAVE.search(nombre_original)
    if not m:
        # Sin clave estructural: usar nombre normalizado como clave de búsqueda
        clave = nombre_original.lower()
        return clave, nombre_original  # siempre guarda alias para texto libre

    # Extraer componentes
    tipo      = m.group(1).upper()
    seg_est   = m.group(2)
    seg_mun   = m.group(3)
    seg_plaza = m.group(4)

    # Corregir 'L' y '1' confundidos con la letra de tipo (primer segmento)
    if tipo in ('L', '1'):
        tipo = 'I'

    # Padding numérico estándar
    seg_est   = seg_est.zfill(2)    # siempre 2 dígitos
    seg_mun   = seg_mun.zfill(3)    # siempre 3 dígitos
    seg_plaza = seg_plaza.zfill(2)  # siempre 2 dígitos

    clave = f"{tipo}-{seg_est}-{seg_mun}-{seg_plaza}".lower()

    # ¿El nombre original era SÓLO la clave (sin decoración)?
    nombre_lower = nombre_original.lower().strip()
    if nombre_lower == clave:
        return clave, None  # sin alias necesario

    # Tenía prefijo, sufijo, notas, espacios, etc. → guardar alias
    return clave, nombre_original


# ===================== AUTENTICACIÓN =====================
def get_drive_service():
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            logging.error(f'❌ Credenciales no encontradas: {CREDENTIALS_FILE}')
            return None
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        logging.error(f'❌ Error de autenticación: {e}')
        return None


# ===================== ÍNDICE EXISTENTE (sync incremental) =====================
def load_existing_files_index() -> dict:
    """
    Lee drive_tree.json y construye índice plano:
      { file_id: { 'modifiedTime': 'YYYY-MM-DD', 'name': '...', 'size': N } }
    """
    if not os.path.exists(DRIVE_TREE_FILE):
        return {}
    try:
        with open(DRIVE_TREE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        index = {}

        def walk(node):
            if 'i' in node:  # archivo compacto
                index[node['i']] = {
                    'modifiedTime': node.get('m', ''),
                    'name':         node.get('n', ''),
                    'size':         node.get('s', 0),
                }
            for child in node.get('children', []):
                walk(child)

        walk(data.get('structure', {}))
        logging.info(f'📦 Índice cargado: {len(index)} archivos existentes')
        return index
    except Exception as e:
        logging.error(f'❌ Error cargando índice: {e}')
        return {}


# ===================== ESCANEO DE DRIVE =====================
def scan_plaza_folder(
    service,
    folder_id: str,
    folder_name: str,
    existing_index: dict,
) -> dict | None:
    """
    Escanea una carpeta de plaza y devuelve nodo compacto con clave normalizada:
      {
        "k":    "i-15-001-09",        ← clave normalizada (NUEVA)
        "alias": "MEXICO_I-15-001-09" ← solo si el nombre original difería
        "type": "folder",
        "children": [ {i, n, s, m}, ... ]
      }
    Devuelve None si la carpeta no tiene imágenes.
    """
    clave, alias = normalizar_clave(folder_name)

    nodo: dict = {'k': clave, 'type': 'folder', 'children': []}
    if alias:
        nodo['alias'] = alias

    try:
        page_token = None
        while True:
            results = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                pageSize=1000,
                fields='nextPageToken, files(id, name, mimeType, modifiedTime, size)',
                pageToken=page_token,
            ).execute()

            for item in results.get('files', []):
                if not item['mimeType'].startswith('image/'):
                    continue

                file_id  = item['id']
                srv_mod  = item.get('modifiedTime', '')[:10]
                cached   = existing_index.get(file_id)

                if cached and cached.get('modifiedTime') == srv_mod:
                    nodo['children'].append({
                        'i': file_id,
                        'n': cached['name'],
                        's': cached['size'],
                        'm': srv_mod,
                    })
                else:
                    logging.info(f'  ✨ Nuevo/Actualizado: {item["name"]}')
                    nodo['children'].append({
                        'i': file_id,
                        'n': item['name'],
                        's': int(item.get('size', 0)),
                        'm': srv_mod,
                    })

            page_token = results.get('nextPageToken')
            if not page_token:
                break

    except Exception as e:
        logging.error(f'❌ Error en plaza {folder_name} ({folder_id}): {e}')

    return nodo if nodo['children'] else None


def scan_estado_folder(
    service,
    estado_folder_id: str,
    estado_nombre: str,
    estado_id: int,
    existing_index: dict,
) -> dict | None:
    """
    Escanea todas las carpetas de plaza dentro de un estado.
    Devuelve nodo de estado o None si está vacío.
    """
    estado_canon = ID_A_ESTADO.get(estado_id, estado_nombre)
    logging.info(f'🗂  Estado: {estado_nombre} → id={estado_id} ({estado_canon})')

    nodo_estado: dict = {
        'id':      estado_id,
        'estado':  estado_canon,   # ← NUEVO: nombre canónico para el cliente
        'type':    'folder',
        'children': [],
    }

    try:
        page_token = None
        while True:
            results = service.files().list(
                q=f"'{estado_folder_id}' in parents and trashed=false "
                  f"and mimeType='application/vnd.google-apps.folder'",
                pageSize=1000,
                fields='nextPageToken, files(id, name)',
                pageToken=page_token,
            ).execute()

            for plaza_item in results.get('files', []):
                logging.info(f'  📂 Plaza: {plaza_item["name"]}')
                plaza_nodo = scan_plaza_folder(
                    service,
                    plaza_item['id'],
                    plaza_item['name'],
                    existing_index,
                )
                if plaza_nodo:
                    nodo_estado['children'].append(plaza_nodo)

            page_token = results.get('nextPageToken')
            if not page_token:
                break

    except Exception as e:
        logging.error(f'❌ Error en estado {estado_nombre}: {e}')

    return nodo_estado if nodo_estado['children'] else None


# ===================== GENERACIÓN PRINCIPAL =====================
def count_images(node: dict) -> int:
    if 'i' in node:
        return 1
    return sum(count_images(c) for c in node.get('children', []))


def generate_drive_tree_json(force: bool = False) -> dict | None:
    service = get_drive_service()
    if not service:
        return None

    start_time     = time.time()
    existing_index = {} if force else load_existing_files_index()

    logging.info('🚀 Iniciando escaneo de Google Drive...')

    # Nombre de la carpeta raíz
    try:
        root_name = service.files().get(
            fileId=DRIVE_FOLDER_ID, fields='name'
        ).execute().get('name', 'fotos_de_plazas')
    except Exception:
        root_name = 'fotos_de_plazas'

    root_structure: dict = {'name': root_name, 'type': 'folder', 'children': []}

    # Listar carpetas de estados bajo la raíz
    try:
        items = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and trashed=false "
              f"and mimeType='application/vnd.google-apps.folder'",
            pageSize=100,
            fields='files(id, name)',
        ).execute().get('files', [])
    except Exception as e:
        logging.error(f'❌ Error listando estados: {e}')
        return None

    estados_omitidos = []
    for estado_item in items:
        estado_nombre = estado_item['name']
        estado_id     = ESTADO_IDS.get(estado_nombre)

        if estado_id is None:
            logging.warning(f'⚠️  Estado no reconocido: "{estado_nombre}" — se omite')
            estados_omitidos.append(estado_nombre)
            continue

        nodo_estado = scan_estado_folder(
            service,
            estado_item['id'],
            estado_nombre,
            estado_id,
            existing_index,
        )
        if nodo_estado:
            root_structure['children'].append(nodo_estado)

    if estados_omitidos:
        logging.warning(f'⚠️  Estados omitidos: {estados_omitidos}')

    total_images = count_images(root_structure)
    elapsed      = time.time() - start_time

    final_data = {
        'generated_at':   datetime.now().isoformat(),
        'source':         'google_drive',
        'root_folder_id': DRIVE_FOLDER_ID,
        'total_images':   total_images,
        'structure':      root_structure,
    }

    # Escritura atómica (tmp → rename)
    tmp = DRIVE_TREE_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, separators=(',', ':'))
    if os.path.exists(DRIVE_TREE_FILE):
        os.remove(DRIVE_TREE_FILE)
    os.rename(tmp, DRIVE_TREE_FILE)

    logging.info(f'✅ Completado en {elapsed:.1f}s — {total_images} imágenes, '
                 f'{sum(len(e["children"]) for e in root_structure["children"])} plazas')
    return final_data


# ===================== SCHEDULER =====================
def scheduled_job():
    logging.info('⏰ Sync programado...')
    generate_drive_tree_json(force=False)


def run_scheduler():
    logging.info('⏳ Scheduler iniciado (cada 12 horas)')
    generate_drive_tree_json(force=False)
    schedule.every(12).hours.do(scheduled_job)
    while True:
        schedule.run_pending()
        time.sleep(60)


# ===================== MAIN =====================
if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) > 1 else '--once'
    if arg == '--force':
        generate_drive_tree_json(force=True)
    elif arg == '--scheduler':
        run_scheduler()
    elif arg == '--once':
        generate_drive_tree_json(force=False)
    else:
        print('Uso: --once | --force | --scheduler')
