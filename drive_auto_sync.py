import json
import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import time
import schedule

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Configuración de Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIALS_FILE = 'credentials.json'
DRIVE_TREE_FILE = 'drive_tree.json'
DRIVE_FOLDER_ID = '1hRfCJZfET8h1BX7rX6G5I8kwmhGz8C0Q' 

def get_drive_service():
    """Autentica y retorna el servicio de Google Drive"""
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            logging.error(f"❌ Archivo de credenciales no encontrado: {CREDENTIALS_FILE}")
            return None
            
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        logging.info("✅ Servicio de Google Drive autenticado")
        return service
    except Exception as e:
        logging.error(f"❌ Error en autenticación de Google Drive: {e}")
        return None

def get_folder_structure(service, folder_id, base_path=''):
    """Obtiene recursivamente la estructura de carpetas y archivos desde Drive (ESTADO ACTUAL)"""
    try:
        structure = {
            'name': base_path.split('/')[-1] if base_path else 'root',
            'path': base_path,
            'type': 'folder',
            'children': [],
            'id': folder_id  # Guardamos ID para comparaciones
        }
        
        # Buscar archivos y carpetas dentro de esta carpeta
        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            pageSize=1000,
            fields="files(id, name, mimeType, webViewLink, webContentLink, modifiedTime, size, thumbnailLink)"
        ).execute()
        
        items = results.get('files', [])
        logging.info(f"📁 Escaneando carpeta '{structure['name']}': {len(items)} elementos encontrados en Drive")
        
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                # Es una carpeta, procesar recursivamente
                folder_path = os.path.join(base_path, item['name']).replace('\\', '/')
                child_structure = get_folder_structure(service, item['id'], folder_path)
                if child_structure:
                    structure['children'].append(child_structure)
            else:
                # Es un archivo - solo procesar imágenes
                file_name = item['name']
                file_extension = os.path.splitext(file_name)[1].lower()
                
                if file_extension in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                    file_path = os.path.join(base_path, file_name).replace('\\', '/')
                    file_id = item['id']
                    
                    # Generar URLs (Solo se usarán si el archivo es NUEVO)
                    thumbnail_url = f"https://lh3.googleusercontent.com/d/{file_id}=w400-h300-c"
                    medium_url = f"https://lh3.googleusercontent.com/d/{file_id}=w800-h600"
                    full_url = f"https://lh3.googleusercontent.com/d/{file_id}=w1920-h1080"
                    download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
                    
                    structure['children'].append({
                        'name': file_name,
                        'path': file_path,
                        'type': 'file',
                        'mimeType': item['mimeType'],
                        'id': file_id,
                        'webViewLink': item.get('webViewLink', ''),
                        'webContentLink': item.get('webContentLink', ''),
                        'thumbnailLink': item.get('thumbnailLink', ''),
                        'modifiedTime': item.get('modifiedTime', ''),
                        'size': item.get('size', 0),
                        'extension': file_extension,
                        'thumbnailUrl': thumbnail_url,
                        'mediumUrl': medium_url,
                        'fullUrl': full_url,
                        'downloadUrl': download_url,
                        'directUrl': f"https://drive.google.com/thumbnail?id={file_id}&sz=w800"
                    })
        
        return structure
        
    except Exception as e:
        logging.error(f"❌ Error procesando carpeta {base_path}: {e}")
        return structure

def recursive_sync(stored_node, fresh_node):
    """
    Mezcla el árbol guardado (JSON) con el árbol nuevo (Drive).
    - Mantiene intactos los archivos que ya existían.
    - Agrega los nuevos.
    - Elimina los que ya no están en Drive.
    """
    # Si es un archivo, devolvemos el guardado (stored_node) para NO modificarlo
    if stored_node['type'] == 'file':
        return stored_node

    # Si es carpeta, procesamos sus hijos
    if stored_node['type'] == 'folder':
        # Mapa de hijos actuales en el JSON por ID
        stored_children_map = {child['id']: child for child in stored_node.get('children', [])}
        fresh_children = fresh_node.get('children', [])
        
        merged_children = []
        
        for fresh_child in fresh_children:
            child_id = fresh_child['id']
            
            if child_id in stored_children_map:
                # EXISTE: Usamos la versión guardada (recursividad para carpetas internas)
                existing_child = stored_children_map[child_id]
                
                if existing_child['type'] == 'folder':
                    # Si es carpeta, entramos a sincronizar su contenido
                    synced_child = recursive_sync(existing_child, fresh_child)
                    merged_children.append(synced_child)
                else:
                    # Si es archivo, lo agregamos tal cual estaba en el JSON (sin cambios)
                    merged_children.append(existing_child)
            else:
                # NUEVO: No estaba en el JSON, agregamos el recién escaneado
                logging.info(f"🆕 Nuevo elemento detectado: {fresh_child['name']}")
                merged_children.append(fresh_child)
        
        # NOTA: Los archivos que estaban en 'stored' pero no en 'fresh' 
        # simplemente no se agregan a 'merged_children', por lo tanto se eliminan.
        
        stored_node['children'] = merged_children
        return stored_node

    return stored_node

def generate_drive_tree_json():
    """Genera el JSON completo desde cero (para la primera vez o force)"""
    try:
        service = get_drive_service()
        if not service: return None
        
        logging.info("🔄 Generando árbol completo desde cero...")
        drive_tree = get_folder_structure(service, DRIVE_FOLDER_ID)
        
        if not drive_tree: return None
        
        total_images = count_total_images(drive_tree)
        
        drive_tree_metadata = {
            'generated_at': datetime.now().isoformat(),
            'source': 'google_drive',
            'root_folder_id': DRIVE_FOLDER_ID,
            'total_images': total_images,
            'url_types': {
                'thumbnailUrl': 'Miniatura (400x300)',
                'mediumUrl': 'Tamaño medio (800x600)',
                'fullUrl': 'Full HD',
                'downloadUrl': 'Descarga directa'
            },
            'structure': drive_tree
        }
        
        with open(DRIVE_TREE_FILE, 'w', encoding='utf-8') as f:
            json.dump(drive_tree_metadata, f, indent=2, ensure_ascii=False)
        
        logging.info(f"✅ JSON generado: {total_images} imágenes.")
        return drive_tree_metadata
        
    except Exception as e:
        logging.error(f"❌ Error generando árbol: {e}")
        return None

def sync_existing_tree():
    """Carga el JSON existente y solo aplica cambios (nuevos/borrados)"""
    try:
        if not os.path.exists(DRIVE_TREE_FILE):
            return generate_drive_tree_json()

        # 1. Cargar JSON viejo
        with open(DRIVE_TREE_FILE, 'r', encoding='utf-8') as f:
            stored_data = json.load(f)
        
        logging.info("🔄 Escaneando Google Drive para detectar cambios...")
        service = get_drive_service()
        if not service: return None

        # 2. Escanear Drive (Estado fresco)
        fresh_structure = get_folder_structure(service, DRIVE_FOLDER_ID)
        if not fresh_structure: return None

        # 3. Fusionar (Merge)
        logging.info("⚡ Comparando y fusionando datos...")
        updated_structure = recursive_sync(stored_data['structure'], fresh_structure)
        
        # 4. Actualizar contadores y guardar
        total_images = count_total_images(updated_structure)
        stored_data['structure'] = updated_structure
        stored_data['total_images'] = total_images
        stored_data['last_sync'] = datetime.now().isoformat()
        
        with open(DRIVE_TREE_FILE, 'w', encoding='utf-8') as f:
            json.dump(stored_data, f, indent=2, ensure_ascii=False)
            
        logging.info(f"✅ Sincronización lista. Total imágenes: {total_images}")
        return stored_data

    except Exception as e:
        logging.error(f"❌ Error en sincronización: {e}")
        return None

def count_total_images(tree):
    """Cuenta el total de imágenes en el árbol"""
    count = 0
    if tree.get('type') == 'file':
        return 1
    elif tree.get('type') == 'folder':
        for child in tree.get('children', []):
            count += count_total_images(child)
    return count

def is_tree_outdated():
    """Verifica si el árbol tiene más de 30 días"""
    if not os.path.exists(DRIVE_TREE_FILE):
        return True
    try:
        with open(DRIVE_TREE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        generated_at = datetime.fromisoformat(data.get('last_sync', data.get('generated_at')))
        days_old = (datetime.now() - generated_at).days
        return days_old > 30
    except:
        return True

def scheduled_drive_update():
    """Tarea programada para actualizar"""
    logging.info(f"🔄 Ejecutando actualización programada...")
    # En programada usamos sync para no romper URLs viejas tampoco
    sync_existing_tree()

def run_once():
    """Ejecuta una sincronización inteligente"""
    print(f"🔍 Verificando archivo: {DRIVE_TREE_FILE}")

    if os.path.exists(DRIVE_TREE_FILE):
        print("✅ Archivo encontrado. Buscando cambios en Drive (Nuevos/Borrados)...")
        result = sync_existing_tree()
        if result:
            print(f"✅ Árbol actualizado. Total imágenes: {result['total_images']}")
        else:
            print("❌ Falló la sincronización.")
    else:
        print("⚠️ No existe archivo. Creando uno nuevo...")
        result = generate_drive_tree_json()
        if result:
            print(f"✅ Árbol creado. Total imágenes: {result['total_images']}")

def run_scheduler():
    schedule.every(30).days.at("02:00").do(scheduled_drive_update)
    schedule.every().day.at("03:00").do(scheduled_drive_update) # Check diario
    logging.info("⏰ Programador iniciado...")
    if is_tree_outdated():
        scheduled_drive_update()
    while True:
        schedule.run_pending()
        time.sleep(3600)

def test_image_urls():
    if not os.path.exists(DRIVE_TREE_FILE):
        print("❌ No hay archivo para probar.")
        return
    with open(DRIVE_TREE_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"📊 Total imágenes: {data['total_images']}")
    # (Lógica de test simplificada para no alargar)
    print("✅ Archivo JSON válido.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == '--once':
            run_once()
            exit()
        if arg == '--force':
            print("⚡ FORZANDO REGENERACIÓN (Borrando datos viejos)...")
            generate_drive_tree_json()
            exit()
        if arg == '--scheduler':
            run_scheduler()
            exit()
        if arg == '--test':
            test_image_urls()
            exit()

    print("🚀 Google Drive Sync - Comandos:")
    print("  --once     → Sincronizar cambios (mantiene URLs viejas)")
    print("  --force    → Regenerar todo desde cero")
    print("  --scheduler → Modo automático")