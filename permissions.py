import json
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

def test_drive_permissions():
    """Test completo de permisos de Google Drive"""
    print("🚀 INICIANDO PRUEBA DE PERMISOS DE GOOGLE DRIVE")
    print("=" * 60)
    
    # Configuración
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    CREDENTIALS_FILE = 'credentials.json'
    TEST_FOLDER_ID = '1hRfCJZfET8h1BX7rX6G5I8kwmhGz8C0Q'
    TEST_FILE_ID = '1oqcfi3vNMWZ7Q1cRQ6l9wR22Qde4ByBd'  # ID de una imagen de prueba
    
    try:
        # 1. Verificar credenciales
        print("1. 🔍 Verificando archivo de credenciales...")
        if not os.path.exists(CREDENTIALS_FILE):
            print("   ❌ ERROR: credentials.json no encontrado")
            return False
        
        with open(CREDENTIALS_FILE, 'r') as f:
            creds_data = json.load(f)
            client_email = creds_data.get('client_email', 'No encontrado')
            print(f"   ✅ Credenciales encontradas")
            print(f"   📧 Service Account: {client_email}")
        
        # 2. Autenticar
        print("\n2. 🔐 Autenticando con Google Drive...")
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        print("   ✅ Autenticación exitosa")
        
        # 3. Verificar acceso a la carpeta
        print(f"\n3. 📁 Verificando acceso a carpeta {TEST_FOLDER_ID}...")
        try:
            folder_info = service.files().get(
                fileId=TEST_FOLDER_ID, 
                fields='id,name,permissions'
            ).execute()
            
            print(f"   ✅ Carpeta accesible: {folder_info.get('name', 'Sin nombre')}")
            print(f"   🆔 ID: {folder_info.get('id')}")
            
            # Verificar permisos
            permissions = folder_info.get('permissions', [])
            print(f"   🔑 Permisos encontrados: {len(permissions)}")
            
            for perm in permissions:
                if 'emailAddress' in perm:
                    print(f"     👤 {perm['emailAddress']} - {perm.get('role', 'Sin rol')}")
            
        except Exception as e:
            print(f"   ❌ ERROR accediendo a la carpeta: {e}")
            return False
        
        # 4. Listar archivos en la carpeta
        print(f"\n4. 📊 Listando archivos en la carpeta...")
        try:
            query = f"'{TEST_FOLDER_ID}' in parents and trashed=false"
            results = service.files().list(
                q=query,
                pageSize=10,
                fields="files(id, name, mimeType, size)"
            ).execute()
            
            items = results.get('files', [])
            print(f"   📁 Archivos encontrados: {len(items)}")
            
            for i, item in enumerate(items[:5]):  # Mostrar solo primeros 5
                print(f"     {i+1}. {item['name']} ({item['mimeType']}) - {item.get('size', '?')} bytes")
            
            if len(items) > 5:
                print(f"     ... y {len(items) - 5} más")
                
        except Exception as e:
            print(f"   ❌ ERROR listando archivos: {e}")
            return False
        
        # 5. Probar descarga de archivo específico
        print(f"\n5. 🖼️ Probando descarga de archivo {TEST_FILE_ID}...")
        try:
            # Primero verificar que el archivo existe
            file_metadata = service.files().get(
                fileId=TEST_FILE_ID, 
                fields='id,name,mimeType,size'
            ).execute()
            
            print(f"   ✅ Archivo encontrado: {file_metadata['name']}")
            print(f"   📦 Tipo: {file_metadata['mimeType']}")
            print(f"   💾 Tamaño: {file_metadata.get('size', 'Desconocido')} bytes")
            
            # Intentar descargar
            request = service.files().get_media(fileId=TEST_FILE_ID)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                progress = int(status.progress() * 100)
                print(f"   📥 Descargando... {progress}%")
            
            file_size = len(file_stream.getvalue())
            print(f"   ✅ Descarga exitosa: {file_size} bytes descargados")
            
        except Exception as e:
            print(f"   ❌ ERROR descargando archivo: {e}")
            return False
        
        # 6. Probar búsqueda por nombre de carpeta
        print(f"\n6. 🔎 Probando búsqueda de carpeta por nombre...")
        try:
            test_folder_name = "P-09-011-22"  # Cambia por una carpeta que exista
            
            query = f"name='{test_folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            
            folders = results.get('files', [])
            if folders:
                print(f"   ✅ Carpeta '{test_folder_name}' encontrada:")
                for folder in folders:
                    print(f"     📁 {folder['name']} (ID: {folder['id']})")
            else:
                print(f"   ❌ Carpeta '{test_folder_name}' NO encontrada")
                
        except Exception as e:
            print(f"   ❌ ERROR en búsqueda: {e}")
            return False
        
        print("\n" + "=" * 60)
        print("🎉 ¡PRUEBA COMPLETADA EXITOSAMENTE!")
        print("✅ El service account tiene los permisos correctos")
        print("✅ Puede acceder a la carpeta y archivos")
        print("✅ Las imágenes deberían cargarse correctamente")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR GENERAL: {e}")
        return False

def test_specific_folder(folder_name):
    """Test específico para una carpeta por nombre"""
    print(f"\n🔍 TEST ESPECÍFICO: Buscando carpeta '{folder_name}'")
    
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    CREDENTIALS_FILE = 'credentials.json'
    
    try:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        
        # Buscar carpeta por nombre
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        
        if not folders:
            print(f"   ❌ Carpeta '{folder_name}' no encontrada")
            return
        
        folder_id = folders[0]['id']
        print(f"   ✅ Carpeta encontrada: {folders[0]['name']} (ID: {folder_id})")
        
        # Listar imágenes en la carpeta
        query = f"'{folder_id}' in parents and trashed=false and (mimeType contains 'image/')"
        results = service.files().list(
            q=query, 
            pageSize=20,
            fields="files(id, name, mimeType, size, webContentLink)"
        ).execute()
        
        images = results.get('files', [])
        print(f"   🖼️ Imágenes encontradas: {len(images)}")
        
        for img in images:
            print(f"     📸 {img['name']} - {img['mimeType']} - {img.get('size', '?')} bytes")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    # Ejecutar test completo
    success = test_drive_permissions()
    
    if success:
        # Probar carpetas específicas
        print("\n" + "=" * 60)
        print("🧪 TESTS ESPECÍFICOS POR CARPETA")
        test_folders = ["P-09-011-22", "R-31-001-14", "1-14-009-17"]
        
        for folder in test_folders:
            test_specific_folder(folder)