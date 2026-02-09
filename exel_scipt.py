import json
import pandas as pd
from typing import Dict, List, Any
import traceback

class PythonDataCalculator:
    """Muestra EXACTAMENTE qué datos calcula el Python desde el JSON"""
    
    def __init__(self, json_data: Dict):
        self.json_data = json_data
        self.calculation_log = []
        
    def log_step(self, step_name: str, data: Any):
        """Registra un paso del cálculo"""
        self.calculation_log.append({
            'step': step_name,
            'data': data,
            'type': type(data).__name__,
            'shape': self._get_shape(data)
        })
        
    def _get_shape(self, data):
        """Obtiene forma del dato"""
        if isinstance(data, (list, tuple)):
            return f"len={len(data)}"
        elif isinstance(data, dict):
            return f"keys={len(data)}"
        elif isinstance(data, pd.DataFrame):
            return f"{data.shape[0]}x{data.shape[1]}"
        elif hasattr(data, '__len__'):
            return f"len={len(data)}"
        return "N/A"
    
    def show_calculation_steps(self):
        """Muestra todos los pasos de cálculo paso a paso"""
        print("=" * 100)
        print("🔍 ANÁLISIS PASO A PASO - ¿QUÉ CALCULA EL PYTHON?")
        print("=" * 100)
        
        for i, step in enumerate(self.calculation_log, 1):
            print(f"\n📝 PASO {i}: {step['step']}")
            print(f"   Tipo: {step['type']}")
            print(f"   Forma: {step['shape']}")
            
            # Mostrar datos según tipo
            if isinstance(step['data'], dict):
                if len(step['data']) <= 10:
                    for k, v in list(step['data'].items())[:10]:
                        print(f"   {k}: {v}")
                else:
                    keys = list(step['data'].keys())[:5]
                    print(f"   Primeras 5 claves: {keys}")
                    print(f"   ... y {len(step['data']) - 5} más")
                    
            elif isinstance(step['data'], list):
                if len(step['data']) <= 10:
                    for j, item in enumerate(step['data'][:10]):
                        print(f"   [{j}]: {item}")
                else:
                    print(f"   Primeros 5 items: {step['data'][:5]}")
                    print(f"   ... y {len(step['data']) - 5} más")
                    
            elif isinstance(step['data'], pd.DataFrame):
                print(f"   Columnas: {list(step['data'].columns)}")
                print(f"   Primeras filas:")
                print(step['data'].head(3).to_string())
                
            else:
                print(f"   Valor: {step['data']}")
    
    def simulate_drive_reader_calculations(self):
        """Simula EXACTAMENTE los cálculos que hace DriveExcelReaderOptimized"""
        
        print("\n" + "=" * 100)
        print("🧮 SIMULANDO CÁLCULOS DE DriveExcelReaderOptimized")
        print("=" * 100)
        
        try:
            # PASO 1: Carga básica del árbol
            self.log_step("Cargar JSON completo", self.json_data)
            
            # PASO 2: Extraer índice (¡ESTE ES CLAVE!)
            index = self.json_data.get('index', {})
            self.log_step("Extraer índice del JSON", index)
            
            # PASO 3: Extraer estadísticas
            stats = self.json_data.get('statistics', {})
            self.log_step("Extraer estadísticas", stats)
            
            # PASO 4: Simular get_available_years()
            years_calculated = self._calculate_available_years()
            self.log_step("Resultado de get_available_years()", years_calculated)
            
            # PASO 5: Simular get_available_months('2025')
            months_2025 = self._calculate_months_for_year('2025')
            self.log_step("Resultado de get_available_months('2025')", months_2025)
            
            # PASO 6: Simular find_excel_file()
            file_2025_01 = self._find_file('2025', '01')
            self.log_step("Resultado de find_excel_file('2025', '01')", file_2025_01)
            
            # PASO 7: Verificar IDs duales (Excel + Parquet)
            dual_files = self._check_dual_ids()
            self.log_step("Archivos con ambos formatos (Excel+Parquet)", dual_files)
            
            # PASO 8: Verificar URLs de descarga
            download_urls = self._check_download_urls()
            self.log_step("URLs de descarga disponibles", download_urls)
            
            # PASO 9: Analizar estructura de árbol
            tree_structure = self._analyze_tree_structure()
            self.log_step("Estructura del árbol analizada", tree_structure)
            
            # PASO 10: Calcular métricas clave
            key_metrics = self._calculate_key_metrics()
            self.log_step("Métricas clave calculadas", key_metrics)
            
            return True
            
        except Exception as e:
            print(f"❌ ERROR durante simulación: {e}")
            print(traceback.format_exc())
            return False
    
    def _calculate_available_years(self) -> List[str]:
        """Calcula años disponibles (igual que DriveExcelReaderOptimized)"""
        years_set = set()
        
        # Método 1: Desde el índice (¡ASÍ ES COMO LO HACE EL CÓDIGO!)
        if hasattr(self, 'json_data'):
            index = self.json_data.get('index', {})
            for clave in index.keys():
                try:
                    partes = str(clave).split("-")
                    if len(partes) >= 1:
                        año = partes[0].strip()
                        if año:
                            years_set.add(año)
                except Exception:
                    continue
        
        # Método 2: Desde estadísticas
        stats = self.json_data.get('statistics', {})
        years_from_stats = stats.get('unique_years', [])
        for year in years_from_stats:
            if year:
                years_set.add(str(year))
        
        return sorted(years_set, reverse=True)
    
    def _calculate_months_for_year(self, year: str) -> List[str]:
        """Calcula meses para un año específico"""
        months = set()
        
        index = self.json_data.get('index', {})
        for key in index.keys():
            try:
                partes = str(key).split("-")
                if len(partes) >= 2:
                    k_year = partes[0].strip()
                    mes = partes[1].strip()
                    
                    if k_year == str(year).strip() and mes:
                        months.add(mes)
            except Exception:
                continue
        
        return sorted(list(months))
    
    def _find_file(self, year: str, month: str) -> Dict:
        """Busca un archivo específico"""
        index = self.json_data.get('index', {})
        
        # Método 1: Búsqueda directa
        key = f"{year}-{month}"
        if key in index:
            return index[key]
        
        # Método 2: Búsqueda flexible (como hace el código real)
        for k, info in index.items():
            try:
                partes = str(k).split("-")
                if len(partes) >= 2:
                    k_year = partes[0].strip()
                    k_month = partes[1].strip()
                    
                    if k_year == year and k_month == month:
                        return info
            except Exception:
                continue
        
        return {"error": "No encontrado"}
    
    def _check_dual_ids(self) -> Dict:
        """Verifica IDs duales (Excel + Parquet)"""
        index = self.json_data.get('index', {})
        
        results = {
            'dual_count': 0,
            'excel_only': 0,
            'parquet_only': 0,
            'examples': []
        }
        
        for key, info in index.items():
            has_excel = 'id_excel' in info and info['id_excel'] is not None
            has_parquet = 'id_parquet' in info and info['id_parquet'] is not None
            
            if has_excel and has_parquet:
                results['dual_count'] += 1
                if len(results['examples']) < 3:
                    results['examples'].append({
                        'key': key,
                        'excel_id': info['id_excel'],
                        'parquet_id': info['id_parquet']
                    })
            elif has_excel:
                results['excel_only'] += 1
            elif has_parquet:
                results['parquet_only'] += 1
        
        return results
    
    def _check_download_urls(self) -> Dict:
        """Verifica URLs de descarga"""
        index = self.json_data.get('index', {})
        tree = self.json_data.get('tree', {})
        
        results = {
            'index_urls': 0,
            'tree_urls': 0,
            'missing_in_index': [],
            'missing_in_tree': []
        }
        
        # Verificar en índice
        for key, info in index.items():
            if info.get('download_url'):
                results['index_urls'] += 1
            else:
                results['missing_in_index'].append(key)
        
        # Verificar en árbol
        def check_tree_urls(node):
            if node.get('type') == 'file':
                if node.get('download_url'):
                    results['tree_urls'] += 1
                else:
                    results['missing_in_tree'].append(node.get('name'))
            
            for child in node.get('children', []):
                check_tree_urls(child)
        
        check_tree_urls(tree)
        
        return results
    
    def _analyze_tree_structure(self) -> Dict:
        """Analiza la estructura del árbol"""
        tree = self.json_data.get('tree', {})
        
        def analyze_node(node, depth=0):
            info = {
                'name': node.get('name'),
                'type': node.get('type'),
                'depth': depth,
                'children_count': len(node.get('children', [])),
                'file_info': {}
            }
            
            if node.get('type') == 'file':
                info['file_info'] = {
                    'format': node.get('file_format'),
                    'year': node.get('year'),
                    'month': node.get('month'),
                    'id': node.get('id'),
                    'size': node.get('size')
                }
            
            return info
        
        return analyze_node(tree)
    
    def _calculate_key_metrics(self) -> Dict:
        """Calcula métricas clave del sistema"""
        index = self.json_data.get('index', {})
        stats = self.json_data.get('statistics', {})
        
        # Contar archivos por formato en índice
        format_counts = {'excel': 0, 'parquet': 0}
        for key, info in index.items():
            if info.get('id_excel'):
                format_counts['excel'] += 1
            if info.get('id_parquet'):
                format_counts['parquet'] += 1
        
        # Verificar consistencia con estadísticas
        stats_excel = stats.get('total_excel', 0)
        stats_parquet = stats.get('total_parquet', 0)
        
        consistency = {
            'excel_match': format_counts['excel'] == stats_excel,
            'parquet_match': format_counts['parquet'] == stats_parquet,
            'excel_diff': format_counts['excel'] - stats_excel,
            'parquet_diff': format_counts['parquet'] - stats_parquet
        }
        
        return {
            'index_entries': len(index),
            'format_counts_index': format_counts,
            'format_counts_stats': {'excel': stats_excel, 'parquet': stats_parquet},
            'consistency_check': consistency,
            'unique_years_stats': stats.get('unique_years', []),
            'unique_months_stats': stats.get('unique_months', [])
        }
    
    def generate_detailed_report(self):
        """Genera un reporte detallado de TODO lo que calcula"""
        print("\n" + "=" * 100)
        print("📊 REPORTE DETALLADO - TODO LO QUE CALCULA EL PYTHON")
        print("=" * 100)
        
        # 1. ESTRUCTURA BÁSICA
        print("\n1. 📁 ESTRUCTURA BÁSICA DEL JSON:")
        print(f"   • Tiene árbol: {'✅' if 'tree' in self.json_data else '❌'}")
        print(f"   • Tiene índice: {'✅' if 'index' in self.json_data else '❌'}")
        print(f"   • Tiene estadísticas: {'✅' if 'statistics' in self.json_data else '❌'}")
        
        # 2. ÍNDICE - ¡LO MÁS IMPORTANTE!
        print("\n2. 📑 ANÁLISIS DEL ÍNDICE:")
        index = self.json_data.get('index', {})
        print(f"   • Total entradas: {len(index)}")
        
        if index:
            first_key = list(index.keys())[0]
            first_entry = index[first_key]
            print(f"   • Primera entrada: {first_key}")
            print(f"   • Estructura de entrada:")
            for k, v in first_entry.items():
                print(f"     - {k}: {v} (tipo: {type(v).__name__})")
        
        # 3. CÁLCULOS DE AÑOS Y MESES
        print("\n3. 📅 CÁLCULOS DE FECHAS:")
        years = self._calculate_available_years()
        print(f"   • Años calculados: {years}")
        print(f"   • Fuente: índice + estadísticas")
        
        for year in years[:3]:  # Mostrar primeros 3 años
            months = self._calculate_months_for_year(year)
            print(f"   • Meses para {year}: {months}")
        
        # 4. BÚSQUEDA DE ARCHIVOS
        print("\n4. 🔍 BÚSQUEDA DE ARCHIVOS (find_excel_file):")
        test_cases = [
            ('2025', '01'),
            ('2025', '12'),
            ('2026', '10'),
            ('2024', '01')  # Debería fallar
        ]
        
        for year, month in test_cases:
            result = self._find_file(year, month)
            if 'error' not in result:
                print(f"   • {year}-{month}: ENCONTRADO - ID: {result.get('id')}")
                print(f"     Excel: {result.get('id_excel')}, Parquet: {result.get('id_parquet')}")
            else:
                print(f"   • {year}-{month}: NO ENCONTRADO")
        
        # 5. FORMATOS Y URLs
        print("\n5. 📊 FORMATOS Y URLs:")
        dual_info = self._check_dual_ids()
        print(f"   • Con ambos formatos: {dual_info['dual_count']}")
        print(f"   • Solo Excel: {dual_info['excel_only']}")
        print(f"   • Solo Parquet: {dual_info['parquet_only']}")
        
        url_info = self._check_download_urls()
        print(f"   • URLs en índice: {url_info['index_urls']}/{len(index)}")
        print(f"   • URLs en árbol: {url_info['tree_urls']}")
        
        # 6. CONSISTENCIA
        print("\n6. ✅ VERIFICACIÓN DE CONSISTENCIA:")
        metrics = self._calculate_key_metrics()
        
        print(f"   • Entradas en índice: {metrics['index_entries']}")
        print(f"   • Estadísticas dicen: {self.json_data.get('statistics', {}).get('total_files', 0)} archivos")
        
        consistency = metrics['consistency_check']
        print(f"   • Excel coincide: {'✅' if consistency['excel_match'] else '❌'} (diff: {consistency['excel_diff']})")
        print(f"   • Parquet coincide: {'✅' if consistency['parquet_match'] else '❌'} (diff: {consistency['parquet_diff']})")
        
        # 7. PROBLEMAS POTENCIALES
        print("\n7. ⚠️ PROBLEMAS POTENCIALES DETECTADOS:")
        
        # Verificar claves vacías
        empty_keys = []
        for key, value in index.items():
            if not key or key.strip() == "":
                empty_keys.append(key)
        
        if empty_keys:
            print(f"   • Claves vacías en índice: {len(empty_keys)}")
        
        # Verificar años/meses inconsistentes
        stats_years = set(self.json_data.get('statistics', {}).get('unique_years', []))
        index_years = set(self._calculate_available_years())
        
        if stats_years != index_years:
            print(f"   • Años inconsistentes: estadísticas={stats_years}, índice={index_years}")
            print(f"   • Diferencia: {stats_years.symmetric_difference(index_years)}")
        
        # Verificar que todas las claves del índice tengan formato correcto
        malformed_keys = []
        for key in index.keys():
            if '-' not in key:
                malformed_keys.append(key)
        
        if malformed_keys:
            print(f"   • Claves mal formadas (sin '-'): {malformed_keys}")
        
        print("\n" + "=" * 100)


def diagnose_json_file(json_file_path: str):
    """Función principal de diagnóstico"""
    print("🔬 INICIANDO DIAGNÓSTICO DEL SISTEMA")
    print(f"Archivo: {json_file_path}")
    print("=" * 100)
    
    try:
        # 1. Cargar JSON
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        print("✅ JSON cargado correctamente")
        
        # 2. Crear calculador
        calculator = PythonDataCalculator(json_data)
        
        # 3. Ejecutar simulación completa
        success = calculator.simulate_drive_reader_calculations()
        
        if success:
            # 4. Mostrar pasos de cálculo
            calculator.show_calculation_steps()
            
            # 5. Generar reporte detallado
            calculator.generate_detailed_report()
            
            # 6. Preguntas clave para diagnóstico
            print("\n" + "=" * 100)
            print("❓ PREGUNTAS CLAVE PARA DIAGNOSTICAR EL PROBLEMA:")
            print("=" * 100)
            
            index = json_data.get('index', {})
            
            print("\n1. ¿El índice tiene todas las claves necesarias?")
            if index:
                sample_key = list(index.keys())[0]
                sample_entry = index[sample_key]
                required_keys = ['year', 'month', 'id', 'download_url']
                missing = [k for k in required_keys if k not in sample_entry]
                print(f"   Claves requeridas: {required_keys}")
                print(f"   Claves faltantes en muestra: {missing if missing else '✅ Ninguna'}")
            
            print("\n2. ¿Las claves del índice tienen formato 'YYYY-MM'?")
            malformed = [k for k in index.keys() if len(k.split('-')) != 2]
            print(f"   Claves mal formadas: {malformed if malformed else '✅ Ninguna'}")
            
            print("\n3. ¿Hay IDs duplicados?")
            all_ids = []
            for entry in index.values():
                for key in ['id', 'id_excel', 'id_parquet']:
                    if key in entry and entry[key]:
                        all_ids.append(entry[key])
            
            duplicates = set([x for x in all_ids if all_ids.count(x) > 1])
            print(f"   IDs duplicados: {list(duplicates) if duplicates else '✅ Ninguno'}")
            
            print("\n4. ¿Las URLs de descarga son accesibles?")
            print(f"   Total URLs en índice: {sum(1 for e in index.values() if e.get('download_url'))}")
            
            print("\n5. ¿La estructura coincide entre árbol e índice?")
            tree_files = []
            def extract_tree_files(node):
                if node.get('type') == 'file':
                    tree_files.append({
                        'id': node.get('id'),
                        'year': node.get('year'),
                        'month': node.get('month')
                    })
                for child in node.get('children', []):
                    extract_tree_files(child)
            
            extract_tree_files(json_data.get('tree', {}))
            print(f"   Archivos en árbol: {len(tree_files)}")
            print(f"   Entradas en índice: {len(index)}")
            
            # Comparar
            tree_ids = {f['id'] for f in tree_files if f['id']}
            index_ids = set()
            for entry in index.values():
                for key in ['id', 'id_excel', 'id_parquet']:
                    if key in entry and entry[key]:
                        index_ids.add(entry[key])
            
            missing_in_tree = index_ids - tree_ids
            missing_in_index = tree_ids - index_ids
            
            print(f"   IDs en índice pero no en árbol: {len(missing_in_tree)}")
            print(f"   IDs en árbol pero no en índice: {len(missing_in_index)}")
            
            if missing_in_tree:
                print(f"     Ejemplos: {list(missing_in_tree)[:3]}")
            
            return {
                'status': 'success',
                'calculator': calculator,
                'tree_files': tree_files,
                'index_entries': len(index),
                'issues': {
                    'malformed_keys': malformed,
                    'duplicate_ids': list(duplicates),
                    'missing_in_tree': list(missing_in_tree),
                    'missing_in_index': list(missing_in_index)
                }
            }
        
    except FileNotFoundError:
        print(f"❌ ERROR: Archivo no encontrado: {json_file_path}")
        return {'status': 'file_not_found'}
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: JSON mal formado: {e}")
        return {'status': 'json_decode_error'}
    except Exception as e:
        print(f"❌ ERROR inesperado: {e}")
        print(traceback.format_exc())
        return {'status': 'unexpected_error'}


def quick_test(json_file_path: str):
    """Prueba rápida de las funciones clave"""
    print("\n" + "=" * 100)
    print("⚡ PRUEBA RÁPIDA - FUNCIONES CLAVE")
    print("=" * 100)
    
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 1. Probar get_available_years
    print("\n1. Probando get_available_years():")
    
    # Simular exactamente lo que hace el código
    years_set = set()
    index = data.get('index', {})
    
    for clave in index.keys():
        try:
            partes = str(clave).split("-")
            if len(partes) >= 1:
                año = partes[0].strip()
                if año:
                    years_set.add(año)
        except Exception as e:
            print(f"   Error procesando clave '{clave}': {e}")
    
    years = sorted(years_set, reverse=True)
    print(f"   Resultado: {years}")
    
    # 2. Probar find_excel_file
    print("\n2. Probando find_excel_file('2025', '01'):")
    
    year = "2025"
    month = "01"
    
    # Método directo
    key = f"{year}-{month}"
    if key in index:
        print(f"   Encontrado directamente con clave: {key}")
        print(f"   Datos: {index[key]}")
    else:
        print(f"   No encontrado con clave directa: {key}")
        
        # Método flexible
        for k, info in index.items():
            try:
                partes = str(k).split("-")
                if len(partes) >= 2:
                    k_year = partes[0].strip()
                    k_month = partes[1].strip()
                    
                    if k_year == year and k_month == month:
                        print(f"   Encontrado con clave flexible: {k}")
                        print(f"   Datos: {info}")
                        break
            except Exception:
                continue
    
    # 3. Verificar estructura del índice
    print("\n3. Estructura de una entrada del índice:")
    if index:
        sample_key = list(index.keys())[0]
        sample = index[sample_key]
        
        print(f"   Clave: {sample_key}")
        for k, v in sample.items():
            print(f"   • {k}: {v} (tipo: {type(v).__name__})")


# Si ejecutas este script directamente
if __name__ == "__main__":
    json_file = "excel_tree_real.json"  # Cambia esto si tu archivo tiene otro nombre
    
    print("🚀 SCRIPT DE DIAGNÓSTICO - ¿DÓNDE ESTÁ EL ERROR?")
    print("=" * 100)
    print("Este script mostrará EXACTAMENTE qué datos calcula el Python")
    print("y te ayudará a identificar si el error está en:")
    print("  1. El código Python (cálculos incorrectos)")
    print("  2. Los datos JSON (estructura incorrecta)")
    print("  3. La comunicación entre ambos")
    print("=" * 100)
    
    # Ejecutar diagnóstico completo
    results = diagnose_json_file(json_file)
    
    # Ejecutar prueba rápida
    quick_test(json_file)
    
    print("\n" + "=" * 100)
    print("🎯 CONCLUSIÓN DEL DIAGNÓSTICO:")
    print("=" * 100)
    
    if results and results['status'] == 'success':
        print("\n📋 RESUMEN DE HALLAZGOS:")
        print(f"   • Archivos en árbol: {results['tree_files']}")
        print(f"   • Entradas en índice: {results['index_entries']}")
        
        issues = results['issues']
        if any(len(v) > 0 for v in issues.values()):
            print("\n⚠️  PROBLEMAS ENCONTRADOS:")
            for issue_name, issue_list in issues.items():
                if issue_list:
                    print(f"   • {issue_name}: {len(issue_list)} casos")
                    if len(issue_list) <= 3:
                        print(f"     Ejemplos: {issue_list}")
        else:
            print("\n✅ ¡No se encontraron problemas evidentes!")
        
        print("\n🔧 RECOMENDACIONES:")
        print("   1. Revisa que todas las claves del índice tengan formato 'YYYY-MM'")
        print("   2. Verifica que no haya IDs duplicados")
        print("   3. Asegura que árbol e índice tengan los mismos archivos")
        print("   4. Comprueba que todas las URLs de descarga sean válidas")
    
    print("\n" + "=" * 100)
    print("💡 TIP: Si el problema persiste, ejecuta tu código con:")
    print("      python -m pdb tu_script.py")
    print("      o agrega 'import pdb; pdb.set_trace()' donde falle")
    print("=" * 100)