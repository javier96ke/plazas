#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONVERTIDOR EXCEL → PARQUET CON ALIAS Y REGLAS ESPECÍFICAS
============================================================
✅ SISTEMA DE ALIAS: Flexible con nombres del Excel original
✅ Reglas aplicadas:
   - Mes: 1=Enero, 2=Febrero... (Cve-mes → entero)
   - Año: valor único por fila
   - Estado: IDs secuenciales 1-32 (en tu orden específico)
   - Situación: 0=SUSPENSIÓN, 1=OPERACIÓN
   - Métricas numéricas: todas como enteros
   - Coordenadas: float
   - Texto: strings normalizados
"""

import pandas as pd
import numpy as np
import json
import os
import sys
import re
from unidecode import unidecode
from pathlib import Path
from datetime import datetime
import pickle
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# 1. SISTEMA DE ALIAS COMPLETO (de tu script original)
# ============================================================================
ALIAS_CONFIG = {
    # Año
    "AÑO": {
        "nombre_estandar": "Año",
        "nombres_posibles": ["Año", "AÑO", "Year", "Anio", "Ejercicio", "Periodo Año"]
    },
    # Cve-mes
    "CVE_MES": {
        "nombre_estandar": "Cve-mes",
        "nombres_posibles": ["Cve-mes", "CVE_MES", "Cve mes", "Clave Mes", "Mes_Clave", "ID Mes", "Código Mes", "Periodo"]
    },
    # Mes (texto)
    "MES": {
        "nombre_estandar": "Mes",
        "nombres_posibles": ["Mes", "MES", "Month", "Nombre del Mes", "Mes Texto"]
    },
    # Clave_Edo
    "CLAVE_EDO": {
        "nombre_estandar": "Clave_Edo",
        "nombres_posibles": ["Clave_Edo", "CLAVE_EDO", "Clave Edo", "Cve_Edo", "Clave Estado", "ID Estado", "Código Entidad"]
    },
    # Estado
    "ESTADO": {
        "nombre_estandar": "Estado",
        "nombres_posibles": ["Estado", "ESTADO", "Entidad", "Estado de la República", "Entidad Federativa", "Edo"]
    },
    # Coord. Zona
    "COORD_ZONA": {
        "nombre_estandar": "Coord. Zona",
        "nombres_posibles": ["Coord. Zona", "COORD. ZONA", "Coordinación de Zona", "Zona", "Coord_Zona", "C. Zona", "Coordinación"]
    },
    # Municipio
    "MUNICIPIO": {
        "nombre_estandar": "Municipio",
        "nombres_posibles": ["Municipio", "MUNICIPIO", "Municipio/Del", "Delegación", "Municipio o Delegación"]
    },
    # Localidad
    "LOCALIDAD": {
        "nombre_estandar": "Localidad",
        "nombres_posibles": ["Localidad", "LOCALIDAD", "Ciudad", "Población", "Comunidad"]
    },
    # Colonia
    "COLONIA": {
        "nombre_estandar": "Colonia",
        "nombres_posibles": ["Colonia", "COLONIA", "Fraccionamiento", "Barrio", "Asentamiento"]
    },
    # Cod_Post
    "COD_POST": {
        "nombre_estandar": "Cod_Post",
        "nombres_posibles": ["Cod_Post", "Código Postal", "CP", "Zip Code", "Código Postal", "Postal Code"]
    },
    # Calle
    "CALLE": {
        "nombre_estandar": "Calle",
        "nombres_posibles": ["Calle", "CALLE", "Calle/Avenida", "Avenida", "Vialidad", "Calle y Número"]
    },
    # Num
    "NUM": {
        "nombre_estandar": "Num",
        "nombres_posibles": ["Num", "NUM", "Número", "Número exterior", "No. Ext", "Número Exterior", "No Ext"]
    },
    # Clave_Plaza
    "CLAVE_PLAZA": {
        "nombre_estandar": "Clave_Plaza",
        "nombres_posibles": ["Clave_Plaza", "Clave Plaza", "CLAVE", "Clave", "CCT", "ID", "Clave de Plaza", "Código de Plaza", "Clave_CCT"]
    },
    # Nombre_PC
    "NOMBRE_PC": {
        "nombre_estandar": "Nombre_PC",
        "nombres_posibles": ["Nombre_PC", "Nombre PC", "Nombre", "Nombre de Plaza", "Plaza", "Nombre Plaza Comunitaria"]
    },
    # Situación
    "SITUACION": {
        "nombre_estandar": "Situación",
        "nombres_posibles": ["Situación", "SITUACION", "Status", "Estado_Plaza", "Estatus Operativo", "Situación Operativa"]
    },
    # Inc_Inicial
    "INC_INICIAL": {
        "nombre_estandar": "Inc_Inicial",
        "nombres_posibles": ["Inc_Inicial", "Inscripciones Inicial", "Inc. Inicial", "Inscritos Inicial", "Inscripciones Nivel Inicial"]
    },
    # Inc_Prim
    "INC_PRIM": {
        "nombre_estandar": "Inc_Prim",
        "nombres_posibles": ["Inc_Prim", "Inscripciones Primaria", "Inc. Prim", "Inscritos Primaria", "Inscripciones Nivel Primaria"]
    },
    # Inc_Sec
    "INC_SEC": {
        "nombre_estandar": "Inc_Sec",
        "nombres_posibles": ["Inc_Sec", "Inscripciones Secundaria", "Inc. Sec", "Inscritos Secundaria", "Inscripciones Nivel Secundaria"]
    },
    # Inc_Total
    "INC_TOTAL": {
        "nombre_estandar": "Inc_Total",
        "nombres_posibles": ["Inc_Total", "Inscripciones Total", "Total Inscripciones", "Inscritos Total", "Matrícula Total"]
    },
    # Aten_Inicial
    "ATEN_INICIAL": {
        "nombre_estandar": "Aten_Inicial",
        "nombres_posibles": ["Aten_Inicial", "Atenciones Inicial", "Aten. Inicial", "Atenciones Nivel Inicial"]
    },
    # Aten_Prim
    "ATEN_PRIM": {
        "nombre_estandar": "Aten_Prim",
        "nombres_posibles": ["Aten_Prim", "Atenciones Primaria", "Aten. Prim", "Atenciones Nivel Primaria"]
    },
    # Aten_Sec
    "ATEN_SEC": {
        "nombre_estandar": "Aten_Sec",
        "nombres_posibles": ["Aten_Sec", "Atenciones Secundaria", "Aten. Sec", "Atenciones Nivel Secundaria"]
    },
    # Aten_Total
    "ATEN_TOTAL": {
        "nombre_estandar": "Aten_Total",
        "nombres_posibles": ["Aten_Total", "Atenciones Total", "Total Atenciones", "Atenciones Totales"]
    },
    # Exámenes aplicados
    "EXAMENES": {
        "nombre_estandar": "Exámenes aplicados",
        "nombres_posibles": ["Exámenes aplicados", "Examenes aplicados", "Exámenes", "Exámenes Aplicados", "Evaluaciones"]
    },
    # CN_Inicial_Acum
    "CN_INICIAL_ACUM": {
        "nombre_estandar": "CN_Inicial_Acum",
        "nombres_posibles": ["CN_Inicial_Acum", "CN Inicial Acum", "Certificados Inicial", "Certificados Nivel Inicial", "CN Inicial"]
    },
    # CN_Prim_Acum
    "CN_PRIM_ACUM": {
        "nombre_estandar": "CN_Prim_Acum",
        "nombres_posibles": ["CN_Prim_Acum", "CN Prim Acum", "Certificados Primaria", "Certificados Nivel Primaria", "CN Primaria"]
    },
    # CN_Sec_Acum
    "CN_SEC_ACUM": {
        "nombre_estandar": "CN_Sec_Acum",
        "nombres_posibles": ["CN_Sec_Acum", "CN Sec Acum", "Certificados Secundaria", "Certificados Nivel Secundaria", "CN Secundaria"]
    },
    # CN_Tot_Acum
    "CN_TOT_ACUM": {
        "nombre_estandar": "CN_Tot_Acum",
        "nombres_posibles": ["CN_Tot_Acum", "CN Total Acum", "Total Certificados", "Certificados Totales", "CN Acumulado Total"]
    },
    # Cert_Emitidos
    "CERT_EMITIDOS": {
        "nombre_estandar": "Cert_Emitidos",
        "nombres_posibles": ["Cert_ Emitidos", "Cert_Emitidos", "Certificados Emitidos", "Total Certificados Emitidos", "Cert Emitidos"]
    },
    # Tec_Doc
    "TEC_DOC": {
        "nombre_estandar": "Tec_Doc",
        "nombres_posibles": ["Tec_Doc", "Tec. Doc", "Técnico Docente", "Responsable Técnico", "Técnico"]
    },
    # Nom_PVS_1
    "NOM_PVS_1": {
        "nombre_estandar": "Nom_PVS_1",
        "nombres_posibles": ["Nom_PVS_1", "PVS 1", "Personal 1", "Promotor 1", "Nombre Promotor 1"]
    },
    # Nom_PVS_2
    "NOM_PVS_2": {
        "nombre_estandar": "Nom_PVS_2",
        "nombres_posibles": ["Nom_PVS_2", "PVS 2", "Personal 2", "Promotor 2", "Nombre Promotor 2"]
    },
    # Tipo_local
    "TIPO_LOCAL": {
        "nombre_estandar": "Tipo_local",
        "nombres_posibles": ["Tipo_local", "Tipo Local", "Tipo de Local", "Tipo de Espacio", "Clasificación Local"]
    },
    # Inst_aliada
    "INST_ALIADA": {
        "nombre_estandar": "Inst_aliada",
        "nombres_posibles": ["Inst_aliada", "Institución Aliada", "Inst Aliada", "Institución", "Aliada"]
    },
    # Arq_Discap.
    "ARQ_DISCAP": {
        "nombre_estandar": "Arq_Discap.",
        "nombres_posibles": ["Arq_Discap.", "Arquitectura Discapacidad", "Accesibilidad", "Instalaciones Accesibles", "Adaptaciones"]
    },
    # Conect_Instalada
    "CONECT_INSTALADA": {
        "nombre_estandar": "Conect_Instalada",
        "nombres_posibles": ["Conect_Instalada", "Conectividad Instalada", "Internet", "Conexión", "Tipo de Conexión", "Servicio Internet"]
    },
    # Tipo_Conect
    "TIPO_CONECT": {
        "nombre_estandar": "Tipo_Conect",
        "nombres_posibles": ["Tipo_Conect", "Tipo Conectividad", "Tipo de Conexión", "Medio de Conexión", "Proveedor Internet"]
    },
    # Latitud
    "LATITUD": {
        "nombre_estandar": "Latitud",
        "nombres_posibles": ["Latitud", "LATITUD", "Lat", "Latitude", "Coordenada X", "Latitud GPS"]
    },
    # Longitud
    "LONGITUD": {
        "nombre_estandar": "Longitud",
        "nombres_posibles": ["Longitud", "LONGITUD", "Lon", "Longitude", "Coordenada Y", "Longitud GPS"]
    },
    # Total de equipos de cómputo en la plaza
    "TOTAL_EQUIPOS_COMPUTO": {
        "nombre_estandar": "Total de equipos de cómputo en la plaza",
        "nombres_posibles": [
            "Total de equipos de cómputo en la plaza",
            "Total equipos cómputo",
            "Equipos de cómputo total",
            "Total Computadoras",
            "Equipos Cómputo Total"
        ]
    },
    # Equipos de cómputo que operan
    "EQUIPOS_COMPUTO_OPERAN": {
        "nombre_estandar": "Equipos de cómputo que operan",
        "nombres_posibles": [
            "Equipos de cómputo que operan",
            "Equipos operativos",
            "Computadoras que funcionan",
            "Equipos funcionando",
            "Equipos en operación"
        ]
    },
    # Tipos de equipos de cómputo
    "TIPOS_EQUIPOS_COMPUTO": {
        "nombre_estandar": "Tipos de equipos de cómputo",
        "nombres_posibles": [
            "Tipos de equipos de cómputo",
            "Tipos de computadoras",
            "Variedad equipos",
            "Tipos equipo",
            "Modelos de equipo"
        ]
    },
    # Impresoras que funcionan
    "IMPRESORAS_FUNCIONAN": {
        "nombre_estandar": "Impresoras que funcionan",
        "nombres_posibles": [
            "Impresoras que funcionan",
            "Impresoras operativas",
            "Impresoras en funcionamiento",
            "Impresoras activas"
        ]
    },
    # Impresoras con suministros (toner, hojas)
    "IMPRESORAS_SUMINISTROS": {
        "nombre_estandar": "Impresoras con suministros (toner, hojas)",
        "nombres_posibles": [
            "Impresoras con suministros (toner, hojas)",
            "Impresoras con suministros",
            "Impresoras con insumos",
            "Impresoras abastecidas"
        ]
    },
    # Total de servidores en la plaza
    "TOTAL_SERVIDORES": {
        "nombre_estandar": "Total de servidores en la plaza",
        "nombres_posibles": [
            "Total de servidores en la plaza",
            "Servidores total",
            "Cantidad servidores",
            "Servidores instalados"
        ]
    },
    # Número de servidores que funcionan correctamente
    "SERVIDORES_FUNCIONAN": {
        "nombre_estandar": "Número de servidores que funcionan correctamente",
        "nombres_posibles": [
            "Número de servidores que funcionan correctamente",
            "Servidores operativos",
            "Servidores funcionando",
            "Servidores activos"
        ]
    },
    # Cuantas mesas funcionan
    "MESAS_FUNCIONAN": {
        "nombre_estandar": "Cuantas mesas funcionan",
        "nombres_posibles": [
            "Cuantas mesas funcionan",
            "Mesas operativas",
            "Mesas en buen estado",
            "Mesas funcionales"
        ]
    },
    # Cuantas sillas funcionan
    "SILLAS_FUNCIONAN": {
        "nombre_estandar": "Cuantas sillas funcionan",
        "nombres_posibles": [
            "Cuantas sillas funcionan",
            "Sillas operativas",
            "Sillas en buen estado",
            "Sillas funcionales"
        ]
    },
    # Cuantos Anaqueles funcionan
    "ANAQUELES_FUNCIONAN": {
        "nombre_estandar": "Cuantos Anaqueles funcionan",
        "nombres_posibles": [
            "Cuantos Anaqueles funcionan",
            "Anaquel operativo",
            "Estantes funcionando",
            "Anaquel funcional",
            "Anaquel en buen estado"
        ]
    }
}

# ============================================================================
# 2. MAPEO A NOMBRES FINALES (para el Parquet)
# ============================================================================
FINAL_COLUMN_MAP = {
    # Identificadores
    "Clave_Plaza": "clave",
    "Nombre_PC": "nombre",
    
    # Fechas
    "Año": "anio",
    # "Cve-mes" se usará para crear 'mes' pero no se guarda
    
    # Ubicación
    "Estado": "estado_id",  # Se convertirá a entero
    "Coord. Zona": "zona",
    "Municipio": "municipio",
    "Localidad": "localidad",
    "Colonia": "colonia",
    "Cod_Post": "cp",
    "Calle": "calle",
    "Num": "num",
    
    # Situación
    "Situación": "situacion",
    
    # Inscripciones
    "Inc_Inicial": "inc_inicial",
    "Inc_Prim": "inc_prim",
    "Inc_Sec": "inc_sec",
    "Inc_Total": "inc_total",
    
    # Atenciones
    "Aten_Inicial": "aten_inicial",
    "Aten_Prim": "aten_prim",
    "Aten_Sec": "aten_sec",
    "Aten_Total": "aten_total",
    "Exámenes aplicados": "examenes",
    
    # Certificaciones
    "CN_Inicial_Acum": "cn_inicial",
    "CN_Prim_Acum": "cn_prim",
    "CN_Sec_Acum": "cn_sec",
    "CN_Tot_Acum": "cn_total",
    "Cert_Emitidos": "certificados",
    
    # Personal
    "Tec_Doc": "tec_doc",
    "Nom_PVS_1": "pvs1",
    "Nom_PVS_2": "pvs2",
    
    # Características
    "Tipo_local": "tipo_local",
    "Inst_aliada": "inst_aliada",
    "Arq_Discap.": "arq_discap",
    "Conect_Instalada": "conectividad",
    "Tipo_Conect": "tipo_conect",
    
    # Coordenadas
    "Latitud": "lat",
    "Longitud": "lng",
    
    # Equipamiento
    "Total de equipos de cómputo en la plaza": "eq_total",
    "Equipos de cómputo que operan": "eq_operan",
    "Tipos de equipos de cómputo": "tipos_eq",
    "Impresoras que funcionan": "imp_funcionan",
    "Impresoras con suministros (toner, hojas)": "imp_suministros",
    "Total de servidores en la plaza": "srv_total",
    "Número de servidores que funcionan correctamente": "srv_operan",
    
    # Mobiliario
    "Cuantas mesas funcionan": "mesas",
    "Cuantas sillas funcionan": "sillas",
    "Cuantos Anaqueles funcionan": "anaqueles"
}

# ============================================================================
# 3. LISTA DE ESTADOS EN ORDEN (para IDs secuenciales)
# ============================================================================
ESTADOS_ORDENADOS = [
    "AGUASCALIENTES",
    "BAJA CALIFORNIA",
    "BAJA CALIFORNIA SUR", 
    "CAMPECHE",
    "COAHUILA",
    "COLIMA",
    "CHIAPAS",
    "CHIHUAHUA",
    "CIUDAD DE MÉXICO",
    "DURANGO",
    "GUANAJUATO",
    "GUERRERO",
    "HIDALGO",
    "JALISCO",
    "MEXICO",
    "MICHOACAN",
    "MORELOS",
    "NAYARIT",
    "NUEVO LEON",
    "OAXACA",
    "PUEBLA",
    "QUERETARO",
    "QUINTANA ROO",
    "SAN LUIS POTOSI",
    "SINALOA",
    "SONORA",
    "TABASCO",
    "TAMAULIPAS",
    "TLAXCALA",
    "VERACRUZ",
    "YUCATAN",
    "ZACATECAS"
]

# Crear diccionario estado → id (1-based)
ESTADO_TO_ID = {estado: i+1 for i, estado in enumerate(ESTADOS_ORDENADOS)}

# ============================================================================
# 4. CLASE PRINCIPAL CON ALIAS
# ============================================================================
class ConversorConAlias:
    def __init__(self, input_path='datos_plazas.xlsx', output_dir='datos_optimizados'):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.df = None
        self.mapeo_encontrado = {}  # alias → nombre_real
        self.columnas_faltantes = []
        
        # Archivos de salida
        self.parquet_path = self.output_dir / 'datos_completos.parquet'
        self.metadata_path = self.output_dir / 'metadata.json'
        self.estados_map_path = self.output_dir / 'estados_map.json'
        self.indices_path = self.output_dir / 'indices.pkl'
        self.alias_report_path = self.output_dir / 'alias_report.json'
    
    def normalizar_texto(self, texto):
        """Normaliza texto para matching (sin acentos, minúsculas)"""
        if pd.isna(texto) or texto is None:
            return ""
        texto = unidecode(str(texto))
        texto = re.sub(r'\s+', ' ', texto)
        return texto.strip().lower()
    
    def encontrar_columnas_por_alias(self, df_original):
        """Usa el sistema de alias para encontrar columnas"""
        print("\n🔍 Buscando columnas usando sistema de ALIAS...")
        print(f"   Total de columnas esperadas: {len(ALIAS_CONFIG)}")
        
        # Crear mapa de columnas normalizadas del Excel
        columnas_excel = {}
        for col in df_original.columns:
            col_norm = self.normalizar_texto(col)
            columnas_excel[col_norm] = col
        
        print(f"\n📋 Columnas disponibles en Excel:")
        for i, col in enumerate(sorted(df_original.columns)[:15]):
            print(f"  {i+1:2d}. '{col}'")
        if len(df_original.columns) > 15:
            print(f"  ... y {len(df_original.columns) - 15} más")
        
        mapeo = {}
        alias_report = {}
        
        print(f"\n🎯 Buscando coincidencias:")
        
        for clave_interna, config in ALIAS_CONFIG.items():
            nombre_estandar = config['nombre_estandar']
            nombres_posibles = config['nombres_posibles']
            
            encontrada = None
            metodo = None
            
            # Buscar coincidencia exacta
            for nombre in nombres_posibles:
                if nombre in df_original.columns:
                    encontrada = nombre
                    metodo = 'exacto'
                    break
            
            # Buscar coincidencia normalizada
            if not encontrada:
                for nombre in nombres_posibles:
                    nombre_norm = self.normalizar_texto(nombre)
                    if nombre_norm in columnas_excel:
                        encontrada = columnas_excel[nombre_norm]
                        metodo = 'normalizado'
                        break
            
            if encontrada:
                mapeo[clave_interna] = encontrada
                alias_report[clave_interna] = {
                    'nombre_estandar': nombre_estandar,
                    'nombre_encontrado': encontrada,
                    'metodo': metodo
                }
                print(f"  ✅ {clave_interna:30} → '{encontrada}' ({metodo})")
            else:
                self.columnas_faltantes.append(clave_interna)
                alias_report[clave_interna] = {
                    'nombre_estandar': nombre_estandar,
                    'nombre_encontrado': None,
                    'metodo': 'no_encontrado'
                }
                print(f"  ❌ {clave_interna:30} → NO ENCONTRADA")
        
        # Guardar reporte de alias
        with open(self.alias_report_path, 'w', encoding='utf-8') as f:
            json.dump(alias_report, f, indent=2, ensure_ascii=False)
        
        return mapeo
    
    def cargar_datos(self):
        """Carga datos desde Excel usando sistema de alias"""
        print(f"\n📂 Cargando datos desde: {self.input_path}")
        
        if not self.input_path.exists():
            print(f"❌ Archivo no encontrado: {self.input_path}")
            return False
        
        try:
            # Cargar Excel
            df_raw = pd.read_excel(self.input_path)
            print(f"✅ Excel cargado: {len(df_raw):,} filas, {len(df_raw.columns)} columnas")
            
            # Encontrar columnas usando alias
            self.mapeo_encontrado = self.encontrar_columnas_por_alias(df_raw)
            
            # Crear DataFrame con nombres estándar (los de ALIAS_CONFIG)
            self.df = pd.DataFrame()
            
            # Mapeo inverso: nombre_real → clave_interna
            real_to_alias = {v: k for k, v in self.mapeo_encontrado.items()}
            
            # Copiar datos con nombres estándar
            for col_real in df_raw.columns:
                if col_real in real_to_alias:
                    alias = real_to_alias[col_real]
                    nombre_estandar = ALIAS_CONFIG[alias]['nombre_estandar']
                    self.df[nombre_estandar] = df_raw[col_real]
            
            # Verificar si encontramos Cve-mes para crear la columna mes
            if 'CVE_MES' in self.mapeo_encontrado:
                col_mes = self.mapeo_encontrado['CVE_MES']
                if col_mes in df_raw.columns:
                    # Guardamos el valor original para procesar después
                    self.df['Cve-mes_raw'] = df_raw[col_mes]
                    print(f"  📅 Columna Cve-mes encontrada para generar mes_num")
            
            return True
            
        except Exception as e:
            print(f"❌ Error cargando datos: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def safe_int(self, valor) -> int:
        """Convierte a entero de forma segura"""
        if pd.isna(valor) or valor is None:
            return 0
        try:
            if isinstance(valor, (int, float, np.integer, np.floating)):
                return int(round(float(valor)))
            if isinstance(valor, str):
                valor_str = re.sub(r'[^\d.-]', '', valor.strip())
                if valor_str and valor_str != '-':
                    return int(round(float(valor_str)))
                return 0
            return 0
        except Exception:
            return 0
    
    def safe_string(self, valor) -> str:
        """Convierte a string de forma segura"""
        if pd.isna(valor) or valor is None:
            return ""
        try:
            return str(valor).strip()
        except Exception:
            return ""
    
    def safe_float(self, valor) -> float:
        """Convierte a float de forma segura"""
        if pd.isna(valor) or valor is None:
            return 0.0
        try:
            if isinstance(valor, (int, float, np.integer, np.floating)):
                return float(valor)
            if isinstance(valor, str):
                valor_str = re.sub(r'[^\d.-]', '', valor.strip())
                if valor_str and valor_str != '-':
                    return float(valor_str)
                return 0.0
            return 0.0
        except Exception:
            return 0.0
    
    def procesar_datos(self):
        """Procesa los datos según las reglas específicas"""
        print("\n🔄 Aplicando reglas de transformación...")
        
        if self.df is None or self.df.empty:
            print("❌ No hay datos para procesar")
            return False
        
        # ========================================================
        # 1. CREAR COLUMNA mes (entero 1-12) desde Cve-mes
        # ========================================================
        if 'Cve-mes_raw' in self.df.columns:
            self.df['mes'] = pd.to_numeric(self.df['Cve-mes_raw'], errors='coerce')
            self.df['mes'] = self.df['mes'].fillna(1).clip(1, 12).astype('int64')
            self.df = self.df.drop(columns=['Cve-mes_raw'])
            print(f"  ✅ Mes creado: {self.df['mes'].min()}-{self.df['mes'].max()}")
        else:
            self.df['mes'] = 1
            print(f"  ⚠️ Mes no encontrado, asignado valor 1")
        
        # ========================================================
        # 2. AÑO - valor único (entero)
        # ========================================================
        if 'Año' in self.df.columns:
            self.df['anio'] = pd.to_numeric(self.df['Año'], errors='coerce').fillna(0).astype('int64')
            self.df = self.df.drop(columns=['Año'])
            print(f"  ✅ Año: {self.df['anio'].min()} - {self.df['anio'].max()}")
        
        # ========================================================
        # 3. ESTADO - convertir a entero secuencial
        # ========================================================
        if 'Estado' in self.df.columns:
            # Crear columna estado_id
            self.df['estado_id'] = 0
            
            for estado_nombre, estado_id in ESTADO_TO_ID.items():
                # Buscar coincidencias (case-insensitive)
                mask = self.df['Estado'].astype(str).str.upper().str.strip() == estado_nombre
                self.df.loc[mask, 'estado_id'] = estado_id
                
                if mask.any():
                    print(f"  ✅ Estado {estado_id:2d}: {estado_nombre} ({mask.sum()} registros)")
            
            # Eliminar columna original
            self.df = self.df.drop(columns=['Estado'])
            
            # Guardar mapa de estados
            with open(self.estados_map_path, 'w', encoding='utf-8') as f:
                json.dump(ESTADO_TO_ID, f, indent=2, ensure_ascii=False)
        else:
            print(f"  ❌ Columna Estado no encontrada")
        
        # ========================================================
        # 4. SITUACIÓN - convertir a binario (0/1)
        # ========================================================
        if 'Situación' in self.df.columns:
            def convertir_situacion(val):
                val_str = str(val).upper().strip()
                if 'OPERACIÓN' in val_str:
                    return 1
                elif 'SUSPENSIÓN' in val_str or 'SUSPENSION' in val_str:
                    return 0
                else:
                    return 0  # Por defecto
            
            self.df['situacion'] = self.df['Situación'].apply(convertir_situacion).astype('int64')
            self.df = self.df.drop(columns=['Situación'])
            print(f"  ✅ Situación: 0=SUSPENSIÓN, 1=OPERACIÓN")
            print(f"     En operación: {(self.df['situacion'] == 1).sum():,} registros")
        
        # ========================================================
        # 5. RENOMBRAR TODAS LAS COLUMNAS A NOMBRES FINALES
        # ========================================================
        rename_dict = {}
        for col in self.df.columns:
            if col in FINAL_COLUMN_MAP:
                rename_dict[col] = FINAL_COLUMN_MAP[col]
        
        self.df = self.df.rename(columns=rename_dict)
        
        # ========================================================
        # 6. UBICACIÓN - todas como strings
        # ========================================================
        string_cols = ['zona', 'municipio', 'localidad', 'colonia', 'cp', 
                       'calle', 'num', 'clave', 'nombre', 'tec_doc', 'pvs1', 
                       'pvs2', 'tipo_local', 'inst_aliada', 'arq_discap',
                       'conectividad', 'tipo_conect', 'tipos_eq']
        
        for col in string_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(self.safe_string)
                print(f"  ✅ String: {col}")
        
        # ========================================================
        # 7. MÉTRICAS NUMÉRICAS - todas como enteros
        # ========================================================
        int_cols = [
            'inc_inicial', 'inc_prim', 'inc_sec', 'inc_total',
            'aten_inicial', 'aten_prim', 'aten_sec', 'aten_total', 'examenes',
            'cn_inicial', 'cn_prim', 'cn_sec', 'cn_total', 'certificados',
            'eq_total', 'eq_operan', 'imp_funcionan', 'imp_suministros',
            'srv_total', 'srv_operan', 'mesas', 'sillas', 'anaqueles'
        ]
        
        for col in int_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(self.safe_int).astype('int64')
                print(f"  ✅ Entero: {col}")
        
        # ========================================================
        # 8. COORDENADAS - float
        # ========================================================
        if 'lat' in self.df.columns:
            self.df['lat'] = self.df['lat'].apply(self.safe_float).astype('float64')
            print(f"  ✅ Float: lat")
        
        if 'lng' in self.df.columns:
            self.df['lng'] = self.df['lng'].apply(self.safe_float).astype('float64')
            print(f"  ✅ Float: lng")
        
        print(f"\n✅ Procesamiento completado: {len(self.df):,} filas, {len(self.df.columns)} columnas")
        return True
    
    def crear_indices(self):
        """Crea índices para búsquedas rápidas"""
        print("\n🔍 Creando índices...")
        
        indices = {}
        
        # Índice por clave
        if 'clave' in self.df.columns:
            indices['por_clave'] = {}
            for i, clave in enumerate(self.df['clave'].values):
                if clave and clave.strip():
                    indices['por_clave'][clave.strip().upper()] = i
            print(f"  ✅ Índice por clave: {len(indices['por_clave'])} registros")
        
        # Índice por estado_id
        if 'estado_id' in self.df.columns:
            indices['por_estado'] = {}
            for i, estado_id in enumerate(self.df['estado_id'].values):
                if estado_id > 0:
                    if estado_id not in indices['por_estado']:
                        indices['por_estado'][estado_id] = []
                    indices['por_estado'][estado_id].append(i)
            print(f"  ✅ Índice por estado: {len(indices['por_estado'])} estados")
        
        # Índice por año/mes
        if 'anio' in self.df.columns and 'mes' in self.df.columns:
            indices['por_fecha'] = {}
            for i, (anio, mes) in enumerate(zip(self.df['anio'].values, self.df['mes'].values)):
                if anio > 0 and mes > 0:
                    key = f"{anio}-{mes:02d}"
                    if key not in indices['por_fecha']:
                        indices['por_fecha'][key] = []
                    indices['por_fecha'][key].append(i)
            print(f"  ✅ Índice por fecha: {len(indices['por_fecha'])} períodos")
        
        # Guardar índices
        with open(self.indices_path, 'wb') as f:
            pickle.dump(indices, f)
        
        return indices
    
    def crear_metadata(self):
        """Crea metadata del dataset"""
        print("\n📋 Generando metadata...")
        
        metadata = {
            'fecha_generacion': datetime.now().isoformat(),
            'version': '2.0 - Con Alias',
            'total_filas': len(self.df),
            'total_columnas': len(self.df.columns),
            'columnas': list(self.df.columns),
            'tipos': {col: str(self.df[col].dtype) for col in self.df.columns},
            'estadisticas_rapidas': {},
            'mapeo_estados': ESTADO_TO_ID,
            'alias_report': str(self.alias_report_path)
        }
        
        # Estadísticas rápidas
        if 'estado_id' in self.df.columns:
            metadata['estadisticas_rapidas']['total_estados'] = int(self.df['estado_id'].nunique())
        
        if 'clave' in self.df.columns:
            metadata['estadisticas_rapidas']['total_plazas'] = int(self.df['clave'].nunique())
        
        if 'anio' in self.df.columns:
            metadata['estadisticas_rapidas']['años'] = sorted(self.df['anio'].unique().tolist())
        
        if 'mes' in self.df.columns:
            metadata['estadisticas_rapidas']['meses'] = sorted(self.df['mes'].unique().tolist())
        
        # Guardar metadata
        with open(self.metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Metadata guardada")
        return metadata
    
    def guardar_parquet(self):
        """Guarda el DataFrame como Parquet optimizado"""
        print(f"\n💾 Guardando Parquet: {self.parquet_path}")
        
        # Optimizar tipos para reducir tamaño
        for col in self.df.select_dtypes(include=['object']).columns:
            if self.df[col].nunique() / len(self.df) < 0.5:  # Si hay repeticiones
                self.df[col] = self.df[col].astype('category')
                print(f"  🟡 Category: {col}")
        
        # Guardar
        self.df.to_parquet(
            self.parquet_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        size_mb = self.parquet_path.stat().st_size / (1024 * 1024)
        print(f"✅ Archivo guardado: {size_mb:.2f} MB")
        
        return True
    
    def generar_reporte(self):
        """Genera reporte final"""
        print("\n" + "="*80)
        print("📊 REPORTE FINAL")
        print("="*80)
        
        print(f"\n📈 Estadísticas generales:")
        print(f"  • Filas: {len(self.df):,}")
        print(f"  • Columnas: {len(self.df.columns)}")
        
        if 'clave' in self.df.columns:
            print(f"  • Plazas únicas: {self.df['clave'].nunique():,}")
        
        if 'estado_id' in self.df.columns:
            print(f"  • Estados: {self.df['estado_id'].nunique()}")
        
        if 'anio' in self.df.columns:
            print(f"  • Años: {sorted(self.df['anio'].unique())}")
        
        print(f"\n📁 Archivos generados:")
        print(f"  • {self.parquet_path.name}")
        print(f"  • {self.metadata_path.name}")
        print(f"  • {self.estados_map_path.name}")
        print(f"  • {self.indices_path.name}")
        print(f"  • {self.alias_report_path.name}")
        
        print(f"\n🔍 Resumen de alias:")
        print(f"  • Columnas encontradas: {len(self.mapeo_encontrado)} de {len(ALIAS_CONFIG)}")
        if self.columnas_faltantes:
            print(f"  • Columnas no encontradas: {len(self.columnas_faltantes)}")
            for col in self.columnas_faltantes[:10]:
                print(f"    - {col}")
        
        print("\n" + "="*80)
    
    def ejecutar(self):
        """Ejecuta todo el proceso"""
        print("""
╔══════════════════════════════════════════════════════════════════════════╗
║   CONVERTIDOR CON ALIAS v2.0                                             ║
║   ✓ Sistema de alias flexible para cualquier Excel                       ║
║   ✓ Aplica reglas específicas de transformación                          ║
║   ✓ Genera archivos optimizados para backend                             ║
╚══════════════════════════════════════════════════════════════════════════╝
        """)
        
        if not self.cargar_datos():
            return False
        
        if not self.procesar_datos():
            return False
        
        self.crear_indices()
        self.crear_metadata()
        self.guardar_parquet()
        self.generar_reporte()
        
        print(f"\n✅ PROCESO COMPLETADO!")
        print(f"📁 Directorio: {self.output_dir.absolute()}")
        
        return True


# ============================================================================
# 5. FUNCIÓN PRINCIPAL
# ============================================================================
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Convierte Excel a Parquet con sistema de alias')
    parser.add_argument('input', nargs='?', default='datos_plazas.xlsx',
                       help='Archivo de entrada (Excel)')
    parser.add_argument('--output-dir', '-o', default='datos_optimizados',
                       help='Directorio de salida')
    
    args = parser.parse_args()
    
    conversor = ConversorConAlias(args.input, args.output_dir)
    success = conversor.ejecutar()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())