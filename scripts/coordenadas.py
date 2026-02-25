"""
generar_coordenadas.py
======================
Genera coordenadasplazas.json a partir de cualquier Excel, Parquet o CSV.

Extrae TODOS los campos que necesita el mapa:
  clave, nombre, estado, municipio, localidad, situacion, lat, lng
  + campos legacy para rust_bridge: Clave_Plaza, Latitud, Longitud
"""

ARCHIVO_FUENTE  = r'datos_plazas.parquet'   # ← Excel, Parquet o CSV
ARCHIVO_SALIDA  = r'coordenadasplazas.json' # ← mismo nombre que usa app.py
SOLO_ULTIMO_MES = True                       # True = filtra el periodo más reciente


import sys, os, json, logging
from pathlib import Path
import pandas as pd
import numpy as np

logging.basicConfig(format='%(asctime)s  %(levelname)s  %(message)s',
                    datefmt='%H:%M:%S', level=logging.INFO)
log = logging.getLogger(__name__)

# ── Aliases por campo ─────────────────────────────────────────────────────────
ALIAS_CLAVE = [
    'Clave_Plaza', 'CLAVE_PLAZA', 'clave_plaza', 'clave', 'CLAVE', 'Clave',
    'CVE_PLAZA', 'cve_plaza', 'ID_PLAZA', 'id_plaza', 'FOLIO', 'folio',
    'Clave Plaza', 'CLAVE PLAZA',
]
ALIAS_LAT = [
    'Latitud', 'LATITUD', 'latitud', 'lat', 'LAT', 'Lat',
    'LATITUD_DEC', 'latitud_dec', 'latitude', 'LATITUDE',
    'Y', 'y_coord', 'COORD_Y', 'coord_y',
]
ALIAS_LNG = [
    'Longitud', 'LONGITUD', 'longitud', 'lng', 'LNG', 'Lng',
    'lon', 'LON', 'Lon', 'LONGITUD_DEC', 'longitud_dec',
    'longitude', 'LONGITUDE', 'X', 'x_coord', 'COORD_X', 'coord_x',
]
ALIAS_PERIODO = [
    'Cve-mes', 'CVE_MES', 'cve_mes', 'CVE-MES', 'PERIODO', 'periodo',
    'MES_CLAVE', 'mes_clave', 'Año_Mes', 'anio_mes', 'AÑO_MES', 'YYYYMM',
]

# ── Aliases para los campos NUEVOS que necesita el mapa ──────────────────────
ALIAS_NOMBRE = [
    'Nombre_PC', 'NOMBRE_PC', 'nombre_pc', 'Nombre', 'NOMBRE', 'nombre',
    'NombrePlaza', 'NOMBRE_PLAZA', 'nombre_plaza', 'Name', 'name',
]
ALIAS_ESTADO = [
    'Estado', 'ESTADO', 'estado', 'Entidad', 'ENTIDAD', 'entidad',
    'Estado_Plaza', 'CVE_EDO', 'Clave_Edo',
]
ALIAS_MUNICIPIO = [
    'Municipio', 'MUNICIPIO', 'municipio', 'Mpio', 'MPIO', 'mpio',
    'Municipio_Plaza', 'MUNICIPIO_PLAZA',
]
ALIAS_LOCALIDAD = [
    'Localidad', 'LOCALIDAD', 'localidad', 'Loc', 'LOC', 'loc',
    'Localidad_Plaza', 'LOCALIDAD_PLAZA',
]
ALIAS_SITUACION = [
    'Situación', 'Situacion', 'SITUACION', 'situacion', 'SITUACIÓN',
    'Estatus', 'ESTATUS', 'estatus', 'Status', 'STATUS', 'status',
    'Estado_Operacion', 'ESTADO_OPERACION',
]


def _norm(s: str) -> str:
    return s.strip().lower().replace(' ', '_').replace('-', '_')


def encontrar_columna(df: pd.DataFrame, aliases: list) -> str | None:
    """Devuelve el nombre real de la columna en df, o None."""
    cols = {_norm(c): c for c in df.columns}
    for alias in aliases:
        real = cols.get(_norm(alias))
        if real is not None:
            return real
    return None


def leer_archivo(ruta: str) -> pd.DataFrame:
    ext = Path(ruta).suffix.lower()
    log.info(f'Leyendo {Path(ruta).name} ...')

    if ext in ('.xlsx', '.xls', '.xlsm', '.xlsb'):
        xl = pd.ExcelFile(ruta)
        mejor, n = None, 0
        for hoja in xl.sheet_names:
            try:
                df = xl.parse(hoja, dtype=str)
                if len(df) > n:
                    mejor, n = df, len(df)
                    log.info(f'  hoja "{hoja}": {len(df)} filas')
            except Exception:
                pass
        if mejor is None or mejor.empty:
            raise ValueError('No se encontraron datos en el Excel')
        return mejor

    if ext == '.parquet':
        return pd.read_parquet(ruta)

    if ext in ('.csv', '.tsv', '.txt'):
        sep = '\t' if ext == '.tsv' else ','
        for enc in ('utf-8', 'latin-1', 'cp1252'):
            try:
                return pd.read_csv(ruta, sep=sep, encoding=enc,
                                   dtype=str, low_memory=False)
            except UnicodeDecodeError:
                continue
        raise ValueError('No se pudo leer el CSV (prueba guardarlo como UTF-8)')

    raise ValueError(f'Formato no soportado: "{ext}"  —  usa .xlsx, .parquet o .csv')


def filtrar_ultimo_periodo(df: pd.DataFrame, col: str) -> pd.DataFrame:
    try:
        nums = pd.to_numeric(df[col], errors='coerce')
        maximo = nums.max()
        if pd.isna(maximo):
            return df
        df2 = df[nums == maximo].copy()
        log.info(f'Periodo más reciente: {int(maximo)}  →  {len(df2)} filas')
        return df2
    except Exception as e:
        log.warning(f'No se pudo filtrar periodo: {e}')
        return df


def str_seguro(valor) -> str:
    """Convierte un valor a string limpio, devuelve '' si es nulo."""
    if valor is None:
        return ''
    s = str(valor).strip()
    return '' if s.lower() in ('nan', 'none', 'nat', '') else s


def extraer(df: pd.DataFrame) -> list:
    # Campos obligatorios
    col_clave   = encontrar_columna(df, ALIAS_CLAVE)
    col_lat     = encontrar_columna(df, ALIAS_LAT)
    col_lng     = encontrar_columna(df, ALIAS_LNG)
    col_periodo = encontrar_columna(df, ALIAS_PERIODO)

    # Campos opcionales (el mapa los muestra en el popup)
    col_nombre    = encontrar_columna(df, ALIAS_NOMBRE)
    col_estado    = encontrar_columna(df, ALIAS_ESTADO)
    col_municipio = encontrar_columna(df, ALIAS_MUNICIPIO)
    col_localidad = encontrar_columna(df, ALIAS_LOCALIDAD)
    col_situacion = encontrar_columna(df, ALIAS_SITUACION)

    log.info('─── Columnas detectadas ───────────────────────────')
    log.info(f'  clave     → "{col_clave}"')
    log.info(f'  lat       → "{col_lat}"')
    log.info(f'  lng       → "{col_lng}"')
    log.info(f'  periodo   → "{col_periodo}"  (para filtro último mes)')
    log.info(f'  nombre    → "{col_nombre}"')
    log.info(f'  estado    → "{col_estado}"')
    log.info(f'  municipio → "{col_municipio}"')
    log.info(f'  localidad → "{col_localidad}"')
    log.info(f'  situacion → "{col_situacion}"')
    log.info('───────────────────────────────────────────────────')

    faltantes = [n for n, c in [('clave', col_clave), ('lat', col_lat), ('lng', col_lng)] if not c]
    if faltantes:
        log.error(f'Faltan columnas OBLIGATORIAS: {faltantes}')
        log.error('Columnas disponibles en el archivo:')
        for c in df.columns:
            log.error(f'  "{c}"')
        sys.exit(1)

    campos_opcionales_faltantes = [
        n for n, c in [
            ('nombre', col_nombre), ('estado', col_estado),
            ('municipio', col_municipio), ('localidad', col_localidad),
            ('situacion', col_situacion)
        ] if not c
    ]
    if campos_opcionales_faltantes:
        log.warning(f'Campos opcionales NO encontrados (quedarán vacíos): {campos_opcionales_faltantes}')
        log.warning('El mapa funcionará pero sin esa info en los popups.')

    # Filtrar último periodo si aplica
    if SOLO_ULTIMO_MES and col_periodo:
        df = filtrar_ultimo_periodo(df, col_periodo)

    resultado = []
    claves_vistas: set = set()
    sin_coords = sin_clave = duplicados = 0
    fuera_de_mexico: list = []   # [(clave, lat, lng, motivo), ...]

    for _, fila in df.iterrows():
        # ── Clave ────────────────────────────────────────────────────────────
        clave = str_seguro(fila[col_clave])
        if not clave:
            sin_clave += 1
            continue

        # ── Coordenadas ──────────────────────────────────────────────────────
        try:
            lat = float(str(fila[col_lat]).replace(',', '.'))
            lng = float(str(fila[col_lng]).replace(',', '.'))
        except (ValueError, TypeError):
            sin_coords += 1
            fuera_de_mexico.append((clave, fila[col_lat], fila[col_lng], 'no_numerico'))
            continue

        # Descartar si son nulas, cero, infinitas o fuera del rango de México
        motivo = None
        if np.isnan(lat) or np.isnan(lng):
            motivo = 'NaN'
        elif np.isinf(lat) or np.isinf(lng):
            motivo = 'infinito'
        elif lat == 0.0 or lng == 0.0:
            motivo = f'cero  (lat={lat}, lng={lng})'
        elif not (14.5 <= lat <= 32.7):
            motivo = f'lat fuera de México  ({lat})'
        elif not (-118.5 <= lng <= -86.7):
            motivo = f'lng fuera de México  ({lng})'

        if motivo:
            sin_coords += 1
            fuera_de_mexico.append((clave, lat, lng, motivo))
            continue

        # ── Deduplicar ───────────────────────────────────────────────────────
        if clave in claves_vistas:
            duplicados += 1
            continue
        claves_vistas.add(clave)

        # ── Campos opcionales ────────────────────────────────────────────────
        nombre    = str_seguro(fila[col_nombre])    if col_nombre    else ''
        estado    = str_seguro(fila[col_estado])    if col_estado    else ''
        municipio = str_seguro(fila[col_municipio]) if col_municipio else ''
        localidad = str_seguro(fila[col_localidad]) if col_localidad else ''
        situacion = str_seguro(fila[col_situacion]) if col_situacion else ''

        # ── Registro final ───────────────────────────────────────────────────
        # Formato completo: satisface app.py + mapa + rust_bridge
        resultado.append({
            # ← app.py y mapa usan estos
            'clave':     clave,
            'nombre':    nombre,
            'estado':    estado,
            'municipio': municipio,
            'localidad': localidad,
            'situacion': situacion,
            'lat':       round(lat, 7),
            'lng':       round(lng, 7),
            # ← rust_bridge usa estos
            'Clave_Plaza': clave,
            'Latitud':     round(lat, 7),
            'Longitud':    round(lng, 7),
        })

    log.info(f'✅ {len(resultado)} plazas con coordenadas válidas')
    if sin_clave:   log.warning(f'   Omitidos sin clave:              {sin_clave}')
    if sin_coords:  log.warning(f'   Omitidos sin coords / inválidas: {sin_coords}  ← ceros, NaN, fuera de México')
    if duplicados:  log.warning(f'   Duplicados omitidos:             {duplicados}')

    if fuera_de_mexico:
        log.warning('')
        log.warning('─── Claves omitidas por coordenadas inválidas ──────────────────────')
        log.warning(f'  {"CLAVE":<20}  {"LAT":>12}  {"LNG":>12}  MOTIVO')
        log.warning(f'  {"─"*20}  {"─"*12}  {"─"*12}  {"─"*30}')
        for clave, lat, lng, motivo in fuera_de_mexico:
            lat_str = f'{lat:>12.6f}' if isinstance(lat, float) else f'{str(lat):>12}'
            lng_str = f'{lng:>12.6f}' if isinstance(lng, float) else f'{str(lng):>12}'
            log.warning(f'  {clave:<20}  {lat_str}  {lng_str}  {motivo}')
        log.warning(f'  Total: {len(fuera_de_mexico)} plazas omitidas')
        log.warning('────────────────────────────────────────────────────────────────────')

        # Guardar también en archivo de texto para revisión
        ruta_reporte = ARCHIVO_SALIDA.replace('.json', '_omitidas.txt')
        try:
            with open(ruta_reporte, 'w', encoding='utf-8') as f:
                f.write(f'Plazas omitidas por coordenadas inválidas\n')
                f.write(f'Generado: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
                f.write(f'Total omitidas: {len(fuera_de_mexico)}\n')
                f.write('─' * 70 + '\n')
                f.write(f'{"CLAVE":<20}  {"LAT":>12}  {"LNG":>12}  MOTIVO\n')
                f.write(f'{"─"*20}  {"─"*12}  {"─"*12}  {"─"*30}\n')
                for clave, lat, lng, motivo in fuera_de_mexico:
                    lat_str = f'{lat:>12.6f}' if isinstance(lat, float) else f'{str(lat):>12}'
                    lng_str = f'{lng:>12.6f}' if isinstance(lng, float) else f'{str(lng):>12}'
                    f.write(f'{clave:<20}  {lat_str}  {lng_str}  {motivo}\n')
            log.info(f'📄 Reporte de omitidas guardado en: {ruta_reporte}')
        except Exception as e:
            log.warning(f'No se pudo guardar reporte de omitidas: {e}')

    if campos_opcionales_faltantes:
        log.warning(f'   Campos vacíos en todos los registros: {campos_opcionales_faltantes}')

    return resultado


def guardar(registros: list, salida: str) -> None:
    tmp = salida + '.tmp'
    try:
        import orjson
        with open(tmp, 'wb') as f:
            f.write(orjson.dumps(registros))
        log.info('  (usando orjson — rápido)')
    except ImportError:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(registros, f, ensure_ascii=False, separators=(',', ':'))
        log.info('  (usando json estándar)')

    if os.path.exists(salida):
        os.remove(salida)
    os.rename(tmp, salida)
    kb = os.path.getsize(salida) / 1024
    log.info(f'💾 Guardado: {salida}  ({kb:.0f} KB, {len(registros)} registros)')


def main():
    import argparse

    p = argparse.ArgumentParser(
        description='Genera coordenadasplazas.json desde Excel / Parquet / CSV')
    p.add_argument('fuente',       nargs='?', default=None,  help='Archivo de datos')
    p.add_argument('--salida',     default=None,             help='Archivo JSON de salida')
    p.add_argument('--ultimo-mes', action='store_true',      help='Filtrar solo el último periodo')
    args = p.parse_args()

    fuente = args.fuente or ARCHIVO_FUENTE
    salida = args.salida or ARCHIVO_SALIDA
    global SOLO_ULTIMO_MES
    if args.ultimo_mes:
        SOLO_ULTIMO_MES = True

    if not fuente:
        p.print_help()
        print()
        log.error('Debes indicar el archivo fuente.')
        sys.exit(1)

    if not os.path.exists(fuente):
        log.error(f'No se encontró el archivo: {fuente}')
        sys.exit(1)

    df = leer_archivo(fuente)
    log.info(f'{len(df)} filas × {len(df.columns)} columnas')

    registros = extraer(df)
    if not registros:
        log.error('No se generaron registros. Verifica las columnas del archivo.')
        sys.exit(1)

    guardar(registros, salida)
    log.info('')
    log.info('═══════════════════════════════════════════════════')
    log.info(f'  JSON listo con {len(registros)} plazas')
    log.info(f'  Campos: clave, nombre, estado, municipio,')
    log.info(f'          localidad, situacion, lat, lng')
    log.info(f'  + Clave_Plaza, Latitud, Longitud (rust_bridge)')
    log.info('═══════════════════════════════════════════════════')


if __name__ == '__main__':
    main()