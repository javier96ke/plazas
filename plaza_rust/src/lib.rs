// ==============================================================================
// plaza_rust/src/lib.rs  v5.1
//
// DOS niveles de cache en Rust:
//
//   ENGINE_PERIODOS  →  datos crudos por periodo (arrays numéricos)
//                        clave: PeriodoKey = año*100+mes  (u32)
//
//   RESULT_CACHE     →  resultados de comparaciones ya calculadas
//                        clave: (key1, key2, filtro_situacion)
//                        valor: HashMap<estado_id, [i64; 6]> x2 + timestamp
//
// Cuando Python llama comparar_periodos(key1, key2, filtro):
//   1. Busca en RESULT_CACHE   → hit: devuelve directo (sin recalcular nada)
//   2. Miss: calcula con Rayon → guarda en RESULT_CACHE → devuelve
//
// Evicción (llamada desde Python watchdog o TTL):
//   - evict_resultado(key1, key2, filtro)  → borra una entrada de RESULT_CACHE
//   - evict_periodo(key)                   → borra datos crudos
//   - limpiar_resultados_expirados(ttl_s)  → borra resultados más viejos que ttl_s
//   - limpiar_periodos_lru(max_n)          → deja solo los max_n más recientes
// ==============================================================================
// ==============================================================================
// plaza_rust/src/lib.rs  v5.2
//
// FIX v5.2: CN_Sec_Acum (cn_sec) ahora se acumula y expone correctamente.
//   - Array [i64; 6] → [i64; 7]  (índice 6 = cn_sec)
//   - agregar(): e[6] += eng.cn_sec[i].max(0)
//   - reduce(): for i in 0..7
//   - to_py_map(): m.insert("cn_sec", v[6])
// ==============================================================================

use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use rayon::prelude::*;

// ---------------------------------------------------------------------------
// Tipos
// ---------------------------------------------------------------------------
type PeriodoKey = u32;
type ResultKey  = (u32, u32, i64);

// ---------------------------------------------------------------------------
// Datos crudos de un periodo
// ---------------------------------------------------------------------------
#[derive(Clone)]
struct EngineData {
    n:             usize,
    lats:          Vec<f64>,
    lngs:          Vec<f64>,
    estado_ids:    Vec<i64>,
    situaciones:   Vec<i64>,
    inc_totales:   Vec<i64>,
    aten_totales:  Vec<i64>,
    cn_totales:    Vec<i64>,
    cn_ini:        Vec<i64>,
    cn_prim:       Vec<i64>,
    cn_sec:        Vec<i64>,
    cargado_at:    u64,
    ultimo_acceso: u64,
}

// ---------------------------------------------------------------------------
// Resultado de una comparación  ← CAMBIADO: [i64; 6] → [i64; 7]
// ---------------------------------------------------------------------------
#[derive(Clone)]
struct ResultadoComp {
    agr1:          HashMap<i64, [i64; 7]>,
    agr2:          HashMap<i64, [i64; 7]>,
    calculado_at:  u64,
    ultimo_acceso: u64,
    accesos:       u64,
}

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------
static ENGINE_PERIODOS: RwLock<Option<HashMap<PeriodoKey, EngineData>>> = RwLock::new(None);
static RESULT_CACHE:    RwLock<Option<HashMap<ResultKey,  ResultadoComp>>> = RwLock::new(None);
static ENGINE:          RwLock<Option<EngineData>> = RwLock::new(None);

const MAX_PERIODOS:   usize = 24;
const MAX_RESULTADOS: usize = 200;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ===========================================================================
// DESCOMPRESIÓN
// ===========================================================================
fn decompress_bytes(data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
        let mut dec = flate2::read::GzDecoder::new(Cursor::new(data));
        let mut out = Vec::new();
        dec.read_to_end(&mut out).map_err(|e| format!("gzip: {e}"))?;
        Ok(out)
    } else if data.len() >= 4 && &data[0..4] == b"\xfd\x2f\xb5\x28" {
        zstd::decode_all(Cursor::new(data)).map_err(|e| format!("zstd: {e}"))
    } else {
        Ok(data.to_vec())
    }
}

// ===========================================================================
// PARSEO PARQUET → EngineData
// ===========================================================================
fn parse_parquet_bytes(raw: &[u8]) -> Result<EngineData, String> {
    use arrow_array::{
        Array,
        Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use bytes::Bytes;

    let cols_interes = [
        "lat", "lng",
        "estado_id", "situacion",
        "inc_total", "aten_total",
        "cn_total", "cn_inicial", "cn_prim", "cn_sec",
        "Latitud", "Longitud",
        "Clave_Edo", "Situacion", "Situación",
        "Inc_Total", "Aten_Total",
        "CN_Tot_Acum", "CN_Inicial_Acum", "CN_Prim_Acum", "CN_Sec_Acum",
    ];

    let bytes = Bytes::copy_from_slice(raw);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| format!("builder: {e}"))?;

    let schema = builder.schema().clone();
    let parquet_schema = builder.parquet_schema().clone();

    let projection: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| cols_interes.contains(&f.name().as_str()))
        .map(|(i, _)| i)
        .collect();

    if projection.is_empty() {
        return Err("No se encontraron columnas esperadas en el parquet".to_string());
    }

    let mask = parquet::arrow::ProjectionMask::roots(&parquet_schema, projection);
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|e| format!("reader: {e}"))?;

    let mut col_map_f64: HashMap<String, Vec<f64>> = HashMap::new();
    let mut col_map_i64: HashMap<String, Vec<i64>> = HashMap::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("batch: {e}"))?;
        for col_idx in 0..batch.num_columns() {
            let name = batch.schema().field(col_idx).name().clone();
            if !cols_interes.contains(&name.as_str()) {
                continue;
            }
            let col = batch.column(col_idx);

            if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                let entry = col_map_f64.entry(name).or_default();
                for j in 0..a.len() {
                    entry.push(if a.is_valid(j) { a.value(j) } else { f64::NAN });
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
                let entry = col_map_f64.entry(name).or_default();
                for j in 0..a.len() {
                    entry.push(if a.is_valid(j) { a.value(j) as f64 } else { f64::NAN });
                }
            } else {
                let entry = col_map_i64.entry(name).or_default();
                macro_rules! try_int {
                    ($ArrayType:ty) => {
                        if let Some(a) = col.as_any().downcast_ref::<$ArrayType>() {
                            for j in 0..a.len() {
                                entry.push(if a.is_valid(j) { a.value(j) as i64 } else { i64::MIN });
                            }
                            continue;
                        }
                    };
                }
                try_int!(Int64Array);
                try_int!(Int32Array);
                try_int!(Int16Array);
                try_int!(Int8Array);
                try_int!(UInt64Array);
                try_int!(UInt32Array);
                try_int!(UInt16Array);
                try_int!(UInt8Array);
            }
        }
    }

    let get_f64 = |names: &[&str]| -> Vec<f64> {
        for n in names {
            if let Some(v) = col_map_f64.get(*n) { return v.clone(); }
        }
        vec![]
    };
    let get_i64 = |names: &[&str]| -> Vec<i64> {
        for n in names {
            if let Some(v) = col_map_i64.get(*n) { return v.clone(); }
        }
        vec![]
    };

    let lats_data = get_f64(&["lat", "Latitud"]);
    let n = lats_data.len();
    let fill_f = |v: Vec<f64>| if v.len() == n { v } else { vec![f64::NAN; n] };
    let fill_i = |v: Vec<i64>| if v.len() == n { v } else { vec![i64::MIN; n] };

    Ok(EngineData {
        n,
        lats:         fill_f(lats_data),
        lngs:         fill_f(get_f64(&["lng",        "Longitud"])),
        estado_ids:   fill_i(get_i64(&["estado_id",  "Clave_Edo"])),
        situaciones:  fill_i(get_i64(&["situacion",  "Situación", "Situacion"])),
        inc_totales:  fill_i(get_i64(&["inc_total",  "Inc_Total"])),
        aten_totales: fill_i(get_i64(&["aten_total", "Aten_Total"])),
        cn_totales:   fill_i(get_i64(&["cn_total",   "CN_Tot_Acum"])),
        cn_ini:       fill_i(get_i64(&["cn_inicial", "CN_Inicial_Acum"])),
        cn_prim:      fill_i(get_i64(&["cn_prim",    "CN_Prim_Acum"])),
        cn_sec:       fill_i(get_i64(&["cn_sec",     "CN_Sec_Acum"])),
        cargado_at:    now_secs(),
        ultimo_acceso: now_secs(),
    })
}

// ===========================================================================
// AGREGACIÓN PARALELA (Rayon)  ← CAMBIADO: [i64; 6] → [i64; 7], +e[6]=cn_sec
// ===========================================================================
fn agregar(eng: &EngineData, filtro_sit: i64) -> HashMap<i64, [i64; 7]> {
    type Local = HashMap<i64, [i64; 7]>;

    (0..eng.n)
        .into_par_iter()
        .fold(Local::new, |mut acc, i| {
            if filtro_sit >= 0 {
                let sit = eng.situaciones[i];
                if sit == i64::MIN || sit != filtro_sit { return acc; }
            }
            let eid = eng.estado_ids[i];
            if eid == i64::MIN { return acc; }

            let e = acc.entry(eid).or_insert([0i64; 7]);
            e[0] += 1;
            e[1] += eng.inc_totales[i].max(0);
            e[2] += eng.aten_totales[i].max(0);
            e[3] += eng.cn_totales[i].max(0);
            e[4] += eng.cn_ini[i].max(0);
            e[5] += eng.cn_prim[i].max(0);
            e[6] += eng.cn_sec[i].max(0);   // ← FIX: CN_Sec_Acum
            acc
        })
        .reduce(Local::new, |mut a, b| {
            for (k, v) in b {
                let e = a.entry(k).or_insert([0i64; 7]);
                for i in 0..7 { e[i] += v[i]; }   // ← FIX: 0..7
            }
            a
        })
}

// ← CAMBIADO: ahora expone cn_sec (v[6])
fn to_py_map(arr: &HashMap<i64, [i64; 7]>) -> HashMap<i64, HashMap<String, i64>> {
    arr.iter().map(|(&eid, v)| {
        let mut m = HashMap::with_capacity(7);
        m.insert("plazas".into(),     v[0]);
        m.insert("inc_total".into(),  v[1]);
        m.insert("aten_total".into(), v[2]);
        m.insert("cn_total".into(),   v[3]);
        m.insert("cn_ini".into(),     v[4]);
        m.insert("cn_prim".into(),    v[5]);
        m.insert("cn_sec".into(),     v[6]);   // ← FIX: CN_Sec_Acum
        (eid, m)
    }).collect()
}

// ===========================================================================
// FUNCIONES EXPORTADAS A PYTHON
// ===========================================================================

#[pyfunction]
fn cargar_periodo_parquet(
    py:          Python<'_>,
    data:        &Bound<'_, PyBytes>,
    periodo_key: u32,
) -> PyResult<usize> {
    let raw = data.as_bytes().to_vec();

    let eng = py.allow_threads(|| -> Result<EngineData, String> {
        let bytes = decompress_bytes(&raw)?;
        parse_parquet_bytes(&bytes)
    }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

    let n = eng.n;

    let mut guard = ENGINE_PERIODOS.write()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let map = guard.get_or_insert_with(HashMap::new);

    if map.len() >= MAX_PERIODOS && !map.contains_key(&periodo_key) {
        if let Some(&lru_key) = map.iter()
            .min_by_key(|(_, v)| v.ultimo_acceso)
            .map(|(k, _)| k)
        {
            map.remove(&lru_key);
        }
    }

    map.insert(periodo_key, eng);
    Ok(n)
}

#[pyfunction]
fn periodo_en_cache(periodo_key: u32) -> PyResult<bool> {
    let guard = ENGINE_PERIODOS.read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    Ok(guard.as_ref().map_or(false, |m| m.contains_key(&periodo_key)))
}

#[pyfunction]
fn comparar_periodos(
    py:               Python<'_>,
    key1:             u32,
    key2:             u32,
    filtro_situacion: i64,
) -> PyResult<HashMap<String, HashMap<i64, HashMap<String, i64>>>> {
    let result_key: ResultKey = (key1, key2, filtro_situacion);

    // 1. Check RESULT_CACHE
    {
        let mut rcache = RESULT_CACHE.write()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
        if let Some(map) = rcache.as_mut() {
            if let Some(hit) = map.get_mut(&result_key) {
                hit.ultimo_acceso = now_secs();
                hit.accesos += 1;
                let mut out = HashMap::new();
                out.insert("periodo1".to_string(), to_py_map(&hit.agr1));
                out.insert("periodo2".to_string(), to_py_map(&hit.agr2));
                return Ok(out);
            }
        }
    }

    // 2. Miss: calcular con Rayon
    let (agr1, agr2) = {
        let guard = ENGINE_PERIODOS.read()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
        let map = guard.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("No hay periodos cargados")
        })?;
        let e1 = map.get(&key1).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Periodo {key1} no cargado"))
        })?;
        let e2 = map.get(&key2).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Periodo {key2} no cargado"))
        })?;
        py.allow_threads(|| {
            rayon::join(
                || agregar(e1, filtro_situacion),
                || agregar(e2, filtro_situacion),
            )
        })
    };

    // 3. Guardar en RESULT_CACHE
    {
        let mut rcache = RESULT_CACHE.write()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
        let map = rcache.get_or_insert_with(HashMap::new);

        if map.len() >= MAX_RESULTADOS && !map.contains_key(&result_key) {
            if let Some(&lru_key) = map.iter()
                .min_by_key(|(_, v)| v.ultimo_acceso)
                .map(|(k, _)| k)
            {
                map.remove(&lru_key);
            }
        }

        map.insert(result_key, ResultadoComp {
            agr1: agr1.clone(),
            agr2: agr2.clone(),
            calculado_at:  now_secs(),
            ultimo_acceso: now_secs(),
            accesos:       1,
        });
    }

    let mut out = HashMap::new();
    out.insert("periodo1".to_string(), to_py_map(&agr1));
    out.insert("periodo2".to_string(), to_py_map(&agr2));
    Ok(out)
}

#[pyfunction]
fn resultado_en_cache(key1: u32, key2: u32, filtro_situacion: i64) -> PyResult<bool> {
    let guard = RESULT_CACHE.read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    Ok(guard.as_ref().map_or(false, |m| m.contains_key(&(key1, key2, filtro_situacion))))
}

#[pyfunction]
fn limpiar_resultados_expirados(ttl_segundos: u64) -> PyResult<usize> {
    let ahora = now_secs();
    let mut guard = RESULT_CACHE.write()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let eliminados = if let Some(map) = guard.as_mut() {
        let antes = map.len();
        map.retain(|_, v| ahora.saturating_sub(v.ultimo_acceso) < ttl_segundos);
        antes - map.len()
    } else { 0 };
    Ok(eliminados)
}

#[pyfunction]
fn limpiar_periodos_lru(mantener: usize, año_actual: u32) -> PyResult<usize> {
    let mut guard = ENGINE_PERIODOS.write()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let eliminados = if let Some(map) = guard.as_mut() {
        let mut historicos: Vec<(PeriodoKey, u64)> = map.iter()
            .filter(|(&k, _)| k / 100 != año_actual)
            .map(|(&k, v)| (k, v.ultimo_acceso))
            .collect();
        historicos.sort_by_key(|&(_, ts)| ts);
        let a_eliminar = historicos.len().saturating_sub(mantener);
        for (k, _) in historicos.iter().take(a_eliminar) {
            map.remove(k);
        }
        a_eliminar
    } else { 0 };
    Ok(eliminados)
}

#[pyfunction]
fn evict_periodo(periodo_key: u32) -> PyResult<bool> {
    let mut guard = ENGINE_PERIODOS.write()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    Ok(guard.as_mut().map_or(false, |m| m.remove(&periodo_key).is_some()))
}

#[pyfunction]
fn evict_resultado(key1: u32, key2: u32, filtro_situacion: i64) -> PyResult<bool> {
    let mut guard = RESULT_CACHE.write()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    Ok(guard.as_mut().map_or(false, |m| m.remove(&(key1, key2, filtro_situacion)).is_some()))
}

#[pyfunction]
fn engine_recursos() -> PyResult<HashMap<String, u64>> {
    let mut stats = HashMap::new();
    if let Ok(g) = ENGINE_PERIODOS.read() {
        let (n_p, filas, ram) = g.as_ref().map_or((0, 0, 0), |m| {
            let f: usize = m.values().map(|e| e.n).sum();
            (m.len(), f, f * 96 / 1024)  // 96 bytes por fila (7 i64 + coords)
        });
        stats.insert("periodos_cargados".into(), n_p as u64);
        stats.insert("filas_totales".into(),     filas as u64);
        stats.insert("ram_datos_kb".into(),      ram as u64);
    }
    if let Ok(g) = RESULT_CACHE.read() {
        let (n_r, hits) = g.as_ref().map_or((0, 0), |m| {
            let h: u64 = m.values().map(|v| v.accesos).sum();
            (m.len(), h)
        });
        stats.insert("resultados_cacheados".into(), n_r as u64);
        stats.insert("cache_hits_total".into(),     hits);
        stats.insert("max_resultados".into(),       MAX_RESULTADOS as u64);
    }
    stats.insert("max_periodos".into(), MAX_PERIODOS as u64);
    Ok(stats)
}

#[pyfunction]
fn cache_info() -> PyResult<Vec<HashMap<String, u64>>> {
    let guard = RESULT_CACHE.read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let ahora = now_secs();
    let mut infos = Vec::new();
    if let Some(map) = guard.as_ref() {
        for (&(k1, k2, filtro), v) in map.iter() {
            let mut info = HashMap::new();
            info.insert("key1".into(),       k1 as u64);
            info.insert("key2".into(),       k2 as u64);
            info.insert("filtro".into(),     filtro as u64);
            info.insert("accesos".into(),    v.accesos);
            info.insert("edad_s".into(),     ahora.saturating_sub(v.calculado_at));
            info.insert("inactivo_s".into(), ahora.saturating_sub(v.ultimo_acceso));
            infos.push(info);
        }
    }
    infos.sort_by(|a, b| b["accesos"].cmp(&a["accesos"]));
    Ok(infos)
}

// ===========================================================================
// FUNCIONES LEGACY
// ===========================================================================

fn extract_f64(list: &Bound<'_, PyList>) -> PyResult<Vec<f64>> {
    list.iter().map(|item| {
        if item.is_none() { Ok(f64::NAN) }
        else { item.extract::<f64>() }
    }).collect()
}

fn extract_i64(list: &Bound<'_, PyList>) -> PyResult<Vec<i64>> {
    list.iter().map(|item| {
        if item.is_none() { Ok(i64::MIN) }
        else {
            item.extract::<i64>().or_else(|_| {
                item.extract::<f64>().map(|f| {
                    if f.is_nan() || f.is_infinite() { i64::MIN } else { f as i64 }
                })
            })
        }
    }).collect()
}

#[inline(always)]
fn haversine(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const R: f64 = 6_371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    R * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
}

#[pyfunction]
fn init_engine(
    lats: &Bound<'_, PyList>, lngs: &Bound<'_, PyList>,
    estado_ids: &Bound<'_, PyList>, situaciones: &Bound<'_, PyList>,
    inc_totales: &Bound<'_, PyList>, aten_totales: &Bound<'_, PyList>,
    cn_totales: &Bound<'_, PyList>,
) -> PyResult<usize> {
    let lv  = extract_f64(lats)?;
    let gnv = extract_f64(lngs)?;
    let ev  = extract_i64(estado_ids)?;
    let sv  = extract_i64(situaciones)?;
    let iv  = extract_i64(inc_totales)?;
    let av  = extract_i64(aten_totales)?;
    let cv  = extract_i64(cn_totales)?;
    let n   = lv.len();
    if [gnv.len(), ev.len(), sv.len(), iv.len(), av.len(), cv.len()].iter().any(|&l| l != n) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Arrays distinta longitud. lats={n}")
        ));
    }
    let now = now_secs();
    *ENGINE.write().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))? =
        Some(EngineData {
            n, lats: lv, lngs: gnv, estado_ids: ev, situaciones: sv,
            inc_totales: iv, aten_totales: av, cn_totales: cv,
            cn_ini:  vec![i64::MIN; n],
            cn_prim: vec![i64::MIN; n],
            cn_sec:  vec![i64::MIN; n],
            cargado_at: now, ultimo_acceso: now,
        });
    Ok(n)
}

#[pyfunction]
fn distancias_cercanas(lat_u: f64, lng_u: f64, dist_max: f64, limite: usize) -> PyResult<Vec<(usize, f64)>> {
    let guard = ENGINE.read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let eng = guard.as_ref()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Motor no init."))?;
    if lat_u.is_nan() || lng_u.is_nan() {
        return Err(pyo3::exceptions::PyValueError::new_err("lat/lng no pueden ser NaN"));
    }
    let mut res: Vec<(usize, f64)> = (0..eng.n).into_par_iter().filter_map(|i| {
        let lat = eng.lats[i];
        let lng = eng.lngs[i];
        if lat.is_nan() || lng.is_nan() { return None; }
        let d = haversine(lat_u, lng_u, lat, lng);
        if d <= dist_max { Some((i, (d * 100.0).round() / 100.0)) } else { None }
    }).collect();
    res.sort_unstable_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    res.truncate(limite);
    Ok(res)
}

#[pyfunction]
fn agregaciones_por_estado(filtro_situacion: i64) -> PyResult<HashMap<i64, HashMap<String, i64>>> {
    let guard = ENGINE.read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let eng = guard.as_ref()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Motor no init."))?;
    Ok(to_py_map(&agregar(eng, filtro_situacion)))
}

#[pyfunction]
fn filtrar_indices(estado_id: i64, situacion: i64) -> PyResult<Vec<usize>> {
    let guard = ENGINE.read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let eng = guard.as_ref()
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Motor no init."))?;
    let mut v: Vec<usize> = (0..eng.n).into_par_iter().filter(|&i| {
        let ok_e = if estado_id < 0 { true } else {
            eng.estado_ids[i] != i64::MIN && eng.estado_ids[i] == estado_id
        };
        let ok_s = if situacion < 0 { true } else {
            eng.situaciones[i] != i64::MIN && eng.situaciones[i] == situacion
        };
        ok_e && ok_s
    }).collect();
    v.sort_unstable();
    Ok(v)
}

#[pyfunction]
fn engine_stats() -> PyResult<HashMap<String, usize>> {
    let guard = ENGINE.read()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("RwLock: {e}")))?;
    let mut s = HashMap::new();
    match guard.as_ref() {
        Some(e) => { s.insert("inicializado".into(), 1); s.insert("n_filas".into(), e.n); }
        None    => { s.insert("inicializado".into(), 0); s.insert("n_filas".into(), 0); }
    }
    Ok(s)
}

// ===========================================================================
// MÓDULO PyO3
// ===========================================================================
#[pymodule]
fn plaza_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cargar_periodo_parquet,       m)?)?;
    m.add_function(wrap_pyfunction!(periodo_en_cache,             m)?)?;
    m.add_function(wrap_pyfunction!(comparar_periodos,            m)?)?;
    m.add_function(wrap_pyfunction!(resultado_en_cache,           m)?)?;
    m.add_function(wrap_pyfunction!(limpiar_resultados_expirados, m)?)?;
    m.add_function(wrap_pyfunction!(limpiar_periodos_lru,         m)?)?;
    m.add_function(wrap_pyfunction!(evict_periodo,                m)?)?;
    m.add_function(wrap_pyfunction!(evict_resultado,              m)?)?;
    m.add_function(wrap_pyfunction!(engine_recursos,              m)?)?;
    m.add_function(wrap_pyfunction!(cache_info,                   m)?)?;
    m.add_function(wrap_pyfunction!(init_engine,                  m)?)?;
    m.add_function(wrap_pyfunction!(distancias_cercanas,          m)?)?;
    m.add_function(wrap_pyfunction!(agregaciones_por_estado,      m)?)?;
    m.add_function(wrap_pyfunction!(filtrar_indices,              m)?)?;
    m.add_function(wrap_pyfunction!(engine_stats,                 m)?)?;
    Ok(())
}