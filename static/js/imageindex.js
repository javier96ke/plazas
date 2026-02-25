// ============================================================
// imageIndex.js — v3.0
// ============================================================
// El JSON del servidor ahora entrega las claves YA normalizadas
// en el campo "k" de cada carpeta de plaza. El cliente ya no
// necesita normalizar nada — toda búsqueda es O(1) exacta.
//
// Estructura del JSON esperada:
//   structure.children[]          → nodos estado { id, estado, children[] }
//   nodo.children[]               → carpetas plaza { k, alias?, children[] }
//   carpeta.children[]            → archivos { i, n, s, m }
//
// Compatibilidad: si un nodo trae "name" en lugar de "k"
// (JSON antiguo), se normaliza en el cliente como fallback.
// ============================================================

'use strict';

import { mostrarNotificacion } from './utils.js';

// --- ESTADO ---
let imagenIndex        = new Map();   // k (clave normalizada) → [{name, url, size}]
let aliasIndex         = new Map();   // alias.lower() → k  (para búsqueda por nombre)
let driveTreeVersion   = null;
let actualizacionInterval = null;

const TTL_CACHE_MS      = 2 * 60 * 60 * 1000;
const TTL_LIMPIEZA_MS   = 7 * 24 * 60 * 60 * 1000;
const CACHE_PREFIX      = 'imagenIndex_v';
const CACHE_META_PREFIX = 'imagenIndex_meta_v';

// --- API PÚBLICA ---

export const construirIndiceImagenes = async (forzarRecarga = false) => {
    try {
        console.log('🔨 Construyendo índice...');

        let versionActual = 'unknown';
        try {
            const res = await fetch('/api/drive-tree-version');
            if (res.ok) {
                const d = await res.json();
                versionActual = d.version || d.lastModified || 'unknown';
            }
        } catch (e) {
            console.warn('⚠️ No se pudo obtener versión:', e);
        }

        if (!forzarRecarga && driveTreeVersion === versionActual && imagenIndex.size > 0) {
            return { success: true, fromCache: true };
        }

        if (!forzarRecarga && versionActual !== 'unknown') {
            const resultado = _cargarDesdeCacheLocal(versionActual);
            if (resultado) return resultado;
        }

        const { nuevoIndice, nuevoAlias, totalCarpetas, totalArchivos } =
            await _descargarYConstruirIndice();

        imagenIndex      = nuevoIndice;
        aliasIndex       = nuevoAlias;
        driveTreeVersion = versionActual;

        _guardarEnCacheLocal(versionActual, totalCarpetas, totalArchivos);

        console.log(`✅ Índice: ${totalCarpetas} plazas, ${totalArchivos} imágenes`);
        mostrarNotificacion(`Índice actualizado: ${totalCarpetas} plazas`, 'success');

        return { success: true, fromCache: false, stats: { carpetas: totalCarpetas, archivos: totalArchivos } };

    } catch (error) {
        console.error('❌ Error construyendo índice:', error);
        const fallback = _cargarFallbackLocal();
        if (fallback) return fallback;
        mostrarNotificacion('Error al cargar índice de imágenes', 'error');
        return { success: false, error: error.message };
    }
};

/**
 * Busca imágenes para una clave de plaza.
 * Con el nuevo JSON la búsqueda exacta cubre el 99.9% de los casos.
 */
export const findImageUrls = async (claveOriginal) => {
    try {
        const claveNorm = claveOriginal.trim().toLowerCase();
        if (!claveNorm) return await _buscarImagenesLocales('');

        if (imagenIndex.size === 0) {
            const r = await construirIndiceImagenes();
            if (!r.success) return await _buscarImagenesLocales(claveNorm);
        }

        // 1. Exacta en índice principal O(1)
        const exacto = imagenIndex.get(claveNorm);
        if (exacto?.length) {
            console.log(`✅ ${exacto.length} imágenes (exacta)`);
            return exacto.map(img => img.url);
        }

        // 2. Exacta en índice de alias O(1) — cubre carpetas con nombre libre
        const porAlias = aliasIndex.get(claveNorm);
        if (porAlias) {
            const desde = imagenIndex.get(porAlias);
            if (desde?.length) {
                console.log(`✅ ${desde.length} imágenes (alias)`);
                return desde.map(img => img.url);
            }
        }

        // 3. Parcial — solo si los dos anteriores fallaron
        const parcial = _busquedaParcial(claveNorm);
        if (parcial.length) return parcial;

        // 4. ¿Índice desactualizado?
        const actualizacion = await verificarActualizacionIndice();
        if (actualizacion.necesitaActualizar) {
            await construirIndiceImagenes(true);
            const reintento = imagenIndex.get(claveNorm);
            if (reintento?.length) return reintento.map(img => img.url);
        }

        return await _buscarImagenesLocales(claveNorm);

    } catch (error) {
        console.error('❌ Error en búsqueda:', error);
        return await _buscarImagenesLocales(claveOriginal.trim().toLowerCase());
    }
};

export const verificarActualizacionIndice = async () => {
    try {
        const res = await fetch('/api/drive-tree-version');
        if (!res.ok) return { necesitaActualizar: false };
        const data = await res.json();
        const nuevaVersion = data.version || data.lastModified;
        if (nuevaVersion && nuevaVersion !== driveTreeVersion)
            return { necesitaActualizar: true, nuevaVersion };
        return { necesitaActualizar: false };
    } catch (error) {
        return { necesitaActualizar: false, error: error.message };
    }
};

export const iniciarActualizacionAutomatica = () => {
    if (actualizacionInterval) clearInterval(actualizacionInterval);
    actualizacionInterval = setInterval(async () => {
        try {
            const act = await verificarActualizacionIndice();
            if (act.necesitaActualizar) {
                const r = await construirIndiceImagenes(true);
                if (r.success)
                    mostrarNotificacion(`Índice actualizado: ${act.nuevaVersion?.substring(0, 8)}...`, 'success');
            }
        } catch (e) {
            console.warn('⚠️ Error en verificación automática:', e);
        }
    }, 15 * 60 * 1000);
};

export const getIndiceSize = () => imagenIndex.size;

// --- PRIVADAS ---

const _driveUrl = (fileId) =>
    `https://drive.google.com/thumbnail?id=${fileId}&sz=w800`;

/**
 * Normalización cliente-side como FALLBACK para JSON antiguo (campo "name").
 * Con el nuevo JSON (campo "k") esto no se invoca.
 */
const _normalizarClaveClienteFallback = (nombre) => {
    const PATRON = /([A-Za-z1l])-\s*(\d{2,3})-(\d{2,3})-(\d{2,3})/;
    const m = PATRON.exec(nombre);
    if (!m) return nombre.trim().toLowerCase();

    let tipo = m[1].toUpperCase();
    if (tipo === 'L' || tipo === '1') tipo = 'I';

    const e = m[2].padStart(2, '0');
    const n = m[3].padStart(3, '0');
    const p = m[4].padStart(2, '0');
    return `${tipo}-${e}-${n}-${p}`.toLowerCase();
};

/**
 * Descarga el árbol y construye el índice en O(n) con iteración plana.
 * Compatible con JSON v2 (campo "k") y v1 (campo "name") como fallback.
 */
const _descargarYConstruirIndice = async () => {
    console.log('⏬ Descargando árbol...');
    const res = await fetch('/api/drive-tree');
    if (!res.ok) throw new Error(`Error ${res.status}`);
    const driveData = await res.json();

    const nuevoIndice = new Map();
    const nuevoAlias  = new Map();
    let totalCarpetas = 0;
    let totalArchivos = 0;

    const nodosEstado = driveData.structure?.children ?? [];

    for (const nodoEstado of nodosEstado) {
        if (!nodoEstado.children) continue;

        for (const carpeta of nodoEstado.children) {
            if (!carpeta.children?.length) continue;

            // Campo "k" (nuevo) o "name" (antiguo como fallback)
            const claveNorm = carpeta.k
                ?? _normalizarClaveClienteFallback(carpeta.name ?? '');

            if (!claveNorm) continue;

            const archivos = [];
            for (const item of carpeta.children) {
                const fileId  = item.i ?? item.id;
                const fileUrl = item.mediumUrl ?? item.thumbnailUrl ?? item.directUrl
                                ?? (fileId ? _driveUrl(fileId) : null);
                if (fileId && fileUrl) {
                    totalArchivos++;
                    archivos.push({ name: item.n ?? item.name, url: fileUrl, size: item.s ?? item.size });
                }
            }

            if (archivos.length > 0) {
                nuevoIndice.set(claveNorm, archivos);

                // Registrar alias si existe (nombre original que difería de la clave)
                if (carpeta.alias) {
                    nuevoAlias.set(carpeta.alias.toLowerCase(), claveNorm);
                }

                totalCarpetas++;
            }
        }
    }

    return { nuevoIndice, nuevoAlias, totalCarpetas, totalArchivos };
};

const _cargarDesdeCacheLocal = (versionActual) => {
    try {
        const cachedIndex = localStorage.getItem(`${CACHE_PREFIX}${versionActual}`);
        const cachedAlias = localStorage.getItem(`${CACHE_PREFIX}alias_${versionActual}`);
        const cachedMeta  = localStorage.getItem(`${CACHE_META_PREFIX}${versionActual}`);
        if (!cachedIndex || !cachedMeta) return null;

        const meta = JSON.parse(cachedMeta);
        if (Date.now() - meta.timestamp >= TTL_CACHE_MS) return null;

        imagenIndex      = new Map(JSON.parse(cachedIndex));
        aliasIndex       = cachedAlias ? new Map(JSON.parse(cachedAlias)) : new Map();
        driveTreeVersion = versionActual;
        console.log(`✅ Índice desde caché: ${imagenIndex.size} claves`);
        mostrarNotificacion('Índice cargado desde caché', 'info');
        return { success: true, fromCache: true };
    } catch (e) {
        console.warn('⚠️ Error cargando caché:', e);
        return null;
    }
};

const _guardarEnCacheLocal = (versionActual, totalCarpetas, totalArchivos) => {
    if (versionActual === 'unknown') return;
    try {
        localStorage.setItem(
            `${CACHE_PREFIX}${versionActual}`,
            JSON.stringify(Array.from(imagenIndex.entries()))
        );
        localStorage.setItem(
            `${CACHE_PREFIX}alias_${versionActual}`,
            JSON.stringify(Array.from(aliasIndex.entries()))
        );
        localStorage.setItem(
            `${CACHE_META_PREFIX}${versionActual}`,
            JSON.stringify({ timestamp: Date.now(), version: versionActual, carpetas: totalCarpetas, archivos: totalArchivos })
        );

        const ahora = Date.now();
        for (let i = localStorage.length - 1; i >= 0; i--) {
            const key = localStorage.key(i);
            if (!key?.startsWith(CACHE_META_PREFIX)) continue;
            try {
                const oldMeta = JSON.parse(localStorage.getItem(key));
                if (ahora - oldMeta.timestamp > TTL_LIMPIEZA_MS) {
                    const ver = key.replace(CACHE_META_PREFIX, '');
                    localStorage.removeItem(`${CACHE_PREFIX}${ver}`);
                    localStorage.removeItem(`${CACHE_PREFIX}alias_${ver}`);
                    localStorage.removeItem(key);
                }
            } catch (_) { /* ignorar */ }
        }
    } catch (e) {
        console.warn('⚠️ No se pudo guardar en localStorage:', e.message);
    }
};

const _cargarFallbackLocal = () => {
    try {
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (!key?.startsWith(CACHE_PREFIX) || key.includes('_meta_') || key.includes('alias_')) continue;
            const ver         = key.replace(CACHE_PREFIX, '');
            const cachedIndex = localStorage.getItem(key);
            const cachedAlias = localStorage.getItem(`${CACHE_PREFIX}alias_${ver}`);
            const cachedMeta  = localStorage.getItem(`${CACHE_META_PREFIX}${ver}`);
            if (cachedIndex && cachedMeta) {
                const meta   = JSON.parse(cachedMeta);
                imagenIndex      = new Map(JSON.parse(cachedIndex));
                aliasIndex       = cachedAlias ? new Map(JSON.parse(cachedAlias)) : new Map();
                driveTreeVersion = ver;
                mostrarNotificacion(`Usando índice en caché: ${meta.carpetas || '?'} plazas`, 'warning');
                return { success: true, fromCache: true, isFallback: true };
            }
        }
    } catch (e) {
        console.warn('⚠️ Fallback también falló:', e);
    }
    return null;
};

/** Búsqueda parcial con early-exit — último recurso cuando la clave no es exacta. */
const _busquedaParcial = (claveNorm) => {
    let mejorScore    = Infinity;
    let mejorArchivos = null;
    let mejorCarpeta  = null;

    for (const [carpeta, archivos] of imagenIndex.entries()) {
        if (carpeta.includes(claveNorm) || claveNorm.includes(carpeta)) {
            const score = Math.abs(carpeta.length - claveNorm.length);
            if (score < mejorScore) {
                mejorScore    = score;
                mejorCarpeta  = carpeta;
                mejorArchivos = archivos;
                if (score === 0) break;
            }
        }
    }

    if (!mejorArchivos) return [];
    console.log(`🎯 Coincidencia parcial: "${mejorCarpeta}"`);
    return mejorArchivos.map(img => img.url);
};

const _buscarImagenesLocales = async (clave) => {
    try {
        const res = await fetch(`/api/imagenes-local?clave=${encodeURIComponent(clave)}`);
        if (res.ok) {
            const imgs = await res.json();
            if (imgs.length > 0) return imgs;
        }
        return [];
    } catch (error) {
        return [];
    }
};