// ============================================================
// main.js — Orquestador principal de la SPA
// ============================================================
// mapa.js y comparativas.js se auto-inicializan con su propio
// DOMContentLoaded — main.js NO los importa ni los toca.
// ============================================================
'use strict';

// Fíjate bien en las mayúsculas y minúsculas de los archivos .js:
import { initTheme, agregarEstilosSistema } from './utils.js';
import { construirIndiceImagenes, iniciarActualizacionAutomatica, getIndiceSize } from './imageindex.js'; // imageIndex.js
import { initImageModal } from './imagemodal.js'; // imageModal.js (NO imagemodal.js)
import { setModalOpenFunction } from './renderizado.js';
import { initBusqueda, cargarEstadosConPlazas } from './busqueda.js';
import {
    cargarEstadisticas,
    cargarEstadisticasCompletasCN,
    getCNResumenData,
    initStatsNavigation,
    mostrarSubvistaDefault,
} from './estadisticas.js';

document.addEventListener('DOMContentLoaded', () => {

    console.log('🚀 Inicializando aplicación...');

    // ── 1. ESTILOS DEL SISTEMA ────────────────────────────────
    agregarEstilosSistema();

    // ── 2. TEMA LIGHT / DARK ──────────────────────────────────
    initTheme();

    // ── 3. MODAL DE IMÁGENES ──────────────────────────────────
    const { openModal } = initImageModal();
    setModalOpenFunction(openModal);

    // ── 4. MAPA DE VISTAS ─────────────────────────────────────
    const VIEW_IDS = [
        'welcome-screen',
        'key-search-view',
        'filter-search-view',
        'results-view',
        'stats-view',
        'estados-view',
        'plazas-por-estado-view',
        'top-plazas-view',
        'map-view',
    ];

    const views = {};
    VIEW_IDS.forEach(id => { views[id] = document.getElementById(id); });

    // ── 5. NAVEGACIÓN SPA ─────────────────────────────────────
    // Flags de lazy init — evitan recargar datos ya cargados
    const _cargado = { estadisticas: false, estados: false };

    const showView = (viewId) => {
        if (!views[viewId]) viewId = 'welcome-screen';

        VIEW_IDS.forEach(id => { if (views[id]) views[id].classList.add('hidden'); });
        views[viewId].classList.remove('hidden');

        // — Efectos de entrada por vista —
        if (viewId === 'stats-view') {
            // initStatsNavigation recibe callback para lazy-init de comparativas
            // (comparativas.js ya se auto-inicializó con su propio DOMContentLoaded)
            initStatsNavigation(() => {
                if (typeof sistemaComparativas !== 'undefined' && sistemaComparativas.init) {
                    sistemaComparativas.init();
                }
            });

            mostrarSubvistaDefault();

            if (!_cargado.estadisticas) {
                _cargado.estadisticas = true;
                cargarEstadisticas();
            } else if (!getCNResumenData()) {
                cargarEstadisticasCompletasCN();
            }
        }

        if (viewId === 'estados-view' && !_cargado.estados) {
            _cargado.estados = true;
            cargarEstadosConPlazas();
        }
    };

    const handleNavigation = () => {
        const viewId = window.location.hash.substring(1) || 'welcome-screen';
        showView(viewId);
    };

    // Exponer globalmente — busqueda.js navega tras una búsqueda exitosa
    window.showView          = showView;
    window.handleNavigation  = handleNavigation;

    window.addEventListener('popstate', handleNavigation);

    document.body.addEventListener('click', (e) => {
        const link = e.target.closest('a[href^="#"]');
        if (!link) return;
        e.preventDefault();
        const viewId = link.getAttribute('href').substring(1);
        if (window.location.hash !== `#${viewId}`) {
            history.pushState({ view: viewId }, '', `#${viewId}`);
        }
        handleNavigation();
    });

    // Botón "volver" en vista de resultados
    const backBtn = document.getElementById('back-to-search-button');
    if (backBtn) backBtn.addEventListener('click', () => history.back());

    // ── 6. BÚSQUEDA ───────────────────────────────────────────
    // initBusqueda recibe handleNavigation para navegar tras buscar
    // Internamente ya gestiona: reset, teclado, progreso, auto-search toggle
    initBusqueda(handleNavigation);

    // ── 7. FECHA DE ACTUALIZACIÓN DEL EXCEL ──────────────────
    fetch('/api/excel/last-update')
        .then(r => { if (!r.ok) throw new Error(); return r.json(); })
        .then(data => {
            const el = document.getElementById('update-date');
            if (data.last_modified && data.status === 'success') {
                const date = new Date(data.last_modified);
                if (el) el.textContent = date.toLocaleDateString('es-MX', {
                    day: '2-digit', month: 'long', year: 'numeric'
                });
                setTimeout(() => {
                    document.getElementById('excel-update-info')?.classList.add('minimal');
                }, 5000);
            } else if (el) {
                el.textContent = 'No disponible';
                el.style.color = '#999';
            }
        })
        .catch(() => {
            const el = document.getElementById('update-date');
            if (el) { el.textContent = 'Error'; el.style.color = '#cc0000'; }
        });

    document.getElementById('excel-update-info')?.addEventListener('click', function () {
        this.classList.toggle('minimal');
    });

    // ── 8. ÍNDICE DE IMÁGENES (segundo plano, 1s tras carga) ──
    setTimeout(async () => {
        console.log('🔍 Inicializando índice de imágenes...');

        const estadoIndice = document.createElement('div');
        estadoIndice.className   = 'indice-status';
        estadoIndice.textContent = 'Cargando índice...';
        document.body.appendChild(estadoIndice);

        const boton = _crearBotonActualizacion();

        try {
            const resultado = await construirIndiceImagenes();

            if (resultado.success) {
                estadoIndice.textContent = `Índice: ${getIndiceSize()} carpetas`;
                estadoIndice.classList.add('show');
                boton.classList.remove('hidden');
                iniciarActualizacionAutomatica();
                setTimeout(() => estadoIndice.classList.remove('show'), 3000);
            } else {
                estadoIndice.textContent = 'Error cargando índice';
                estadoIndice.classList.add('show');
                estadoIndice.style.background = 'var(--error-color)';
                estadoIndice.style.color      = 'white';
            }
        } catch (err) {
            console.error('Error inicializando índice:', err);
            estadoIndice.textContent = 'Error en índice';
            estadoIndice.classList.add('show');
        }
    }, 1000);

    // ── 9. NAVEGACIÓN INICIAL ─────────────────────────────────
    handleNavigation();

    window.addEventListener('error', e => console.error('Error global:', e.error));

    console.log('✅ Aplicación inicializada');
});

// ── HELPER: botón de actualización manual del índice ─────────
function _crearBotonActualizacion() {
    const boton = document.createElement('button');
    boton.id          = 'indice-update-button';
    boton.className   = 'indice-update-button hidden';
    boton.title       = 'Actualizar índice de imágenes';
    boton.textContent = '🔄';

    boton.addEventListener('click', async () => {
        boton.classList.add('updating');
        boton.title = 'Actualizando...';
        await construirIndiceImagenes(true);
        boton.classList.remove('updating');
        boton.title = 'Índice actualizado';
        setTimeout(() => { boton.title = 'Actualizar índice de imágenes'; }, 2000);
    });

    document.body.appendChild(boton);
    return boton;
}