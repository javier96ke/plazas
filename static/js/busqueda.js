// ============================================================
// busqueda.js — Búsqueda por clave y búsqueda por filtros
// ============================================================
// Responsabilidades:
//   - Búsqueda directa por clave (input libre)
//   - Búsqueda en cascada por Estado → Zona → Municipio →
//     Localidad → Clave
//   - Vista de estados con listado y búsqueda
//   - Vista de plazas por estado con búsqueda
//   - Barra de progreso de los filtros
// ============================================================

'use strict';

import { fetchData, showAlert, setLoaderVisible, debounce, showLoader, hideLoader } from './utils.js';
import { renderPlazaResultados } from './renderizado.js';
import { findImageUrls } from './imageindex.js';

// --- REFERENCIAS DOM (se resuelven en init) ---
let selects      = {};
let filterSteps  = {};
let filterLoader = null;
let progressFill = null;
let progressSteps= null;
let estadosGrid  = null;
let estadosSearchInput = null;
let plazasPorEstadoTitle = null;
let plazasListContainer  = null;
let plazasSearchInput    = null;
let _handleNavigation    = null;   // inyectado desde app.js

// --- ESTADO LOCAL ---
let todosEstadosData = [];
let plazasDelEstado  = [];
let estadoSeleccionado = '';

// ============================================================
// INICIALIZACIÓN
// ============================================================

/**
 * Inicializa el módulo de búsqueda.
 * @param {function} handleNavigation  Función de navegación SPA de app.js
 */
export const initBusqueda = (handleNavigation) => {
    _handleNavigation = handleNavigation;

    // Resolver referencias DOM
    selects = {
        estado   : document.getElementById('state-select'),
        zona     : document.getElementById('zona-select'),
        municipio: document.getElementById('municipio-select'),
        localidad: document.getElementById('localidad-select'),
        clave    : document.getElementById('clave-select')
    };

    filterSteps = {
        estado   : document.querySelector('.filter-step[data-step="estado"]'),
        zona     : document.querySelector('.filter-step[data-step="zona"]'),
        municipio: document.querySelector('.filter-step[data-step="municipio"]'),
        localidad: document.querySelector('.filter-step[data-step="localidad"]'),
        clave    : document.querySelector('.filter-step[data-step="clave"]')
    };

    filterLoader          = document.getElementById('filter-loader');
    progressFill          = document.getElementById('progress-fill');
    progressSteps         = document.querySelectorAll('.progress-step');
    estadosGrid           = document.getElementById('estados-grid');
    estadosSearchInput    = document.getElementById('estados-search-input');
    plazasPorEstadoTitle  = document.getElementById('plazas-por-estado-title');
    plazasListContainer   = document.getElementById('plazas-list-container');
    plazasSearchInput     = document.getElementById('plazas-search-input');

    // Poblar el select de estados al inicio
    if (selects.estado) {
        populateSelect(selects.estado, '/api/estados', 'Selecciona un Estado');
    }

    _setupFilterListeners();
    _setupKeyboardNavigation();
    _setupProgressBarNavigation();
    _addAutoSearchToggle();
    actualizarProgreso();

    // Botón de búsqueda por clave
    const searchByKeyButton = document.getElementById('search-by-key-button');
    const claveInput        = document.getElementById('clave-input');
    const keyLoader         = document.getElementById('key-loader');

    if (searchByKeyButton) {
        searchByKeyButton.addEventListener('click', () => {
            const clave = claveInput?.value.trim();
            if (clave) buscarYMostrarClave(clave, keyLoader);
            else showAlert('Por favor, introduce una clave para buscar.', 'warning');
        });
    }
    if (claveInput) {
        claveInput.addEventListener('keyup', (e) => {
            if (e.key === 'Enter') {
                const clave = claveInput.value.trim();
                if (clave) buscarYMostrarClave(clave, keyLoader);
                else showAlert('Por favor, introduce una clave para buscar.', 'warning');
            }
        });
    }

    // Botón buscar por filtros
    const searchFilterButton = document.getElementById('search-filter-button');
    if (searchFilterButton) {
        searchFilterButton.addEventListener('click', _handleFilterSearch);
    }

    // Botón reset
    const resetButton = document.getElementById('reset-search-button');
    if (resetButton) resetButton.addEventListener('click', resetSearch);
};

// ============================================================
// BÚSQUEDA PRINCIPAL
// ============================================================

/**
 * Busca una plaza por clave, obtiene sus imágenes e historial,
 * y renderiza los resultados.
 * @param {string}      clave   Clave de la plaza
 * @param {HTMLElement} loader  Loader inline (opcional)
 */
export const buscarYMostrarClave = async (clave, loader) => {
    if (!clave) { showAlert('Por favor, introduce una clave válida', 'warning'); return; }

    showLoader(`Buscando plaza con clave: ${clave}`);

    try {
        const data = await fetchData(`/api/search?clave=${encodeURIComponent(clave)}`);
        if (!data || (!data.excel_info && !data.datos_organizados)) {
            throw new Error('No se encontraron datos para esta clave');
        }

        // Historial (no crítico)
        let historial = [];
        try {
            historial = await fetchData(`/api/plaza-historial?clave=${encodeURIComponent(clave)}`);
        } catch (e) {
            console.warn('No se pudo cargar el historial', e);
        }

        // Imágenes desde el índice
        console.log(`🔄 Buscando imágenes para: ${clave}`);
        const imagenesDrive = await findImageUrls(clave);

        // Construir objeto de datos completo
        let datosCompletos;
        if (data.datos_organizados) {
            const infoAplanada = {
                ...(data.datos_organizados.informacion_general || {}),
                ...(data.datos_organizados.ubicacion           || {}),
                ...(data.datos_organizados.fecha_periodo       || {}),
                ...(data.datos_organizados.incripciones        || {}),
                ...(data.datos_organizados.atenciones          || {}),
                ...(data.datos_organizados.certificaciones      || {}),
                ...(data.datos_organizados.personal            || {}),
                ...(data.datos_organizados.equipamiento        || {}),
                ...(data.datos_organizados.mobiliario          || {})
            };
            datosCompletos = {
                ...data,
                images    : imagenesDrive.length > 0 ? imagenesDrive : (data.images || []),
                excel_info: infoAplanada,
                historial
            };
        } else {
            datosCompletos = {
                ...data,
                images  : imagenesDrive.length > 0 ? imagenesDrive : (data.images || []),
                historial
            };
        }

        renderPlazaResultados(datosCompletos);

        history.pushState({ view: 'results-view' }, '', '#results-view');
        if (_handleNavigation) _handleNavigation();

    } catch (error) {
        console.error('Error en búsqueda:', error);
        showAlert(`Error al buscar la clave: ${error.message}`, 'error');
    } finally {
        hideLoader();
        if (loader) setLoaderVisible(loader, false);
    }
};

// Exponer globalmente para compatibilidad (llamado desde renderizado de estados)
window.buscarYMostrarClave = buscarYMostrarClave;

// ============================================================
// ESTADOS CON PLAZAS
// ============================================================

export const cargarEstadosConPlazas = async () => {
    try {
        showLoader('Obteniendo lista de estados y plazas...');
        const estados = await fetchData('/api/estados_con_conteo');
        estados.sort((a, b) => b.cantidad - a.cantidad);
        todosEstadosData = estados;
        renderEstadosConPlazas(estados);
        _setupBusquedaEstadosView(estados);
    } catch (error) {
        console.error('Error cargando estados con plazas:', error);
        showAlert('Error al cargar los estados', 'error');
    } finally {
        hideLoader();
    }
};

export const getTodosEstadosData = () => todosEstadosData;
export const getPlazasDelEstado  = () => plazasDelEstado;
export const getEstadoSeleccionado = () => estadoSeleccionado;

export const renderEstadosConPlazas = (estados) => {
    if (!estadosGrid) return;
    estadosGrid.innerHTML = '';

    if (!estados.length) {
        const p = document.createElement('p');
        p.className   = 'no-results';
        p.textContent = 'No se encontraron estados.';
        estadosGrid.appendChild(p);
        return;
    }

    estados.forEach(estado => {
        const item = document.createElement('div');
        item.className = 'state-menu-item';

        const nameDiv  = document.createElement('div');
        nameDiv.className   = 'state-menu-name';
        nameDiv.textContent = estado.nombre;

        const countDiv = document.createElement('div');
        countDiv.className   = 'state-menu-count';
        countDiv.textContent = `${estado.cantidad || 'N/A'} plazas`;

        item.appendChild(nameDiv);
        item.appendChild(countDiv);

        item.addEventListener('click', () => {
            estadoSeleccionado = estado.nombre;
            history.pushState({ view: 'plazas-por-estado-view' }, '', '#plazas-por-estado-view');
            if (_handleNavigation) _handleNavigation();
            cargarPlazasPorEstado(estado.nombre);
        });

        estadosGrid.appendChild(item);
    });
};

export const cargarPlazasPorEstado = async (estado) => {
    try {
        showLoader(`Cargando plazas del estado: ${estado}`);
        const plazas = await fetchData(`/api/plazas_por_estado/${encodeURIComponent(estado)}`);
        plazasDelEstado = plazas;
        renderPlazasList(plazas, estado);
        if (plazasPorEstadoTitle) {
            plazasPorEstadoTitle.textContent = `Plazas de ${estado} (${plazas.length})`;
        }
    } catch (error) {
        console.error('Error cargando plazas por estado:', error);
        showAlert('Error al cargar las plazas del estado', 'error');
    } finally {
        hideLoader();
    }
};

export const renderPlazasList = (plazas, estado) => {
    if (!plazasListContainer) return;
    plazasListContainer.innerHTML = '';

    if (!plazas.length) {
        const p = document.createElement('p');
        p.className   = 'no-results';
        p.textContent = 'No se encontraron plazas para este estado.';
        plazasListContainer.appendChild(p);
        return;
    }

    plazas.forEach(plaza => {
        const item = document.createElement('div');
        item.className = 'plaza-list-item';

        const claveDiv = document.createElement('div');
        claveDiv.className   = 'plaza-clave';
        claveDiv.textContent = plaza.clave;

        const direccionDiv = document.createElement('div');
        direccionDiv.className   = 'plaza-direccion';
        direccionDiv.textContent = plaza.direccion || 'Dirección no disponible';

        const ubicacionDiv = document.createElement('div');
        ubicacionDiv.className = 'plaza-ubicacion';

        const municipioSpan = document.createElement('span');
        municipioSpan.textContent = plaza.municipio;

        const localidadSpan = document.createElement('span');
        localidadSpan.textContent = plaza.localidad;

        ubicacionDiv.appendChild(municipioSpan);
        ubicacionDiv.appendChild(localidadSpan);

        item.appendChild(claveDiv);
        item.appendChild(direccionDiv);
        item.appendChild(ubicacionDiv);

        item.addEventListener('click', () => {
            buscarYMostrarClave(plaza.clave, filterLoader);
        });

        plazasListContainer.appendChild(item);
    });

    _setupBusquedaPlazasView();
};

// ============================================================
// PROGRESS BAR Y PASOS
// ============================================================

export const actualizarProgreso = () => {
    const steps = ['estado','zona','municipio','localidad','clave'];
    let completedSteps = 0;
    let activeStepIndex = -1;

    steps.forEach((stepName, index) => {
        if (selects[stepName]?.value)                           completedSteps++;
        if (filterSteps[stepName] && !filterSteps[stepName].classList.contains('hidden')) {
            activeStepIndex = index;
        }
    });

    if (progressFill) {
        progressFill.style.width = `${Math.max(20, (completedSteps / steps.length) * 100)}%`;
    }

    progressSteps?.forEach((stepElement) => {
        const stepName  = stepElement.getAttribute('data-step');
        const stepIndex = steps.indexOf(stepName);
        stepElement.classList.remove('active','completed');
        if (stepIndex < completedSteps)                                      stepElement.classList.add('completed');
        else if (stepIndex === completedSteps && activeStepIndex >= stepIndex) stepElement.classList.add('active');
    });
};

export const resetSearch = () => {
    showLoader('Reiniciando búsqueda...', 'compact');
    setTimeout(() => {
        Object.values(selects).forEach(select => {
            if (select) { select.selectedIndex = 0; select.disabled = select.id !== 'state-select'; }
        });
        Object.keys(filterSteps).forEach(stepName => {
            if (filterSteps[stepName]) {
                filterSteps[stepName].classList.toggle('hidden', stepName !== 'estado');
                _updateStepIndicator(stepName, stepName === 'estado' ? 'active' : 'default');
            }
        });
        actualizarProgreso();
        const filterSection = document.querySelector('.search-container');
        if (filterSection) {
            window.scrollTo({ top: filterSection.getBoundingClientRect().top + window.pageYOffset - 80, behavior: 'smooth' });
        }
        setTimeout(() => selects.estado?.focus(), 500);
        hideLoader();
        showAlert('Búsqueda reiniciada correctamente', 'success');
    }, 800);
};

// ============================================================
// HELPERS PRIVADOS
// ============================================================

const _updateStepIndicator = (stepName, status) => {
    const indicator = filterSteps[stepName]?.querySelector('.step-indicator');
    if (indicator) {
        indicator.classList.remove('active','completed');
        if (status === 'active')    indicator.classList.add('active');
        if (status === 'completed') indicator.classList.add('completed');
    }
};

export const populateSelect = async (selectElement, url, placeholder) => {
    if (!selectElement) return;
    selectElement.disabled = true;
    selectElement.innerHTML = '<option value="">Cargando...</option>';
    setLoaderVisible(filterLoader, true);

    try {
        const options = await fetchData(url);
        selectElement.innerHTML = '';

        const defaultOption       = document.createElement('option');
        defaultOption.value       = '';
        defaultOption.textContent = `-- ${placeholder} --`;
        selectElement.appendChild(defaultOption);

        if (options?.length > 0) {
            options.forEach(opt => {
                const el       = document.createElement('option');
                el.value       = opt;
                el.textContent = opt;
                selectElement.appendChild(el);
            });
            selectElement.disabled = false;
        } else {
            selectElement.innerHTML += '<option value="">No hay opciones</option>';
        }
    } catch (error) {
        showAlert(`Error al cargar: ${error.message}`, 'error');
        selectElement.innerHTML = '<option value="">Error</option>';
    } finally {
        setLoaderVisible(filterLoader, false);
    }
};

const _resetSteps = (fromStepName) => {
    const stepNames  = ['zona','municipio','localidad','clave'];
    const startIndex = stepNames.indexOf(fromStepName);
    if (startIndex === -1) return;
    for (let i = startIndex; i < stepNames.length; i++) {
        const s = stepNames[i];
        if (filterSteps[s]) filterSteps[s].classList.add('hidden');
        _updateStepIndicator(s, 'default');
        if (selects[s]) selects[s].disabled = true;
    }
};

const _navigateToNextStep = (currentStepName) => {
    const stepOrder      = ['estado','zona','municipio','localidad','clave'];
    const currentIndex   = stepOrder.indexOf(currentStepName);
    if (currentIndex === -1 || currentIndex >= stepOrder.length - 1) return;

    const nextStepName = stepOrder[currentIndex + 1];
    if (filterSteps[nextStepName]) {
        filterSteps[nextStepName].classList.remove('hidden');
        _updateStepIndicator(nextStepName, 'active');
    }

    for (let i = currentIndex + 2; i < stepOrder.length; i++) {
        const s = stepOrder[i];
        if (filterSteps[s]) { filterSteps[s].classList.add('hidden'); _updateStepIndicator(s, 'default'); }
    }

    actualizarProgreso();

    setTimeout(() => {
        if (filterSteps[nextStepName]) {
            const pos = filterSteps[nextStepName].getBoundingClientRect().top + window.pageYOffset - 100;
            window.scrollTo({ top: pos, behavior: 'smooth' });
            if (selects[nextStepName] && !selects[nextStepName].disabled) selects[nextStepName].focus();
        }
    }, 300);
};

const _handleFilterSearch = () => {
    const clave = selects.clave?.value;
    if (clave) buscarYMostrarClave(clave, filterLoader);
    else showAlert('Por favor, completa todos los filtros hasta seleccionar una clave de plaza.', 'warning');
};

const _setupFilterListeners = () => {
    if (selects.estado) {
        selects.estado.addEventListener('change', () => {
            _resetSteps('zona');
            const estado = selects.estado.value;
            _updateStepIndicator('estado', estado ? 'completed' : 'active');
            if (estado) {
                populateSelect(selects.zona, `/api/zonas?estado=${encodeURIComponent(estado)}`, 'Selecciona una Zona');
                setTimeout(() => _navigateToNextStep('estado'), 100);
            }
            actualizarProgreso();
        });
    }

    if (selects.zona) {
        selects.zona.addEventListener('change', () => {
            _resetSteps('municipio');
            const zona = selects.zona.value;
            _updateStepIndicator('zona', zona ? 'completed' : 'active');
            if (zona) {
                populateSelect(selects.municipio, `/api/municipios?estado=${encodeURIComponent(selects.estado.value)}&zona=${encodeURIComponent(zona)}`, 'Selecciona un Municipio');
                setTimeout(() => _navigateToNextStep('zona'), 100);
            }
            actualizarProgreso();
        });
    }

    if (selects.municipio) {
        selects.municipio.addEventListener('change', () => {
            _resetSteps('localidad');
            const municipio = selects.municipio.value;
            _updateStepIndicator('municipio', municipio ? 'completed' : 'active');
            if (municipio) {
                populateSelect(selects.localidad, `/api/localidades?estado=${encodeURIComponent(selects.estado.value)}&zona=${encodeURIComponent(selects.zona.value)}&municipio=${encodeURIComponent(municipio)}`, 'Selecciona una Localidad');
                setTimeout(() => _navigateToNextStep('municipio'), 100);
            }
            actualizarProgreso();
        });
    }

    if (selects.localidad) {
        selects.localidad.addEventListener('change', () => {
            _resetSteps('clave');
            const localidad = selects.localidad.value;
            _updateStepIndicator('localidad', localidad ? 'completed' : 'active');
            if (localidad) {
                populateSelect(selects.clave, `/api/claves_plaza?estado=${encodeURIComponent(selects.estado.value)}&zona=${encodeURIComponent(selects.zona.value)}&municipio=${encodeURIComponent(selects.municipio.value)}&localidad=${encodeURIComponent(localidad)}`, 'Selecciona la Clave');
                setTimeout(() => _navigateToNextStep('localidad'), 100);
            }
            actualizarProgreso();
        });
    }

    if (selects.clave) {
        selects.clave.addEventListener('change', () => {
            const clave = selects.clave.value;
            _updateStepIndicator('clave', clave ? 'completed' : 'active');
            actualizarProgreso();
            if (clave && document.getElementById('auto-search-toggle')?.checked) {
                setTimeout(() => _handleFilterSearch(), 500);
            }
        });
    }
};

const _setupKeyboardNavigation = () => {
    Object.values(selects).forEach((select) => {
        if (!select) return;
        select.addEventListener('keydown', (e) => {
            if (e.key !== 'Enter') return;
            e.preventDefault();
            const arr  = Object.values(selects);
            const idx  = arr.indexOf(select);
            if (idx < arr.length - 1) {
                const next = arr[idx + 1];
                if (next && !next.disabled) next.focus();
            } else {
                _handleFilterSearch();
            }
        });
    });

    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') { history.back(); return; }
        const welcomeScreen = document.getElementById('welcome-screen');
        if (!welcomeScreen?.classList.contains('hidden')) return;
        if (e.key === '1') document.querySelector('a[href="#key-search-view"]')?.click();
        else if (e.key === '2') document.querySelector('a[href="#filter-search-view"]')?.click();
        else if (e.key === '3') document.querySelector('a[href="#stats-view"]')?.click();
        else if (e.key === '4') document.querySelector('a[href="#estados-view"]')?.click();
    });
};

const _setupProgressBarNavigation = () => {
    if (!progressSteps) return;
    progressSteps.forEach(step => {
        const nuevo = step.cloneNode(true);
        step.parentNode.replaceChild(nuevo, step);
        nuevo.addEventListener('click', () => {
            const stepName   = nuevo.getAttribute('data-step');
            const stepOrder  = ['estado','zona','municipio','localidad','clave'];
            const targetIndex = stepOrder.indexOf(stepName);
            if (targetIndex > 0) {
                const prevStepName = stepOrder[targetIndex - 1];
                if (!selects[prevStepName]?.value) {
                    showAlert(`Por favor, completa primero el paso de '${prevStepName}'.`, 'warning');
                    return;
                }
            }
            if (filterSteps[stepName]) {
                const pos = filterSteps[stepName].getBoundingClientRect().top + window.pageYOffset - 100;
                window.scrollTo({ top: pos, behavior: 'smooth' });
                if (selects[stepName] && !selects[stepName].disabled) selects[stepName].focus();
            }
        });
    });
};

const _addAutoSearchToggle = () => {
    const searchButton = document.getElementById('search-filter-button');
    if (!searchButton) return;

    const toggleContainer = document.createElement('div');
    toggleContainer.className = 'auto-search-toggle';
    toggleContainer.style.cssText = 'margin:1rem 0;display:flex;align-items:center;gap:0.5rem;';

    const checkbox  = document.createElement('input');
    checkbox.type   = 'checkbox';
    checkbox.id     = 'auto-search-toggle';
    checkbox.style.margin = '0';

    const label      = document.createElement('label');
    label.htmlFor    = 'auto-search-toggle';
    label.style.cssText = 'font-size:0.875rem;color:var(--text-muted);cursor:pointer;';
    label.textContent   = 'Búsqueda automática al seleccionar clave';

    toggleContainer.appendChild(checkbox);
    toggleContainer.appendChild(label);
    searchButton.insertAdjacentElement('beforebegin', toggleContainer);
};

const _setupBusquedaEstadosView = (estadosData) => {
    if (!estadosSearchInput) return;
    const debouncedSearch = debounce((query) => {
        const q = query.toLowerCase().trim();
        renderEstadosConPlazas(
            !q ? estadosData : estadosData.filter(e => e.nombre.toLowerCase().includes(q))
        );
    }, 300);
    estadosSearchInput.addEventListener('input', (e) => debouncedSearch(e.target.value));
};

const _setupBusquedaPlazasView = () => {
    if (!plazasSearchInput) return;
    const debouncedSearch = debounce((query) => {
        const q = query.toLowerCase().trim();
        renderPlazasList(
            !q ? plazasDelEstado : plazasDelEstado.filter(p =>
                p.clave.toLowerCase().includes(q)         ||
                p.municipio.toLowerCase().includes(q)     ||
                p.localidad.toLowerCase().includes(q)     ||
                (p.direccion && p.direccion.toLowerCase().includes(q))
            ),
            estadoSeleccionado
        );
    }, 300);
    plazasSearchInput.addEventListener('input', (e) => debouncedSearch(e.target.value));
};