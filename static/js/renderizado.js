// ============================================================
// renderizado.js — Renderizado de resultados de plaza
// ============================================================
// Responsabilidades:
//   - renderPlazaResultados(data)  → vista principal de plaza
//   - renderizarGridAislado()      → tablas con CSS aislado
//   - Galería de imágenes (lazy) y conexión con el modal
// ============================================================
// DEPENDENCIAS:
//   - utils.js → (no directa, la usa app.js)
//   - El modal se inyecta via setModalOpenFunction()
// ============================================================

'use strict';

// Referencia al openModal del módulo imageModal.js
// Se asigna desde app.js después de initImageModal()
let _openModal = null;

/** Permite a app.js inyectar la función openModal del módulo de modal. */
export const setModalOpenFunction = (fn) => { _openModal = fn; };

// --- DEFINICIÓN DE COLUMNAS ---
const COLUMNAS_UBICACION = [
    'Clave_Plaza','Nombre_PC','Estado','Coord. Zona',
    'Municipio','Localidad','Calle','Num','Colonia',
    'Cod_Post','Situación','Tipo_Conect','Conect_Instalada',
    'Latitud','Longitud'
];

const COLUMNAS_ATENCION = [
    'Inc_Inicial','Inc_Prim','Inc_Sec','Inc_Total',
    'Aten_Inicial','Aten_Prim','Aten_Sec','Aten_Total','Exámenes aplicados',
    'CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum','CN_Tot_Acum',
    'Cert_Emitidos'
];

const COLUMNAS_PERSONAL        = ['Tec_Doc','Nom_PVS_1','Nom_PVS_2'];
const COLUMNAS_INFRAESTRUCTURA = ['Tipo_local','Inst_aliada','Arq_Discap.'];
const COLUMNAS_INVENTARIO      = [
    'Total de equipos de cómputo en la plaza','Equipos de cómputo que operan',
    'Tipos de equipos de cómputo','Impresoras que funcionan',
    'Impresoras con suministros (toner, hojas)','Total de servidores en la plaza',
    'Número de servidores que funcionan correctamente','Cuantas mesas funcionan',
    'Cuantas sillas funcionan','Cuantos Anaqueles funcionan'
];

const NOMBRES_PERSONALIZADOS = {
    'CN_Inicial_Acum' : 'CN Inicial',
    'CN_Prim_Acum'    : 'CN Primaria',
    'CN_Sec_Acum'     : 'CN Secundaria',
    'CN_Tot_Acum'     : 'CN Total',
    'Arq_Discap.'     : 'Arquitectura para Discap.'
};

// ============================================================
// RENDERIZADO PRINCIPAL
// ============================================================

/**
 * Renderiza los resultados de una plaza en #results-content.
 * @param {object} data  Respuesta de /api/search enriquecida con images[] e historial[].
 */
export const renderPlazaResultados = (data) => {
    const { excel_info, images, google_maps_url, direccion_completa, historial } = data;

    const template = document.getElementById('plaza-results-template');
    if (!template) { console.error('Template "plaza-results-template" no encontrado'); return; }
    const clone = template.content.cloneNode(true);

    // --- Cabecera ---
    _bind(clone, 'clave_plaza',      excel_info.Clave_Plaza || '');
    _bindDireccion(clone, direccion_completa);
    _bindMapsLink(clone, google_maps_url);

    // --- Grids de datos ---
    const gridUbicacion = clone.querySelector('[data-bind="grid_ubicacion"]');
    const gridInfra     = clone.querySelector('[data-bind="grid_infraestructura"]');
    const gridInventario= clone.querySelector('[data-bind="grid_inventario"]');
    const gridPersonal  = clone.querySelector('[data-bind="grid_personal"]');
    const gridAtencion  = clone.querySelector('[data-bind="grid_atencion"]');

    if (gridUbicacion)  renderizarGridAislado(gridUbicacion,  excel_info, COLUMNAS_UBICACION);
    if (gridInfra)      renderizarGridAislado(gridInfra,      excel_info, COLUMNAS_INFRAESTRUCTURA);
    if (gridInventario) renderizarGridAislado(gridInventario, excel_info, COLUMNAS_INVENTARIO);
    if (gridPersonal)   renderizarGridAislado(gridPersonal,   excel_info, COLUMNAS_PERSONAL);

    // --- Grid de Atención con selector de periodo ---
    if (gridAtencion) {
        _renderGridAtencionConHistorial(clone, gridAtencion, excel_info, historial);
    }

    // --- Galería de imágenes ---
    _renderImagenes(clone, images);

    // --- Insertar en DOM ---
    const resultsContent = document.getElementById('results-content');
    if (resultsContent) {
        resultsContent.innerHTML = '';
        resultsContent.appendChild(clone);
    }

    // --- Conectar modal (después de insertar en DOM) ---
    setTimeout(() => _conectarModal(images || []), 100);
};

// ============================================================
// GRID AISLADO
// ============================================================

/**
 * Renderiza un grid de datos dentro de un contenedor, usando CSS aislado.
 * @param {HTMLElement} container  Elemento donde se insertará el grid.
 * @param {object}      info       Objeto con los datos.
 * @param {string[]}    columns    Columnas a mostrar.
 */
export const renderizarGridAislado = (container, info, columns) => {
    if (!container) return;
    container.innerHTML = '';

    const isolated = document.createElement('div');
    isolated.className = 'plaza-data-isolated';

    const wrapper = document.createElement('div');
    wrapper.className = 'custom-plaza-table';

    const grid = document.createElement('div');
    grid.className = 'section-grid';

    columns.forEach(key => {
        const value        = info[key];
        const displayValue = (value !== null && value !== undefined && value !== '') ? value : 'N/A';
        const displayKey   = NOMBRES_PERSONALIZADOS[key] || key.replace(/_/g, ' ');
        const valueStr     = String(displayValue);

        const item = document.createElement('div');
        item.className = 'data-item';
        if (valueStr.includes('\n')) item.setAttribute('data-multiline', 'true');
        if (valueStr.length > 50)   item.setAttribute('data-truncate', 'true');

        const label = document.createElement('span');
        label.className   = 'data-label';
        label.textContent = displayKey + ':';

        const val = document.createElement('span');
        val.className = 'data-value';

        if (key === 'Latitud' || key === 'Longitud' || key === 'Coord. Zona') val.classList.add('coord');
        if (!isNaN(value) && value !== null && value !== '' && value > 1000)   val.classList.add('highlight');

        if (value === null || value === undefined || value === '') {
            val.classList.add('empty');
            val.textContent = 'No disponible';
        } else {
            val.textContent = valueStr;
        }

        item.appendChild(label);
        item.appendChild(val);
        grid.appendChild(item);
    });

    wrapper.appendChild(grid);
    isolated.appendChild(wrapper);
    container.appendChild(isolated);
};

// ============================================================
// FUNCIONES PRIVADAS
// ============================================================

/** Asigna textContent a un elemento con data-bind. */
const _bind = (clone, bindKey, value) => {
    const el = clone.querySelector(`[data-bind="${bindKey}"]`);
    if (el && value) el.textContent = value;
};

/** Inserta la dirección con etiqueta <strong>. */
const _bindDireccion = (clone, direccion_completa) => {
    const el = clone.querySelector('[data-bind="direccion_completa"]');
    if (!el) return;
    if (direccion_completa) {
        const strong = document.createElement('strong');
        strong.textContent = 'Dirección:';
        el.innerHTML = '';
        el.appendChild(strong);
        el.appendChild(document.createTextNode(` ${direccion_completa}`));
    } else {
        el.style.display = 'none';
    }
};

/** Configura el enlace a Google Maps. */
const _bindMapsLink = (clone, google_maps_url) => {
    const el = clone.querySelector('[data-bind="google_maps_url"]');
    if (!el) return;
    if (google_maps_url) {
        el.href        = google_maps_url;
        el.textContent = 'Ver en Google Maps';
    } else {
        el.style.display = 'none';
    }
};

/**
 * Renderiza el grid de Atención.
 * Si hay historial, agrega un <select> para elegir el periodo.
 */
const _renderGridAtencionConHistorial = (clone, gridAtencion, excel_info, historial) => {
    // Buscar el select pre-existente en el template
    let selectAtencion = clone.querySelector('#atencion-periodo-select');

    // Si no existe en el template, crear el selector de periodo dinámicamente.
    // IMPORTANTE: previousElementSibling no funciona en DocumentFragment,
    // así que buscamos el H2 hermano dentro del mismo contenedor padre del gridAtencion.
    if (!selectAtencion && gridAtencion) {
        // Buscar H2 hermano anterior recorriendo el parent
        const parent = gridAtencion.parentNode;
        let header = null;
        if (parent) {
            // Iterar hijos del padre para encontrar el H2 que precede al grid
            const hijos = Array.from(parent.children);
            const idxGrid = hijos.indexOf(gridAtencion);
            for (let i = idxGrid - 1; i >= 0; i--) {
                if (hijos[i].tagName === 'H2') { header = hijos[i]; break; }
            }
        }

        if (header) {
            const container = document.createElement('div');
            container.style.cssText = 'display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;';

            header.parentNode.insertBefore(container, header);
            container.appendChild(header);
            header.style.marginBottom = '0';

            const selectDiv = document.createElement('div');
            selectDiv.innerHTML = `<select id="atencion-periodo-select" style="padding:5px;border-radius:4px;border:1px solid #ccc;font-size:0.9rem;"></select>`;
            container.appendChild(selectDiv);
            selectAtencion = selectDiv.querySelector('select');
        }
    }

    if (historial?.length > 0 && selectAtencion) {
        const MESES = ['','Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];

        selectAtencion.innerHTML = '';
        historial.forEach((item, index) => {
            const mesNum    = parseInt(item['Cve-mes'] || item['Mes'] || 0);
            const nombreMes = (!isNaN(mesNum) && mesNum > 0) ? MESES[mesNum] : (item['Mes'] || 'Mes ' + mesNum);
            const anio      = item['Año'] || '';

            const option       = document.createElement('option');
            option.value       = index;
            option.textContent = `${nombreMes} ${anio}`;
            selectAtencion.appendChild(option);
        });

        selectAtencion.addEventListener('change', (e) => {
            renderizarGridAislado(gridAtencion, historial[e.target.value], COLUMNAS_ATENCION);
        });

        renderizarGridAislado(gridAtencion, historial[0], COLUMNAS_ATENCION);
    } else {
        // Sin historial: mostrar datos actuales del excel_info
        // Ocultar el selector si no hay periodos disponibles
        if (selectAtencion) {
            const labelEl = selectAtencion.closest('.period-selector-container') ||
                            selectAtencion.parentElement;
            if (labelEl) labelEl.style.display = 'none';
            else selectAtencion.style.display = 'none';
        }
        renderizarGridAislado(gridAtencion, excel_info, COLUMNAS_ATENCION);
    }
};

/** Renderiza la galería de imágenes con lazy loading. */
const _renderImagenes = (clone, images) => {
    const imagesContainer = clone.querySelector('[data-bind="images_grid"]');
    if (!imagesContainer) return;
    imagesContainer.innerHTML = '';

    if (images?.length > 0) {
        const imageTemplate = document.getElementById('image-item-template');
        images.forEach((url, index) => {
            const imageClone = imageTemplate.content.cloneNode(true);
            const img        = imageClone.querySelector('img');
            if (img) {
                img.src     = url;
                img.alt     = `Imagen ${index + 1}`;
                img.loading = 'lazy';
            }
            imagesContainer.appendChild(imageClone);
        });
    } else {
        const noImagesTemplate = document.getElementById('no-images-template');
        if (noImagesTemplate) imagesContainer.appendChild(noImagesTemplate.content.cloneNode(true));
    }
};

/**
 * Conecta los contenedores de imágenes renderizados al modal.
 * Se ejecuta después de que el clone está en el DOM.
 */
const _conectarModal = (allImages) => {
    const imageContainers = document.querySelectorAll('.image-container');
    imageContainers.forEach((container, index) => {
        container.style.cursor = 'pointer';
        // Clonar nodo para eliminar listeners anteriores
        const nuevo = container.cloneNode(true);
        container.parentNode.replaceChild(nuevo, container);
        nuevo.addEventListener('click', () => {
            if (allImages.length > 0 && _openModal) {
                _openModal(allImages, index);
            }
        });
    });
};