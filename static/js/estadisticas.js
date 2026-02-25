// ============================================================
// estadisticas.js — Vista de estadísticas generales y CN
// ============================================================
// Responsabilidades:
//   - Cargar y renderizar estadísticas generales del sistema
//   - Cargar y renderizar estadísticas de Certificaciones
//     Nacionales (CN): resumen, tabla por estado, top 5, destacados
//   - Navegación entre sub-vistas (general / comparativas)
// ============================================================

'use strict';

import { fetchData, showAlert, showLoader, hideLoader } from './utils.js';

// --- ESTADO DEL MÓDULO ---
let estadisticasData         = null;
let cnResumenData            = null;
let cnPorEstadoData          = null;
let cnTopEstadosData         = null;
let cnEstadosDestacadosData  = null;
let cnTop5TodosData          = null;

// ============================================================
// INICIALIZACIÓN DE NAVEGACIÓN DE SUB-VISTAS
// ============================================================

// onComparativasActivada se inyecta desde app.js para lazy-init sin acoplamiento directo.
export const initStatsNavigation = (onComparativasActivada = null) => {
    const statsNavBtns  = document.querySelectorAll('.stats-nav-btn');
    const statsSubviews = document.querySelectorAll('.stats-subview');

    statsNavBtns.forEach(btn => {
        // Clonar para eliminar listeners duplicados si se llama varias veces
        const nuevo = btn.cloneNode(true);
        btn.parentNode.replaceChild(nuevo, btn);

        nuevo.addEventListener('click', function () {
            const targetSubview = this.getAttribute('data-subview');

            document.querySelectorAll('.stats-nav-btn').forEach(b => b.classList.remove('active'));
            this.classList.add('active');

            statsSubviews.forEach(view => {
                view.classList.toggle('hidden', view.id !== `${targetSubview}-view`);
            });

            if (targetSubview === 'comparativas-stats') {
                if (typeof onComparativasActivada === 'function') {
                    // Módulo ES: callback inyectado desde app.js
                    onComparativasActivada();
                } else if (typeof sistemaComparativas !== 'undefined' && sistemaComparativas.init) {
                    // Compatibilidad legacy con script global
                    sistemaComparativas.init();
                }
            }
        });
    });
};

/**
 * Muestra la sub-vista por defecto al abrir la vista de estadísticas.
 * Debe llamarse después de initStatsNavigation.
 */
export const mostrarSubvistaDefault = () => {
    setTimeout(() => {
        const generalView      = document.getElementById('general-stats-view');
        const comparativasView = document.getElementById('comparativas-stats-view');
        const generalBtn       = document.querySelector('[data-subview="general-stats"]');
        const comparativasBtn  = document.querySelector('[data-subview="comparativas-stats"]');

        if (generalView && comparativasView) {
            generalView.classList.remove('hidden');
            comparativasView.classList.add('hidden');
        }
        if (generalBtn && comparativasBtn) {
            generalBtn.classList.add('active');
            comparativasBtn.classList.remove('active');
        }
    }, 50);
};

// ============================================================
// CARGA DE DATOS
// ============================================================

/**
 * Carga estadísticas generales y luego las CN.
 * Llama internamente a cargarEstadisticasCompletasCN().
 */
export const cargarEstadisticas = async () => {
    try {
        showLoader('Cargando estadísticas del sistema...', 'compact');
        document.getElementById('stats-view')?.classList.add('shimmer');

        const stats = await fetchData('/api/estadisticas');
        estadisticasData = stats;
        _renderEstadisticasGenerales(stats);

        await cargarEstadisticasCompletasCN();

    } catch (error) {
        console.error('Error cargando estadísticas:', error);
        showAlert('Error al cargar las estadísticas desde el servidor.', 'error');
    } finally {
        hideLoader();
        document.getElementById('stats-view')?.classList.remove('shimmer');
    }
};

export const cargarEstadisticasCompletasCN = async () => {
    if (cnResumenData) return; // ya cargadas
    try {
        showLoader('Cargando estadísticas completas...', 'compact');

        const [resumen, porEstado, topEstados, estadosDestacados, top5Todos] = await Promise.all([
            fetchData('/api/cn_resumen'),
            fetchData('/api/cn_por_estado'),
            fetchData('/api/cn_top_estados?metric=inicial&n=10'),
            fetchData('/api/cn_estados_destacados'),
            fetchData('/api/cn_top5_todos')
        ]);

        cnResumenData           = resumen;
        cnPorEstadoData         = porEstado;
        cnTopEstadosData        = topEstados;
        cnEstadosDestacadosData = estadosDestacados;
        cnTop5TodosData         = top5Todos;

        _renderEstadisticasCN();
        _renderEstadosDestacadosCN(estadosDestacados);
        _renderTop5TodosCN(top5Todos);

    } catch (error) {
        console.error('Error cargando estadísticas completas CN:', error);
        showAlert('Error al cargar las estadísticas', 'error');
    } finally {
        hideLoader();
    }
};

// Getters para que app.js pueda consultar si ya están cargadas
export const getEstadisticasData = () => estadisticasData;
export const getCNResumenData    = () => cnResumenData;

// ============================================================
// RENDER — ESTADÍSTICAS GENERALES
// ============================================================

const _renderEstadisticasGenerales = (stats) => {
    const bind = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value ?? 'N/A';
    };

    bind('total-plazas',                   stats.totalPlazas?.toLocaleString() || '0');
    bind('plazas-operacion',               stats.plazasOperacion?.toLocaleString() || '0');
    bind('total-estados',                  stats.totalEstados?.toLocaleString() || '0');
    bind('estado-mas-plazas-nombre',       stats.estadoMasPlazas?.nombre);
    bind('estado-mas-plazas-cantidad',     stats.estadoMasPlazas?.cantidad?.toLocaleString() || '0');
    bind('estado-mayor-conectividad-nombre',     stats.estadoMayorConectividad?.nombre);
    bind('estado-mayor-conectividad-porcentaje', `${stats.estadoMayorConectividad?.porcentaje || 0}%`);
    bind('estado-mas-operacion-nombre',    stats.estadoMasOperacion?.nombre);
    bind('estado-mas-operacion-porcentaje',`${stats.estadoMasOperacion?.porcentaje || 0}%`);
    bind('estado-mas-suspension-nombre',   stats.estadoMasSuspension?.nombre);
    bind('estado-mas-suspension-porcentaje',`${stats.estadoMasSuspension?.porcentaje || 0}%`);
};

// ============================================================
// RENDER — CN: RESUMEN
// ============================================================

const _renderResumenCN = () => {
    const cnResumenCards = document.getElementById('cn-resumen-cards');
    if (!cnResumenCards || !cnResumenData?.resumen_nacional) return;

    const { resumen_nacional, top5_estados_por_CN_Total } = cnResumenData;
    cnResumenCards.innerHTML = '';

    // Card resumen nacional
    const resumenCard = document.createElement('div');
    resumenCard.className = 'cn-card';

    const tituloResumen = document.createElement('h4');
    tituloResumen.textContent = '📊 Resumen Nacional';

    const statsGrid = document.createElement('div');
    statsGrid.className = 'cn-stats-grid';

    ['CN_Inicial_Acum','CN_Prim_Acum','CN_Sec_Acum','CN_Total'].forEach(key => {
        const data = resumen_nacional[key];
        if (!data) return;

        const statItem  = document.createElement('div');
        statItem.className = `cn-stat-item ${key === 'CN_Total' ? 'cn-total-item' : ''}`;

        const label     = document.createElement('span');
        label.className = 'cn-stat-label';
        label.textContent = key === 'CN_Total' ? 'CN TOTAL' : key.replace(/_/g, ' ');

        const value     = document.createElement('span');
        value.className = 'cn-stat-value';
        value.textContent = data.suma.toLocaleString();

        const subvalue  = document.createElement('span');
        subvalue.className = 'cn-stat-subvalue';
        subvalue.textContent = `Plazas en operación: ${data.plazasOperacion.toLocaleString()}`;

        statItem.appendChild(label);
        statItem.appendChild(value);
        statItem.appendChild(subvalue);
        statsGrid.appendChild(statItem);
    });

    resumenCard.appendChild(tituloResumen);
    resumenCard.appendChild(statsGrid);
    cnResumenCards.appendChild(resumenCard);

    // Card top 5 estados por CN Total
    if (top5_estados_por_CN_Total?.length) {
        const top5Card = document.createElement('div');
        top5Card.className = 'cn-card';

        const tituloTop5 = document.createElement('h4');
        tituloTop5.textContent = '🏆 Top 5 Estados - CN Total';

        const top5Grid = document.createElement('div');
        top5Grid.className = 'cn-stats-grid';

        const MEDALS = ['🥇','🥈','🥉','🏅','🏅'];
        top5_estados_por_CN_Total.forEach((item, index) => {
            const statItem  = document.createElement('div');
            statItem.className = 'cn-stat-item';

            const label     = document.createElement('span');
            label.className = 'cn-stat-label';
            label.textContent = `${MEDALS[index] || '🏅'} ${item.estado}`;

            const value     = document.createElement('span');
            value.className = 'cn-stat-value';
            value.textContent = item.suma_CN_Total.toLocaleString();

            statItem.appendChild(label);
            statItem.appendChild(value);
            top5Grid.appendChild(statItem);
        });

        top5Card.appendChild(tituloTop5);
        top5Card.appendChild(top5Grid);
        cnResumenCards.appendChild(top5Card);
    }
};

// ============================================================
// RENDER — CN: TABLA POR ESTADO (con ordenamiento)
// ============================================================

const _renderTablaEstadosCN = () => {
    const cnEstadosTable = document.getElementById('cn-estados-table');
    if (!cnEstadosTable || !cnPorEstadoData?.estados) return;

    const { estados } = cnPorEstadoData;
    let currentData = [...estados];

    cnEstadosTable.innerHTML = '';
    cnEstadosTable.className = 'cn-table-container';
    Object.assign(cnEstadosTable.style, {
        background   : 'var(--surface-color)',
        border       : '1px solid var(--border-color)',
        borderRadius : 'var(--border-radius-lg)',
        overflowX    : 'auto',
        maxHeight    : '650px',
        boxShadow    : 'var(--shadow)',
        marginBottom : '2rem',
        position     : 'relative'
    });

    const table = document.createElement('table');
    table.className = 'cn-table';
    Object.assign(table.style, { width: '100%', borderCollapse: 'separate', borderSpacing: '0', position: 'relative' });

    // Calcular totales para el footer
    const totales = estados.reduce((acc, curr) => ({
        total_plazas     : acc.total_plazas      + (curr.total_plazas              || 0),
        plazas_operacion : acc.plazas_operacion   + (curr.plazas_operacion          || 0),
        conectados_actual: acc.conectados_actual  + (curr.conectados_actual         || 0),
        cn_inicial       : acc.cn_inicial         + (curr.suma_CN_Inicial_Acum      || 0),
        cn_primaria      : acc.cn_primaria        + (curr.suma_CN_Prim_Acum         || 0),
        cn_secundaria    : acc.cn_secundaria      + (curr.suma_CN_Sec_Acum          || 0),
        cn_total         : acc.cn_total           + (curr.suma_CN_Total             || 0)
    }), { total_plazas: 0, plazas_operacion: 0, conectados_actual: 0, cn_inicial: 0, cn_primaria: 0, cn_secundaria: 0, cn_total: 0 });

    const pctGlobalConectividad = totales.total_plazas > 0
        ? ((totales.conectados_actual / totales.total_plazas) * 100).toFixed(1)
        : 0;

    // HEADER
    const thead   = document.createElement('thead');
    const headerRow = document.createElement('tr');

    const headers = [
        { text: 'Estado',            sort: 'estado',         order: 'asc'  },
        { text: 'Total Plazas',      sort: 'total_plazas',   order: 'desc' },
        { text: 'Plazas Operación',  sort: 'plazas_operacion',order: 'desc'},
        { text: '% Conectividad',    sort: 'pct_conectividad',order: 'desc'},
        { text: 'CN Inicial',        sort: 'cn_inicial',     order: 'desc' },
        { text: 'CN Primaria',       sort: 'cn_primaria',    order: 'desc' },
        { text: 'CN Secundaria',     sort: 'cn_secundaria',  order: 'desc' },
        { text: 'CN Total',          sort: 'cn_total',       order: 'desc' },
        { text: '% Sobre Nacional',  sort: 'pct_nacional',   order: 'desc' }
    ];

    headers.forEach(h => {
        const th  = document.createElement('th');
        Object.assign(th.style, {
            background : 'linear-gradient(135deg, var(--primary-color), var(--primary-dark))',
            color      : 'var(--surface-color)',
            padding    : '1rem',
            textAlign  : 'left',
            fontWeight : '600',
            fontSize   : 'var(--font-size-sm)',
            position   : 'sticky',
            top        : '0',
            zIndex     : '5'
        });
        const btn = document.createElement('button');
        btn.className         = 'sort-btn';
        btn.textContent       = `${h.text} ${h.order === 'asc' ? '▲' : '▼'}`;
        btn.dataset.sort      = h.sort;
        btn.dataset.order     = h.order;
        Object.assign(btn.style, { background: 'transparent', border: 'none', color: 'inherit', cursor: 'pointer', fontWeight: 'inherit', fontSize: 'inherit', display: 'flex', alignItems: 'center', gap: '0.25rem', padding: '0', width: '100%' });
        th.appendChild(btn);
        headerRow.appendChild(th);
    });

    thead.appendChild(headerRow);
    table.appendChild(thead);

    // BODY
    const tbody = document.createElement('tbody');
    table.appendChild(tbody);

    const _crearTd = (texto, strong = false) => {
        const td = document.createElement('td');
        Object.assign(td.style, { padding: '0.875rem 1rem', borderBottom: '1px solid var(--border-color)', fontSize: 'var(--font-size-sm)' });
        td.textContent = strong ? '' : texto;
        if (strong) { const s = document.createElement('strong'); s.textContent = texto; td.appendChild(s); }
        return td;
    };

    const _crearBadgeTd = (texto, badgeClass, tooltip = '') => {
        const td    = document.createElement('td');
        Object.assign(td.style, { padding: '0.875rem 1rem', borderBottom: '1px solid var(--border-color)', fontSize: 'var(--font-size-sm)' });
        const badge = document.createElement('span');
        badge.className   = `cn-badge ${badgeClass}`;
        badge.textContent = texto;
        if (tooltip) { td.title = tooltip; td.style.cursor = 'help'; }
        td.appendChild(badge);
        return td;
    };

    const _crearPillTd = (porcentaje, color, bg, tooltip = '') => { // <--- Agregamos tooltip
    const td   = document.createElement('td');
    Object.assign(td.style, { padding: '0.875rem 1rem', borderBottom: '1px solid var(--border-color)' });
    
    if (tooltip) { 
        td.title = tooltip; 
        td.style.cursor = 'help'; 
    }

    const pill = _crearPill(`${porcentaje}%`, color, bg);
    td.appendChild(pill);
    return td;
};

    const _crearPill = (texto, color, bg) => {
        const pill = document.createElement('span');
        pill.textContent = texto;
        Object.assign(pill.style, {
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
            minWidth: '70px', padding: '6px 16px', borderRadius: '50px',
            fontSize: '0.9rem', fontWeight: '700', lineHeight: '1',
            boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
            color, background: bg, border: `1px solid ${color}20`
        });
        return pill;
    };

    const _colorConectividad = (pct) => {
        if (pct < 50)  return { color: '#dc2626', bg: 'rgba(220,38,38,0.1)' };
        if (pct < 70)  return { color: '#f59e0b', bg: 'rgba(245,158,11,0.1)' };
        return { color: '#059669', bg: 'rgba(5,150,105,0.1)' };
    };

    const renderRows = (data) => {
        tbody.innerHTML = '';
        data.forEach(estado => {
            const row = document.createElement('tr');
            row.style.transition = 'background var(--transition-fast)';
            row.addEventListener('mouseenter', () => { row.style.background = 'var(--light-color)'; });
            row.addEventListener('mouseleave', () => { row.style.background = 'transparent'; });

            const pct = estado.pct_conectividad || 0;
            const plazasOp = (estado.plazas_operacion || 0).toLocaleString(); 
            const colores = _colorConectividad(pct);

            // Estado (fijo)
            const tdEstado = _crearTd(estado.estado, true);
            Object.assign(tdEstado.style, { fontWeight: '700', color: 'var(--primary-color)', position: 'sticky', left: '0', background: 'var(--surface-color)', zIndex: '2' });
            row.appendChild(tdEstado);

            row.appendChild(_crearTd(estado.total_plazas?.toLocaleString() || '0'));
            row.appendChild(_crearBadgeTd((estado.plazas_operacion || 0).toLocaleString(), 'badge-success', 'Plazas en operación del último mes'));
            row.appendChild(_crearPillTd(pct, colores.color, colores.bg));
            row.appendChild(_crearTd(estado.suma_CN_Inicial_Acum?.toLocaleString() || '0'));
            row.appendChild(_crearTd(estado.suma_CN_Prim_Acum?.toLocaleString()    || '0'));
            row.appendChild(_crearTd(estado.suma_CN_Sec_Acum?.toLocaleString()     || '0'));
            row.appendChild(_crearTd(estado.suma_CN_Total?.toLocaleString()        || '0'));
            row.appendChild(_crearBadgeTd((estado.plazas_operacion || 0).toLocaleString(), 'badge-success', 'Plazas en operación del último mes'));
            row.appendChild(_crearPillTd(pct, colores.color, colores.bg, `Operativas: ${plazasOp} plazas`));
           
            const tdPctNac = document.createElement('td');
            Object.assign(tdPctNac.style, { padding: '0.875rem 1rem', borderBottom: '1px solid var(--border-color)', textAlign: 'center', cursor: 'help' });
            tdPctNac.title = `Contribución del estado al total nacional`;
            tdPctNac.appendChild(_crearPill(`${estado.pct_sobre_nacional || 0}%`, '#059669', 'rgba(5,150,105,0.1)'));
            row.appendChild(tdPctNac);

            tbody.appendChild(row);
        });
    };

    renderRows(currentData);

    // FOOTER
    const tfoot = document.createElement('tfoot');
    const fr    = document.createElement('tr');
    Object.assign(fr.style, { background: 'var(--light-color)', fontWeight: '700', position: 'sticky', bottom: '0', zIndex: '4', boxShadow: '0 -2px 10px rgba(0,0,0,0.05)' });

    const tdFirst = document.createElement('td');
    Object.assign(tdFirst.style, { padding: '0.875rem 1rem', borderTop: '2px solid var(--border-color)', fontWeight: '700', position: 'sticky', left: '0', background: 'var(--light-color)', zIndex: '3' });
    tdFirst.textContent = 'TOTALES';
    fr.appendChild(tdFirst);

    const coloresGlobal = _colorConectividad(parseFloat(pctGlobalConectividad));

    const footerCells = [
        { texto: totales.total_plazas.toLocaleString(),     pill: false },
        { texto: totales.plazas_operacion.toLocaleString(), pill: false },
        { texto: `${pctGlobalConectividad}%`,               pill: true,  color: coloresGlobal.color, bg: coloresGlobal.bg },
        { texto: totales.cn_inicial.toLocaleString(),       pill: false },
        { texto: totales.cn_primaria.toLocaleString(),      pill: false },
        { texto: totales.cn_secundaria.toLocaleString(),    pill: false },
        { texto: totales.cn_total.toLocaleString(),         pill: false },
        { texto: '100%',                                    pill: true,  color: '#059669', bg: 'rgba(5,150,105,0.15)' }
    ];

    footerCells.forEach(cell => {
        const td = document.createElement('td');
        Object.assign(td.style, { padding: '0.875rem 1rem', borderTop: '2px solid var(--border-color)', fontWeight: '600', textAlign: 'center' });
        if (cell.pill) {
            td.appendChild(_crearPill(cell.texto, cell.color, cell.bg));
        } else {
            td.textContent = cell.texto;
            td.style.fontWeight = '700';
        }
        fr.appendChild(td);
    });

    tfoot.appendChild(fr);
    table.appendChild(tfoot);
    cnEstadosTable.appendChild(table);

    // SORTING
    const SORT_MAP = {
        total_plazas    : 'total_plazas',
        plazas_operacion: 'plazas_operacion',
        pct_conectividad: 'pct_conectividad',
        cn_inicial      : 'suma_CN_Inicial_Acum',
        cn_primaria     : 'suma_CN_Prim_Acum',
        cn_secundaria   : 'suma_CN_Sec_Acum',
        cn_total        : 'suma_CN_Total',
        pct_nacional    : 'pct_sobre_nacional'
    };

    cnEstadosTable.querySelectorAll('.sort-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const field = btn.dataset.sort;
            const order = btn.dataset.order === 'asc' ? 'desc' : 'asc';

            cnEstadosTable.querySelectorAll('.sort-btn').forEach(b => {
                b.dataset.order = 'desc';
                b.textContent   = b.textContent.replace(/[▲▼]/g, '') + ' ▼';
            });
            btn.dataset.order = order;
            btn.textContent   = btn.textContent.replace(/[▲▼]/g, '') + (order === 'asc' ? ' ▲' : ' ▼');
            btn.style.transform = 'scale(0.95)';
            setTimeout(() => { btn.style.transform = 'scale(1)'; }, 150);

            currentData.sort((a, b) => {
                if (field === 'estado') {
                    return order === 'asc' ? a.estado.localeCompare(b.estado) : b.estado.localeCompare(a.estado);
                }
                const A = a[SORT_MAP[field]] || 0;
                const B = b[SORT_MAP[field]] || 0;
                return order === 'asc' ? A - B : B - A;
            });
            renderRows(currentData);
        });
    });
};

// ============================================================
// RENDER — CN: ESTADOS DESTACADOS Y TOP 5
// ============================================================

const _renderEstadosDestacadosCN = (estadosDestacados) => {
    if (!estadosDestacados) return;

    const bind = (idNombre, idCantidad, estadoObj) => {
        if (!estadoObj) return;
        const elN = document.getElementById(idNombre);
        const elC = document.getElementById(idCantidad);
        if (elN) elN.textContent = estadoObj.estado;
        if (elC) elC.textContent = estadoObj.valor.toLocaleString();
    };

    bind('estado-mas-cn-inicial-nombre',    'estado-mas-cn-inicial-cantidad',    estadosDestacados.CN_Inicial_Acum);
    bind('estado-mas-cn-primaria-nombre',   'estado-mas-cn-primaria-cantidad',   estadosDestacados.CN_Prim_Acum);
    bind('estado-mas-cn-secundaria-nombre', 'estado-mas-cn-secundaria-cantidad', estadosDestacados.CN_Sec_Acum);
};

const _renderTop5TodosCN = (top5Todos) => {
    if (!top5Todos) return;

    const renderList = (listId, data) => {
        const listEl = document.getElementById(listId);
        if (!listEl || !data) return;
        listEl.innerHTML = '';
        data.forEach((item, index) => {
            const div = document.createElement('div');
            div.className = 'top10-item';

            const rank  = document.createElement('span');
            rank.className   = 'top10-rank';
            rank.textContent = `#${index + 1}`;

            const state = document.createElement('span');
            state.className   = 'top10-state';
            state.textContent = item.estado;

            const value = document.createElement('span');
            value.className   = 'top10-value';
            value.textContent = item.valor.toLocaleString();

            div.appendChild(rank);
            div.appendChild(state);
            div.appendChild(value);
            listEl.appendChild(div);
        });
    };

    renderList('cn-top5-inicial-list',    top5Todos.inicial);
    renderList('cn-top5-primaria-list',   top5Todos.primaria);
    renderList('cn-top5-secundaria-list', top5Todos.secundaria);
};

// ============================================================
// RENDER — COORDINADOR
// ============================================================

const _renderEstadisticasCN = () => {
    if (!cnResumenData || !cnPorEstadoData || !cnTopEstadosData) return;
    _renderResumenCN();
    _renderTablaEstadosCN();
};