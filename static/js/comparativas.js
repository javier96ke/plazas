/**
 * SistemaComparativas — v3.0
 *
 * Refactorización completa alineada con drive_excel_reader.py v3:
 *
 *  - Modo "periodo": llama comparar_periodos_avanzado_con_años_diferentes
 *    (year1, periodo1, year2, periodo2) → permite cruzar años distintos.
 *  - Modo "anual":  llama comparar_años (year1, year2) → estructura
 *    resumen_año1/año2 + diferencias.metricas + por_estado.
 *  - Regla ⑤: meses disponibles del año actual vienen del parquet local;
 *    años históricos sin diciembre NO aparecen en el selector.
 *  - Normalización de respuesta centralizada en _normalizarRespuesta()
 *    para que el render sea idéntico en ambos modos.
 */

'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// CONSTANTES
// ─────────────────────────────────────────────────────────────────────────────

const NOMBRES_MESES = {
  '01': 'Enero',    '02': 'Febrero',  '03': 'Marzo',
  '04': 'Abril',    '05': 'Mayo',     '06': 'Junio',
  '07': 'Julio',    '08': 'Agosto',   '09': 'Septiembre',
  '10': 'Octubre',  '11': 'Noviembre','12': 'Diciembre',
};

const METRICAS_ORDEN = [
  'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum',
];

const NOMBRES_METRICAS = {
  CN_Inicial_Acum: 'CN Inicial',
  CN_Prim_Acum:    'CN Primaria',
  CN_Sec_Acum:     'CN Secundaria',
  CN_Tot_Acum:     'CN Total',
};

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS DE FORMATO (puros, sin estado)
// ─────────────────────────────────────────────────────────────────────────────

const fmt = {
  number:  (n) => isNaN(n) ? '0' : new Intl.NumberFormat('es-MX').format(Math.round(n || 0)),
  pct:     (p) => { const s = p > 0 ? '+' : ''; return `${s}${parseFloat(p || 0).toFixed(2)}%`; },
  cambio:  (v) => v === 0 ? '0' : `${v > 0 ? '+' : ''}${fmt.number(v)}`,
  classCambio: (v) => v > 0 ? 'cambio-positivo' : v < 0 ? 'cambio-negativo' : 'cambio-neutral',
  tendencia:   (v) => v > 0 ? '📈' : v < 0 ? '📉' : '➡️',
  tendenciaTexto: (v) => v > 0 ? '📈 Aumento' : v < 0 ? '📉 Disminución' : '➡️ Sin cambio',
};

const getNombreMes   = (m) => NOMBRES_MESES[String(m).padStart(2, '0')] ?? `Mes ${m}`;
const getNombreMetrica = (k) => NOMBRES_METRICAS[k] ?? k;

// ─────────────────────────────────────────────────────────────────────────────
// CLASE PRINCIPAL
// ─────────────────────────────────────────────────────────────────────────────

class SistemaComparativas {
  constructor() {
    this._añosDisponibles = [];
    this._mesesPorAño     = {};      // { '2023': ['01','02',...] }
    this._datos           = null;    // resultado normalizado de la última comparativa
    this._modo            = 'periodo'; // 'periodo' | 'anual'
    this._inicializado    = false;
    this._el              = {};      // referencias DOM cacheadas
  }

  // ───────────────────────── INICIALIZACIÓN ──────────────────────────────────

  init() {
    if (this._inicializado) return;

    this._el = {
      yearSelect:  document.getElementById('comparativa-year'),
      p1Select:    document.getElementById('comparativa-periodo1'),
      p2Select:    document.getElementById('comparativa-periodo2'),
      compararBtn: document.getElementById('comparar-periodos-btn'),
      resultados:  document.getElementById('comparativa-resultados'),
    };

    if (!this._el.yearSelect) return; // vista no activa

    this._inyectarSelectorModo();
    this._inyectarSelectorAño2();
    this._attachListeners();
    this._cargarPeriodosDisponibles();
    this._inicializado = true;
  }

  /** Inserta el <select> de modo de comparación después del <h3> del panel. */
  _inyectarSelectorModo() {
    const panel = document.querySelector('.periodo-selector');
    if (!panel) return;
    const h3 = panel.querySelector('h3');
    if (!h3) return;
    h3.insertAdjacentHTML('afterend', `
      <div class="modo-comparacion-group">
        <label for="modo-comparacion">Tipo de Comparación</label>
        <select id="modo-comparacion" class="modo-comparacion-select">
          <option value="periodo">Período con Período</option>
          <option value="anual">Año con Año</option>
        </select>
      </div>
    `);
  }

  /**
   * Inserta un segundo selector de año para el modo "periodo"
   * (permite cruzar años distintos, ej: Marzo 2023 vs Marzo 2024).
   */
  _inyectarSelectorAño2() {
    const p2Container = this._el.p2Select?.parentElement;
    if (!p2Container) return;

    const wrapper = document.createElement('div');
    wrapper.className = 'selector-group year2-group';
    wrapper.style.display = 'none'; // oculto hasta activarse
    wrapper.innerHTML = `
      <label for="comparativa-year2">Año Período 2</label>
      <select id="comparativa-year2" disabled>
        <option value="">Selecciona año primero</option>
      </select>
    `;
    p2Container.insertBefore(wrapper, this._el.p2Select);
    this._el.year2Group  = wrapper;
    this._el.year2Select = wrapper.querySelector('select');
  }

  _attachListeners() {
    const on = (el, ev, fn) => el?.addEventListener(ev, fn);

    on(document.getElementById('modo-comparacion'), 'change', (e) => {
      this._modo = e.target.value;
      this._onModoChange();
    });

    on(this._el.yearSelect,  'change', () => this._onYear1Change());
    on(this._el.year2Select, 'change', () => this._onYear2Change());
    on(this._el.p1Select,    'change', () => this._validarBotón());
    on(this._el.p2Select,    'change', () => this._validarBotón());
    on(this._el.compararBtn, 'click',  () => this._ejecutar());

    // Cerrar modal al clic exterior
    const modal = document.getElementById('modal-detalle-estado');
    if (modal) {
      modal.querySelector('.modal-close')?.addEventListener('click', () =>
        modal.classList.add('hidden'));
      modal.addEventListener('click', (e) => {
        if (e.target === modal) modal.classList.add('hidden');
      });
    }
  }

  // ───────────────────────── CARGA DE PERÍODOS ──────────────────────────────

  async _cargarPeriodosDisponibles() {
    try {
      this._loader('Cargando períodos disponibles…');
      const res  = await fetch('/api/drive-comparativas/periodos');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      if (data.status !== 'success') throw new Error(data.message || 'Error del servidor');

      this._añosDisponibles = data.years ?? [];
      this._mesesPorAño     = {};

      // Normalizar: claves string, valores ordenados
      for (const [año, meses] of Object.entries(data.meses_por_anio ?? {})) {
        this._mesesPorAño[String(año)] = (Array.isArray(meses) ? meses : [])
          .map(String).sort();
      }
      // Garantizar entrada para cada año
      for (const año of this._añosDisponibles) {
        const k = String(año);
        if (!this._mesesPorAño[k]) this._mesesPorAño[k] = [];
      }

      this._poblarSelectYear(this._el.yearSelect);
      this._poblarSelectYear(this._el.year2Select, false);

    } catch (err) {
      this._alerta(`Error al cargar períodos: ${err.message}`, 'error');
    } finally {
      this._loaderOff();
    }
  }

  _poblarSelectYear(select, withDefault = true) {
    if (!select) return;
    select.innerHTML = withDefault
      ? '<option value="">Selecciona un año</option>'
      : '<option value="">Selecciona año período 2</option>';

    if (!this._añosDisponibles.length) {
      select.innerHTML = '<option value="">Sin años disponibles</option>';
      return;
    }
    for (const año of this._añosDisponibles) {
      const meses = this._mesesPorAño[String(año)] ?? [];
      const label = this._tiene12Meses(meses) ? String(año) : `${año} ⚠️ incompleto`;
      select.insertAdjacentHTML('beforeend',
        `<option value="${año}">${label}</option>`);
    }
    select.disabled = false;
  }

  // ─────────────────── CAMBIO DE MODO ──────────────────────────────────────

  _onModoChange() {
    const esAnual = this._modo === 'anual';

    // Año selector principal
    const labelP1 = document.querySelectorAll('.selector-group label')[1];
    const labelP2 = document.querySelectorAll('.selector-group label')[2];

    if (esAnual) {
      // Ocultar meses, mostrar solo años
      this._el.p1Select.style.display = 'none';
      this._el.p2Select.style.display = 'none';
      if (this._el.year2Group) this._el.year2Group.style.display = 'none';
      if (labelP1) labelP1.textContent = 'Año 1';
      if (labelP2) labelP2.textContent = 'Año 2';
      this._el.yearSelect.disabled = false;
      this._resetMeses();
    } else {
      this._el.p1Select.style.display = '';
      this._el.p2Select.style.display = '';
      if (this._el.year2Group) this._el.year2Group.style.display = '';
      if (labelP1) labelP1.textContent = 'Período 1';
      if (labelP2) labelP2.textContent = 'Período 2';
    }

    this._limpiarResultados();
    this._validarBotón();
  }

  // ─────────────────── EVENTOS DE SELECTORES ───────────────────────────────

  _onYear1Change() {
    const año = this._el.yearSelect.value;
    this._el.p1Select.value = '';
    this._el.p2Select.value = '';
    if (año) {
      this._poblarMeses(this._el.p1Select, año, 'Selecciona el primer período');
      // Si year2 no está seleccionado, poblar p2 con el mismo año por defecto
      const año2 = this._el.year2Select?.value || año;
      this._poblarMeses(this._el.p2Select, año2, 'Selecciona el segundo período');
    } else {
      this._resetMeses();
    }
    this._validarBotón();
  }

  _onYear2Change() {
    const año2 = this._el.year2Select?.value;
    const año1 = this._el.yearSelect.value;
    this._el.p2Select.value = '';
    if (año2 && año1) {
      this._poblarMeses(this._el.p2Select, año2, 'Selecciona el segundo período');
    }
    this._validarBotón();
  }

  _poblarMeses(select, año, placeholder = 'Selecciona un mes') {
    if (!select) return;
    const meses = this._mesesPorAño[String(año)] ?? [];
    select.innerHTML = `<option value="">${placeholder}</option>`;

    if (!meses.length) {
      select.innerHTML = '<option value="">Sin meses disponibles</option>';
      select.disabled = true;
      return;
    }
    for (const mes of meses) {
      const m = String(mes).padStart(2, '0');
      select.insertAdjacentHTML('beforeend',
        `<option value="${m}">${getNombreMes(m)}</option>`);
    }
    select.disabled = false;
  }

  _resetMeses() {
    for (const sel of [this._el.p1Select, this._el.p2Select]) {
      if (!sel) continue;
      sel.innerHTML = '<option value="">Selecciona año primero</option>';
      sel.disabled  = true;
    }
  }

  // ─────────────────── VALIDACIÓN DEL BOTÓN ───────────────────────────────

  _validarBotón() {
    const btn = this._el.compararBtn;
    if (!btn) return;

    if (this._modo === 'anual') {
      // Solo necesita yearSelect (año1) — el "año2" lo ingresa el usuario
      // a través del mismo panel (la comparación anual usa un select independiente)
      const año1 = this._el.yearSelect.value;
      // Para modo anual se usa un select separado inyectado dinámicamente
      const año2Select = document.getElementById('comparativa-año2-anual');
      const año2 = año2Select?.value;

      const ok = año1 && año2 && año1 !== año2
        && this._tiene12Meses(this._mesesPorAño[año1] ?? [])
        && this._tiene12Meses(this._mesesPorAño[año2] ?? []);

      btn.disabled = !ok;
      btn.title    = !ok
        ? (!año1 || !año2 ? 'Selecciona ambos años' :
           año1 === año2  ? 'Selecciona años diferentes' :
           'Ambos años requieren los 12 meses completos')
        : '';
      return;
    }

    // Modo período
    const año1 = this._el.yearSelect.value;
    const año2 = this._el.year2Select?.value || año1;
    const p1   = this._el.p1Select?.value;
    const p2   = this._el.p2Select?.value;
    const iguales = año1 === año2 && p1 === p2;

    btn.disabled = !(año1 && p1 && p2) || iguales;
    btn.title    = iguales ? 'Selecciona períodos diferentes' : '';
  }

  // ─────────────────── EJECUCIÓN ───────────────────────────────────────────

  async _ejecutar() {
    if (this._modo === 'anual') {
      await this._ejecutarAnual();
    } else {
      await this._ejecutarPeriodo();
    }
  }

  async _ejecutarAnual() {
    const año1 = this._el.yearSelect.value;
    const año2Select = document.getElementById('comparativa-año2-anual');
    const año2 = año2Select?.value;
    if (!año1 || !año2) return;

    try {
      this._loader('Comparando años…');
      const url  = `/api/drive-comparativas/comparar-años?year1=${año1}&year2=${año2}`;
      const res  = await fetch(url);
      if (!res.ok) await this._throwHTTPError(res);
      const raw  = await res.json();

      if (raw.status === 'error') throw new Error(raw.error ?? 'Error en comparativa');

      // Normalizar la respuesta de comparar_años → formato canónico interno
      this._datos = this._normalizarRespuestaAnual(raw, año1, año2);
      this._renderizar();

    } catch (err) {
      this._alerta(`❌ ${err.message}`, 'error');
    } finally {
      this._loaderOff();
    }
  }

  async _ejecutarPeriodo() {
    const año1 = this._el.yearSelect.value;
    const año2 = this._el.year2Select?.value || año1;
    const p1   = this._el.p1Select?.value;
    const p2   = this._el.p2Select?.value;
    if (!año1 || !p1 || !p2) return;

    try {
      this._loader('Comparando períodos…');
      // Siempre usa el endpoint que soporta años distintos (Regla ③ del backend)
      const url = `/api/drive-comparativas/comparar-avanzado`
        + `?year1=${año1}&periodo1=${p1}&year2=${año2}&periodo2=${p2}`;
      const res = await fetch(url);
      if (!res.ok) await this._throwHTTPError(res);
      const raw = await res.json();

      if (raw.status === 'error') throw new Error(raw.error ?? 'Error en comparativa');

      this._datos = this._normalizarRespuestaPeriodo(raw, año1, p1, año2, p2);
      this._renderizar();

    } catch (err) {
      this._alerta(`❌ ${err.message}`, 'error');
    } finally {
      this._loaderOff();
    }
  }

  async _throwHTTPError(res) {
    let msg = `Error HTTP ${res.status}`;
    try { msg = (await res.json()).message || msg; } catch { /**/ }
    throw new Error(msg);
  }

  // ─────────────────── NORMALIZACIÓN DE RESPUESTAS ─────────────────────────

  /**
   * Convierte la respuesta de comparar_años (Regla ④) en el formato
   * canónico que usa el render, idéntico al de comparar_periodos_avanzado.
   *
   * Backend devuelve:
   *   { resumen_año1, resumen_año2, diferencias.metricas, por_estado }
   *
   * Canonical interno:
   *   { modo, label1, label2, analisisPlazas, metricasGlobales, analisisPorEstado, metricasPrincipales }
   */
  _normalizarRespuestaAnual(raw, año1, año2) {
    const r1 = raw.resumen_año1 ?? {};
    const r2 = raw.resumen_año2 ?? {};
    const diff = raw.diferencias ?? {};

    // Construir metricasGlobales en el mismo formato que comparar_periodos
    const metricasGlobales = {};
    for (const [m, d] of Object.entries(diff.metricas ?? {})) {
      metricasGlobales[m] = {
        periodo1:         d.año1            ?? 0,
        periodo2:         d.año2            ?? 0,
        incremento:       d.cambio          ?? 0,
        cambio:           d.cambio          ?? 0,
        porcentaje_cambio: d.porcentaje_cambio ?? 0,
        tipo:             'numerica',
      };
    }

    // analisisPorEstado desde por_estado (estructura distinta en modo anual)
    const analisisPorEstado = {};
    for (const [estado, datos] of Object.entries(raw.por_estado ?? {})) {
      const rE1 = datos.resumen_año1 ?? {};
      const rE2 = datos.resumen_año2 ?? {};
      const mE  = {};
      for (const [m, d] of Object.entries(datos.diferencias?.metricas ?? {})) {
        mE[m] = {
          periodo1:         d.año1   ?? 0,
          periodo2:         d.año2   ?? 0,
          cambio:           d.cambio ?? 0,
          porcentaje_cambio: rE1.metricas?.[m]
            ? ((d.cambio ?? 0) / Math.max(rE1.metricas[m], 1) * 100) : 0,
        };
      }
      analisisPorEstado[estado] = {
        total_plazas_periodo1: rE1.total_plazas ?? 0,
        total_plazas_periodo2: rE2.total_plazas ?? 0,
        plazas_operacion_periodo2: rE2.plazas_op ?? 0,
        metricas: mE,
      };
    }

    const cnTot = metricasGlobales.CN_Tot_Acum ?? {};
    return {
      modo:    'anual',
      label1:  `Año ${año1} (Acumulado)`,
      label2:  `Año ${año2} (Acumulado)`,
      analisisPlazas: {
        total_plazas_periodo1: r1.total_plazas ?? 0,
        total_plazas_periodo2: r2.total_plazas ?? 0,
        plazas_nuevas:         0, // no disponible en modo anual
        plazas_eliminadas:     0,
        plazas_operacion_periodo2: r2.plazas_op ?? 0,
      },
      metricasGlobales,
      analisisPorEstado,
      metricasPrincipales: {
        plazas_nuevas:       0,
        plazas_eliminadas:   0,
        incremento_cn_total: cnTot.cambio ?? 0,
        resumen_cambios:     `CN Total ${cnTot.cambio >= 0 ? '+' : ''}${fmt.number(cnTot.cambio ?? 0)}`,
      },
    };
  }

  /**
   * Convierte la respuesta de comparar_periodos_avanzado_con_años_diferentes
   * en el formato canónico interno.
   */
  _normalizarRespuestaPeriodo(raw, año1, p1, año2, p2) {
    const comp = raw.comparacion ?? {};
    const labelMes = (a, p) => `${getNombreMes(p)} ${a}`;
    return {
      modo:    'periodo',
      label1:  labelMes(año1, p1),
      label2:  labelMes(año2, p2),
      analisisPlazas:     comp.analisis_plazas     ?? {},
      metricasGlobales:   comp.metricas_globales   ?? {},
      analisisPorEstado:  comp.analisis_por_estado  ?? {},
      metricasPrincipales: raw.metricas_principales ?? {},
    };
  }

  // ─────────────────── RENDER PRINCIPAL ────────────────────────────────────

  _renderizar() {
    if (!this._datos || !this._el.resultados) return;

    const template = document.getElementById('comparativa-resultados-template');
    if (!template) return;

    const clone = template.content.cloneNode(true);
    this._bindTemplate(clone);

    this._el.resultados.innerHTML = '';
    this._el.resultados.appendChild(clone);
    this._el.resultados.classList.remove('hidden');

    // Renderizado secundario (requiere que el DOM ya esté insertado)
    this._renderTablaEstados();
    this._renderAnalisisAvanzado();
  }

  _bindTemplate(clone) {
    const { label1, label2, analisisPlazas, metricasGlobales, metricasPrincipales } = this._datos;

    // Nombres de períodos
    clone.querySelectorAll('[data-bind="periodo1_nombre"]')
      .forEach(el => { el.textContent = label1; });
    clone.querySelectorAll('[data-bind="periodo2_nombre"]')
      .forEach(el => { el.textContent = label2; });

    // Cards resumen
    const ap     = analisisPlazas;
    const totP1  = ap.total_plazas_periodo1 ?? 0;
    const totP2  = ap.total_plazas_periodo2 ?? 0;
    const diff   = totP2 - totP1;

    this._bind(clone, 'total_periodo1',      fmt.number(totP1));
    this._bind(clone, 'total_periodo2',      fmt.number(totP2));
    this._bind(clone, 'diferencia_total',    fmt.cambio(diff));
    this._bind(clone, 'tendencia_diferencia',fmt.tendenciaTexto(diff));

    // Métricas principales
    const mp = metricasPrincipales;
    this._bind(clone, 'plazas_nuevas',       fmt.number(mp.plazas_nuevas ?? 0));
    this._bind(clone, 'plazas_eliminadas',   fmt.number(mp.plazas_eliminadas ?? 0));
    this._bind(clone, 'incremento_cn_total', fmt.number(mp.incremento_cn_total ?? 0));
    this._bind(clone, 'resumen_cambios',     mp.resumen_cambios ?? '—');

    // Tabla comparativa
    const tbody = clone.getElementById?.('comparativa-tbody')
      ?? clone.querySelector('#comparativa-tbody');
    if (tbody) this._llenarTablaMetricas(tbody, metricasGlobales);
  }

  _bind(root, key, value) {
    root.querySelectorAll(`[data-bind="${key}"]`).forEach(el => {
      el.textContent = value;
    });
  }

  _llenarTablaMetricas(tbody, metricasGlobales) {
    tbody.innerHTML = '';
    const disponibles = METRICAS_ORDEN.filter(m => m in metricasGlobales);

    if (!disponibles.length) {
      tbody.innerHTML = '<tr><td colspan="6" class="sin-datos">Sin datos disponibles</td></tr>';
      return;
    }

    const fragment = document.createDocumentFragment();
    for (const metrica of disponibles) {
      const d    = metricasGlobales[metrica];
      if (d.tipo === 'categorica') continue; // salta categóricas

      const inc  = d.incremento ?? d.cambio ?? 0;
      const pct  = d.porcentaje_cambio ?? 0;
      const cls  = fmt.classCambio(inc);
      const tr   = document.createElement('tr');
      tr.innerHTML = `
        <td><strong>${getNombreMetrica(metrica)}</strong></td>
        <td>${fmt.number(d.periodo1 ?? 0)}</td>
        <td>${fmt.number(d.periodo2 ?? 0)}</td>
        <td class="${cls}">${fmt.cambio(inc)}</td>
        <td class="${cls}">${fmt.tendencia(inc)}</td>
        <td class="${cls}">${fmt.pct(pct)}</td>
      `;
      fragment.appendChild(tr);
    }
    tbody.appendChild(fragment);
  }

  // ─────────────────── TABLA DE ESTADOS ────────────────────────────────────

 _renderTablaEstados() {
    const tbody = document.getElementById('estados-comparativa-tbody');
    if (!tbody) return;

    const ape = this._datos?.analisisPorEstado ?? {};
    tbody.innerHTML = '';

    if (!Object.keys(ape).length) {
      tbody.innerHTML = `<tr><td colspan="16" class="sin-datos">
        Sin datos de estados para esta comparativa
      </td></tr>`;
      return;
    }

    // Acumuladores para la fila de totales
    const totales = {
      p1: 0, p2: 0,
      iniP1: 0, iniP2: 0,
      priP1: 0, priP2: 0,
      secP1: 0, secP2: 0,
      totP1: 0, totP2: 0,
    };

    // Crear array de estados con sus datos para poder ordenar
    const estadosArray = [];
    for (const estado of Object.keys(ape)) {
      const datos = ape[estado];
      
      // Calcular métricas para ordenamiento
      const p1 = datos.total_plazas_periodo1 ?? 0;
      const p2 = datos.total_plazas_periodo2 ?? 0;
      const mTot = datos.metricas?.['CN_Tot_Acum'] ?? {};
      const mIni = datos.metricas?.['CN_Inicial_Acum'] ?? {};
      const mPri = datos.metricas?.['CN_Prim_Acum'] ?? {};
      const mSec = datos.metricas?.['CN_Sec_Acum'] ?? {};
      
      estadosArray.push({
        nombre: estado,
        datos: datos,
        // Valores numéricos para ordenamiento
        p1: p1,
        p2: p2,
        deltaPlazas: p2 - p1,
        cnP1: mTot.periodo1 ?? 0,
        cnP2: mTot.periodo2 ?? 0,
        iniP1: mIni.periodo1 ?? 0,
        iniP2: mIni.periodo2 ?? 0,
        priP1: mPri.periodo1 ?? 0,
        priP2: mPri.periodo2 ?? 0,
        secP1: mSec.periodo1 ?? 0,
        secP2: mSec.periodo2 ?? 0,
        // Porcentajes de cambio
        pctCN: mTot.porcentaje_cambio ?? 0,
        pctIni: mIni.porcentaje_cambio ?? 0,
        pctPri: mPri.porcentaje_cambio ?? 0,
        pctSec: mSec.porcentaje_cambio ?? 0,
      });
    }

    // Aplicar ordenamiento si hay un criterio seleccionado
    const ordenSelect = document.getElementById('orden-estados');
    if (ordenSelect && ordenSelect.value !== 'default') {
      const [criterio, direccion] = ordenSelect.value.split('-');
      const multiplicador = direccion === 'asc' ? 1 : -1;
      
      estadosArray.sort((a, b) => {
        let valorA, valorB;
        
        switch(criterio) {
          case 'nombre':
            valorA = a.nombre;
            valorB = b.nombre;
            return multiplicador * (valorA.localeCompare(valorB, 'es', { sensitivity: 'base' }));
            
          case 'plazas':
            valorA = a.p2;
            valorB = b.p2;
            break;
            
          case 'cn':
            valorA = a.cnP2;
            valorB = b.cnP2;
            break;
            
          case 'pct':
            valorA = a.pctCN;
            valorB = b.pctCN;
            break;
            
          case 'inicial':
            valorA = a.pctIni;
            valorB = b.pctIni;
            break;
            
          case 'primaria':
            valorA = a.pctPri;
            valorB = b.pctPri;
            break;
            
          case 'secundaria':
            valorA = a.pctSec;
            valorB = b.pctSec;
            break;
            
          case 'delta':
            valorA = a.deltaPlazas;
            valorB = b.deltaPlazas;
            break;
            
          case 'cnp1':
            valorA = a.cnP1;
            valorB = b.cnP1;
            break;
            
          case 'cnabs':
            // Ordenar por valor absoluto de CN en período 2
            valorA = Math.abs(a.cnP2);
            valorB = Math.abs(b.cnP2);
            break;
            
          default:
            valorA = a.nombre;
            valorB = b.nombre;
            return multiplicador * (valorA.localeCompare(valorB, 'es', { sensitivity: 'base' }));
        }
        
        // Detectar tipo de dato para ordenamiento numérico
        if (typeof valorA === 'number' && typeof valorB === 'number') {
          // Ordenamiento numérico
          if (valorA < valorB) return -multiplicador;
          if (valorA > valorB) return multiplicador;
          return 0;
        } else {
          // Ordenamiento de texto (fallback)
          return multiplicador * (String(valorA).localeCompare(String(valorB), 'es', { sensitivity: 'base' }));
        }
      });
    } else {
      // Ordenamiento por defecto (alfabético)
      estadosArray.sort((a, b) => a.nombre.localeCompare(b.nombre, 'es', { sensitivity: 'base' }));
    }

    const fragment = document.createDocumentFragment();
    for (const item of estadosArray) {
      const tr = this._crearFilaEstado(item.nombre, item.datos);
      if (tr) fragment.appendChild(tr);

      // Acumular totales usando los valores pre-calculados para mayor precisión
      totales.p1    += item.p1;
      totales.p2    += item.p2;
      totales.iniP1 += item.iniP1;
      totales.iniP2 += item.iniP2;
      totales.priP1 += item.priP1;
      totales.priP2 += item.priP2;
      totales.secP1 += item.secP1;
      totales.secP2 += item.secP2;
      totales.totP1 += item.cnP1;
      totales.totP2 += item.cnP2;
    }
    tbody.appendChild(fragment);

    // Renderizar footer de totales
    this._renderFooterTotales(totales);

    const totalEl = document.getElementById('estados-total');
    if (totalEl) {
      const totalVisibles = document.querySelectorAll('.estado-row:not([style*="display: none"])').length;
      totalEl.textContent = `${totalVisibles} de ${estadosArray.length} estados`;
    }

    this._attachBusquedaEstados();
    this._attachOrdenamientoEstados();
    this._attachModalEstados();
  }

  _renderFooterTotales(totales) {
    // Calcular deltas de totales con manejo de NaN
    const deltaPlazas = totales.p2 - totales.p1;
    const iniDelta    = totales.iniP2 - totales.iniP1;
    const priDelta    = totales.priP2 - totales.priP1;
    const secDelta    = totales.secP2 - totales.secP1;
    const totDelta    = totales.totP2 - totales.totP1;
    
    // Calcular porcentajes con protección contra división por cero
    const pctIni = totales.iniP1 !== 0 ? (iniDelta / totales.iniP1) * 100 : 0;
    const pctPri = totales.priP1 !== 0 ? (priDelta / totales.priP1) * 100 : 0;
    const pctSec = totales.secP1 !== 0 ? (secDelta / totales.secP1) * 100 : 0;
    const pctTot = totales.totP1 !== 0 ? (totDelta / totales.totP1) * 100 : 0;

    // Calcular total de registros para el tooltip
    const totalRegistros = document.querySelectorAll('.estado-row').length;

    const tfoot = document.getElementById('estados-comparativa-tfoot');
    if (tfoot) {
      tfoot.innerHTML = `
        <tr class="totales-row" title="Total nacional basado en ${totalRegistros} estados">
          <td class="totales-label">
            <span class="totales-icon">📊</span>
            <span class="totales-text">TOTAL NACIONAL</span>
            <span class="totales-badge">${totalRegistros} estados</span>
          </td>
          <td><span class="totales-valor">${fmt.number(totales.p1)}</span></td>
          <td><span class="totales-valor">${fmt.number(totales.p2)}</span></td>
          <td class="${fmt.classCambio(deltaPlazas)}">
            <span class="totales-delta">${fmt.cambio(deltaPlazas)}</span>
            <span class="totales-abs">(${fmt.number(Math.abs(deltaPlazas))})</span>
          </td>
          <td>${fmt.number(totales.iniP1)}</td>
          <td>${fmt.number(totales.iniP2)}</td>
          <td class="${fmt.classCambio(pctIni)}">
            <span class="totales-pct">${fmt.pct(pctIni)}</span>
            <span class="totales-abs">(${fmt.number(Math.abs(iniDelta))})</span>
          </td>
          <td>${fmt.number(totales.priP1)}</td>
          <td>${fmt.number(totales.priP2)}</td>
          <td class="${fmt.classCambio(pctPri)}">
            <span class="totales-pct">${fmt.pct(pctPri)}</span>
            <span class="totales-abs">(${fmt.number(Math.abs(priDelta))})</span>
          </td>
          <td>${fmt.number(totales.secP1)}</td>
          <td>${fmt.number(totales.secP2)}</td>
          <td class="${fmt.classCambio(pctSec)}">
            <span class="totales-pct">${fmt.pct(pctSec)}</span>
            <span class="totales-abs">(${fmt.number(Math.abs(secDelta))})</span>
          </td>
          <td>${fmt.number(totales.totP1)}</td>
          <td>${fmt.number(totales.totP2)}</td>
          <td class="${fmt.classCambio(pctTot)}">
            <span class="totales-pct">${fmt.pct(pctTot)}</span>
            <span class="totales-abs">(${fmt.number(Math.abs(totDelta))})</span>
          </td>
        </tr>`;
    }
  }

  _crearFilaEstado(estado, datos) {
    const getM = (key) => {
      const m = datos.metricas?.[key] ?? {};
      return { p1: m.periodo1 ?? 0, p2: m.periodo2 ?? 0, porc: m.porcentaje_cambio ?? 0 };
    };

    const p1    = datos.total_plazas_periodo1 ?? 0;
    const p2    = datos.total_plazas_periodo2 ?? 0;
    const delta = p2 - p1;
    const mIni  = getM('CN_Inicial_Acum');
    const mPri  = getM('CN_Prim_Acum');
    const mSec  = getM('CN_Sec_Acum');
    const mTot  = getM('CN_Tot_Acum');

    const tr = document.createElement('tr');
    tr.className    = 'estado-row';
    tr.dataset.estado = estado;
    
    // Agregar atributos de datos para ordenamiento dinámico (valores sin formato)
    tr.dataset.nombre = estado;
    tr.dataset.plazas = p2;
    tr.dataset.plazasP1 = p1;
    tr.dataset.cn = mTot.p2;
    tr.dataset.cnP1 = mTot.p1;
    tr.dataset.pct = mTot.porc;
    tr.dataset.inicial = mIni.porc;
    tr.dataset.primaria = mPri.porc;
    tr.dataset.secundaria = mSec.porc;
    tr.dataset.delta = delta;
    tr.dataset.deltaAbs = Math.abs(delta);
    
    // Crear contenido con tooltips informativos
    tr.innerHTML = `
      <td><strong title="Código: ${estado}">${estado}</strong></td>
      <td title="Período 1: ${fmt.number(p1)} plazas">${fmt.number(p1)}</td>
      <td title="Período 2: ${fmt.number(p2)} plazas">${fmt.number(p2)}</td>
      <td class="${fmt.classCambio(delta)}" title="Variación absoluta: ${fmt.number(delta)} plazas">
        ${fmt.cambio(delta)}
      </td>
      <td title="CN Inicial P1: ${fmt.number(mIni.p1)}">${fmt.number(mIni.p1)}</td>
      <td title="CN Inicial P2: ${fmt.number(mIni.p2)}">${fmt.number(mIni.p2)}</td>
      <td class="${fmt.classCambio(mIni.porc)}" title="Variación % CN Inicial">
        ${fmt.pct(mIni.porc)}
      </td>
      <td title="CN Primaria P1: ${fmt.number(mPri.p1)}">${fmt.number(mPri.p1)}</td>
      <td title="CN Primaria P2: ${fmt.number(mPri.p2)}">${fmt.number(mPri.p2)}</td>
      <td class="${fmt.classCambio(mPri.porc)}" title="Variación % CN Primaria">
        ${fmt.pct(mPri.porc)}
      </td>
      <td title="CN Secundaria P1: ${fmt.number(mSec.p1)}">${fmt.number(mSec.p1)}</td>
      <td title="CN Secundaria P2: ${fmt.number(mSec.p2)}">${fmt.number(mSec.p2)}</td>
      <td class="${fmt.classCambio(mSec.porc)}" title="Variación % CN Secundaria">
        ${fmt.pct(mSec.porc)}
      </td>
      <td title="CN Total P1: ${fmt.number(mTot.p1)}">${fmt.number(mTot.p1)}</td>
      <td title="CN Total P2: ${fmt.number(mTot.p2)}">${fmt.number(mTot.p2)}</td>
      <td class="${fmt.classCambio(mTot.porc)}" title="Variación % CN Total">
        ${fmt.pct(mTot.porc)}
      </td>
    `;
    
    tr.addEventListener('click', () => this._abrirModalEstado(estado, datos));
    return tr;
  }

  _attachBusquedaEstados() {
    const input = document.getElementById('estados-search');
    if (!input) return;
    
    // Remover listener previo clonando el nodo
    const nuevo = input.cloneNode(true);
    input.parentNode.replaceChild(nuevo, input);
    
    nuevo.addEventListener('input', (e) => {
      const q = e.target.value.toLowerCase().trim();
      const rows = document.querySelectorAll('.estado-row');
      let visibles = 0;
      
      rows.forEach(row => {
        const estado = row.dataset.estado.toLowerCase();
        const matches = q === '' || estado.includes(q);
        row.style.display = matches ? '' : 'none';
        if (matches) visibles++;
      });
      
      // Actualizar contador de estados visibles
      const totalEl = document.getElementById('estados-total');
      if (totalEl) {
        const totalOriginal = rows.length;
        totalEl.textContent = q === '' 
          ? `${totalOriginal} estados` 
          : `${visibles} de ${totalOriginal} estados`;
      }
      
      // Actualizar tooltip del footer
      const totalesRow = document.querySelector('.totales-row');
      if (totalesRow) {
        totalesRow.title = `Total nacional basado en ${visibles} estados visibles`;
      }
    });
  }

  _attachOrdenamientoEstados() {
    const ordenSelect = document.getElementById('orden-estados');
    if (!ordenSelect) return;
    
    // Remover listener previo
    const nuevo = ordenSelect.cloneNode(true);
    ordenSelect.parentNode.replaceChild(nuevo, ordenSelect);
    
    nuevo.addEventListener('change', (e) => {
      const valor = e.target.value;
      
      // Mostrar indicador de carga
      const tbody = document.getElementById('estados-comparativa-tbody');
      tbody.style.opacity = '0.5';
      
      // Re-renderizar con el nuevo orden (usar setTimeout para permitir actualización de UI)
      setTimeout(() => {
        this._renderTablaEstados();
        tbody.style.opacity = '1';
      }, 50);
    });
  }

  _attachModalEstados() {
    // El attach del modal se hace en _attachListeners (una sola vez)
  }

  // ─────────────────── MODAL DE ESTADO ─────────────────────────────────────

  _abrirModalEstado(estado, datos) {
    const modal    = document.getElementById('modal-detalle-estado');
    const titulo   = document.getElementById('modal-estado-titulo');
    const contenido = document.getElementById('modal-estado-contenido');
    if (!modal) return;

    titulo.textContent   = `📊 Comparativa Detallada — ${estado}`;
    contenido.innerHTML  = this._htmlModalEstado(datos);
    modal.classList.remove('hidden');
  }

  _htmlModalEstado(datos) {
    const p1    = datos.total_plazas_periodo1 ?? 0;
    const p2    = datos.total_plazas_periodo2 ?? 0;
    const delta = p2 - p1;
    const pct   = p1 ? (delta / p1) * 100 : 0;

    const LISTA = [
    { key: 'CN_Inicial_Acum', nombre: 'CN Inicial',    icono: '👶' },
    { key: 'CN_Prim_Acum',    nombre: 'CN Primaria',   icono: '🎒' },
    { key: 'CN_Sec_Acum',     nombre: 'CN Secundaria', icono: '📚' },
    { key: 'CN_Tot_Acum',     nombre: 'CN Total',      icono: '📊' },
    ];

    const metricas = datos.metricas ?? {};
    const metricasHTML = LISTA.map(({ key, nombre, icono }) => {
      const d = metricas[key] ?? {};
      if (d.periodo1 === undefined && d.periodo2 === undefined) return '';
      const c   = d.cambio ?? 0;
      const cls = fmt.classCambio(c);
      return `
        <div class="metrica-item">
          <div class="metrica-header">
            <span class="metrica-icon">${icono}</span>
            <span class="metrica-name">${nombre}</span>
          </div>
          <div class="metrica-values">
            <div class="value-periodo1">${fmt.number(d.periodo1 ?? 0)}</div>
            <div class="value-periodo2">${fmt.number(d.periodo2 ?? 0)}</div>
            <div class="value-cambio ${cls}">${fmt.cambio(c)}</div>
            <div class="value-porcentaje ${cls}">${fmt.pct(d.porcentaje_cambio ?? 0)}</div>
          </div>
        </div>`;
    }).join('');

    return `
      <div class="estado-resumen">
        <h4>Resumen de Plazas</h4>
        <div class="plazas-comparison">
          <div class="plaza-value"><span class="label">Período 1:</span>
            <span class="value">${fmt.number(p1)}</span></div>
          <div class="plaza-value"><span class="label">Período 2:</span>
            <span class="value">${fmt.number(p2)}</span></div>
          <div class="plaza-value ${fmt.classCambio(delta)}">
            <span class="label">Cambio:</span>
            <span class="value">${fmt.cambio(delta)}</span></div>
          <div class="plaza-value ${fmt.classCambio(pct)}">
            <span class="label">% Cambio:</span>
            <span class="value">${fmt.pct(pct)}</span></div>
        </div>
      </div>
      <div class="estado-metricas">
        <h4>Métricas Detalladas</h4>
        ${metricasHTML || '<p>Sin métricas disponibles.</p>'}
      </div>`;
  }

  // ─────────────────── ANÁLISIS AVANZADO ──────────────────────────────────

  _renderAnalisisAvanzado() {
    const { analisisPorEstado, metricasGlobales, analisisPlazas } = this._datos;
    const mAvanz = this._calcularMetricasAvanzadas(metricasGlobales, analisisPlazas, analisisPorEstado);

    this._renderMetricasAvanzadas(mAvanz, metricasGlobales);
    this._renderRanking(analisisPorEstado);
    this._renderTendencias(analisisPorEstado);
    this._renderResumenEjecutivo(mAvanz, analisisPorEstado);
  }

  _calcularMetricasAvanzadas(mg, ap, ape) {
    const cnTot  = mg.CN_Tot_Acum ?? {};
    const plazasOp = ap.plazas_operacion_periodo2 ?? ap.total_plazas_periodo2 ?? 1;
    const tasaCrecimiento  = cnTot.porcentaje_cambio ?? 0;
    const eficiencia       = plazasOp > 0 ? (cnTot.periodo2 ?? 0) / plazasOp : 0;
    const estadoMasDinamico = this._estadoMasDinamico(ape);
    const distP1 = this._distribucionCN(mg, 'periodo1');
    const distP2 = this._distribucionCN(mg, 'periodo2');
    return { tasaCrecimiento, eficiencia, estadoMasDinamico, distP1, distP2 };
  }

  _distribucionCN(mg, campo) {
    const ini = (mg.CN_Inicial_Acum ?? {})[campo] ?? 0;
    const pri = (mg.CN_Prim_Acum    ?? {})[campo] ?? 0;
    const sec = (mg.CN_Sec_Acum     ?? {})[campo] ?? 0;
    const tot = ini + pri + sec || 1;
    return { inicial: ini / tot * 100, primaria: pri / tot * 100, secundaria: sec / tot * 100 };
  }

  _estadoMasDinamico(ape) {
    let max = { nombre: 'N/A', cambio: 0 };
    for (const [estado, datos] of Object.entries(ape)) {
      const c = Math.abs(datos.metricas?.CN_Tot_Acum?.cambio ?? 0);
      if (c > max.cambio) max = { nombre: estado, cambio: c };
    }
    return max;
  }

  _renderMetricasAvanzadas(m, mg) {
    const set = (bind, val) => {
      document.querySelectorAll(`[data-bind="${bind}"]`)
        .forEach(el => { el.textContent = val; });
    };
    set('tasa_crecimiento',   fmt.pct(m.tasaCrecimiento));
    set('detalle_crecimiento', m.tasaCrecimiento > 0 ? 'Crecimiento positivo' :
                               m.tasaCrecimiento < 0 ? 'Crecimiento negativo' : 'Sin cambios');
    set('eficiencia_operativa',  Math.round(m.eficiencia).toLocaleString());
    set('detalle_eficiencia',    `CN Total / Plazas en Operación = ${Math.round(m.eficiencia)}`);
    set('estado_mas_dinamico',   m.estadoMasDinamico.nombre);
    set('detalle_dinamico',      `Cambio absoluto: ${fmt.number(m.estadoMasDinamico.cambio)}`);

    // Distribución comparativa (barras)
    const cont = document.querySelector('[data-bind="distribucion_cn_comparativa"]');
    if (cont) cont.innerHTML = this._htmlDistribucion(m.distP1, m.distP2);
  }

  _htmlDistribucion(d1, d2) {
    // Recuperar totales reales de metricasGlobales para mostrar números absolutos
    const mg  = this._datos?.metricasGlobales ?? {};
    const ini1 = (mg.CN_Inicial_Acum ?? {}).periodo1 ?? 0;
    const pri1 = (mg.CN_Prim_Acum    ?? {}).periodo1 ?? 0;
    const sec1 = (mg.CN_Sec_Acum     ?? {}).periodo1 ?? 0;
    const tot1 = ini1 + pri1 + sec1 || 1;

    const ini2 = (mg.CN_Inicial_Acum ?? {}).periodo2 ?? 0;
    const pri2 = (mg.CN_Prim_Acum    ?? {}).periodo2 ?? 0;
    const sec2 = (mg.CN_Sec_Acum     ?? {}).periodo2 ?? 0;
    const tot2 = ini2 + pri2 + sec2 || 1;

    const fmtN = (n) => new Intl.NumberFormat('es-MX').format(Math.round(n || 0));

    // Calcular cambios entre períodos para mostrar delta
    const deltaIni = ini2 - ini1;
    const deltaPri = pri2 - pri1;
    const deltaSec = sec2 - sec1;
    const deltaTot = tot2 - tot1;

    const clsDelta = (v) => v > 0 ? 'dist-delta positivo' : v < 0 ? 'dist-delta negativo' : 'dist-delta neutral';
    const fmtDelta = (v) => v === 0 ? '—' : `${v > 0 ? '+' : ''}${fmtN(v)}`;

    const barra = (d, lbl, totReal, vals) => {
      // Segmento solo si > 0.5% para evitar sliver invisibles
      const segs = [
        { cls: 'inicial',    pct: d.inicial,    label: 'Inicial',    val: vals[0] },
        { cls: 'primaria',   pct: d.primaria,   label: 'Primaria',   val: vals[1] },
        { cls: 'secundaria', pct: d.secundaria, label: 'Secundaria', val: vals[2] },
      ];
      const segHTML = segs.map(s => `
        <div class="distribucion-segmento ${s.cls}" style="width:${s.pct.toFixed(2)}%"
             title="${s.label}: ${fmtN(s.val)} (${s.pct.toFixed(1)}%)">
          ${s.pct > 8 ? `<span>${s.pct.toFixed(1)}%</span>` : ''}
        </div>`).join('');

      return `
        <div class="distribucion-card">
          <div class="dist-card-header">
            <span class="dist-label">${lbl}</span>
            <span class="dist-total">Total: <strong>${fmtN(totReal)}</strong></span>
          </div>
          <div class="distribucion-bar">${segHTML}</div>
          <div class="dist-valores">
            <div class="dist-valor-item">
              <span class="dist-dot dist-dot-inicial"></span>
              <span class="dist-nombre">Inicial</span>
              <span class="dist-num">${fmtN(vals[0])}</span>
              <span class="dist-pct">${d.inicial.toFixed(1)}%</span>
            </div>
            <div class="dist-valor-item">
              <span class="dist-dot dist-dot-primaria"></span>
              <span class="dist-nombre">Primaria</span>
              <span class="dist-num">${fmtN(vals[1])}</span>
              <span class="dist-pct">${d.primaria.toFixed(1)}%</span>
            </div>
            <div class="dist-valor-item">
              <span class="dist-dot dist-dot-secundaria"></span>
              <span class="dist-nombre">Secundaria</span>
              <span class="dist-num">${fmtN(vals[2])}</span>
              <span class="dist-pct">${d.secundaria.toFixed(1)}%</span>
            </div>
          </div>
        </div>`;
    };

    // Flecha de cambio entre períodos
    const resumenCambio = `
      <div class="dist-resumen-cambio">
        <div class="dist-cambio-titulo">Variación entre períodos</div>
        <div class="dist-cambio-grid">
          <div class="dist-cambio-item">
            <span class="dist-dot dist-dot-inicial"></span>
            <span>Inicial</span>
            <span class="${clsDelta(deltaIni)}">${fmtDelta(deltaIni)}</span>
          </div>
          <div class="dist-cambio-item">
            <span class="dist-dot dist-dot-primaria"></span>
            <span>Primaria</span>
            <span class="${clsDelta(deltaPri)}">${fmtDelta(deltaPri)}</span>
          </div>
          <div class="dist-cambio-item">
            <span class="dist-dot dist-dot-secundaria"></span>
            <span>Secundaria</span>
            <span class="${clsDelta(deltaSec)}">${fmtDelta(deltaSec)}</span>
          </div>
          <div class="dist-cambio-item dist-cambio-total">
            <span>📊</span>
            <span><strong>Total</strong></span>
            <span class="${clsDelta(deltaTot)}">${fmtDelta(deltaTot)}</span>
          </div>
        </div>
      </div>`;

    return `
      <div class="grid-distribucion">
        ${barra(d1, this._datos?.label1 ?? 'Período 1', tot1, [ini1, pri1, sec1])}
        ${barra(d2, this._datos?.label2 ?? 'Período 2', tot2, [ini2, pri2, sec2])}
      </div>
      ${resumenCambio}`;
  }

  _renderRanking(ape) {
    const tbody   = document.getElementById('ranking-estados-tbody');
    const totalEl = document.getElementById('ranking-total');
    if (!tbody) return;

    tbody.innerHTML = '';
    const estados = this._puntajesEstados(ape);
    estados.sort((a, b) => b.pctCrecimiento - a.pctCrecimiento);

    if (!estados.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="sin-datos">Sin datos de ranking</td></tr>';
      return;
    }

    estados.forEach(({ estado, puntaje, cambioCN, pctCrecimiento, plazasOp, eficiencia }, i) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${i + 1}</td>
        <td><strong>${estado}</strong></td>
        <td><span class="puntaje-badge ${this._classPuntaje(puntaje)}">${puntaje.toFixed(1)}</span></td>
        <td class="${fmt.classCambio(cambioCN)}">${fmt.cambio(cambioCN)}</td>
        <td class="${fmt.classCambio(pctCrecimiento)}">${fmt.pct(pctCrecimiento)}</td>
        <td>${fmt.number(plazasOp)}</td>
        <td>${Math.round(eficiencia).toLocaleString()}</td>
      `;
      tbody.appendChild(tr);
    });

    if (totalEl) totalEl.textContent = `${estados.length} estados evaluados`;
  }

  _puntajesEstados(ape) {
    return Object.entries(ape).map(([estado, datos]) => {
      const cn        = datos.metricas?.CN_Tot_Acum ?? {};
      const cambioCN      = cn.cambio ?? 0;
      const pctCrecimiento = cn.porcentaje_cambio ?? 0;
      const plazasOp  = datos.plazas_operacion_periodo2 ?? datos.total_plazas_periodo2 ?? 1;
      const cnP2      = cn.periodo2 ?? 0;
      const eficiencia = plazasOp > 0 ? cnP2 / plazasOp : 0;

      const puntaje =
        Math.min(pctCrecimiento / 100, 1) * 4 +
        Math.min(cambioCN       / 1000, 1) * 3 +
        Math.min(eficiencia     / 10,   1) * 3;

      return { estado, puntaje: +puntaje.toFixed(1), cambioCN, pctCrecimiento, plazasOp, eficiencia };
    });
  }

  _classPuntaje(p) {
    if (p >= 8) return 'puntaje-excelente';
    if (p >= 6) return 'puntaje-bueno';
    if (p >= 4) return 'puntaje-regular';
    return 'puntaje-bajo';
  }

 _renderTendencias(ape) {
    const mg = this._datos?.metricasGlobales ?? {};
    const map = {
      inicial:    'CN_Inicial_Acum',
      primaria:   'CN_Prim_Acum',
      secundaria: 'CN_Sec_Acum',
    };

    const cats = {};
    for (const [cat, key] of Object.entries(map)) {
      const vals = [];
      for (const [estado, datos] of Object.entries(ape)) {
        const m = datos.metricas?.[key] ?? {};
        if (m.porcentaje_cambio !== undefined) {
          vals.push({ estado, crecimiento: m.porcentaje_cambio, cambio: m.cambio ?? 0 });
        }
      }
      vals.sort((a, b) => b.crecimiento - a.crecimiento);
      const tasaTotal = mg[key]?.porcentaje_cambio ?? 0;
      cats[cat] = { tasaTotal, top: vals.slice(0, 3) };
    }

    const set = (bind, val) => document.querySelectorAll(`[data-bind="${bind}"]`)
      .forEach(el => { el.textContent = val; });

    const MEDALLAS    = ['🥇', '🥈', '🥉'];
    const PODIO_CLASS = ['podio-oro', 'podio-plata', 'podio-bronce'];

    for (const [cat, { tasaTotal, top }] of Object.entries(cats)) {
      // ── Valor con clase de color ─────────────────────────────────────────
      const valorEl = document.querySelectorAll(`[data-bind="tendencia_${cat}_valor"]`);
      valorEl.forEach(el => {
        el.textContent = fmt.pct(tasaTotal);
        // Limpiar clases anteriores y añadir la correcta
        el.classList.remove('positivo', 'negativo', 'neutro');
        if (tasaTotal > 0)      el.classList.add('positivo');
        else if (tasaTotal < 0) el.classList.add('negativo');
        else                    el.classList.add('neutro');
      });

      // ── Descripción ──────────────────────────────────────────────────────
      set(`tendencia_${cat}_desc`,
        tasaTotal > 0 ? 'Crecimiento' : tasaTotal < 0 ? 'Decrecimiento' : 'Estable');

      // ── Podio top3 ───────────────────────────────────────────────────────
      const podioEl = document.querySelector(`[data-bind="tendencia_${cat}_top3"]`);
      if (podioEl) {
        if (!top.length) {
          podioEl.innerHTML = '<span class="sin-top3">Sin datos suficientes</span>';
        } else {
          podioEl.innerHTML = top.map((e, i) => `
            <div class="podio-item ${PODIO_CLASS[i]}">
              <span class="podio-medalla">${MEDALLAS[i]}</span>
              <span class="podio-estado" title="${e.estado}">${e.estado}</span>
              <span class="podio-pct ${e.crecimiento >= 0 ? 'cambio-positivo' : 'cambio-negativo'}">${fmt.pct(e.crecimiento)}</span>
              <span class="podio-abs">${fmt.cambio(e.cambio)}</span>
            </div>`).join('');
        }
      }

      // ── Fallback texto plano (legacy) ────────────────────────────────────
      set(`tendencia_${cat}_detalle`,
        top.length
          ? `Mayores crecimientos: ${top.map(e => `${e.estado} (${fmt.pct(e.crecimiento)})`).join(', ')}`
          : 'Sin datos suficientes');
    }
  }

  _renderResumenEjecutivo(m, ape) {
    const stats = this._statsGenerales(ape);
    const set   = (bind, val) => document.querySelectorAll(`[data-bind="${bind}"]`)
      .forEach(el => { el.textContent = val; });

    const { label1, label2 } = this._datos;
    set('resumen_periodo', `${label1} vs ${label2}`);

    const t = m.tasaCrecimiento;
    set('resumen_tendencia',
      t > 5  ? '📈 Fuertemente Positiva' :
      t > 0  ? '📈 Positiva' :
      t < -5 ? '📉 Fuertemente Negativa' :
      t < 0  ? '📉 Negativa' : '➡️ Estable');

    set('resumen_estados_crecimiento',  stats.crecimiento);
    set('resumen_estados_decrecimiento',stats.decrecimiento);

    const pctEstados = stats.total ? (stats.crecimiento / stats.total) * 100 : 0;
    set('resumen_recomendacion',
      t > 10 && pctEstados > 70 ? 'Excelente desempeño general. Mantener estrategias actuales.' :
      t > 5  && pctEstados > 50 ? 'Buen desempeño. Considerar expansión en estados de mayor crecimiento.' :
      t > 0                     ? 'Crecimiento moderado. Revisar estrategia en estados con decrecimiento.' :
      t === 0                   ? 'Situación estable. Evaluar oportunidades de mejora.' :
                                  'Desempeño negativo. Se requiere análisis detallado y plan de acción.');
  }

  _statsGenerales(ape) {
    let crecimiento = 0, decrecimiento = 0, estables = 0;
    for (const datos of Object.values(ape)) {
      const p = datos.metricas?.CN_Tot_Acum?.porcentaje_cambio ?? 0;
      if (p > 1)  crecimiento++;
      else if (p < -1) decrecimiento++;
      else         estables++;
    }
    return { crecimiento, decrecimiento, estables, total: Object.keys(ape).length };
  }

  // ─────────────────── HELPERS GENERALES ───────────────────────────────────

  _tiene12Meses(meses) {
    if (!meses || meses.length < 12) return false;
    const nums = new Set(meses.map(m => parseInt(m)));
    for (let i = 1; i <= 12; i++) if (!nums.has(i)) return false;
    return true;
  }

  _loader(msg = 'Cargando…') {
    const el = document.getElementById('global-loader');
    if (!el) return;
    const t = el.querySelector('.loader-text');
    if (t) t.textContent = msg;
    el.classList.remove('hidden');
  }

  _loaderOff() {
    document.getElementById('global-loader')?.classList.add('hidden');
  }

  _alerta(msg, tipo = 'info') {
    const cont = document.getElementById('alert-container');
    if (!cont) return;
    const cls = { error: 'alert error', success: 'alert success',
                  warning: 'alert warning', info: 'alert info' }[tipo] ?? 'alert info';
    cont.innerHTML = `<div class="${cls}">${msg}</div>`;
    setTimeout(() => { cont.innerHTML = ''; }, 5000);
  }

  _limpiarResultados() {
    if (this._el.resultados) {
      this._el.resultados.innerHTML = '';
      this._el.resultados.classList.add('hidden');
    }
    this._datos = null;
  }

  // API pública para reset externo
  reiniciarFormulario() {
    this._el.yearSelect.selectedIndex = 0;
    this._resetMeses();
    if (this._el.compararBtn) this._el.compararBtn.disabled = true;
    const modoSel = document.getElementById('modo-comparacion');
    if (modoSel) { modoSel.value = 'periodo'; this._modo = 'periodo'; this._onModoChange(); }
    this._limpiarResultados();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// BOOTSTRAP
// ─────────────────────────────────────────────────────────────────────────────

const sistemaComparativas = new SistemaComparativas();

document.addEventListener('DOMContentLoaded', () => sistemaComparativas.init());

if (typeof module !== 'undefined' && module.exports) {
  module.exports = SistemaComparativas;
}