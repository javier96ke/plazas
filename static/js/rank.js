document.addEventListener('DOMContentLoaded', () => {

    const selectEstado     = document.getElementById('top-state-select');
    const winnersContainer = document.getElementById('top-winners-container');
    const winnersGrid      = winnersContainer.querySelector('.winners-grid');

    const tableHead = document.getElementById('top-plazas-thead');
    const tableBody = document.getElementById('top-plazas-tbody');
    const tableFoot = document.getElementById('top-plazas-tfoot');

    const muniHead  = document.getElementById('municipios-thead');
    const muniBody  = document.getElementById('municipios-tbody');
    const muniFoot  = document.getElementById('municipios-tfoot');

    const metricsConfig = {
        'Aten_Inicial':       'Atención Inicial',
        'Aten_Prim':          'Atención Primaria',
        'Aten_Sec':           'Atención Secundaria',
        'Aten_Total':         'Atención Total',
        'Exámenes aplicados': 'Exámenes Aplicados',
        'CN_Inicial_Acum':    'CN Inicial',
        'CN_Prim_Acum':       'CN Primaria',
        'CN_Sec_Acum':        'CN Secundaria',
        'CN_Tot_Acum':        'CN Total',
        'Cert_Emitidos':      'Certificados Emitidos'
    };
    const metricKeys = Object.keys(metricsConfig);

    // ── Clic en enlace plaza ─────────────────────────────────────────────────
    if (tableBody) {
        tableBody.addEventListener('click', (e) => {
            const link = e.target.closest('.enlace-plaza');
            if (link) {
                e.preventDefault();
                const clave = link.getAttribute('data-clave');
                if (clave && window.buscarYMostrarClave) {
                    window.buscarYMostrarClave(clave);
                }
            }
        });
    }

    // ── Init estados ─────────────────────────────────────────────────────────
    const initTopPlazas = async () => {
        try {
            const res    = await fetch('/api/estados');
            const estados = await res.json();
            selectEstado.innerHTML = '<option value="">-- Selecciona un Estado --</option>';
            estados.forEach(edo => {
                const opt = document.createElement('option');
                opt.value = opt.textContent = edo;
                selectEstado.appendChild(opt);
            });
            selectEstado.addEventListener('change', handleStateChange);
        } catch (err) {
            console.error('Error cargando estados:', err);
        }
    };

    // ── Cambio de estado ─────────────────────────────────────────────────────
    const handleStateChange = async (e) => {
        const estado = e.target.value;
        if (!estado) {
            winnersContainer.classList.add('hidden');
            if (muniBody) muniBody.innerHTML = '';
            return;
        }
        if (typeof showLoader === 'function') showLoader(`Analizando ${estado}...`);
        try {
            const res  = await fetch(`/api/metricas-por-estado/${encodeURIComponent(estado)}`);
            const data = await res.json();
            const plazasData     = data.plazas     || [];
            const municipiosData = data.municipios  || [];

            if (plazasData.length > 0) {
                renderStateSummary(plazasData, estado);
                renderTablePlazas(plazasData);
                renderTableMunicipios(municipiosData);
                winnersContainer.classList.remove('hidden');
            } else {
                alert('No hay datos para este estado');
                winnersContainer.classList.add('hidden');
            }
        } catch (err) {
            console.error(err);
        } finally {
            if (typeof hideLoader === 'function') hideLoader();
        }
    };

    // ── Helpers ──────────────────────────────────────────────────────────────
    const metricCell = (val, isFooter = false) => {
        const color = val > 0 ? (isFooter ? '#1e293b' : '#059669') : '#cbd5e1';
        const fw    = val > 0 ? '600' : '400';
        return `<td style="text-align:center;color:${color};font-weight:${fw};padding:${isFooter?'10px':'0.75rem'} 1rem;">${val.toLocaleString()}</td>`;
    };

    // ── TABLA PLAZAS ─────────────────────────────────────────────────────────
    const renderTablePlazas = (plazasData) => {

        // Encabezados
        let hHTML = `
            <th class="sortable" data-key="Clave_Plaza">Clave ⇅</th>
            <th class="sortable" data-key="Nombre_PC">Plaza ⇅</th>
            <th class="sortable" data-key="Municipio">Municipio ⇅</th>`;
        Object.entries(metricsConfig).forEach(([k, l]) => {
            hHTML += `<th class="sortable metric-header" data-key="${k}">${l} ⇅</th>`;
        });
        tableHead.innerHTML = hHTML;

        fillPlazas(plazasData);

        // Ordenamiento
        tableHead.querySelectorAll('th.sortable').forEach(th => {
            th.addEventListener('click', () => {
                const key   = th.getAttribute('data-key');
                const isAsc = th.classList.contains('asc');
                tableHead.querySelectorAll('th.sortable').forEach(h => h.classList.remove('asc','desc'));
                const sorted = [...plazasData].sort((a, b) => {
                    const vA = a[key], vB = b[key];
                    return typeof vA === 'string'
                        ? (isAsc ? vB.localeCompare(vA) : vA.localeCompare(vB))
                        : (isAsc ? vA - vB : vB - vA);
                });
                th.classList.add(isAsc ? 'desc' : 'asc');
                fillPlazas(sorted);
            });
        });
    };

    const fillPlazas = (rows) => {
        tableBody.innerHTML = '';
        const sums = {};
        const munis = new Set();
        metricKeys.forEach(k => sums[k] = 0);

        rows.forEach(row => {
            if (row.Municipio) munis.add(row.Municipio);
            const tr = document.createElement('tr');
            let html = `
                <td><a href="#" class="enlace-plaza" data-clave="${row.Clave_Plaza}">${row.Clave_Plaza}</a></td>
                <td style="font-size:0.9em;">${row.Nombre_PC}</td>
                <td style="font-size:0.9em;">${row.Municipio}</td>`;
            metricKeys.forEach(k => {
                const v = parseFloat(row[k]) || 0;
                sums[k] += v;
                html += metricCell(v);
            });
            tr.innerHTML = html;
            tableBody.appendChild(tr);
        });

        // Footer:  col1=TOTAL  col2=N plazas  col3=N municipios  + métricas
        if (tableFoot) {
            const nP = rows.length;
            const nM = munis.size;
            let fHTML = `<tr>
                <td style="text-align:center;font-weight:700;color:#475569;padding:10px 1rem;font-size:0.85rem;">
                    TOTAL
                </td>
                <td style="text-align:center;font-weight:700;color:#1e293b;padding:10px 1rem;font-size:0.85rem;">
                    ${nP}<br><span style="font-size:0.72rem;color:#64748b;font-weight:500;">plazas</span>
                </td>
                <td style="text-align:center;font-weight:700;color:#1e293b;padding:10px 1rem;font-size:0.85rem;">
                    ${nM}<br><span style="font-size:0.72rem;color:#64748b;font-weight:500;">municipio${nM!==1?'s':''}</span>
                </td>`;
            metricKeys.forEach(k => { fHTML += metricCell(sums[k], true); });
            fHTML += `</tr>`;
            tableFoot.innerHTML = fHTML;
        }
    };

    // ── TABLA MUNICIPIOS ─────────────────────────────────────────────────────
    const renderTableMunicipios = (data) => {
        if (!muniHead || !muniBody) return;

        // Encabezados
        let hHTML = `
            <th class="sortable-muni" data-key="Municipio"
                style="min-width:180px;width:180px;background:#475569;color:white;padding:10px;cursor:pointer;">
                Municipio ⇅
            </th>`;
        Object.entries(metricsConfig).forEach(([k, l]) => {
            hHTML += `<th class="sortable-muni metric-header" data-key="${k}"
                style="background:#475569;color:white;padding:10px;cursor:pointer;">${l} ⇅</th>`;
        });
        muniHead.innerHTML = hHTML;

        fillMunicipios(data);

        // Ordenamiento
        muniHead.querySelectorAll('th.sortable-muni').forEach(th => {
            th.addEventListener('click', () => {
                const key   = th.getAttribute('data-key');
                const isAsc = th.classList.contains('asc');
                muniHead.querySelectorAll('th.sortable-muni').forEach(h => h.classList.remove('asc','desc'));
                const sorted = [...data].sort((a, b) => {
                    const vA = a[key], vB = b[key];
                    return typeof vA === 'string'
                        ? (isAsc ? vB.localeCompare(vA) : vA.localeCompare(vB))
                        : (isAsc ? vA - vB : vB - vA);
                });
                th.classList.add(isAsc ? 'desc' : 'asc');
                fillMunicipios(sorted);
            });
        });
    };

    const fillMunicipios = (rows) => {
        muniBody.innerHTML = '';
        const sums = {};
        metricKeys.forEach(k => sums[k] = 0);

        rows.forEach(row => {
            const tr = document.createElement('tr');
            let html = `
                <td style="padding:8px 1rem;font-weight:bold;min-width:180px;width:180px;
                           white-space:normal;word-break:break-word;line-height:1.3;">
                    ${row.Municipio}
                </td>`;
            metricKeys.forEach(k => {
                const v = row[k] || 0;
                sums[k] += v;
                html += metricCell(v);
            });
            tr.innerHTML = html;
            muniBody.appendChild(tr);
        });

        // Footer: col1=TOTAL  + métricas
        if (muniFoot) {
            let fHTML = `<tr>
                <td style="text-align:right;font-weight:700;color:#475569;padding:10px 1rem;
                           min-width:180px;width:180px;">
                    TOTAL:
                </td>`;
            metricKeys.forEach(k => { fHTML += metricCell(sums[k], true); });
            fHTML += `</tr>`;
            muniFoot.innerHTML = fHTML;
        }
    };

    // ── CARDS RESUMEN ────────────────────────────────────────────────────────
    const renderStateSummary = (plazasData, nombreEstado) => {
        winnersGrid.innerHTML = '';
        const titleDiv = document.createElement('div');
        titleDiv.style.cssText = 'grid-column:1/-1;margin-bottom:10px;';
        titleDiv.innerHTML = `<h4 style="color:#2563eb;margin:0;">Resultados Globales: ${nombreEstado}</h4>`;
        winnersGrid.appendChild(titleDiv);

        metricKeys.forEach(key => {
            let sum = 0, maxVal = -1, winner = null;
            plazasData.forEach(p => {
                const v = parseFloat(p[key]) || 0;
                sum += v;
                if (v > maxVal) { maxVal = v; winner = p; }
            });
            if (sum <= 0) return;

            const card = document.createElement('div');
            card.className = 'winner-card';
            card.style.cssText = `
                background:white;padding:15px;border-radius:8px;
                border:1px solid #e2e8f0;box-shadow:0 2px 4px rgba(0,0,0,0.05);
                border-top:4px solid #3b82f6;display:flex;
                flex-direction:column;justify-content:space-between;`;
            const pct = ((maxVal / sum) * 100).toFixed(1);
            card.innerHTML = `
                <div>
                    <div style="font-size:0.8rem;text-transform:uppercase;color:#64748b;font-weight:bold;margin-bottom:5px;">
                        ${metricsConfig[key]} (Total)
                    </div>
                    <div style="font-size:1.8rem;font-weight:800;color:#1e293b;margin-bottom:10px;">
                        ${sum.toLocaleString()}
                    </div>
                </div>
                <div style="background:#f8fafc;padding:8px;border-radius:6px;font-size:0.85rem;border:1px solid #e2e8f0;">
                    <div style="color:#64748b;font-size:0.75rem;font-weight:bold;">🏆 LÍDER:</div>
                    <div style="font-weight:600;color:#334155;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"
                         title="${winner.Nombre_PC}">
                        ${winner.Nombre_PC || 'Sin Nombre'}
                    </div>
                    <div style="color:#3b82f6;font-weight:bold;font-size:0.8rem;margin-top:2px;">
                        ${maxVal.toLocaleString()} (${pct}%)
                    </div>
                </div>`;
            winnersGrid.appendChild(card);
        });
    };

    initTopPlazas();
});