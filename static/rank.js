document.addEventListener('DOMContentLoaded', () => {
    // --- REFERENCIAS DOM EXISTENTES ---
    const selectEstado = document.getElementById('top-state-select');
    const winnersContainer = document.getElementById('top-winners-container');
    const winnersGrid = winnersContainer.querySelector('.winners-grid');
    
    // Tabla Plazas
    const tableHead = document.getElementById('top-plazas-thead');
    const tableBody = document.getElementById('top-plazas-tbody');
    const tableFoot = document.getElementById('top-plazas-tfoot');

    // --- NUEVAS REFERENCIAS DOM (TABLA MUNICIPIOS) ---
    const muniHead = document.getElementById('municipios-thead');
    const muniBody = document.getElementById('municipios-tbody');
    const muniFoot = document.getElementById('municipios-tfoot');

    const metricsConfig = {
        'Aten_Inicial': 'Atención Inicial',
        'Aten_Prim': 'Atención Primaria',
        'Aten_Sec': 'Atención Secundaria',
        'Aten_Total': 'Atención Total',
        'Exámenes aplicados': 'Exámenes Aplicados',
        'CN_Inicial_Acum': 'CN Inicial',
        'CN_Prim_Acum': 'CN Primaria',
        'CN_Sec_Acum': 'CN Secundaria',
        'CN_Tot_Acum': 'CN Total',
        'Cert_Emitidos': 'Certificados Emitidos'
    };

    // --- ESCUCHADOR DE CLICS EN LA TABLA (NUEVO) ---
    if (tableBody) {
        tableBody.addEventListener('click', (e) => {
            const link = e.target.closest('.enlace-plaza');
            
            if (link) {
                e.preventDefault();
                const clave = link.getAttribute('data-clave');
                
                if (clave && window.buscarYMostrarClave) {
                    console.log(`Navegando a plaza: ${clave}`);
                    window.buscarYMostrarClave(clave);
                } else {
                    console.error("No se encontró la función buscarYMostrarClave o la clave es inválida.");
                }
            }
        });
    }

    const initTopPlazas = async () => {
        try {
            const response = await fetch('/api/estados');
            const estados = await response.json();
            
            selectEstado.innerHTML = '<option value="">-- Selecciona un Estado --</option>';
            estados.forEach(edo => {
                const opt = document.createElement('option');
                opt.value = edo;
                opt.textContent = edo;
                selectEstado.appendChild(opt);
            });

            selectEstado.addEventListener('change', handleStateChange);
        } catch (error) {
            console.error("Error cargando estados:", error);
        }
    };

    const handleStateChange = async (e) => {
        const estado = e.target.value;
        if (!estado) {
            winnersContainer.classList.add('hidden');
            if (muniBody) muniBody.innerHTML = ''; // Limpiar tabla municipios
            return;
        }

        if (typeof showLoader === 'function') showLoader(`Analizando datos de ${estado}...`);

        try {
            const response = await fetch(`/api/metricas-por-estado/${encodeURIComponent(estado)}`);
            const data = await response.json();
            
            // Ahora 'data' es un objeto con dos propiedades: plazas y municipios
            const plazasData = data.plazas || [];
            const municipiosData = data.municipios || [];
            
            if (plazasData.length > 0) {
                renderStateSummary(plazasData, estado);
                
                // 1. Renderizar tabla original (Plazas)
                renderTable(plazasData);

                // 2. Renderizar tabla acumulada (Municipios)
                renderMunicipiosTable(municipiosData);

                winnersContainer.classList.remove('hidden');
            } else {
                alert('No hay datos para este estado');
                winnersContainer.classList.add('hidden');
            }

        } catch (error) {
            console.error(error);
        } finally {
            if (typeof hideLoader === 'function') hideLoader();
        }
    };

    // --- NUEVA FUNCIÓN DE RENDERIZADO: MUNICIPIOS ---
    const renderMunicipiosTable = (data) => {
        if (!muniHead || !muniBody) return;

        // 1. Encabezados
        let headersHTML = `
            <th class="sortable-muni" data-key="Municipio" style="cursor:pointer; background:#475569; color:white; padding:10px;">Municipio ⇅</th>
        `;
        Object.entries(metricsConfig).forEach(([key, label]) => {
            headersHTML += `<th class="sortable-muni" data-key="${key}" style="cursor:pointer; background:#475569; color:white; padding:10px; text-align:center;">${label} ⇅</th>`;
        });
        muniHead.innerHTML = headersHTML;

        // 2. Función interna para pintar filas
        const updateMuniRows = (rows) => {
            muniBody.innerHTML = '';
            let footerSums = {};
            Object.keys(metricsConfig).forEach(k => footerSums[k] = 0);

            rows.forEach(row => {
                const tr = document.createElement('tr');
                tr.style.borderBottom = "1px solid #e2e8f0";
                
                let html = `<td style="padding:8px; font-weight:bold;">${row.Municipio}</td>`;

                Object.keys(metricsConfig).forEach(key => {
                    const val = row[key] || 0;
                    footerSums[key] += val;
                    const style = val > 0 ? 'color: #059669; font-weight:600;' : 'color: #cbd5e1;';
                    html += `<td style="${style} text-align: center; padding:8px;">${val.toLocaleString()}</td>`;
                });

                tr.innerHTML = html;
                muniBody.appendChild(tr);
            });

            // Footer de la tabla de Municipios
            if (muniFoot) {
                let footerHTML = `
                    <tr style="background:#f1f5f9; font-weight:bold;">
                        <td style="text-align:right; padding:12px; color:#475569;">TOTALES:</td>
                `;
                Object.keys(metricsConfig).forEach(key => {
                    footerHTML += `<td style="text-align:center; padding:12px;">${footerSums[key].toLocaleString()}</td>`;
                });
                footerHTML += `</tr>`;
                muniFoot.innerHTML = footerHTML;
            }
        };

        // 3. Pintar inicial
        updateMuniRows(data);

        // 4. Lógica de Ordenamiento (Sorting) para Municipios
        const headers = muniHead.querySelectorAll('th.sortable-muni');
        headers.forEach(th => {
            th.addEventListener('click', () => {
                const key = th.getAttribute('data-key');
                const isAsc = th.classList.contains('asc');
                
                headers.forEach(h => h.classList.remove('asc', 'desc'));
                
                const sorted = [...data].sort((a, b) => {
                    const valA = a[key];
                    const valB = b[key];
                    
                    if (typeof valA === 'string') {
                        return isAsc ? valB.localeCompare(valA) : valA.localeCompare(valB);
                    } else {
                        return isAsc ? valA - valB : valB - valA;
                    }
                });

                th.classList.add(isAsc ? 'desc' : 'asc');
                updateMuniRows(sorted);
            });
        });
    };

    const renderStateSummary = (plazasData, nombreEstado) => {
        winnersGrid.innerHTML = '';
        
        const titleDiv = document.createElement('div');
        titleDiv.style.gridColumn = "1 / -1";
        titleDiv.style.marginBottom = "10px";
        titleDiv.innerHTML = `<h4 style="color:#2563eb; margin:0;">Resultados Globales: ${nombreEstado}</h4>`;
        winnersGrid.appendChild(titleDiv);

        Object.keys(metricsConfig).forEach(key => {
            let sumTotal = 0;
            let maxVal = -1;
            let winnerPlaza = null;

            plazasData.forEach(plaza => {
                const val = parseFloat(plaza[key]) || 0;
                sumTotal += val;
                if (val > maxVal) {
                    maxVal = val;
                    winnerPlaza = plaza;
                }
            });

            if (sumTotal > 0) {
                const card = document.createElement('div');
                card.className = 'winner-card';
                card.style.cssText = `
                    background: white;
                    padding: 15px;
                    border-radius: 8px;
                    border: 1px solid #e2e8f0;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                    border-top: 4px solid #3b82f6;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                `;

                const porcentajeLider = ((maxVal / sumTotal) * 100).toFixed(1);

                card.innerHTML = `
                    <div>
                        <div style="font-size: 0.8rem; text-transform: uppercase; color: #64748b; font-weight: bold; margin-bottom: 5px;">
                            ${metricsConfig[key]} (Total)
                        </div>
                        <div style="font-size: 1.8rem; font-weight: 800; color: #1e293b; margin-bottom: 10px;">
                            ${sumTotal.toLocaleString()}
                        </div>
                    </div>
                    <div style="background: #f8fafc; padding: 8px; border-radius: 6px; font-size: 0.85rem; border: 1px solid #e2e8f0;">
                        <div style="color: #64748b; font-size: 0.75rem; font-weight:bold;">🏆 LÍDER:</div>
                        <div style="font-weight: 600; color: #334155; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="${winnerPlaza.Nombre_PC}">
                            ${winnerPlaza.Nombre_PC || 'Sin Nombre'}
                        </div>
                        <div style="color: #3b82f6; font-weight: bold; font-size: 0.8rem; margin-top:2px;">
                            ${maxVal.toLocaleString()} (${porcentajeLider}%)
                        </div>
                    </div>
                `;
                winnersGrid.appendChild(card);
            }
        });
    };

    const renderTable = (plazasData) => {
        let headersHTML = `
            <th class="sortable" data-key="Clave_Plaza" style="cursor:pointer; background:#1e293b; color:white; padding:10px;">Clave ⇅</th>
            <th class="sortable" data-key="Nombre_PC" style="cursor:pointer; background:#1e293b; color:white; padding:10px;">Plaza ⇅</th>
            <th class="sortable" data-key="Municipio" style="cursor:pointer; background:#1e293b; color:white; padding:10px;">Municipio ⇅</th>
        `;

        Object.entries(metricsConfig).forEach(([key, label]) => {
            headersHTML += `<th class="sortable" data-key="${key}" style="cursor:pointer; background:#1e293b; color:white; padding:10px; text-align:center;">${label} ⇅</th>`;
        });
        tableHead.innerHTML = headersHTML;

        updateTableContent(plazasData);

        const headers = tableHead.querySelectorAll('th.sortable');
        headers.forEach(th => {
            th.addEventListener('click', () => {
                const key = th.getAttribute('data-key');
                const isAsc = th.classList.contains('asc');
                
                headers.forEach(h => h.classList.remove('asc', 'desc'));
                
                const sorted = [...plazasData].sort((a, b) => {
                    const valA = a[key];
                    const valB = b[key];
                    
                    if (typeof valA === 'string') {
                        return isAsc ? valB.localeCompare(valA) : valA.localeCompare(valB);
                    } else {
                        return isAsc ? valA - valB : valB - valA;
                    }
                });

                th.classList.add(isAsc ? 'desc' : 'asc');
                updateTableContent(sorted);
            });
        });
    };

    const updateTableContent = (rows) => {
        tableBody.innerHTML = '';
        let footerSums = {};
        Object.keys(metricsConfig).forEach(k => footerSums[k] = 0);

        rows.forEach(row => {
            const tr = document.createElement('tr');
            tr.style.borderBottom = "1px solid #e2e8f0";
            
            let html = `
                <td style="padding:8px;">
                    <a href="#" class="enlace-plaza" data-clave="${row.Clave_Plaza}" 
                       style="color: #2563eb; text-decoration: underline; font-weight: 700; cursor: pointer;">
                        ${row.Clave_Plaza}
                    </a>
                </td>
                <td style="padding:8px; font-size:0.9em;">${row.Nombre_PC}</td>
                <td style="padding:8px; font-size:0.9em;">${row.Municipio}</td>
            `;

            Object.keys(metricsConfig).forEach(key => {
                const val = parseFloat(row[key]) || 0;
                footerSums[key] += val;
                
                const style = val > 0 ? 'color: #059669; font-weight:600;' : 'color: #cbd5e1;';
                html += `<td style="${style} text-align: center; padding:8px;">${val.toLocaleString()}</td>`;
            });

            tr.innerHTML = html;
            tableBody.appendChild(tr);
        });

        if (tableFoot) {
            let footerHTML = `
               <tr style="background:#e2e8f0; font-weight:bold;">
                    <td style="text-align:right; padding:12px; color:#475569; border-right: 1px solid #cbd5e1;">TOTAL:</td>
                    
                    <td style="border-right: 1px solid #cbd5e1;"></td>
                    
                    <td style="border-right: 1px solid #cbd5e1;"></td>
            `;
            
            Object.keys(metricsConfig).forEach(key => {
                const val = footerSums[key];
                const style = val > 0 ? 'color:#1e293b;' : 'color:#94a3b8;';
                footerHTML += `<td style="text-align:center; padding:12px; ${style}">${val.toLocaleString()}</td>`;
            });
            
            footerHTML += `</tr>`;
            tableFoot.innerHTML = footerHTML;
        }
    };

    initTopPlazas();
});