document.addEventListener('DOMContentLoaded', () => {
    // --- CONFIGURACIÓN INICIAL ---
    const views = {
        'welcome-screen': document.getElementById('welcome-screen'),
        'key-search-view': document.getElementById('key-search-view'),
        'filter-search-view': document.getElementById('filter-search-view'),
        'results-view': document.getElementById('results-view'),
        'stats-view': document.getElementById('stats-view'),
        'estados-view': document.getElementById('estados-view'),
        'plazas-por-estado-view': document.getElementById('plazas-por-estado-view')
    };

    // --- REFERENCIAS A ELEMENTOS DEL DOM ---
    // Búsqueda por Clave
    const claveInput = document.getElementById('clave-input');
    const searchByKeyButton = document.getElementById('search-by-key-button');
    const keyLoader = document.getElementById('key-loader');
    
    // Búsqueda por Filtros
    const selects = {
        estado: document.getElementById('state-select'),
        zona: document.getElementById('zona-select'),
        municipio: document.getElementById('municipio-select'),
        localidad: document.getElementById('localidad-select'),
        clave: document.getElementById('clave-select')
    };
    const filterSteps = {
        estado: document.querySelector('.filter-step[data-step="estado"]'),
        zona: document.querySelector('.filter-step[data-step="zona"]'),
        municipio: document.querySelector('.filter-step[data-step="municipio"]'),
        localidad: document.querySelector('.filter-step[data-step="localidad"]'),
        clave: document.querySelector('.filter-step[data-step="clave"]')
    };
    const filterLoader = document.getElementById('filter-loader');
    const searchFilterButton = document.getElementById('search-filter-button');

    // Resultados y Alertas
    const resultsContent = document.getElementById('results-content');
    const backToSearchButton = document.getElementById('back-to-search-button');
    const alertContainer = document.getElementById('alert-container');
    
    // Barra de Progreso
    const progressFill = document.getElementById('progress-fill');
    const progressSteps = document.querySelectorAll('.progress-step');

    // Referencias para Estadísticas Generales
    const totalPlazas = document.getElementById('total-plazas');
    const totalEstados = document.getElementById('total-estados');
    const estadoMasPlazasNombre = document.getElementById('estado-mas-plazas-nombre');
    const estadoMasPlazasCantidad = document.getElementById('estado-mas-plazas-cantidad');
    const estadoMayorConectividadNombre = document.getElementById('estado-mayor-conectividad-nombre');
    const estadoMayorConectividadPorcentaje = document.getElementById('estado-mayor-conectividad-porcentaje');
    const estadoMasOperacionNombre = document.getElementById('estado-mas-operacion-nombre');
    const estadoMasOperacionPorcentaje = document.getElementById('estado-mas-operacion-porcentaje');
    const estadoMasSuspensionNombre = document.getElementById('estado-mas-suspension-nombre');
    const estadoMasSuspensionPorcentaje = document.getElementById('estado-mas-suspension-porcentaje');

    // Referencias para Estadísticas  (CN)
    const estadoMasCNInicialNombre = document.getElementById('estado-mas-cn-inicial-nombre');
    const estadoMasCNInicialCantidad = document.getElementById('estado-mas-cn-inicial-cantidad');
    const estadoMasCNPrimariaNombre = document.getElementById('estado-mas-cn-primaria-nombre');
    const estadoMasCNPrimariaCantidad = document.getElementById('estado-mas-cn-primaria-cantidad');
    const estadoMasCNSecundariaNombre = document.getElementById('estado-mas-cn-secundaria-nombre');
    const estadoMasCNSecundariaCantidad = document.getElementById('estado-mas-cn-secundaria-cantidad');
    const cnTop5InicialList = document.getElementById('cn-top5-inicial-list');
    const cnTop5PrimariaList = document.getElementById('cn-top5-primaria-list');
    const cnTop5SecundariaList = document.getElementById('cn-top5-secundaria-list');
    const cnResumenCards = document.getElementById('cn-resumen-cards');
    const cnEstadosTable = document.getElementById('cn-estados-table');
    const cnTop10List = document.getElementById('cn-top10-list');

    // Estados con más plazas
    const estadosGrid = document.getElementById('estados-grid');
    const estadosSearchInput = document.getElementById('estados-search-input');

    // Vista de Plazas por Estado
    const plazasPorEstadoTitle = document.getElementById('plazas-por-estado-title');
    const plazasListContainer = document.getElementById('plazas-list-container');
    const plazasSearchInput = document.getElementById('plazas-search-input');

    // --- VARIABLES DE ESTADO ---
    let lastView = 'welcome-screen';
    let estadisticasData = null;
    let todosEstadosData = [];
    let estadoSeleccionado = '';
    let plazasDelEstado = [];
    let cnResumenData = null;
    let cnPorEstadoData = null;
    let cnTopEstadosData = null;
    let cnEstadosDestacadosData = null;
    let cnTop5TodosData = null;

    // --- SISTEMA DE TEMA ---
    const themeLight = document.getElementById('theme-light');
    const themeDark = document.getElementById('theme-dark');

    const applyTheme = (theme) => {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        setTimeout(() => {
            const estadosView = document.getElementById('estados-view');
            const plazasView = document.getElementById('plazas-por-estado-view');
            
            if (estadosView && !estadosView.classList.contains('hidden')) {
                if (todosEstadosData.length > 0) {
                    renderEstadosConPlazas(todosEstadosData);
                }
            }
            
            if (plazasView && !plazasView.classList.contains('hidden')) {
                if (plazasDelEstado.length > 0) {
                    renderPlazasList(plazasDelEstado, estadoSeleccionado);
                }
            }
        }, 100);
    };

    const initTheme = () => {
        const savedTheme = localStorage.getItem('theme');
        applyTheme(savedTheme === 'dark' ? 'dark' : 'light');
    };

    themeLight.addEventListener('click', () => applyTheme('light'));
    themeDark.addEventListener('click', () => applyTheme('dark'));
    initTheme();

    // --- SISTEMA DE PANTALLA DE CARGA ---
    function showLoader(message = null, type = 'default') {
        const loader = document.getElementById('global-loader');
        const loaderMessage = loader.querySelector('.loader-message');
        
        if (message) {
            loaderMessage.textContent = message;
        }
        
        loader.className = 'loader-overlay';
        if (type !== 'default') {
            loader.classList.add(type);
        }
        
        loader.classList.remove('hidden');
        loader.style.zIndex = '9999';
    }

    function hideLoader() {
        const loader = document.getElementById('global-loader');
        loader.classList.add('hidden');
        
        setTimeout(() => {
            loader.className = 'loader-overlay hidden';
        }, 500);
    }

    // --- NAVEGACIÓN SPA ---
    const showView = (viewId) => {
        const currentView = Object.keys(views).find(key => !views[key].classList.contains('hidden'));
        if (currentView && currentView !== viewId) {
            lastView = currentView;
        }
        if (!views[viewId]) viewId = 'welcome-screen';
        Object.values(views).forEach(v => v.classList.add('hidden'));
        views[viewId].classList.remove('hidden');

        if (viewId === 'stats-view') {
            if (!estadisticasData) {
                cargarEstadisticas();
            } else if (!cnResumenData) { // En caso de que la carga de CN haya fallado
                cargarEstadisticasCompletasCN();
            }
        }
        
        if (viewId === 'estados-view' && todosEstadosData.length === 0) {
            cargarEstadosConPlazas();
        }
    };
    
    const handleNavigation = () => {
        const viewId = window.location.hash.substring(1) || 'welcome-screen';
        showView(viewId);
    };
    
    window.addEventListener('popstate', handleNavigation);
    document.body.addEventListener('click', (e) => {
        const link = e.target.closest('a[href^="#"]');
        if (link) {
            e.preventDefault();
            const viewId = link.getAttribute('href').substring(1);
            if (window.location.hash !== `#${viewId}`) {
                history.pushState({ view: viewId }, '', `#${viewId}`);
            }
            handleNavigation();
        }
    });
    
    backToSearchButton.addEventListener('click', () => {
        history.back();
    });

    // --- UTILIDADES ---
    const setLoaderVisible = (loader, visible) => loader.classList.toggle('hidden', !visible);
    
    const showAlert = (message, type = 'info') => {
        const alertClass = type === 'error' ? 'alert' :
                             type === 'success' ? 'alert success' :
                             type === 'warning' ? 'alert warning' :
                             'alert info';
        
        alertContainer.innerHTML = `<div class="${alertClass}">${message}</div>`;
        setTimeout(() => {
            alertContainer.innerHTML = '';
        }, 5000);
    };
    
    const debounce = (func, wait) => {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    };

    const cache = new Map();
    const fetchData = async (url) => {
        if (cache.has(url)) {
            return cache.get(url);
        }
        const response = await fetch(url);
        if (!response.ok) {
            const data = await response.json().catch(() => ({ error: `Error del servidor: ${response.status}` }));
            throw new Error(data.error);
        }
        const result = await response.json();
        cache.set(url, result);
        return result;
    };
    
    // --- RENDERIZADO DE RESULTADOS ---
    const renderPlazaResultados = (data) => {
        const { excel_info, images, google_maps_url, direccion_completa } = data;
        const mapHtml = google_maps_url ? `<a href="${google_maps_url}" target="_blank" class="btn btn-primary" rel="noopener noreferrer">Ver en Google Maps</a>` : '';
        const direccionHtml = direccion_completa ? `<div class="direccion-completa"><strong>Dirección:</strong> ${direccion_completa}</div>` : '';

        const columnasUbicacion = ['Estado', 'Coord. Zona', 'Municipio', 'Localidad', 'Colonia', 'Cod_Post', 'Calle', 'Num', 'Clave_Plaza', 'Nombre_PC', 'Situación', 'Latitud', 'Longitud'];
        const columnasUso = ['Aten_Ult_mes', 'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum', 'Tipo_local', 'Inst_aliada', 'Arq_Discap.', 'Conect_Instalada', 'Tipo_Conect'];
        const columnasInventario = ['Total de equipos de cómputo en la plaza', 'Equipos de cómputo que operan', 'Tipos de equipos de cómputo', 'Impresoras que funcionan', 'Impresoras con suministros (toner, hojas)', 'Total de servidores en la plaza', 'Número de servidores que funcionan correctamente', 'Cuantas mesas funcionan', 'Cuantas sillas funcionan', 'Cuantos Anaqueles funcionan'];

        const generateGridHtml = (info, columns) => {
            return columns.map(key => {
                const displayKey = key.replace(/_/g, ' ');
                const value = info[key];
                return `<p><strong>${displayKey}:</strong> ${value !== null && value !== undefined ? value : '<em>N/A</em>'}</p>`;
            }).join('');
        };

        const gridUbicacionHtml = generateGridHtml(excel_info, columnasUbicacion);
        const gridUsoHtml = generateGridHtml(excel_info, columnasUso);
        const gridInventarioHtml = generateGridHtml(excel_info, columnasInventario);
        
        const imagesHtml = images?.length > 0 ?
            images.map(url =>
                `<img src="${url}" alt="Imagen de la plaza" loading="lazy" onclick="window.open('${url}', '_blank')">`
            ).join('') :
            '<p>No se encontraron imágenes.</p>';
        
        resultsContent.innerHTML = `
            <h1>${excel_info.Clave_Plaza || 'Detalles de la Plaza'}</h1>
            <div class="results-card">${direccionHtml}<div style="text-align:center; margin: 1.5rem 0;">${mapHtml}</div></div>
            
            <div class="results-card">
                <h2>Ubicación y Datos Generales</h2>
                <div class="info-grid info-grid-ubicacion">${gridUbicacionHtml}</div>
            </div>
            
            <div class="results-card">
                <h2>Atención-productividad-Tipo</h2>
                <div class="info-grid info-grid-uso">${gridUsoHtml}</div>
            </div>
            
            <div class="results-card">
                <h2>Inventario de Equipos</h2>
                <div class="info-grid info-grid-inventario">${gridInventarioHtml}</div>
            </div>

            <div class="results-card"><h2>Imágenes</h2><div id="images-container">${imagesHtml}</div></div>`;
    };

    // --- BÚSQUEDA Y FILTROS ---
    const buscarYMostrarClave = async (clave, loader) => {
        if (!clave) return;
        
        showLoader(`Buscando plaza con clave: ${clave}`);
        
        try {
            const data = await fetchData(`/api/search?clave=${encodeURIComponent(clave)}`);
            renderPlazaResultados(data);
            history.pushState({ view: 'results-view' }, '', '#results-view');
            handleNavigation();
        } catch (error) {
            showAlert(`Error al buscar la clave: ${error.message}`, 'error');
        } finally {
            hideLoader();
            if (loader) setLoaderVisible(loader, false);
        }
    };
    
    const updateStepIndicator = (stepName, status) => {
        const indicator = filterSteps[stepName]?.querySelector('.step-indicator');
        if(indicator) {
            indicator.classList.remove('active', 'completed');
            if (status === 'active') indicator.classList.add('active');
            if (status === 'completed') indicator.classList.add('completed');
        }
    };
    
    const populateSelect = async (selectElement, url, placeholder) => {
        selectElement.disabled = true;
        selectElement.innerHTML = `<option value="">Cargando...</option>`;
        setLoaderVisible(filterLoader, true);
        try {
            const options = await fetchData(url);
            selectElement.innerHTML = `<option value="">-- ${placeholder} --</option>`;
            if (options.length > 0) {
                options.forEach(option => selectElement.innerHTML += `<option value="${option}">${option}</option>`);
                selectElement.disabled = false;
            } else {
                selectElement.innerHTML = `<option value="">No hay opciones</option>`;
            }
        } catch (error) {
            showAlert(`Error al cargar: ${error.message}`, 'error');
            selectElement.innerHTML = `<option value="">Error</option>`;
        } finally {
            setLoaderVisible(filterLoader, false);
        }
    };
    
    const resetSteps = (fromStepName) => {
        const stepNames = ['zona', 'municipio', 'localidad', 'clave'];
        const startIndex = stepNames.indexOf(fromStepName);
        if (startIndex === -1) return;
        for (let i = startIndex; i < stepNames.length; i++) {
            const stepName = stepNames[i];
            if(filterSteps[stepName]) filterSteps[stepName].classList.add('hidden');
            updateStepIndicator(stepName, 'default');
            if(selects[stepName]) selects[stepName].disabled = true;
        }
    };

    // ==== FUNCIÓN MEJORADA: NAVEGACIÓN AUTOMÁTICA ENTRE PASOS ====
    const navigateToNextStep = (currentStepName) => {
        const stepOrder = ['estado', 'zona', 'municipio', 'localidad', 'clave'];
        const currentStepIndex = stepOrder.indexOf(currentStepName);
        
        if (currentStepIndex === -1 || currentStepIndex >= stepOrder.length - 1) return;
        
        const nextStepName = stepOrder[currentStepIndex + 1];
        
        if (filterSteps[nextStepName]) {
            filterSteps[nextStepName].classList.remove('hidden');
            updateStepIndicator(nextStepName, 'active');
        }
        
        for (let i = currentStepIndex + 2; i < stepOrder.length; i++) {
            const step = stepOrder[i];
            if (filterSteps[step]) {
                filterSteps[step].classList.add('hidden');
                updateStepIndicator(step, 'default');
            }
        }
        
        actualizarProgreso();
        
        setTimeout(() => {
            if (filterSteps[nextStepName]) {
                const elementPosition = filterSteps[nextStepName].getBoundingClientRect().top + window.pageYOffset;
                const offsetPosition = elementPosition - 100;
                
                window.scrollTo({
                    top: offsetPosition,
                    behavior: 'smooth'
                });
                
                if (selects[nextStepName] && !selects[nextStepName].disabled) {
                    selects[nextStepName].focus();
                }
            }
        }, 300);
    };

    const actualizarProgreso = () => {
        const steps = ['estado', 'zona', 'municipio', 'localidad', 'clave'];
        let completedSteps = 0;
        let activeStepIndex = -1;
        
        steps.forEach((stepName, index) => {
            if (selects[stepName] && selects[stepName].value) {
                completedSteps++;
            }
            if (filterSteps[stepName] && !filterSteps[stepName].classList.contains('hidden')) {
                activeStepIndex = index;
            }
        });
        
        const progressPercentage = Math.max(20, (completedSteps / steps.length) * 100);
        if (progressFill) {
            progressFill.style.width = `${progressPercentage}%`;
        }
        
        progressSteps.forEach((stepElement, index) => {
            const stepName = stepElement.getAttribute('data-step');
            const stepIndex = steps.indexOf(stepName);
            
            stepElement.classList.remove('active', 'completed');
            
            if (stepIndex < completedSteps) {
                stepElement.classList.add('completed');
            } else if (stepIndex === completedSteps && activeStepIndex >= stepIndex) {
                stepElement.classList.add('active');
            }
        });
    };
    
    const resetSearch = () => {
        showLoader('Reiniciando búsqueda...', 'compact');
        
        setTimeout(() => {
            Object.values(selects).forEach(select => {
                if (select) {
                    select.selectedIndex = 0;
                    select.disabled = select.id !== 'state-select';
                }
            });
            
            Object.keys(filterSteps).forEach(stepName => {
                if (filterSteps[stepName]) {
                    filterSteps[stepName].classList.toggle('hidden', stepName !== 'estado');
                    updateStepIndicator(stepName, stepName === 'estado' ? 'active' : 'default');
                }
            });
            
            actualizarProgreso();
            
            const filterSection = document.querySelector('.search-container');
            if (filterSection) {
                const elementPosition = filterSection.getBoundingClientRect().top + window.pageYOffset;
                window.scrollTo({
                    top: elementPosition - 80,
                    behavior: 'smooth'
                });
            }
            
            if (selects.estado) {
                setTimeout(() => selects.estado.focus(), 500);
            }
            
            hideLoader();
            showAlert('Búsqueda reiniciada correctamente', 'success');
        }, 800);
    };

    // ==== CONFIGURACIÓN MEJORADA DE LA BARRA DE PROGRESO ====
    const setupProgressBarNavigation = () => {
        progressSteps.forEach(step => {
            const newStep = step.cloneNode(true);
            step.parentNode.replaceChild(newStep, step);
            
            newStep.addEventListener('click', () => {
                const stepName = newStep.getAttribute('data-step');
                const stepOrder = ['estado', 'zona', 'municipio', 'localidad', 'clave'];
                const targetIndex = stepOrder.indexOf(stepName);
                if (targetIndex > 0) {
                    const prevStepName = stepOrder[targetIndex - 1];
                    if (!selects[prevStepName] || !selects[prevStepName].value) {
                        showAlert(`Por favor, completa primero el paso de '${prevStepName}'.`, 'warning');
                        return;
                    }
                }

                if (filterSteps[stepName]) {
                    const elementPosition = filterSteps[stepName].getBoundingClientRect().top + window.pageYOffset;
                    const offsetPosition = elementPosition - 100;
                    window.scrollTo({
                        top: offsetPosition,
                        behavior: 'smooth'
                    });
                    if (selects[stepName] && !selects[stepName].disabled) {
                        selects[stepName].focus();
                    }
                }
            });
        });
    };

    // ==== FUNCIÓN PARA BÚSQUEDA MANUAL CON FILTROS ====
    const handleFilterSearch = () => {
        const clave = selects.clave.value;
        if (clave) {
            buscarYMostrarClave(clave, filterLoader);
        } else {
            showAlert('Por favor, completa todos los filtros hasta seleccionar una clave de plaza.', 'warning');
        }
    };

    // ==== FUNCIÓN PARA BÚSQUEDA POR CLAVE ====
    const handleSearchByKey = () => {
        const clave = claveInput.value.trim();
        if (clave) {
            buscarYMostrarClave(clave, keyLoader);
        } else {
            showAlert('Por favor, introduce una clave para buscar.', 'warning');
        }
    };
    
    // ==== FUNCIÓN OPCIONAL: TOGGLE PARA AUTO-BÚSQUEDA ====
    const addAutoSearchToggle = () => {
        const searchButton = document.getElementById('search-filter-button');
        if (!searchButton) return;
        
        const toggleHtml = `
            <div class="auto-search-toggle" style="margin: 1rem 0; display: flex; align-items: center; gap: 0.5rem;">
                <input type="checkbox" id="auto-search-toggle" style="margin: 0;">
                <label for="auto-search-toggle" style="font-size: 0.875rem; color: var(--text-muted); cursor: pointer;">
                    Búsqueda automática al seleccionar clave
                </label>
            </div>
        `;
        searchButton.insertAdjacentHTML('beforebegin', toggleHtml);
    };

    // ==== SISTEMA DE NAVEGACIÓN POR TECLADO ====
    const setupKeyboardNavigation = () => {
        Object.values(selects).forEach((select) => {
            if (select) {
                select.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        const selectArray = Object.values(selects);
                        const currentIndex = selectArray.indexOf(select);
                        
                        if (currentIndex < selectArray.length - 1) {
                            const nextSelect = selectArray[currentIndex + 1];
                            if (nextSelect && !nextSelect.disabled) {
                                nextSelect.focus();
                            }
                        } else {
                            handleFilterSearch();
                        }
                    }
                });
            }
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                history.back();
            }
            if (!views['welcome-screen'].classList.contains('hidden')) {
                if (e.key === '1') document.querySelector('a[href="#key-search-view"]').click();
                else if (e.key === '2') document.querySelector('a[href="#filter-search-view"]').click();
                else if (e.key === '3') document.querySelector('a[href="#stats-view"]').click();
                else if (e.key === '4') document.querySelector('a[href="#estados-view"]').click();
            }
        });
    };
    
    // Event listeners para búsquedas
    if (searchFilterButton) {
        searchFilterButton.addEventListener('click', handleFilterSearch);
    }
    searchByKeyButton.addEventListener('click', handleSearchByKey);
    claveInput.addEventListener('keyup', (e) => e.key === 'Enter' && handleSearchByKey());

    // Event listeners para filtros
    selects.estado.addEventListener('change', () => {
        resetSteps('zona');
        const estado = selects.estado.value;
        updateStepIndicator('estado', estado ? 'completed' : 'active');
        if (estado) {
            populateSelect(selects.zona, `/api/zonas?estado=${encodeURIComponent(estado)}`, 'Selecciona una Zona');
            setTimeout(() => navigateToNextStep('estado'), 100);
        }
        actualizarProgreso();
    });

    selects.zona.addEventListener('change', () => {
        resetSteps('municipio');
        const zona = selects.zona.value;
        updateStepIndicator('zona', zona ? 'completed' : 'active');
        if (zona) {
            populateSelect(selects.municipio, `/api/municipios?estado=${encodeURIComponent(selects.estado.value)}&zona=${encodeURIComponent(zona)}`, 'Selecciona un Municipio');
            setTimeout(() => navigateToNextStep('zona'), 100);
        }
        actualizarProgreso();
    });

    selects.municipio.addEventListener('change', () => {
        resetSteps('localidad');
        const municipio = selects.municipio.value;
        updateStepIndicator('municipio', municipio ? 'completed' : 'active');
        if (municipio) {
            populateSelect(selects.localidad, `/api/localidades?estado=${encodeURIComponent(selects.estado.value)}&zona=${encodeURIComponent(selects.zona.value)}&municipio=${encodeURIComponent(municipio)}`, 'Selecciona una Localidad');
            setTimeout(() => navigateToNextStep('municipio'), 100);
        }
        actualizarProgreso();
    });

    selects.localidad.addEventListener('change', () => {
        resetSteps('clave');
        const localidad = selects.localidad.value;
        updateStepIndicator('localidad', localidad ? 'completed' : 'active');
        if (localidad) {
            populateSelect(selects.clave, `/api/claves_plaza?estado=${encodeURIComponent(selects.estado.value)}&zona=${encodeURIComponent(selects.zona.value)}&municipio=${encodeURIComponent(selects.municipio.value)}&localidad=${encodeURIComponent(localidad)}`, 'Selecciona la Clave');
            setTimeout(() => navigateToNextStep('localidad'), 100);
        }
        actualizarProgreso();
    });

    selects.clave.addEventListener('change', () => {
        const clave = selects.clave.value;
        updateStepIndicator('clave', clave ? 'completed' : 'active');
        actualizarProgreso();
        
        if (clave && document.getElementById('auto-search-toggle')?.checked) {
            setTimeout(() => handleFilterSearch(), 500);
        }
    });

    // --- LÓGICA DE ESTADÍSTICAS ---

    const renderResumenCN = () => {
    if (!cnResumenCards || !cnResumenData?.resumen_nacional) return;
    
    const { resumen_nacional, top5_estados_por_CN_Total } = cnResumenData;
    
    let html = `
        <div class="cn-card">
            <h4>📊 Resumen Nacional</h4>
            <div class="cn-stats-grid">
    `;
    
    // Mostrar solo las categorías individuales y el total
    const categoriasMostrar = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Total'];
    
    categoriasMostrar.forEach(key => {
        const data = resumen_nacional[key];
        if (!data) return;
        
        const nombre = key === 'CN_Total' ? 'CN TOTAL' : key.replace(/_/g, ' ');
        
        html += `
            <div class="cn-stat-item ${key === 'CN_Total' ? 'cn-total-item' : ''}">
                <span class="cn-stat-label">${nombre}</span>
                <span class="cn-stat-value">${data.suma.toLocaleString()}</span>
                <span class="cn-stat-subvalue">Total registros: ${data.total_registros.toLocaleString()}</span>
            </div>
        `;
    });
    
    html += `</div></div>`;
    
    // Top 5 estados por CN_Total
    if (top5_estados_por_CN_Total && top5_estados_por_CN_Total.length > 0) {
        html += `
            <div class="cn-card">
                <h4>🏆 Top 5 Estados - CN Total</h4>
                <div class="cn-stats-grid">
        `;
        
        top5_estados_por_CN_Total.forEach((item, index) => {
            const medal = index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : '🏅';
            html += `
                <div class="cn-stat-item">
                    <span class="cn-stat-label">${medal} ${item.estado}</span>
                    <span class="cn-stat-value">${item.suma_CN_Total.toLocaleString()}</span>
                </div>
            `;
        });
        
        html += `</div></div>`;
    }
    
    cnResumenCards.innerHTML = html;
};
const renderTablaEstadosCN = () => {
    if (!cnEstadosTable || !cnPorEstadoData?.estados) return;
    
    const { estados, cn_total_nacional } = cnPorEstadoData;
    
    // Crear encabezados con botones de ordenamiento
    let html = `
        <thead>
            <tr>
                <th>
                    <button class="sort-btn" data-sort="estado" data-order="asc">
                        Estado ▲
                    </button>
                </th>
                <th>
                    <button class="sort-btn" data-sort="total_plazas" data-order="desc">
                        Total Plazas ▼
                    </button>
                </th>
                <th>
                    <button class="sort-btn" data-sort="cn_inicial" data-order="desc">
                        CN Inicial ▼
                    </button>
                </th>
                <th>
                    <button class="sort-btn" data-sort="cn_primaria" data-order="desc">
                        CN Primaria ▼
                    </button>
                </th>
                <th>
                    <button class="sort-btn" data-sort="cn_secundaria" data-order="desc">
                        CN Secundaria ▼
                    </button>
                </th>
                <th>
                    <button class="sort-btn" data-sort="cn_total" data-order="desc">
                        CN Total ▼
                    </button>
                </th>
                <th>
                    <button class="sort-btn" data-sort="pct_nacional" data-order="desc">
                        % Sobre Nacional ▼
                    </button>
                </th>
            </tr>
        </thead>
        <tbody>
    `;
    
    // Renderizar filas iniciales
    estados.forEach(estado => {
        html += `
            <tr>
                <td><strong>${estado.estado}</strong></td>
                <td>${estado.total_plazas.toLocaleString()}</td>
                <td>${estado.suma_CN_Inicial_Acum.toLocaleString()}</td>
                <td>${estado.suma_CN_Prim_Acum.toLocaleString()}</td>
                <td>${estado.suma_CN_Sec_Acum.toLocaleString()}</td>
                <td><span class="cn-badge badge-primary">${estado.suma_CN_Total.toLocaleString()}</span></td>
                <td><span class="cn-badge badge-info">${estado.pct_sobre_nacional}%</span></td>
            </tr>
        `;
    });
    
    html += `</tbody>`;
    cnEstadosTable.innerHTML = html;
    
    // Agregar event listeners para los botones de ordenamiento
    setupSorting();
};

// Actualizar la función de ordenamiento para incluir el nuevo campo
const setupSorting = () => {
    const sortButtons = document.querySelectorAll('.sort-btn');
    let currentData = [...cnPorEstadoData.estados];
    
    sortButtons.forEach(button => {
        button.addEventListener('click', () => {
            const sortBy = button.getAttribute('data-sort');
            const currentOrder = button.getAttribute('data-order');
            const newOrder = currentOrder === 'asc' ? 'desc' : 'asc';
            
            // Resetear todos los botones
            sortButtons.forEach(btn => {
                btn.textContent = btn.textContent.replace('▲', '').replace('▼', '');
                btn.setAttribute('data-order', 'desc');
            });
            
            // Actualizar el botón actual
            button.setAttribute('data-order', newOrder);
            button.textContent = button.textContent.replace('▲', '').replace('▼', '') + (newOrder === 'asc' ? ' ▲' : ' ▼');
            
            // Ordenar los datos
            sortTableData(sortBy, newOrder);
        });
    });
    
    const sortTableData = (sortBy, order) => {
        const sortedData = [...currentData].sort((a, b) => {
            let valueA, valueB;
            
            switch(sortBy) {
                case 'estado':
                    valueA = a.estado.toLowerCase();
                    valueB = b.estado.toLowerCase();
                    return order === 'asc' ? valueA.localeCompare(valueB) : valueB.localeCompare(valueA);
                    
                case 'total_plazas':
                    valueA = a.total_plazas;
                    valueB = b.total_plazas;
                    break;
                    
                case 'cn_inicial':
                    valueA = a.suma_CN_Inicial_Acum;
                    valueB = b.suma_CN_Inicial_Acum;
                    break;
                    
                case 'cn_primaria':
                    valueA = a.suma_CN_Prim_Acum;
                    valueB = b.suma_CN_Prim_Acum;
                    break;
                    
                case 'cn_secundaria':
                    valueA = a.suma_CN_Sec_Acum;
                    valueB = b.suma_CN_Sec_Acum;
                    break;
                    
                case 'cn_total':
                    valueA = a.suma_CN_Total;
                    valueB = b.suma_CN_Total;
                    break;
                    
                case 'pct_nacional':  // Nuevo caso para ordenar por porcentaje
                    valueA = a.pct_sobre_nacional;
                    valueB = b.pct_sobre_nacional;
                    break;
                    
                default:
                    return 0;
            }
            
            return order === 'asc' ? valueA - valueB : valueB - valueA;
        });
        
        updateTableRows(sortedData);
    };
    
    const updateTableRows = (data) => {
        const tbody = cnEstadosTable.querySelector('tbody');
        let html = '';
        
        data.forEach(estado => {
            html += `
                <tr>
                    <td><strong>${estado.estado}</strong></td>
                    <td>${estado.total_plazas.toLocaleString()}</td>
                    <td>${estado.suma_CN_Inicial_Acum.toLocaleString()}</td>
                    <td>${estado.suma_CN_Prim_Acum.toLocaleString()}</td>
                    <td>${estado.suma_CN_Sec_Acum.toLocaleString()}</td>
                    <td><span class="cn-badge badge-primary">${estado.suma_CN_Total.toLocaleString()}</span></td>
                    <td><span class="cn-badge badge-info">${estado.pct_sobre_nacional}%</span></td>
                </tr>
            `;
        });
        
        tbody.innerHTML = html;
    };
};
    const renderTop10CN = () => {
        if (!cnTop10List || !cnTopEstadosData?.top) return;
        let html = '';
        cnTopEstadosData.top.forEach((item, index) => {
            html += `
                <div class="top10-item">
                    <span class="top10-rank">#${index + 1}</span>
                    <span class="top10-state">${item.estado}</span>
                    <span class="top10-value">${item.valor.toLocaleString()}</span>
                </div>
            `;
        });
        cnTop10List.innerHTML = html;
    };

    const renderEstadisticasCN = () => {
    if (!cnResumenData || !cnPorEstadoData || !cnTopEstadosData) return;
    renderResumenCN(); // Esta es la función modificada
    renderTablaEstadosCN();
    renderTop10CN();
};
    const renderEstadosDestacadosCN = (estadosDestacados) => {
        if (!estadosDestacados) return;
        if (estadosDestacados.CN_Inicial_Acum) {
            estadoMasCNInicialNombre.textContent = estadosDestacados.CN_Inicial_Acum.estado;
            estadoMasCNInicialCantidad.textContent = estadosDestacados.CN_Inicial_Acum.valor.toLocaleString();
        }
        if (estadosDestacados.CN_Prim_Acum) {
            estadoMasCNPrimariaNombre.textContent = estadosDestacados.CN_Prim_Acum.estado;
            estadoMasCNPrimariaCantidad.textContent = estadosDestacados.CN_Prim_Acum.valor.toLocaleString();
        }
        if (estadosDestacados.CN_Sec_Acum) {
            estadoMasCNSecundariaNombre.textContent = estadosDestacados.CN_Sec_Acum.estado;
            estadoMasCNSecundariaCantidad.textContent = estadosDestacados.CN_Sec_Acum.valor.toLocaleString();
        }
    };
    
    const renderTop5TodosCN = (top5Todos) => {
        if (!top5Todos) return;
        const renderList = (listElement, data) => {
            if (!listElement || !data) return;
            let html = '';
            data.forEach((item, index) => {
                html += `
                    <div class="top10-item">
                        <span class="top10-rank">#${index + 1}</span>
                        <span class="top10-state">${item.estado}</span>
                        <span class="top10-value">${item.valor.toLocaleString()}</span>
                    </div>
                `;
            });
            listElement.innerHTML = html;
        };
        renderList(cnTop5InicialList, top5Todos.inicial);
        renderList(cnTop5PrimariaList, top5Todos.primaria);
        renderList(cnTop5SecundariaList, top5Todos.secundaria);
    };

    const cargarEstadisticasCompletasCN = async () => {
        try {
            showLoader('Cargando estadísticas ...', 'compact');
            
            const [resumen, porEstado, topEstados, estadosDestacados, top5Todos] = await Promise.all([
                fetchData('/api/cn_resumen'),
                fetchData('/api/cn_por_estado'),
                fetchData('/api/cn_top_estados?metric=inicial&n=10'),
                fetchData('/api/cn_estados_destacados'),
                fetchData('/api/cn_top5_todos')
            ]);
            
            cnResumenData = resumen;
            cnPorEstadoData = porEstado;
            cnTopEstadosData = topEstados;
            cnEstadosDestacadosData = estadosDestacados;
            cnTop5TodosData = top5Todos;
            
            renderEstadisticasCN();
            renderEstadosDestacadosCN(estadosDestacados);
            renderTop5TodosCN(top5Todos);
            
        } catch (error) {
            console.error('Error cargando estadísticas completas CN:', error);
            showAlert('Error al cargar las estadísticas', 'error');
        } finally {
            hideLoader();
        }
    };

    const cargarEstadisticas = async () => {
        try {
            showLoader('Cargando estadísticas del sistema...', 'compact');
            const statsContainer = document.getElementById('stats-view');
            if (statsContainer) statsContainer.classList.add('shimmer');
            
            const stats = await fetchData('/api/estadisticas');
            estadisticasData = stats;

            if (totalPlazas) totalPlazas.textContent = stats.totalPlazas?.toLocaleString() || '0';
            if (totalEstados) totalEstados.textContent = stats.totalEstados?.toLocaleString() || '0';
            if (estadoMasPlazasNombre) estadoMasPlazasNombre.textContent = stats.estadoMasPlazas?.nombre || 'N/A';
            if (estadoMasPlazasCantidad) estadoMasPlazasCantidad.textContent = stats.estadoMasPlazas?.cantidad?.toLocaleString() || '0';
            if (estadoMayorConectividadNombre) estadoMayorConectividadNombre.textContent = stats.estadoMayorConectividad?.nombre || 'N/A';
            if (estadoMayorConectividadPorcentaje) estadoMayorConectividadPorcentaje.textContent = `${stats.estadoMayorConectividad?.porcentaje || 0}%`;
            if (estadoMasOperacionNombre) estadoMasOperacionNombre.textContent = stats.estadoMasOperacion?.nombre || 'N/A';
            if (estadoMasOperacionPorcentaje) estadoMasOperacionPorcentaje.textContent = `${stats.estadoMasOperacion?.porcentaje || 0}%`;
            if (estadoMasSuspensionNombre) estadoMasSuspensionNombre.textContent = stats.estadoMasSuspension?.nombre || 'N/A';
            if (estadoMasSuspensionPorcentaje) estadoMasSuspensionPorcentaje.textContent = `${stats.estadoMasSuspension?.porcentaje || 0}%`;

            await cargarEstadisticasCompletasCN();

        } catch (error) {
            console.error('Error cargando estadísticas:', error);
            showAlert('Error al cargar las estadísticas desde el servidor.', 'error');
        } finally {
            hideLoader();
            const statsContainer = document.getElementById('stats-view');
            if (statsContainer) statsContainer.classList.remove('shimmer');
        }
    };

    // --- LÓGICA DE VISTAS DE ESTADOS Y PLAZAS ---

    const cargarEstadosConPlazas = async () => {
        try {
            showLoader('Obteniendo lista de estados y plazas...');
            
            const estados = await fetchData('/api/estados_con_conteo'); // Assuming an optimized endpoint exists
            estados.sort((a, b) => b.cantidad - a.cantidad);
            
            todosEstadosData = estados;
            renderEstadosConPlazas(estados);
            setupBusquedaEstadosView(estados);
            
        } catch (error) {
            console.error('Error cargando estados con plazas:', error);
            showAlert('Error al cargar los estados', 'error');
        } finally {
            hideLoader();
        }
    };

    const renderEstadosConPlazas = (estados) => {
        if (!estadosGrid) return;
        estadosGrid.innerHTML = '';
        
        if (estados.length === 0) {
            estadosGrid.innerHTML = '<p class="no-results">No se encontraron estados.</p>';
            return;
        }
        
        estados.forEach(estado => {
            const item = document.createElement('div');
            item.className = 'state-menu-item';
            item.innerHTML = `
                <div class="state-menu-name">${estado.nombre}</div>
                <div class="state-menu-count">${estado.cantidad || 'N/A'} plazas</div>
            `;
            item.addEventListener('click', () => {
                estadoSeleccionado = estado.nombre;
                history.pushState({ view: 'plazas-por-estado-view' }, '', '#plazas-por-estado-view');
                handleNavigation();
                cargarPlazasPorEstado(estado.nombre);
            });
            estadosGrid.appendChild(item);
        });
    };

    const setupBusquedaEstadosView = (estadosData) => {
        if (!estadosSearchInput) return;
        const debouncedSearch = debounce((query) => {
            const queryLower = query.toLowerCase().trim();
            if (!queryLower) {
                renderEstadosConPlazas(estadosData);
                return;
            }
            const estadosFiltrados = estadosData.filter(estado =>
                estado.nombre.toLowerCase().includes(queryLower)
            );
            renderEstadosConPlazas(estadosFiltrados);
        }, 300);
        estadosSearchInput.addEventListener('input', (e) => debouncedSearch(e.target.value));
    };

    const cargarPlazasPorEstado = async (estado) => {
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

    const renderPlazasList = (plazas, estado) => {
        if (!plazasListContainer) return;
        plazasListContainer.innerHTML = '';
        
        if (plazas.length === 0) {
            plazasListContainer.innerHTML = '<p class="no-results">No se encontraron plazas para este estado.</p>';
            return;
        }
        
        plazas.forEach(plaza => {
            const item = document.createElement('div');
            item.className = 'plaza-list-item';
            item.innerHTML = `
                <div class="plaza-clave">${plaza.clave}</div>
                <div class="plaza-direccion">${plaza.direccion || 'Dirección no disponible'}</div>
                <div class="plaza-ubicacion">
                    <span>${plaza.municipio}</span>
                    <span>${plaza.localidad}</span>
                </div>
            `;
            item.addEventListener('click', () => {
                buscarYMostrarClave(plaza.clave, filterLoader);
            });
            plazasListContainer.appendChild(item);
        });
        setupBusquedaPlazasView();
    };

    const setupBusquedaPlazasView = () => {
        if (!plazasSearchInput) return;
        const debouncedSearch = debounce((query) => {
            const queryLower = query.toLowerCase().trim();
            if (!queryLower) {
                renderPlazasList(plazasDelEstado, estadoSeleccionado);
                return;
            }
            const plazasFiltradas = plazasDelEstado.filter(plaza =>
                plaza.clave.toLowerCase().includes(queryLower) ||
                plaza.municipio.toLowerCase().includes(queryLower) ||
                plaza.localidad.toLowerCase().includes(queryLower) ||
                (plaza.direccion && plaza.direccion.toLowerCase().includes(queryLower))
            );
            renderPlazasList(plazasFiltradas, estadoSeleccionado);
        }, 300);
        plazasSearchInput.addEventListener('input', (e) => debouncedSearch(e.target.value));
    };

    // --- INICIALIZACIÓN FINAL ---
    const initApp = () => {
        const resetButton = document.getElementById('reset-search-button');
        if (resetButton) {
            resetButton.addEventListener('click', resetSearch);
        }
        populateSelect(selects.estado, '/api/estados', 'Selecciona un Estado');
        setupKeyboardNavigation();
        setupProgressBarNavigation();
        actualizarProgreso();
        addAutoSearchToggle();
        handleNavigation();
    };

    initApp();
});

// Función adicional para cambiar fondos de tema
(function() {
    const body = document.body;

    function setLightTheme() {
        body.style.backgroundImage = "url('/static/claro.jpg')";
        body.style.backgroundSize = "cover";
    }

    function setDarkTheme() {
        body.style.backgroundImage = "url('/static/noche.jpg')";
        body.style.backgroundSize = "cover";
    }

    const btnLight = document.getElementById('theme-light');
    const btnDark = document.getElementById('theme-dark');

    if (btnLight) btnLight.addEventListener('click', setLightTheme);
    if (btnDark) btnDark.addEventListener('click', setDarkTheme);
})();