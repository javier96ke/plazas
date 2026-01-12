document.addEventListener('DOMContentLoaded', () => {
    // --- CONFIGURACIÓN INICIAL ---
    const views = {
        'welcome-screen': document.getElementById('welcome-screen'),
        'key-search-view': document.getElementById('key-search-view'),
        'filter-search-view': document.getElementById('filter-search-view'),
        'results-view': document.getElementById('results-view'),
        'stats-view': document.getElementById('stats-view'),
        'estados-view': document.getElementById('estados-view'),
        'plazas-por-estado-view': document.getElementById('plazas-por-estado-view'),
        'top-plazas-view': document.getElementById('top-plazas-view') 
    };

    // --- REFERENCIAS A ELEMENTOS DEL DOM ---
    // Búsqueda por Clave
    const claveInput = document.getElementById('clave-input');
    const searchByKeyButton = document.getElementById('search-by-key-button');
    const keyLoader = document.getElementById('key-loader');
    // --- REFERENCIAS PARA NAVEGACIÓN DE ESTADÍSTICAS ---
    let statsNavBtns = document.querySelectorAll('.stats-nav-btn');
    const statsSubviews = document.querySelectorAll('.stats-subview');
    
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
    const plazasOperacion = document.getElementById('plazas-operacion'); 
    const totalEstados = document.getElementById('total-estados');
    const estadoMasPlazasNombre = document.getElementById('estado-mas-plazas-nombre');
    const estadoMasPlazasCantidad = document.getElementById('estado-mas-plazas-cantidad');
    const estadoMayorConectividadNombre = document.getElementById('estado-mayor-conectividad-nombre');
    const estadoMayorConectividadPorcentaje = document.getElementById('estado-mayor-conectividad-porcentaje');
    const estadoMasOperacionNombre = document.getElementById('estado-mas-operacion-nombre');
    const estadoMasOperacionPorcentaje = document.getElementById('estado-mas-operacion-porcentaje');
    const estadoMasSuspensionNombre = document.getElementById('estado-mas-suspension-nombre');
    const estadoMasSuspensionPorcentaje = document.getElementById('estado-mas-suspension-porcentaje');

    // Referencias para Estadísticas (CN)
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
    const cnEstadosTbody = document.getElementById('cn-estados-tbody');

    // Estados con más plazas
    const estadosGrid = document.getElementById('estados-grid');
    const estadosSearchInput = document.getElementById('estados-search-input');

    // Vista de Plazas por Estado
    const plazasPorEstadoTitle = document.getElementById('plazas-por-estado-title');
    const plazasListContainer = document.getElementById('plazas-list-container');
    const plazasSearchInput = document.getElementById('plazas-search-input');

    // Referencias para Tabla de Análisis CN
    const analisisCNTable = document.getElementById('analisis-cn-table');
    const analisisCNTableBody = document.getElementById('analisis-cn-table-body');

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
    let analisisCNData = null;
    let analisisCNCurrentSort = { column: 1, direction: 'desc' }; // Para ordenamiento de la tabla CN

    // --- SISTEMA DE MODAL PARA IMÁGENES ---
    let modalOpenFunction = null;

    // --- SISTEMA DE TEMA ---
    const themeLight = document.getElementById('theme-light');
    const themeDark = document.getElementById('theme-dark');

    const applyTheme = (theme) => {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        
        if (theme === 'dark') {
            document.body.style.backgroundImage = "url('/static/noche.jpg')";
        } else {
            document.body.style.backgroundImage = "url('/static/claro.jpg')";
        }
        document.body.style.backgroundSize = "cover";
        
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
            // Inicializar navegación de estadísticas
            setTimeout(() => {
                initStatsNavigation();
                
                // Mostrar estadísticas generales por defecto
                const generalStatsView = document.getElementById('general-stats-view');
                const comparativasStatsView = document.getElementById('comparativas-stats-view');
                const generalStatsBtn = document.querySelector('[data-subview="general-stats"]');
                const comparativasStatsBtn = document.querySelector('[data-subview="comparativas-stats"]');
                
                // Ocultar todas las subvistas primero
                if (generalStatsView) generalStatsView.classList.add('hidden');
                if (comparativasStatsView) comparativasStatsView.classList.add('hidden');
                
                // Mostrar solo general-stats por defecto
                if (generalStatsView) {
                    generalStatsView.classList.remove('hidden');
                    console.log('📊 Vista general de estadísticas mostrada');
                    
                    // Inicializar análisis CN después de que la vista sea visible
                    setTimeout(() => {
                        initAnalisisCN();
                    }, 500);
                }
                
                // Actualizar botones activos
                if (generalStatsBtn && comparativasStatsBtn) {
                    generalStatsBtn.classList.add('active');
                    comparativasStatsBtn.classList.remove('active');
                }
            }, 100);
            
            if (!estadisticasData) {
                cargarEstadisticas();
            }
        }
        
        if (viewId === 'estados-view' && todosEstadosData.length === 0) {
            cargarEstadosConPlazas();
        }
    };
    
    const handleNavigation = () => {
        const viewId = window.location.hash.substring(1) || 'welcome-screen';
        
        if (!views[viewId]) {
            console.warn(`Vista no encontrada: ${viewId}`);
            showView('welcome-screen');
            return;
        }
        
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
        const alertDiv = document.createElement('div');
        alertDiv.className = type === 'error' ? 'alert' :
                             type === 'success' ? 'alert success' :
                             type === 'warning' ? 'alert warning' :
                             'alert info';
        alertDiv.textContent = message;
        
        alertContainer.innerHTML = '';
        alertContainer.appendChild(alertDiv);
        
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

    // Función auxiliar para buscar imágenes locales (fallback)
    const buscarImagenesLocales = async (clave_lower) => {
        try {
            console.log(`🔄 Buscando imágenes locales para: ${clave_lower}`);
            const response = await fetch(`/api/imagenes-local?clave=${encodeURIComponent(clave_lower)}`);
            if (response.ok) {
                const imagenesLocales = await response.json();
                if (imagenesLocales.length > 0) {
                    console.log(`📸 ${imagenesLocales.length} imágenes locales encontradas`);
                    return imagenesLocales;
                }
            }
            return [];
        } catch (error) {
            console.error('❌ Error en búsqueda local de imágenes:', error);
            return [];
        }
    };

    // ==== FUNCIÓN ACTUALIZADA PARA GOOGLE DRIVE ====
    const find_image_urls = async (clave_original) => {
        try {
            console.log(`🔍 Buscando imágenes para clave: ${clave_original}`);
            
            const clave_lower = clave_original.trim().toLowerCase();
            if (!clave_lower) return [];
            
            const driveTreeResponse = await fetch('/api/drive-tree');
            if (!driveTreeResponse.ok) {
                console.warn('❌ No se pudo cargar el árbol de Drive');
                return await buscarImagenesLocales(clave_lower);
            }
            
            const driveData = await driveTreeResponse.json();
            const imagenesEncontradas = [];
            
            function buscarEnArbol(arbol, carpetaTarget) {
                if (arbol.type === 'folder' && arbol.name.toLowerCase() === carpetaTarget) {
                    console.log(`✅ Carpeta encontrada: ${arbol.name} con ${arbol.children?.length || 0} elementos`);
                    
                    arbol.children?.forEach(archivo => {
                        if (archivo.type === 'file') {
                            let imageUrl = archivo.mediumUrl || 
                                           archivo.thumbnailUrl || 
                                           archivo.directUrl;
                            
                            if (imageUrl) {
                                imagenesEncontradas.push(imageUrl);
                                console.log(`📸 ${archivo.name}`);
                                console.log(`   📏 Tamaño: ${archivo.size} bytes`);
                                console.log(`   🔗 URL: ${imageUrl}`);
                            }
                        }
                    });
                    return true;
                }
                
                if (arbol.children) {
                    for (const hijo of arbol.children) {
                        if (buscarEnArbol(hijo, carpetaTarget)) return true;
                    }
                }
                return false;
            }
            
            const encontrado = buscarEnArbol(driveData.structure, clave_lower);
            
            if (encontrado && imagenesEncontradas.length > 0) {
                console.log(`🎉 ${imagenesEncontradas.length} imágenes encontradas para "${clave_lower}"`);
                return imagenesEncontradas;
            } else {
                console.warn(`⚠️ No hay imágenes en Drive para "${clave_lower}"`);
                return await buscarImagenesLocales(clave_original.trim().toLowerCase());
            }
            
        } catch (error) {
            console.error('❌ Error en búsqueda de imágenes:', error);
            return await buscarImagenesLocales(clave_original.trim().toLowerCase());
        }
    };

   
   // --- RENDERIZADO DE RESULTADOS ---
   // --- RENDERIZADO DE RESULTADOS COMPLETO (4 TABLAS + SELECTOR INTERACTIVO) ---
     const renderPlazaResultados = (data) => {
    const { excel_info, images, google_maps_url, direccion_completa, historial } = data;
    
    const template = document.getElementById('plaza-results-template');
    const clone = template.content.cloneNode(true);
    
    // --- 1. Cabecera y Dirección ---
    const clavePlazaElement = clone.querySelector('[data-bind="clave_plaza"]');
    if (clavePlazaElement && excel_info.Clave_Plaza) {
        clavePlazaElement.textContent = excel_info.Clave_Plaza;
    }
    
    const direccionElement = clone.querySelector('[data-bind="direccion_completa"]');
    if (direccionElement && direccion_completa) {
        const strong = document.createElement('strong');
        strong.textContent = 'Dirección:';
        direccionElement.innerHTML = '';
        direccionElement.appendChild(strong);
        direccionElement.appendChild(document.createTextNode(` ${direccion_completa}`));
    } else if (direccionElement) {
        direccionElement.style.display = 'none';
    }
    
    const mapsLink = clone.querySelector('[data-bind="google_maps_url"]');
    if (mapsLink && google_maps_url) {
        mapsLink.href = google_maps_url;
        mapsLink.textContent = 'Ver en Google Maps';
    } else if (mapsLink) {
        mapsLink.style.display = 'none';
    }

    // --- 2. DEFINICIÓN DE COLUMNAS (AHORA SON 5 TABLAS) ---
    
    const columnasUbicacion = [
       'Clave_Plaza' , 'Estado', 'Coord. Zona', 
        'Municipio', 'Localidad', 'Colonia', 
        'Cod_Post', 'Calle', 'Num', 
        'Nombre_PC', 'Situación', 
        'Latitud', 'Longitud' ,'Tipo_Conect' ,'Conect_Instalada'
    ];
    
    const columnasAtencion = [
        'Inc_Inicial', 'Inc_Prim', 'Inc_Sec', 'Inc_Total',
        'Aten_Inicial', 'Aten_Prim', 'Aten_Sec', 'Aten_Total',
        'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum',
        'Exámenes aplicados' , 'Cert_Emitidos'
    ];

    // NUEVA TABLA: Personal
    const columnasPersonal = [
        'Tec_Doc', 'Nom_PVS_1', 'Nom_PVS_2'
    ];

    const columnasInfraestructura = [
        'Tipo_local', 'Inst_aliada', 'Arq_Discap.'
    ];
    
    const columnasInventario = [
        'Total de equipos de cómputo en la plaza', 'Equipos de cómputo que operan',
        'Tipos de equipos de cómputo', 'Impresoras que funcionan',
        'Impresoras con suministros (toner, hojas)', 'Total de servidores en la plaza',
        'Número de servidores que funcionan correctamente', 'Cuantas mesas funcionan',
        'Cuantas sillas funcionan', 'Cuantas Anaqueles funcionan'
    ];

    // --- 3. FUNCIÓN RENDERIZADORA (CSS AISLADO) ---
    const renderizarGridAislado = (container, info, columns) => {
        container.innerHTML = '';
        
        const wrapper = document.createElement('div');
        wrapper.className = 'custom-plaza-table'; 
        
        const grid = document.createElement('div');
        grid.className = 'section-grid';
        
        columns.forEach(key => {
            const value = info[key];
            const displayValue = (value !== null && value !== undefined && value !== '') ? value : 'N/A';
            const displayKey = key.replace(/_/g, ' ');
            
            const item = document.createElement('div');
            item.className = 'data-item';
            
            const label = document.createElement('span');
            label.className = 'data-label';
            label.textContent = displayKey + ':';
            
            const val = document.createElement('span');
            val.className = 'data-value';
            val.textContent = displayValue;
            
            item.appendChild(label);
            item.appendChild(val);
            grid.appendChild(item);
        });
        
        wrapper.appendChild(grid);
        container.appendChild(wrapper);
    };

    // --- 4. RENDERIZADO DE LAS TABLAS (AHORA 5 TABLAS) ---

    // A. Tablas Estáticas (Ubicación, Infraestructura, Inventario, Personal)
    // Usan siempre los datos del último mes ('excel_info')
    const gridUbicacionEl = clone.querySelector('[data-bind="grid_ubicacion"]');
    const gridInfraEl = clone.querySelector('[data-bind="grid_infraestructura"]');
    const gridInventarioEl = clone.querySelector('[data-bind="grid_inventario"]');
    const gridPersonalEl = clone.querySelector('[data-bind="grid_personal"]'); // NUEVO ELEMENTO

    if (gridUbicacionEl) renderizarGridAislado(gridUbicacionEl, excel_info, columnasUbicacion);
    if (gridInfraEl) renderizarGridAislado(gridInfraEl, excel_info, columnasInfraestructura);
    if (gridInventarioEl) renderizarGridAislado(gridInventarioEl, excel_info, columnasInventario);
    if (gridPersonalEl) renderizarGridAislado(gridPersonalEl, excel_info, columnasPersonal); // NUEVA TABLA

    // B. Tabla Interactiva (Atención)
    // Esta tiene lógica especial para el selector de historial
    const gridAtencionEl = clone.querySelector('[data-bind="grid_atencion"]');
    
    // Buscamos o creamos el select dinámicamente si no está en el HTML base
    let selectAtencion = clone.querySelector('#atencion-periodo-select');
    
    // Si el HTML no tiene el select aún, lo inyectamos (opcional, por seguridad)
    if (!selectAtencion && gridAtencionEl) {
        const header = gridAtencionEl.previousElementSibling; // El H2
        if (header && header.tagName === 'H2') {
            const container = document.createElement('div');
            container.style.display = 'flex';
            container.style.justifyContent = 'space-between';
            container.style.alignItems = 'center';
            container.style.marginBottom = '15px';
            
            // Mover H2 adentro
            header.parentNode.insertBefore(container, header);
            container.appendChild(header);
            header.style.marginBottom = '0';
            
            // Crear select
            const selectDiv = document.createElement('div');
            selectDiv.innerHTML = `<select id="atencion-periodo-select" style="padding: 5px; border-radius: 4px; border: 1px solid #ccc; font-size: 0.9rem;"></select>`;
            container.appendChild(selectDiv);
            selectAtencion = selectDiv.querySelector('select');
        }
    }

    if (gridAtencionEl) {
        if (historial && historial.length > 0 && selectAtencion) {
            // Hay historial: Llenar select y activar interactividad
            selectAtencion.innerHTML = '';
            const mesesNombres = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"];

            historial.forEach((item, index) => {
                const option = document.createElement('option');
                option.value = index; // Índice en el array
                
                const mesNum = parseInt(item['Cve-mes'] || item['Mes'] || 0);
                const nombreMes = !isNaN(mesNum) && mesNum > 0 ? mesesNombres[mesNum] : (item['Mes'] || 'Mes ' + mesNum);
                const anio = item['Año'] || '';
                
                option.textContent = `${nombreMes} ${anio}`;
                selectAtencion.appendChild(option);
            });

            // Evento al cambiar
            selectAtencion.addEventListener('change', (e) => {
                const idx = e.target.value;
                const datosMes = historial[idx];
                renderizarGridAislado(gridAtencionEl, datosMes, columnasAtencion);
            });

            // Render inicial (índice 0 = más reciente)
            renderizarGridAislado(gridAtencionEl, historial[0], columnasAtencion);
            
        } else {
            // No hay historial o falló: Usar datos estáticos y ocultar select
            if (selectAtencion) selectAtencion.style.display = 'none';
            renderizarGridAislado(gridAtencionEl, excel_info, columnasAtencion);
        }
    }

    // --- 5. Imágenes (Estilo Original) ---
    const imagesContainer = clone.querySelector('[data-bind="images_grid"]');
    if (imagesContainer) {
        imagesContainer.innerHTML = '';
        if (images?.length > 0) {
            const imageTemplate = document.getElementById('image-item-template');
            images.forEach((url, index) => {
                const imageClone = imageTemplate.content.cloneNode(true);
                const img = imageClone.querySelector('img');
                img.src = url;
                img.alt = `Imagen ${index + 1}`;
                img.loading = "lazy";
                imagesContainer.appendChild(imageClone);
            });
        } else {
            const noImagesTemplate = document.getElementById('no-images-template');
            if(noImagesTemplate) imagesContainer.appendChild(noImagesTemplate.content.cloneNode(true));
        }
    }
    
    // --- 6. Insertar en el DOM ---
    resultsContent.innerHTML = '';
    resultsContent.appendChild(clone);
    
    // --- 7. Configuración Modal ---
    setTimeout(() => {
        const imageContainers = document.querySelectorAll('.image-container');
        const allImages = images || [];
        imageContainers.forEach((container, index) => {
            container.style.cursor = 'pointer';
            const newContainer = container.cloneNode(true);
            container.parentNode.replaceChild(newContainer, container);
            newContainer.addEventListener('click', () => {
                if (allImages.length > 0 && modalOpenFunction) modalOpenFunction(allImages, index);
            });
        });
    }, 100);
};

    // Nueva función para renderizar datos organizados
    const renderDatosOrganizados = (data) => {
        const { datos_organizados, images, google_maps_url } = data;
        
        if (!datos_organizados) {
            console.error('No hay datos organizados para renderizar');
            return;
        }
        
        const template = document.getElementById('plaza-results-template');
        const clone = template.content.cloneNode(true);
        
        // Información general
        const infoGeneral = datos_organizados.informacion_general || {};
        const ubicacion = datos_organizados.ubicacion || {};
        
        const clavePlazaElement = clone.querySelector('[data-bind="clave_plaza"]');
        if (clavePlazaElement && infoGeneral.Clave_Plaza) {
            clavePlazaElement.textContent = infoGeneral.Clave_Plaza;
        }
        
        const nombrePCElement = clone.querySelector('[data-bind="nombre_pc"]');
        if (nombrePCElement && infoGeneral.Nombre_PC) {
            nombrePCElement.textContent = infoGeneral.Nombre_PC;
        }
        
        const situacionElement = clone.querySelector('[data-bind="situacion"]');
        if (situacionElement && infoGeneral.Situación) {
            situacionElement.textContent = infoGeneral.Situación;
        }
        
        // Dirección completa
        const direccionElement = clone.querySelector('[data-bind="direccion_completa"]');
        if (direccionElement && ubicacion.Direccion_Completa) {
            const strong = document.createElement('strong');
            strong.textContent = 'Dirección:';
            direccionElement.innerHTML = '';
            direccionElement.appendChild(strong);
            direccionElement.appendChild(document.createTextNode(` ${ubicacion.Direccion_Completa}`));
        }
        
        // Google Maps
        const mapsLink = clone.querySelector('[data-bind="google_maps_url"]');
        if (mapsLink && google_maps_url) {
            mapsLink.href = google_maps_url;
            mapsLink.textContent = 'Ver en Google Maps';
        }
        
        // Función para crear secciones organizadas
        const crearSeccion = (titulo, datos) => {
            if (!datos || Object.keys(datos).length === 0) return null;
            
            const section = document.createElement('div');
            section.className = 'data-section';
            
            const sectionTitle = document.createElement('h3');
            sectionTitle.className = 'section-title';
            sectionTitle.textContent = titulo;
            section.appendChild(sectionTitle);
            
            const sectionGrid = document.createElement('div');
            sectionGrid.className = 'section-grid';
            
            Object.entries(datos).forEach(([key, value]) => {
                if (value !== null && value !== undefined) {
                    const item = document.createElement('div');
                    item.className = 'data-item';
                    
                    const label = document.createElement('span');
                    label.className = 'data-label';
                    label.textContent = key.replace(/_/g, ' ') + ':';
                    
                    const val = document.createElement('span');
                    val.className = 'data-value';
                    val.textContent = value;
                    
                    item.appendChild(label);
                    item.appendChild(val);
                    sectionGrid.appendChild(item);
                }
            });
            
            section.appendChild(sectionGrid);
            return section;
        };
        
        // Reemplazar la lógica de grid por secciones organizadas
        const gridContainer = clone.querySelector('[data-bind="grid_container"]');
        if (gridContainer) {
            gridContainer.innerHTML = '';
            
            // Sección 1: Información General
            const seccionInfoGeneral = crearSeccion('📋 Información General', infoGeneral);
            if (seccionInfoGeneral) gridContainer.appendChild(seccionInfoGeneral);
            
            // Sección 2: Ubicación
            const seccionUbicacion = crearSeccion('📍 Ubicación', ubicacion);
            if (seccionUbicacion) gridContainer.appendChild(seccionUbicacion);
            
            // Sección 3: Fecha y Período
            const fechaPeriodo = datos_organizados.fecha_periodo || {};
            const seccionFecha = crearSeccion('📅 Fecha y Período', fechaPeriodo);
            if (seccionFecha) gridContainer.appendChild(seccionFecha);
            
            // Sección 4: Inscripciones
            const inscripciones = datos_organizados.incripciones || {};
            const seccionInscripciones = crearSeccion('📝 Inscripciones', inscripciones);
            if (seccionInscripciones) gridContainer.appendChild(seccionInscripciones);
            
            // Sección 5: Atenciones
            const atenciones = datos_organizados.atenciones || {};
            const seccionAtenciones = crearSeccion('🎓 Atenciones', atenciones);
            if (seccionAtenciones) gridContainer.appendChild(seccionAtenciones);
            
            // Sección 6: Certificaciones
            const certificaciones = datos_organizados.certificaciones || {};
            const seccionCertificaciones = crearSeccion('🏆 Certificaciones', certificaciones);
            if (seccionCertificaciones) gridContainer.appendChild(seccionCertificaciones);
            
            // Sección 7: Personal
            const personal = datos_organizados.personal || {};
            const seccionPersonal = crearSeccion('👥 Personal', personal);
            if (seccionPersonal) gridContainer.appendChild(seccionPersonal);
            
            // Sección 8: Equipamiento
            const equipamiento = datos_organizados.equipamiento || {};
            const seccionEquipamiento = crearSeccion('💻 Equipamiento', equipamiento);
            if (seccionEquipamiento) gridContainer.appendChild(seccionEquipamiento);
            
            // Sección 9: Mobiliario
            const mobiliario = datos_organizados.mobiliario || {};
            const seccionMobiliario = crearSeccion('🪑 Mobiliario', mobiliario);
            if (seccionMobiliario) gridContainer.appendChild(seccionMobiliario);
        }
        
        // Imágenes
        const imagesContainer = clone.querySelector('[data-bind="images_grid"]');
        if (imagesContainer) {
            imagesContainer.innerHTML = '';
            
            if (images?.length > 0) {
                const imageTemplate = document.getElementById('image-item-template');
                
                images.forEach((url, index) => {
                    const imageClone = imageTemplate.content.cloneNode(true);
                    const img = imageClone.querySelector('img');
                    img.src = url;
                    img.alt = `Imagen de la plaza ${index + 1}`;
                    
                    imagesContainer.appendChild(imageClone);
                });
            } else {
                const noImagesTemplate = document.getElementById('no-images-template');
                const noImagesClone = noImagesTemplate.content.cloneNode(true);
                imagesContainer.appendChild(noImagesClone);
            }
        }
        
        resultsContent.innerHTML = '';
        resultsContent.appendChild(clone);
        
        // Configurar modal de imágenes
        setTimeout(() => {
            const imageContainers = document.querySelectorAll('.image-container');
            const allImages = images || [];
            
            imageContainers.forEach((container, index) => {
                container.style.cursor = 'pointer';
                container.replaceWith(container.cloneNode(true));
                
                const newContainer = document.querySelectorAll('.image-container')[index];
                newContainer.addEventListener('click', () => {
                    if (allImages.length > 0 && modalOpenFunction) {
                        modalOpenFunction(allImages, index);
                    }
                });
            });
        }, 100);
    };

    // --- BÚSQUEDA Y FILTROS ---
    const buscarYMostrarClave = async (clave, loader) => {
     if (!clave) {
            showAlert('Por favor, introduce una clave válida', 'warning');
            return;
        }
        
        showLoader(`Buscando plaza con clave: ${clave}`);
        
        try {
            // 1. Usar el endpoint de búsqueda principal
            const data = await fetchData(`/api/search?clave=${encodeURIComponent(clave)}`);
            
            if (!data || (!data.excel_info && !data.datos_organizados)) {
                throw new Error('No se encontraron datos para esta clave');
            }

            // 2. Obtener historial (NUEVO ENDPOINT) para el selector de fechas
            let historial = [];
            try {
                historial = await fetchData(`/api/plaza-historial?clave=${encodeURIComponent(clave)}`);
            } catch (e) {
                console.warn("No se pudo cargar el historial", e);
            }
            
            console.log(`🔄 Buscando imágenes para: ${clave}`);
            const imagenesDrive = await find_image_urls(clave);
            
            // 3. Preparar el objeto de datos completo
            let datosCompletos;
            
            if (data.datos_organizados) {
                // Aplanamos todas las categorías para que 'excel_info' tenga todos los campos por defecto (último mes)
                const infoAplanada = {
                    ...(data.datos_organizados.informacion_general || {}),
                    ...(data.datos_organizados.ubicacion || {}),
                    ...(data.datos_organizados.fecha_periodo || {}),
                    ...(data.datos_organizados.incripciones || {}),
                    ...(data.datos_organizados.atenciones || {}),
                    ...(data.datos_organizados.certificaciones || {}),
                    ...(data.datos_organizados.personal || {}),
                    ...(data.datos_organizados.equipamiento || {}),
                    ...(data.datos_organizados.mobiliario || {})
                };

                datosCompletos = {
                    ...data,
                    images: imagenesDrive.length > 0 ? imagenesDrive : (data.images || []),
                    excel_info: infoAplanada, // Datos planos para carga inicial
                    historial: historial      // Historial para la interactividad
                };
            } else {
                // Fallback por si el backend responde con el formato antiguo
                datosCompletos = {
                    ...data,
                    images: imagenesDrive.length > 0 ? imagenesDrive : (data.images || []),
                    historial: historial
                };
            }
            
            // 4. Renderizar resultados
            renderPlazaResultados(datosCompletos);
            
            history.pushState({ view: 'results-view' }, '', '#results-view');
            handleNavigation();
            
        } catch (error) {
            console.error('Error en búsqueda:', error);
            showAlert(`Error al buscar la clave: ${error.message}`, 'error');
        } finally {
            hideLoader();
            if (loader) setLoaderVisible(loader, false);
        }
    };
    window.buscarYMostrarClave = buscarYMostrarClave;
    
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
        
        const loadingOption = document.createElement('option');
        loadingOption.value = '';
        loadingOption.textContent = 'Cargando...';
        selectElement.innerHTML = '';
        selectElement.appendChild(loadingOption);
        
        setLoaderVisible(filterLoader, true);
        try {
            const options = await fetchData(url);
            selectElement.innerHTML = '';
            
            const defaultOption = document.createElement('option');
            defaultOption.value = '';
            defaultOption.textContent = `-- ${placeholder} --`;
            selectElement.appendChild(defaultOption);
            
            if (options.length > 0) {
                options.forEach(option => {
                    const optionElement = document.createElement('option');
                    optionElement.value = option;
                    optionElement.textContent = option;
                    selectElement.appendChild(optionElement);
                });
                selectElement.disabled = false;
            } else {
                const noOptions = document.createElement('option');
                noOptions.value = '';
                noOptions.textContent = 'No hay opciones';
                selectElement.appendChild(noOptions);
            }
        } catch (error) {
            showAlert(`Error al cargar: ${error.message}`, 'error');
            const errorOption = document.createElement('option');
            errorOption.value = '';
            errorOption.textContent = 'Error';
            selectElement.innerHTML = '';
            selectElement.appendChild(errorOption);
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

    const handleFilterSearch = () => {
        const clave = selects.clave.value;
        if (clave) {
            buscarYMostrarClave(clave, filterLoader);
        } else {
            showAlert('Por favor, completa todos los filtros hasta seleccionar una clave de plaza.', 'warning');
        }
    };

    const handleSearchByKey = () => {
        const clave = claveInput.value.trim();
        if (clave) {
            buscarYMostrarClave(clave, keyLoader);
        } else {
            showAlert('Por favor, introduce una clave para buscar.', 'warning');
        }
    };
    
    const addAutoSearchToggle = () => {
        const searchButton = document.getElementById('search-filter-button');
        if (!searchButton) return;
        
        const toggleContainer = document.createElement('div');
        toggleContainer.className = 'auto-search-toggle';
        toggleContainer.style.margin = '1rem 0';
        toggleContainer.style.display = 'flex';
        toggleContainer.style.alignItems = 'center';
        toggleContainer.style.gap = '0.5rem';
        
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = 'auto-search-toggle';
        checkbox.style.margin = '0';
        
        const label = document.createElement('label');
        label.htmlFor = 'auto-search-toggle';
        label.style.fontSize = '0.875rem';
        label.style.color = 'var(--text-muted)';
        label.style.cursor = 'pointer';
        label.textContent = 'Búsqueda automática al seleccionar clave';
        
        toggleContainer.appendChild(checkbox);
        toggleContainer.appendChild(label);
        searchButton.insertAdjacentElement('beforebegin', toggleContainer);
    };

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
    if (!cnResumenCards || !cnResumenData?.resumen_nacional) {
        console.warn('No hay datos de resumen CN para renderizar');
        return;
    }
    
    const { resumen_nacional, top5_estados_por_CN_Total } = cnResumenData;
    
    cnResumenCards.innerHTML = '';
    
    // Resumen Nacional
    const resumenCard = document.createElement('div');
    resumenCard.className = 'cn-card';
    
    const tituloResumen = document.createElement('h4');
    tituloResumen.textContent = '📊 Resumen Nacional';
    
    const statsGrid = document.createElement('div');
    statsGrid.className = 'cn-stats-grid';
    
    const categoriasMostrar = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Total'];
    
    categoriasMostrar.forEach(key => {
        const data = resumen_nacional[key];
        if (!data) return;
        
        const statItem = document.createElement('div');
        statItem.className = `cn-stat-item ${key === 'CN_Total' ? 'cn-total-item' : ''}`;
        
        const nombre = key === 'CN_Total' ? 'CN TOTAL' : key.replace(/_/g, ' ');
        
        const label = document.createElement('span');
        label.className = 'cn-stat-label';
        label.textContent = nombre;
        
        const value = document.createElement('span');
        value.className = 'cn-stat-value';
        value.textContent = data.suma.toLocaleString();
        
        const subvalue = document.createElement('span');
        subvalue.className = 'cn-stat-subvalue';
        subvalue.textContent = `Plazas con actividad: ${data.plazasOperacion.toLocaleString()}`;
        
        statItem.appendChild(label);
        statItem.appendChild(value);
        statItem.appendChild(subvalue);
        statsGrid.appendChild(statItem);
    });
    
    resumenCard.appendChild(tituloResumen);
    resumenCard.appendChild(statsGrid);
    cnResumenCards.appendChild(resumenCard);
    
    // Top 5 estados - solo si viene en los datos
    if (top5_estados_por_CN_Total && top5_estados_por_CN_Total.length > 0) {
        const top5Card = document.createElement('div');
        top5Card.className = 'cn-card';
        
        const tituloTop5 = document.createElement('h4');
        tituloTop5.textContent = '🏆 Top 5 Estados - CN Total';
        
        const top5Grid = document.createElement('div');
        top5Grid.className = 'cn-stats-grid';
        
        top5_estados_por_CN_Total.forEach((item, index) => {
            const medal = index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : '🏅';
            
            const statItem = document.createElement('div');
            statItem.className = 'cn-stat-item';
            
            const label = document.createElement('span');
            label.className = 'cn-stat-label';
            label.textContent = `${medal} ${item.estado}`;
            
            const value = document.createElement('span');
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

const renderTablaEstadosCN = () => {
    if (!cnEstadosTable || !cnPorEstadoData?.estados) return;
    
    const { estados } = cnPorEstadoData;
    
    // Calcular totales
    const totales = {
        total_plazas: 0,
        suma_CN_Inicial_Acum: 0,
        suma_CN_Prim_Acum: 0,
        suma_CN_Sec_Acum: 0,
        suma_CN_Total: 0,
        pct_sobre_nacional: 100 // Siempre será 100% para el total nacional
    };
    
    // Sumar todos los valores
    estados.forEach(estado => {
        totales.total_plazas += estado.total_plazas;
        totales.suma_CN_Inicial_Acum += estado.suma_CN_Inicial_Acum;
        totales.suma_CN_Prim_Acum += estado.suma_CN_Prim_Acum;
        totales.suma_CN_Sec_Acum += estado.suma_CN_Sec_Acum;
        totales.suma_CN_Total += estado.suma_CN_Total;
    });
    
    // Limpiar tabla
    cnEstadosTable.innerHTML = '';
    
    // Crear thead
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    
    const headers = [
        { text: 'Estado', sort: 'estado', order: 'asc' },
        { text: 'Total Plazas', sort: 'total_plazas', order: 'desc' },
        { text: 'CN Inicial', sort: 'cn_inicial', order: 'desc' },
        { text: 'CN Primaria', sort: 'cn_primaria', order: 'desc' },
        { text: 'CN Secundaria', sort: 'cn_secundaria', order: 'desc' },
        { text: 'CN Total', sort: 'cn_total', order: 'desc' },
        { text: '% Sobre Nacional', sort: 'pct_nacional', order: 'desc' }
    ];
    
    headers.forEach(header => {
        const th = document.createElement('th');
        const button = document.createElement('button');
        button.className = 'sort-btn';
        button.setAttribute('data-sort', header.sort);
        button.setAttribute('data-order', header.order);
        button.textContent = `${header.text} ${header.order === 'asc' ? '▲' : '▼'}`;
        th.appendChild(button);
        headerRow.appendChild(th);
    });
    
    thead.appendChild(headerRow);
    cnEstadosTable.appendChild(thead);
    
    // Crear tbody con un ID para el contenedor de scroll
    const tbody = document.createElement('tbody');
    tbody.id = 'tbody-estados';
    
    estados.forEach(estado => {
        const row = document.createElement('tr');
        
        const crearCelda = (contenido, esStrong = false, badgeClass = '') => {
            const td = document.createElement('td');
            if (esStrong) {
                const strong = document.createElement('strong');
                strong.textContent = contenido;
                td.appendChild(strong);
            } else if (badgeClass) {
                const badge = document.createElement('span');
                badge.className = `cn-badge ${badgeClass}`;
                badge.textContent = contenido;
                td.appendChild(badge);
            } else {
                td.textContent = contenido;
            }
            return td;
        };
        
        row.appendChild(crearCelda(estado.estado, true));
        row.appendChild(crearCelda(estado.total_plazas.toLocaleString()));
        row.appendChild(crearCelda(estado.suma_CN_Inicial_Acum.toLocaleString()));
        row.appendChild(crearCelda(estado.suma_CN_Prim_Acum.toLocaleString()));
        row.appendChild(crearCelda(estado.suma_CN_Sec_Acum.toLocaleString()));
        row.appendChild(crearCelda(estado.suma_CN_Total.toLocaleString(), false, 'badge-primary'));
        row.appendChild(crearCelda(`${estado.pct_sobre_nacional}%`, false, 'badge-info'));
        
        tbody.appendChild(row);
    });
    
    cnEstadosTable.appendChild(tbody);
    
    // Crear tfoot para los totales (STICKY)
    const tfoot = document.createElement('tfoot');
    tfoot.id = 'totales-footer';
    
    const totalRow = document.createElement('tr');
    totalRow.className = 'total-row-sticky';
    
    totalRow.appendChild(crearCelda('TOTAL NACIONAL', true));
    totalRow.appendChild(crearCelda(totales.total_plazas.toLocaleString(), false, 'badge-secondary'));
    totalRow.appendChild(crearCelda(totales.suma_CN_Inicial_Acum.toLocaleString(), false, 'badge-secondary'));
    totalRow.appendChild(crearCelda(totales.suma_CN_Prim_Acum.toLocaleString(), false, 'badge-secondary'));
    totalRow.appendChild(crearCelda(totales.suma_CN_Sec_Acum.toLocaleString(), false, 'badge-secondary'));
    totalRow.appendChild(crearCelda(totales.suma_CN_Total.toLocaleString(), false, 'badge-success'));
    totalRow.appendChild(crearCelda(`${totales.pct_sobre_nacional}%`, false, 'badge-info'));
    
    tfoot.appendChild(totalRow);
    cnEstadosTable.appendChild(tfoot);
    
    // Agregar estilos CSS dinámicamente si no existen
    agregarEstilosSticky();
    
    // También necesitas actualizar la función updateTableRows en setupSorting para incluir los totales
    updateSortingWithTotalesSticky(totales);
};

// Función para agregar estilos CSS para el sticky
const agregarEstilosSticky = () => {
    // Verificar si los estilos ya existen
    if (document.getElementById('estilos-sticky-totales')) return;
    
    const style = document.createElement('style');
    style.id = 'estilos-sticky-totales';
    style.textContent = `
        .cn-table-container {
            position: relative;
            max-height: 500px;
            overflow-y: auto;
        }
        
        .cn-table {
            margin-bottom: 0;
        }
        
        .cn-table thead {
            position: sticky;
            top: 0;
            background-color: #f8fafc;
            z-index: 20;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        #totales-footer {
            position: sticky;
            bottom: 0;
            background-color: #f8f9fa;
            z-index: 10;
            box-shadow: 0 -2px 4px rgba(0,0,0,0.1);
        }
        
        .total-row-sticky {
            background-color: #f8f9fa;
            font-weight: bold;
            border-top: 2px solid #dee2e6;
        }
        
        .total-row-sticky td {
            padding: 12px 8px;
        }
        
        .cn-badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }
        
        .badge-primary {
            background-color: #007bff;
            color: white;
        }
        
        .badge-secondary {
            background-color: #6c757d;
            color: white;
        }
        
        .badge-success {
            background-color: #28a745;
            color: white;
        }
        
        .badge-info {
            background-color: #17a2b8;
            color: white;
        }
        
        /* Asegurar que las celdas tengan el mismo ancho en header, body y footer */
        .cn-table th, .cn-table td {
            padding: 12px 8px;
            text-align: center;
        }
        
        .cn-table th {
            padding: 15px 8px;
        }
    `;
    
    document.head.appendChild(style);
};

// Función auxiliar para crear celdas (también se usa en setupSorting)
const crearCelda = (contenido, esStrong = false, badgeClass = '') => {
    const td = document.createElement('td');
    if (esStrong) {
        const strong = document.createElement('strong');
        strong.textContent = contenido;
        td.appendChild(strong);
    } else if (badgeClass) {
        const badge = document.createElement('span');
        badge.className = `cn-badge ${badgeClass}`;
        badge.textContent = contenido;
        td.appendChild(badge);
    } else {
        td.textContent = contenido;
    }
    return td;
};

// Función para calcular totales
const calcularTotales = (estados) => {
    const totales = {
        total_plazas: 0,
        suma_CN_Inicial_Acum: 0,
        suma_CN_Prim_Acum: 0,
        suma_CN_Sec_Acum: 0,
        suma_CN_Total: 0,
        pct_sobre_nacional: 100
    };
    
    estados.forEach(estado => {
        totales.total_plazas += estado.total_plazas;
        totales.suma_CN_Inicial_Acum += estado.suma_CN_Inicial_Acum;
        totales.suma_CN_Prim_Acum += estado.suma_CN_Prim_Acum;
        totales.suma_CN_Sec_Acum += estado.suma_CN_Sec_Acum;
        totales.suma_CN_Total += estado.suma_CN_Total;
    });
    
    return totales;
};

// Actualizar la función para incluir totales en el sorting (versión sticky)
const updateSortingWithTotalesSticky = (totales) => {
    const sortButtons = document.querySelectorAll('.sort-btn');
    if (sortButtons.length === 0) return;
    
    let currentData = [...cnPorEstadoData.estados];
    
    sortButtons.forEach(button => {
        button.addEventListener('click', () => {
            const sortBy = button.getAttribute('data-sort');
            const currentOrder = button.getAttribute('data-order');
            const newOrder = currentOrder === 'asc' ? 'desc' : 'asc';
            
            sortButtons.forEach(btn => {
                btn.textContent = btn.textContent.replace('▲', '').replace('▼', '');
                btn.setAttribute('data-order', 'desc');
            });
            
            button.setAttribute('data-order', newOrder);
            button.textContent = button.textContent.replace('▲', '').replace('▼', '') + (newOrder === 'asc' ? ' ▲' : ' ▼');
            
            sortTableDataWithTotalesSticky(sortBy, newOrder, currentData, totales);
        });
    });
    
    const sortTableDataWithTotalesSticky = (sortBy, order, data, totales) => {
        const sortedData = [...data].sort((a, b) => {
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
                    
                case 'pct_nacional':
                    valueA = a.pct_sobre_nacional;
                    valueB = b.pct_sobre_nacional;
                    break;
                    
                default:
                    return 0;
            }
            
            return order === 'asc' ? valueA - valueB : valueB - valueA;
        });
        
        updateTableRowsWithTotalesSticky(sortedData, totales);
    };
    
    const updateTableRowsWithTotalesSticky = (data, totales) => {
        const tbody = document.getElementById('tbody-estados');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        // Filas de datos
        data.forEach(estado => {
            const row = document.createElement('tr');
            
            row.appendChild(crearCelda(estado.estado, true));
            row.appendChild(crearCelda(estado.total_plazas.toLocaleString()));
            row.appendChild(crearCelda(estado.suma_CN_Inicial_Acum.toLocaleString()));
            row.appendChild(crearCelda(estado.suma_CN_Prim_Acum.toLocaleString()));
            row.appendChild(crearCelda(estado.suma_CN_Sec_Acum.toLocaleString()));
            row.appendChild(crearCelda(estado.suma_CN_Total.toLocaleString(), false, 'badge-primary'));
            row.appendChild(crearCelda(`${estado.pct_sobre_nacional}%`, false, 'badge-info'));
            
            tbody.appendChild(row);
        });
        
        // El footer de totales ya está en el tfoot y se mantiene sticky automáticamente
        // Solo actualizamos los totales en el footer existente si es necesario
        const totalesFooter = document.getElementById('totales-footer');
        if (totalesFooter) {
            const totalRow = totalesFooter.querySelector('.total-row-sticky');
            if (totalRow) {
                totalRow.innerHTML = '';
                totalRow.appendChild(crearCelda('TOTAL NACIONAL', true));
                totalRow.appendChild(crearCelda(totales.total_plazas.toLocaleString(), false, 'badge-secondary'));
                totalRow.appendChild(crearCelda(totales.suma_CN_Inicial_Acum.toLocaleString(), false, 'badge-secondary'));
                totalRow.appendChild(crearCelda(totales.suma_CN_Prim_Acum.toLocaleString(), false, 'badge-secondary'));
                totalRow.appendChild(crearCelda(totales.suma_CN_Sec_Acum.toLocaleString(), false, 'badge-secondary'));
                totalRow.appendChild(crearCelda(totales.suma_CN_Total.toLocaleString(), false, 'badge-success'));
                totalRow.appendChild(crearCelda(`${totales.pct_sobre_nacional}%`, false, 'badge-info'));
            }
        }
    };
};

const renderEstadisticasCN = () => {
    if (!cnResumenData || !cnPorEstadoData || !cnTopEstadosData) {
        console.warn('Datos CN incompletos');
        return;
    }
    renderResumenCN();
    renderTablaEstadosCN();
};

const renderEstadosDestacadosCN = (estadosDestacados) => {
    if (!estadosDestacados) return;
    
    // Asegurar que existen los elementos DOM
    if (estadoMasCNInicialNombre && estadosDestacados.CN_Inicial_Acum) {
        estadoMasCNInicialNombre.textContent = estadosDestacados.CN_Inicial_Acum.estado;
        estadoMasCNInicialCantidad.textContent = estadosDestacados.CN_Inicial_Acum.valor.toLocaleString();
    }
    if (estadoMasCNPrimariaNombre && estadosDestacados.CN_Prim_Acum) {
        estadoMasCNPrimariaNombre.textContent = estadosDestacados.CN_Prim_Acum.estado;
        estadoMasCNPrimariaCantidad.textContent = estadosDestacados.CN_Prim_Acum.valor.toLocaleString();
    }
    if (estadoMasCNSecundariaNombre && estadosDestacados.CN_Sec_Acum) {
        estadoMasCNSecundariaNombre.textContent = estadosDestacados.CN_Sec_Acum.estado;
        estadoMasCNSecundariaCantidad.textContent = estadosDestacados.CN_Sec_Acum.valor.toLocaleString();
    }
};
const renderTop5TodosCN = (top5Todos) => {
    if (!top5Todos) return;
    
    const renderList = (listElement, data) => {
        if (!listElement || !data) return;
        listElement.innerHTML = '';
        
        data.forEach((item, index) => {
            const itemDiv = document.createElement('div');
            itemDiv.className = 'top10-item';
            
            const rank = document.createElement('span');
            rank.className = 'top10-rank';
            rank.textContent = `#${index + 1}`;
            
            const state = document.createElement('span');
            state.className = 'top10-state';
            state.textContent = item.estado;
            
            const value = document.createElement('span');
            value.className = 'top10-value';
            value.textContent = item.valor.toLocaleString();
            
            itemDiv.appendChild(rank);
            itemDiv.appendChild(state);
            itemDiv.appendChild(value);
            listElement.appendChild(itemDiv);
        });
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
        
        // Verificar que los datos vengan correctamente
        cnResumenData = resumen;
        cnPorEstadoData = porEstado;
        cnTopEstadosData = topEstados;
        cnEstadosDestacadosData = estadosDestacados;
        cnTop5TodosData = top5Todos;
        
        console.log('Datos CN cargados:', {
            resumen: cnResumenData,
            porEstado: cnPorEstadoData
        });
        
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
        console.log('Estadísticas generales recibidas:', stats);
        
        // 🔥 CAMBIO CRÍTICO: Inicializar cnResumenData si no existe
        if (!cnResumenData) {
            cnResumenData = {};
        }
        
        // 🔥 CONEXIÓN DIRECTA: Guardar el resumen_nacional del backend en cnResumenData
        if (stats.resumen_nacional) {
            cnResumenData.resumen_nacional = stats.resumen_nacional;
            console.log('resumen_nacional cargado desde /api/estadisticas:', stats.resumen_nacional);
            
            // Renderizar inmediatamente si el elemento existe
            if (cnResumenCards) {
                renderResumenCN();
            }
        }

        // Cargar las estadísticas generales
        if (totalPlazas) totalPlazas.textContent = stats.totalPlazas?.toLocaleString() || '0';
        if (plazasOperacion) plazasOperacion.textContent = stats.plazasOperacion?.toLocaleString() || '0';
        if (totalEstados) totalEstados.textContent = stats.totalEstados?.toLocaleString() || '0';
        if (estadoMasPlazasNombre) estadoMasPlazasNombre.textContent = stats.estadoMasPlazas?.nombre || 'N/A';
        if (estadoMasPlazasCantidad) estadoMasPlazasCantidad.textContent = stats.estadoMasPlazas?.cantidad?.toLocaleString() || '0';
        if (estadoMayorConectividadNombre) estadoMayorConectividadNombre.textContent = stats.estadoMayorConectividad?.nombre || 'N/A';
        if (estadoMayorConectividadPorcentaje) estadoMayorConectividadPorcentaje.textContent = `${stats.estadoMayorConectividad?.porcentaje || 0}%`;
        if (estadoMasOperacionNombre) estadoMasOperacionNombre.textContent = stats.estadoMasOperacion?.nombre || 'N/A';
        if (estadoMasOperacionPorcentaje) estadoMasOperacionPorcentaje.textContent = `${stats.estadoMasOperacion?.porcentaje || 0}%`;
        if (estadoMasSuspensionNombre) estadoMasSuspensionNombre.textContent = stats.estadoMasSuspension?.nombre || 'N/A';
        if (estadoMasSuspensionPorcentaje) estadoMasSuspensionPorcentaje.textContent = `${stats.estadoMasSuspension?.porcentaje || 0}%`;

        // Cargar las estadísticas completas de CN
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

// ==========================================
// ANÁLISIS CN - FUNCIONES MEJORADAS (EXPANDIDO 14 COLUMNAS)
// ==========================================

// Función para inicializar el análisis CN
const initAnalisisCN = () => {
    console.log('🚀 Inicializando análisis CN (versión expandida)...');
    
    const table = document.getElementById('analisis-cn-table');
    if (!table) {
        console.error('La tabla de análisis CN no existe en el DOM');
        return;
    }
    
    // Configurar ordenamiento después de un momento
    setTimeout(() => {
        setupAnalisisCNTableSorting();
    }, 500);
    
    // Cargar datos
    if (!analisisCNData) {
        cargarAnalisisCN();
    } else if (analisisCNData.desglose_estados && analisisCNData.desglose_estados.length > 0) {
        renderAnalisisCNTable(analisisCNData.desglose_estados);
        
        setTimeout(() => {
            sortAnalisisCNTable(analisisCNCurrentSort.column, analisisCNCurrentSort.direction);
        }, 500);
    } else {
        cargarAnalisisCN();
    }
};

// 1. NUEVA FUNCIÓN: Definir las 14 columnas del encabezado
const rebuildAnalisisCNHeaders = () => {
    const table = document.getElementById('analisis-cn-table');
    if (!table) return;

    let thead = table.querySelector('thead');
    if (!thead) {
        thead = document.createElement('thead');
        table.prepend(thead);
    }

    // Estructura: Estado + Generales (3) + Inicial (3) + Primaria (3) + Secundaria (3) + Total (1) = 14 Cols
    thead.innerHTML = `
        <tr>
            <th><button class="sort-btn" data-sort="estado" data-order="asc">Estado ▼</button></th>
            
            <th class="text-center"><button class="sort-btn" data-sort="total_plazas" data-order="desc">Total ▼</button></th>
            <th class="text-center"><button class="sort-btn" data-sort="plazas_activas" data-order="desc">Con actividad ▼</button></th>
            <th class="text-center"><button class="sort-btn" data-sort="porcentaje" data-order="desc">% Efic. ▼</button></th>
            
            <th class="text-center bg-gray-50"><button class="sort-btn" data-sort="ini_plazas" data-order="desc">Ini: Plazas ▼</button></th>
            <th class="text-center bg-gray-50"><button class="sort-btn" data-sort="ini_pct" data-order="desc">Ini: % ▼</button></th>
            <th class="text-center bg-gray-50"><button class="sort-btn" data-sort="ini_cn" data-order="desc">Ini: CN ▼</button></th>
            
            <th class="text-center"><button class="sort-btn" data-sort="prim_plazas" data-order="desc">Prim: Plazas ▼</button></th>
            <th class="text-center"><button class="sort-btn" data-sort="prim_pct" data-order="desc">Prim: % ▼</button></th>
            <th class="text-center"><button class="sort-btn" data-sort="prim_cn" data-order="desc">Prim: CN ▼</button></th>
            
            <th class="text-center bg-gray-50"><button class="sort-btn" data-sort="sec_plazas" data-order="desc">Sec: Plazas ▼</button></th>
            <th class="text-center bg-gray-50"><button class="sort-btn" data-sort="sec_pct" data-order="desc">Sec: % ▼</button></th>
            <th class="text-center bg-gray-50"><button class="sort-btn" data-sort="sec_cn" data-order="desc">Sec: CN ▼</button></th>
            
            <th class="text-center"><button class="sort-btn" data-sort="cn_total" data-order="desc">TOTAL CN ▼</button></th>
        </tr>
    `;

    // Re-vincular eventos de ordenamiento
    setTimeout(setupAnalisisCNTableSorting, 100);
};

// Cargar datos de análisis CN
const cargarAnalisisCN = async () => {
    try {
        console.log('🚀 Iniciando carga de análisis CN...');
        
        const tbody = document.getElementById('analisis-cn-table-body');
        if (tbody) {
            // COLSPAN ACTUALIZADO A 14
            tbody.innerHTML = `
                <tr class="loading-state">
                    <td colspan="14" class="text-center">
                        <div class="spinner"></div>
                        <p style="margin-top: 10px; color: #666;">Cargando análisis detallado...</p>
                    </td>
                </tr>`;
        }
        
        const data = await fetchData('/api/analisis-cn-script');
        
        if (data.status === 'error') throw new Error(data.message || 'Error en los datos');

        analisisCNData = data;
        
        if (data.desglose_estados && data.desglose_estados.length > 0) {
            renderAnalisisCNTable(data.desglose_estados);
            
            setTimeout(() => {
                sortAnalisisCNTable(analisisCNCurrentSort.column, analisisCNCurrentSort.direction);
            }, 300);
            
            actualizarResumenAnalisisCN(data.analisis_global);
        } else {
            throw new Error('No hay datos de estados disponibles');
        }
        
    } catch (error) {
        console.error('❌ Error al cargar:', error);
        const tbody = document.getElementById('analisis-cn-table-body');
        if (tbody) {
            tbody.innerHTML = `
                <tr class="error-state">
                    <td colspan="14" class="text-center text-danger">
                        <i class="fas fa-exclamation-triangle"></i> Error: ${error.message}
                    </td>
                </tr>`;
        }
    }
};

// 2. RENDERIZADO EXPANDIDO (14 Columnas)
const renderAnalisisCNTable = (estados) => {
    const tbody = document.getElementById('analisis-cn-table-body');
    if (!tbody) return;
    
    // IMPORTANTE: Reconstruir cabeceras antes de llenar el cuerpo
    rebuildAnalisisCNHeaders();

    tbody.innerHTML = '';
    
    if (!estados || estados.length === 0) {
        tbody.innerHTML = `<tr><td colspan="14" class="text-center text-muted">No hay datos disponibles</td></tr>`;
        return;
    }
    
    const totalNacional = analisisCNData?.info_general?.total_plazas_base || 0;

    estados.forEach((estado, index) => {
        const row = document.createElement('tr');
        row.className = 'hover:bg-gray-50';
        
        // Datos generales
        const nombreEstado = estado.estado || `Estado ${index + 1}`;
        const totalPlazas = estado.total_plazas || 0;
        const plazasConActividad = estado.plazas_con_actividad || 0;
        const porcentajeActividad = estado.porcentaje || 0;
        
        // Datos Inicial
        const pInicial = estado.plazas_inicial || 0;
        const pctInicial = estado.pct_inicial || 0;
        const cnInicial = estado.cn_inicial || 0;

        // Datos Primaria
        const pPrimaria = estado.plazas_primaria || 0;
        const pctPrimaria = estado.pct_primaria || 0;
        const cnPrimaria = estado.cn_primaria || 0;

        // Datos Secundaria
        const pSecundaria = estado.plazas_secundaria || 0;
        const pctSecundaria = estado.pct_secundaria || 0;
        const cnSecundaria = estado.cn_secundaria || 0;

        // Total
        const cnTotal = estado.cn_total || (cnInicial + cnPrimaria + cnSecundaria);
        
        // Construcción de la fila columna por columna
        row.innerHTML = `
            <td data-sort-value="${nombreEstado.toLowerCase()}">
                <strong>${nombreEstado}</strong>
            </td>
            
            <td class="text-center" data-sort-value="${totalPlazas}">
                ${totalPlazas.toLocaleString('es-MX')}
            </td>
            
            <td class="text-center" data-sort-value="${plazasConActividad}">
                ${plazasConActividad.toLocaleString('es-MX')}
            </td>
            
            <td class="text-center" data-sort-value="${porcentajeActividad}">
                ${getAnalisisCNBadgeHTML(porcentajeActividad)}
            </td>
            
            <td class="text-center bg-light" data-sort-value="${pInicial}">
                <span style="color: #2f855a;">${pInicial.toLocaleString('es-MX')}</span>
            </td>
            <td class="text-center bg-light" data-sort-value="${pctInicial}">
                <span style="font-size: 0.85rem; color: #666;">${pctInicial}%</span>
            </td>
            <td class="text-center bg-light" data-sort-value="${cnInicial}">
                <strong style="color: #276749;">${cnInicial.toLocaleString('es-MX')}</strong>
            </td>
            
            <td class="text-center" data-sort-value="${pPrimaria}">
                <span style="color: #2b6cb0;">${pPrimaria.toLocaleString('es-MX')}</span>
            </td>
            <td class="text-center" data-sort-value="${pctPrimaria}">
                <span style="font-size: 0.85rem; color: #666;">${pctPrimaria}%</span>
            </td>
            <td class="text-center" data-sort-value="${cnPrimaria}">
                <strong style="color: #2c5282;">${cnPrimaria.toLocaleString('es-MX')}</strong>
            </td>
            
            <td class="text-center bg-light" data-sort-value="${pSecundaria}">
                <span style="color: #c05621;">${pSecundaria.toLocaleString('es-MX')}</span>
            </td>
            <td class="text-center bg-light" data-sort-value="${pctSecundaria}">
                <span style="font-size: 0.85rem; color: #666;">${pctSecundaria}%</span>
            </td>
            <td class="text-center bg-light" data-sort-value="${cnSecundaria}">
                <strong style="color: #9c4221;">${cnSecundaria.toLocaleString('es-MX')}</strong>
            </td>
            
            <td class="text-center" data-sort-value="${cnTotal}">
                <strong style="font-size: 1rem; color: #000;">${cnTotal.toLocaleString('es-MX')}</strong>
            </td>
        `;
        
        tbody.appendChild(row);
    });
    
    // Agregar pie de tabla
    agregarPieTablaAnalisisCNMejorada(tbody, estados, totalNacional);
};

// 3. PIE DE TABLA EXPANDIDO (14 Columnas)
const agregarPieTablaAnalisisCNMejorada = (tbody, estados, totalNacional) => {
    // Función suma rápida
    const sum = (key) => estados.reduce((acc, e) => acc + (e[key] || 0), 0);
    
    const tPlazas = sum('total_plazas');
    const tActivas = sum('plazas_con_actividad');
    const pctGlobal = tPlazas > 0 ? ((tActivas / tPlazas) * 100).toFixed(1) : '0.0';

    // Sumatorias por nivel
    const tIniPlazas = sum('plazas_inicial');
    const tIniCN = sum('cn_inicial');
    // Calculo promedio del % de distribución para el total (referencial)
    const tIniPct = tPlazas > 0 ? ((tIniPlazas / tPlazas) * 100).toFixed(1) : '0.0';

    const tPrimPlazas = sum('plazas_primaria');
    const tPrimCN = sum('cn_primaria');
    const tPrimPct = tPlazas > 0 ? ((tPrimPlazas / tPlazas) * 100).toFixed(1) : '0.0';

    const tSecPlazas = sum('plazas_secundaria');
    const tSecCN = sum('cn_secundaria');
    const tSecPct = tPlazas > 0 ? ((tSecPlazas / tPlazas) * 100).toFixed(1) : '0.0';
    
    const tTotalCN = tIniCN + tPrimCN + tSecCN;
    
    const totalRow = document.createElement('tr');
    totalRow.className = 'totales-fila';
    totalRow.style.backgroundColor = '#e2e8f0'; 
    totalRow.style.fontWeight = 'bold';
    
    totalRow.innerHTML = `
        <td>TOTAL NACIONAL</td>
        <td class="text-center">${tPlazas.toLocaleString('es-MX')}</td>
        <td class="text-center">${tActivas.toLocaleString('es-MX')}</td>
        <td class="text-center">${pctGlobal}%</td>
        
        <td class="text-center">${tIniPlazas.toLocaleString('es-MX')}</td>
        <td class="text-center text-muted" style="font-size:0.8rem">${tIniPct}%</td>
        <td class="text-center">${tIniCN.toLocaleString('es-MX')}</td>
        
        <td class="text-center">${tPrimPlazas.toLocaleString('es-MX')}</td>
        <td class="text-center text-muted" style="font-size:0.8rem">${tPrimPct}%</td>
        <td class="text-center">${tPrimCN.toLocaleString('es-MX')}</td>
        
        <td class="text-center">${tSecPlazas.toLocaleString('es-MX')}</td>
        <td class="text-center text-muted" style="font-size:0.8rem">${tSecPct}%</td>
        <td class="text-center">${tSecCN.toLocaleString('es-MX')}</td>
        
        <td class="text-center" style="font-size: 1.1rem;">${tTotalCN.toLocaleString('es-MX')}</td>
    `;
    
    tbody.appendChild(totalRow);
};

// Actualizar resumen de análisis CN en la parte superior
const actualizarResumenAnalisisCN = (analisisGlobal) => {
    if (!analisisGlobal) return;
    
    // Actualizar los valores en la tarjeta de resumen
    const actualizarValor = (elementId, valor) => {
        const element = document.getElementById(elementId);
        if (element && valor !== undefined && valor !== null) {
            element.textContent = valor.toLocaleString('es-MX');
        }
    };
    
    const actualizarSubvalor = (elementId, valor) => {
        const element = document.getElementById(elementId);
        if (element && valor !== undefined && valor !== null) {
            element.textContent = `Plazas en operación: ${valor.toLocaleString('es-MX')}`;
        }
    };
    
    // Actualizar CN Inicial, Primaria, Secundaria y Total
    if (analisisGlobal.cn_inicial) {
        actualizarValor('cn-inicial-valor', analisisGlobal.cn_inicial.suma_total);
        actualizarSubvalor('cn-inicial-subvalor', analisisGlobal.cn_inicial.plazas_con_actividad);
    }
    if (analisisGlobal.cn_primaria) {
        actualizarValor('cn-primaria-valor', analisisGlobal.cn_primaria.suma_total);
        actualizarSubvalor('cn-primaria-subvalor', analisisGlobal.cn_primaria.plazas_con_actividad);
    }
    if (analisisGlobal.cn_secundaria) {
        actualizarValor('cn-secundaria-valor', analisisGlobal.cn_secundaria.suma_total);
        actualizarSubvalor('cn-secundaria-subvalor', analisisGlobal.cn_secundaria.plazas_con_actividad);
    }
    if (analisisGlobal.cn_total) {
        actualizarValor('cn-total-valor', analisisGlobal.cn_total.suma_total);
        actualizarSubvalor('cn-total-subvalor', analisisGlobal.cn_total.plazas_con_actividad);
    }
    if (analisisGlobal.combinado_alguna_cn) {
        const element = document.getElementById('plazas-operacion');
        if (element) {
            element.textContent = `${analisisGlobal.combinado_alguna_cn.plazas_unicas_con_actividad.toLocaleString('es-MX')} en operación`;
        }
    }
};

// Función auxiliar para crear badges de porcentaje
const getAnalisisCNBadgeHTML = (porcentaje) => {
    let badgeClass = 'badge-danger';
    if (porcentaje >= 75) badgeClass = 'badge-success';
    else if (porcentaje >= 50) badgeClass = 'badge-warning';
    else if (porcentaje >= 25) badgeClass = 'badge-info';
    
    return `<span class="badge ${badgeClass}">${typeof porcentaje === 'number' ? porcentaje.toFixed(1) : porcentaje}%</span>`;
};

// Configurar ordenamiento de la tabla de análisis CN
const setupAnalisisCNTableSorting = () => {
    const table = document.getElementById('analisis-cn-table');
    if (!table) return;
    
    const sortButtons = table.querySelectorAll('thead .sort-btn');
    if (sortButtons.length === 0) return;
    
    sortButtons.forEach((button, index) => {
        // Clonar para limpiar eventos
        const newButton = button.cloneNode(true);
        button.parentNode.replaceChild(newButton, button);
        
        newButton.addEventListener('click', () => {
            const sortBy = newButton.getAttribute('data-sort');
            const currentOrder = newButton.getAttribute('data-order');
            const newOrder = currentOrder === 'asc' ? 'desc' : 'asc';
            
            analisisCNCurrentSort = { column: index, direction: newOrder };
            
            updateAnalisisCNSortButtonsUI(sortButtons, index, newOrder);
            sortAnalisisCNTable(index, newOrder);
        });
    });
};

// Actualizar UI de botones de ordenamiento
const updateAnalisisCNSortButtonsUI = (buttons, activeIndex, activeDirection) => {
    buttons.forEach((button, index) => {
        const text = button.textContent.replace(/[▲▼]/g, '').trim();
        if (index === activeIndex) {
            button.textContent = `${text} ${activeDirection === 'asc' ? '▲' : '▼'}`;
            button.setAttribute('data-order', activeDirection);
            button.style.fontWeight = 'bold';
        } else {
            button.textContent = `${text} ▼`;
            button.setAttribute('data-order', 'desc');
            button.style.fontWeight = 'normal';
        }
    });
};

// Ordenar tabla de análisis CN
const sortAnalisisCNTable = (columnIndex, direction) => {
    const tbody = document.getElementById('analisis-cn-table-body');
    if (!tbody) return;
    
    const rows = Array.from(tbody.querySelectorAll('tr:not(.totales-fila)'));
    if (rows.length === 0) return;
    
    rows.sort((rowA, rowB) => {
        const cellA = rowA.children[columnIndex];
        const cellB = rowB.children[columnIndex];
        if (!cellA || !cellB) return 0;
        
        const sortValueA = cellA.getAttribute('data-sort-value') || cellA.textContent.trim();
        const sortValueB = cellB.getAttribute('data-sort-value') || cellB.textContent.trim();
        
        const numA = parseFloat(sortValueA);
        const numB = parseFloat(sortValueB);
        
        if (!isNaN(numA) && !isNaN(numB)) {
            return direction === 'asc' ? numA - numB : numB - numA;
        }
        return direction === 'asc' 
            ? sortValueA.localeCompare(sortValueB, 'es') 
            : sortValueB.localeCompare(sortValueA, 'es');
    });
    
    const totalesRow = tbody.querySelector('.totales-fila');
    tbody.innerHTML = '';
    rows.forEach(row => tbody.appendChild(row));
    if (totalesRow) tbody.appendChild(totalesRow);
};

// Función para inicializar navegación (sin cambios)
const initStatsNavigation = () => {
    const statsNavBtns = document.querySelectorAll('.stats-nav-btn');
    const statsSubviews = document.querySelectorAll('.stats-subview');
    
    if (statsNavBtns.length > 0 && statsSubviews.length > 0) {
        statsNavBtns.forEach(btn => {
            const newBtn = btn.cloneNode(true);
            btn.parentNode.replaceChild(newBtn, btn);
            newBtn.addEventListener('click', () => {
                const targetId = newBtn.getAttribute('data-subview');
                statsNavBtns.forEach(b => b.classList.remove('active'));
                newBtn.classList.add('active');
                statsSubviews.forEach(view => view.classList.add('hidden'));
                const targetView = document.getElementById(`${targetId}-view`);
                if (targetView) {
                    targetView.classList.remove('hidden');
                    if (targetId === 'general-stats') {
                        setTimeout(() => { initAnalisisCN(); }, 300);
                    }
                }
            });
        });
    }
    
    if (document.querySelector('.stats-nav-btn.active')?.getAttribute('data-subview') === 'general-stats') {
        setTimeout(() => { initAnalisisCN(); }, 1000);
    }
};

    // --- LÓGICA DE VISTAS DE ESTADOS Y PLAZAS ---

    const cargarEstadosConPlazas = async () => {
        try {
            showLoader('Obteniendo lista de estados y plazas...');
            
            const estados = await fetchData('/api/estados_con_conteo');
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
            const noResults = document.createElement('p');
            noResults.className = 'no-results';
            noResults.textContent = 'No se encontraron estados.';
            estadosGrid.appendChild(noResults);
            return;
        }
        
        estados.forEach(estado => {
            const item = document.createElement('div');
            item.className = 'state-menu-item';
            
            const nameDiv = document.createElement('div');
            nameDiv.className = 'state-menu-name';
            nameDiv.textContent = estado.nombre;
            
            const countDiv = document.createElement('div');
            countDiv.className = 'state-menu-count';
            countDiv.textContent = `${estado.cantidad || 'N/A'} plazas`;
            
            item.appendChild(nameDiv);
            item.appendChild(countDiv);
            
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
            const noResults = document.createElement('p');
            noResults.className = 'no-results';
            noResults.textContent = 'No se encontraron plazas para este estado.';
            plazasListContainer.appendChild(noResults);
            return;
        }
        
        plazas.forEach(plaza => {
            const item = document.createElement('div');
            item.className = 'plaza-list-item';
            
            const claveDiv = document.createElement('div');
            claveDiv.className = 'plaza-clave';
            claveDiv.textContent = plaza.clave;
            
            const direccionDiv = document.createElement('div');
            direccionDiv.className = 'plaza-direccion';
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

    // ===== SISTEMA DE MODAL PARA IMÁGENES =====
    function initImageModal() {
        if (!document.getElementById('image-modal')) {
            const modalOverlay = document.createElement('div');
            modalOverlay.id = 'image-modal';
            modalOverlay.className = 'modal-overlay';
            
            const modalContent = document.createElement('div');
            modalContent.className = 'modal-content';
            
            const modalCounter = document.createElement('div');
            modalCounter.className = 'modal-counter';
            modalCounter.innerHTML = '<span id="modal-current">1</span> / <span id="modal-total">1</span>';
            
            const modalControls = document.createElement('div');
            modalControls.className = 'modal-controls';
            const closeButton = document.createElement('button');
            closeButton.className = 'modal-btn modal-close';
            closeButton.title = 'Cerrar (Esc)';
            closeButton.textContent = '×';
            modalControls.appendChild(closeButton);
            
            const prevButton = document.createElement('button');
            prevButton.className = 'modal-nav modal-prev';
            prevButton.title = 'Anterior (←)';
            prevButton.textContent = '‹';
            
            const nextButton = document.createElement('button');
            nextButton.className = 'modal-nav modal-next';
            nextButton.title = 'Siguiente (→)';
            nextButton.textContent = '›';
            
            const modalImage = document.createElement('img');
            modalImage.className = 'modal-image';
            modalImage.src = '';
            modalImage.alt = '';
            
            const modalInfo = document.createElement('div');
            modalInfo.className = 'modal-info';
            const filenameDiv = document.createElement('div');
            filenameDiv.id = 'modal-filename';
            filenameDiv.className = 'modal-filename';
            filenameDiv.textContent = 'Imagen';
            const sourceDiv = document.createElement('div');
            sourceDiv.id = 'modal-source';
            sourceDiv.className = 'modal-source';
            sourceDiv.textContent = 'Desde Google Drive';
            
            modalInfo.appendChild(filenameDiv);
            modalInfo.appendChild(sourceDiv);
            
            modalContent.appendChild(modalCounter);
            modalContent.appendChild(modalControls);
            modalContent.appendChild(prevButton);
            modalContent.appendChild(nextButton);
            modalContent.appendChild(modalImage);
            modalContent.appendChild(modalInfo);
            
            modalOverlay.appendChild(modalContent);
            document.body.appendChild(modalOverlay);
        }

        const modal = document.getElementById('image-modal');
        const modalImage = modal.querySelector('.modal-image');
        const modalClose = modal.querySelector('.modal-close');
        const modalPrev = modal.querySelector('.modal-prev');
        const modalNext = modal.querySelector('.modal-next');
        const modalCurrent = document.getElementById('modal-current');
        const modalTotal = document.getElementById('modal-total');
        const modalFilename = document.getElementById('modal-filename');
        const modalSource = document.getElementById('modal-source');

        let currentImages = [];
        let currentIndex = 0;

        function openModal(images, startIndex = 0) {
            currentImages = images;
            currentIndex = startIndex;
            
            modalTotal.textContent = images.length;
            updateModalImage();
            modal.classList.add('active');
            document.body.style.overflow = 'hidden';
        }

        function closeModal() {
            modal.classList.remove('active');
            document.body.style.overflow = '';
            currentImages = [];
            currentIndex = 0;
        }

        function updateModalImage() {
            if (currentImages.length === 0) return;
            
            const imageUrl = currentImages[currentIndex];
            modalImage.src = imageUrl;
            modalCurrent.textContent = currentIndex + 1;
            
            const filename = imageUrl.split('/').pop() || 'imagen.jpg';
            modalFilename.textContent = decodeURIComponent(filename);
            
            modalPrev.style.display = currentIndex > 0 ? 'flex' : 'none';
            modalNext.style.display = currentIndex < currentImages.length - 1 ? 'flex' : 'none';
        }

        function nextImage() {
            if (currentIndex < currentImages.length - 1) {
                currentIndex++;
                updateModalImage();
            }
        }

        function prevImage() {
            if (currentIndex > 0) {
                currentIndex--;
                updateModalImage();
            }
        }

        modalClose.addEventListener('click', closeModal);
        modalPrev.addEventListener('click', prevImage);
        modalNext.addEventListener('click', nextImage);

        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                closeModal();
            }
        });

        document.addEventListener('keydown', (e) => {
            if (!modal.classList.contains('active')) return;
            
            switch(e.key) {
                case 'Escape':
                    closeModal();
                    break;
                case 'ArrowLeft':
                    prevImage();
                    break;
                case 'ArrowRight':
                    nextImage();
                    break;
            }
        });

        return { openModal };
    }

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
        
        const { openModal } = initImageModal();
        modalOpenFunction = openModal;
        
        loadExcelLastUpdate();
        setupUpdateBadgeInteractions();
        
        // Inicializar navegación de estadísticas
        initStatsNavigation();
        
        handleNavigation();
        
        // La navegación de análisis CN se manejará a través de initStatsNavigation
    };

    initApp();
});

// ===== FUNCIÓN PARA CARGAR FECHA DE ACTUALIZACIÓN =====
function loadExcelLastUpdate() {
    fetch('/api/excel/last-update')
        .then(response => {
            if (!response.ok) throw new Error('Error en la respuesta');
            return response.json();
        })
        .then(data => {
            const updateElement = document.getElementById('update-date');
            
            if (data.last_modified && data.status === 'success') {
                const date = new Date(data.last_modified);
                const formattedDate = date.toLocaleDateString('es-MX', {
                    day: '2-digit',
                    month: 'long',
                    year: 'numeric'
                });
                
                updateElement.textContent = formattedDate;
                
                setTimeout(() => {
                    const badge = document.getElementById('excel-update-info');
                    if (badge) badge.classList.add('minimal');
                }, 5000);
                
            } else {
                updateElement.textContent = 'No disponible';
                updateElement.style.color = '#999';
            }
        })
        .catch(error => {
            console.error('Error cargando fecha de actualización:', error);
            const updateElement = document.getElementById('update-date');
            if (updateElement) {
                updateElement.textContent = 'Error';
                updateElement.style.color = '#cc0000';
            }
        });
}

// ===== TOGGLE DISCRETO AL HACER CLIC =====
function setupUpdateBadgeInteractions() {
    const badge = document.getElementById('excel-update-info');
    if (badge) {
        badge.addEventListener('click', function() {
            this.classList.toggle('minimal');
        });
    }
}

// ===== INICIALIZAR CUANDO LA PÁGINA CARGUE =====
document.addEventListener('DOMContentLoaded', function() {
    loadExcelLastUpdate();
    setupUpdateBadgeInteractions();
});