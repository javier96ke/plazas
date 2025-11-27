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
    // --- REFERENCIAS PARA NAVEGACIÓN DE ESTADÍSTICAS ---
    const statsNavBtns = document.querySelectorAll('.stats-nav-btn');
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
                
                if (generalStatsView && comparativasStatsView) {
                    generalStatsView.classList.remove('hidden');
                    comparativasStatsView.classList.add('hidden');
                }
                
                if (generalStatsBtn && comparativasStatsBtn) {
                    generalStatsBtn.classList.add('active');
                    comparativasStatsBtn.classList.remove('active');
                }
            }, 50);
            
            if (!estadisticasData) {
                cargarEstadisticas();
            } else if (!cnResumenData) {
                cargarEstadisticasCompletasCN();
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
                return await buscarImagenesLocales(clave_lower);
            }
            
        } catch (error) {
            console.error('❌ Error en búsqueda de imágenes:', error);
            return await buscarImagenesLocales(clave_original.trim().toLowerCase());
        }
    };

    // --- RENDERIZADO DE RESULTADOS ACTUALIZADO ---
    const renderPlazaResultados = (data) => {
        const { excel_info, images, google_maps_url, direccion_completa } = data;
        
        const template = document.getElementById('plaza-results-template');
        const clone = template.content.cloneNode(true);
        
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
        
        const generateGridContent = (container, info, columns) => {
            container.innerHTML = '';
            columns.forEach(key => {
                const displayKey = key.replace(/_/g, ' ');
                const value = info[key];
                const p = document.createElement('p');
                const strong = document.createElement('strong');
                strong.textContent = `${displayKey}:`;
                p.appendChild(strong);
                p.appendChild(document.createTextNode(` ${value !== null && value !== undefined ? value : 'N/A'}`));
                container.appendChild(p);
            });
        };

        const gridUbicacionElement = clone.querySelector('[data-bind="grid_ubicacion"]');
        const gridUsoElement = clone.querySelector('[data-bind="grid_uso"]');
        const gridInventarioElement = clone.querySelector('[data-bind="grid_inventario"]');
        
        const columnasUbicacion = ['Estado', 'Coord. Zona', 'Municipio', 'Localidad', 'Colonia', 'Cod_Post', 'Calle', 'Num', 'Clave_Plaza', 'Nombre_PC', 'Situación', 'Latitud', 'Longitud'];
        const columnasUso = ['Aten_Ult_mes', 'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum', 'Tipo_local', 'Inst_aliada', 'Arq_Discap.', 'Conect_Instalada', 'Tipo_Conect'];
        const columnasInventario = ['Total de equipos de cómputo en la plaza', 'Equipos de cómputo que operan', 'Tipos de equipos de cómputo', 'Impresoras que funcionan', 'Impresoras con suministros (toner, hojas)', 'Total de servidores en la plaza', 'Número de servidores que funcionan correctamente', 'Cuantas mesas funcionan', 'Cuantas sillas funcionan', 'Cuantos Anaqueles funcionan'];

        if (gridUbicacionElement) {
            generateGridContent(gridUbicacionElement, excel_info, columnasUbicacion);
        }
        
        if (gridUsoElement) {
            generateGridContent(gridUsoElement, excel_info, columnasUso);
        }
        
        if (gridInventarioElement) {
            generateGridContent(gridInventarioElement, excel_info, columnasInventario);
        }
        
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
        
        setTimeout(() => {
            const images = document.querySelectorAll('.plaza-image');
            console.log(`🔍 Verificando ${images.length} imágenes...`);
            images.forEach((img, index) => {
                if (!img.complete || img.naturalHeight === 0) {
                    console.warn(`❌ Imagen ${index} no se cargó:`, img.src);
                } else {
                    console.log(`✅ Imagen ${index} cargada correctamente`);
                }
            });
        }, 1000);
    };

    // --- BÚSQUEDA Y FILTROS ---
    const buscarYMostrarClave = async (clave, loader) => {
        if (!clave) {
            showAlert('Por favor, introduce una clave válida', 'warning');
            return;
        }
        
        showLoader(`Buscando plaza con clave: ${clave}`);
        
        try {
            const data = await fetchData(`/api/search?clave=${encodeURIComponent(clave)}`);
            
            if (!data || !data.excel_info) {
                throw new Error('No se encontraron datos para esta clave');
            }
            
            console.log(`🔄 Buscando imágenes para: ${clave}`);
            const imagenesDrive = await find_image_urls(clave);
            
            const datosCompletos = {
                ...data,
                images: imagenesDrive.length > 0 ? imagenesDrive : (data.images || [])
            };
            
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
        if (!cnResumenCards || !cnResumenData?.resumen_nacional) return;
        
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
            subvalue.textContent = `Plazas en operación: ${data.plazasOperacion.toLocaleString()}`;
            
            statItem.appendChild(label);
            statItem.appendChild(value);
            statItem.appendChild(subvalue);
            statsGrid.appendChild(statItem);
        });
        
        resumenCard.appendChild(tituloResumen);
        resumenCard.appendChild(statsGrid);
        cnResumenCards.appendChild(resumenCard);
        
        // Top 5 estados
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
        
        // Crear tbody
        const tbody = document.createElement('tbody');
        
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
        setupSorting();
    };

    const setupSorting = () => {
        const sortButtons = document.querySelectorAll('.sort-btn');
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
                        
                    case 'pct_nacional':
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
            tbody.innerHTML = '';
            
            data.forEach(estado => {
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
        };
    };
     
    const renderEstadisticasCN = () => {
        if (!cnResumenData || !cnPorEstadoData || !cnTopEstadosData) return;
        renderResumenCN();
        renderTablaEstadosCN();
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

    // --- NAVEGACIÓN ENTRE SUB-VISTAS DE ESTADÍSTICAS ---
    function initStatsNavigation() {
        statsNavBtns.forEach(btn => {
            btn.addEventListener('click', function() {
                const targetSubview = this.getAttribute('data-subview');
                
                // Actualizar botones activos
                statsNavBtns.forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                
                // Mostrar sub-vista correspondiente
                statsSubviews.forEach(view => {
                    if (view.id === `${targetSubview}-view`) {
                        view.classList.remove('hidden');
                    } else {
                        view.classList.add('hidden');
                    }
                });
                
                // Si es la vista de comparativas, inicializar el sistema
                if (targetSubview === 'comparativas-stats') {
                    setTimeout(() => {
                        if (typeof sistemaComparativas !== 'undefined') {
                            sistemaComparativas.init();
                        }
                    }, 100);
                }
            });
        });
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
                    month: '2-digit',
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
