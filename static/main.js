document.addEventListener('DOMContentLoaded', () => {
    // ==== SISTEMA DE INDEXACIÓN DE IMÁGENES ====
    let imagenIndex = new Map();
    let driveTreeVersion = null;
    let actualizacionInterval = null;

    // ==== CONFIGURACIÓN PRINCIPAL ====
    const config = {
        selects: ['estado', 'zona', 'municipio', 'localidad', 'clave'],
        stepOrder: ['estado', 'zona', 'municipio', 'localidad', 'clave'],
        columns: {
            ubicacion: [
                'Clave_Plaza', 'Nombre_PC', 'Estado', 'Coord. Zona', 
                'Municipio', 'Localidad', 'Calle', 'Num', 'Colonia', 
                'Cod_Post', 'Situación', 'Tipo_Conect', 'Conect_Instalada',
                'Latitud', 'Longitud'
            ],
            atencion: [
                'Inc_Inicial', 'Inc_Prim', 'Inc_Sec', 'Inc_Total',
                'Aten_Inicial', 'Aten_Prim', 'Aten_Sec', 'Aten_Total', 'Exámenes aplicados',
                'CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum',
                'Cert_Emitidos'
            ],
            personal: ['Tec_Doc', 'Nom_PVS_1', 'Nom_PVS_2'],
            infraestructura: ['Tipo_local', 'Inst_aliada', 'Arq_Discap.'],
            inventario: [
                'Total de equipos de cómputo en la plaza', 'Equipos de cómputo que operan',
                'Tipos de equipos de cómputo', 'Impresoras que funcionan',
                'Impresoras con suministros (toner, hojas)', 'Total de servidores en la plaza',
                'Número de servidores que funcionan correctamente', 'Cuantas mesas funcionan',
                'Cuantas sillas funcionan', 'Cuantos Anaqueles funcionan'
            ]
        }
    };

    // ==== REFERENCIAS AGRUPADAS DEL DOM ====
    const refs = {
        views: {
            'welcome-screen': document.getElementById('welcome-screen'),
            'key-search-view': document.getElementById('key-search-view'),
            'filter-search-view': document.getElementById('filter-search-view'),
            'results-view': document.getElementById('results-view'),
            'stats-view': document.getElementById('stats-view'),
            'estados-view': document.getElementById('estados-view'),
            'plazas-por-estado-view': document.getElementById('plazas-por-estado-view'),
            'top-plazas-view': document.getElementById('top-plazas-view'),
            'map-view': document.getElementById('map-view')
        },
        search: {
            claveInput: document.getElementById('clave-input'),
            searchByKeyButton: document.getElementById('search-by-key-button'),
            keyLoader: document.getElementById('key-loader')
        },
        selects: {
            estado: document.getElementById('state-select'),
            zona: document.getElementById('zona-select'),
            municipio: document.getElementById('municipio-select'),
            localidad: document.getElementById('localidad-select'),
            clave: document.getElementById('clave-select')
        },
        filterSteps: {
            estado: document.querySelector('.filter-step[data-step="estado"]'),
            zona: document.querySelector('.filter-step[data-step="zona"]'),
            municipio: document.querySelector('.filter-step[data-step="municipio"]'),
            localidad: document.querySelector('.filter-step[data-step="localidad"]'),
            clave: document.querySelector('.filter-step[data-step="clave"]')
        },
        stats: {
            navBtns: document.querySelectorAll('.stats-nav-btn'),
            subviews: document.querySelectorAll('.stats-subview'),
            general: {
                totalPlazas: document.getElementById('total-plazas'),
                plazasOperacion: document.getElementById('plazas-operacion'),
                totalEstados: document.getElementById('total-estados'),
                estadoMasPlazasNombre: document.getElementById('estado-mas-plazas-nombre'),
                estadoMasPlazasCantidad: document.getElementById('estado-mas-plazas-cantidad'),
                estadoMayorConectividadNombre: document.getElementById('estado-mayor-conectividad-nombre'),
                estadoMayorConectividadPorcentaje: document.getElementById('estado-mayor-conectividad-porcentaje'),
                estadoMasOperacionNombre: document.getElementById('estado-mas-operacion-nombre'),
                estadoMasOperacionPorcentaje: document.getElementById('estado-mas-operacion-porcentaje'),
                estadoMasSuspensionNombre: document.getElementById('estado-mas-suspension-nombre'),
                estadoMasSuspensionPorcentaje: document.getElementById('estado-mas-suspension-porcentaje')
            },
            cn: {
                estadoMasCNInicialNombre: document.getElementById('estado-mas-cn-inicial-nombre'),
                estadoMasCNInicialCantidad: document.getElementById('estado-mas-cn-inicial-cantidad'),
                estadoMasCNPrimariaNombre: document.getElementById('estado-mas-cn-primaria-nombre'),
                estadoMasCNPrimariaCantidad: document.getElementById('estado-mas-cn-primaria-cantidad'),
                estadoMasCNSecundariaNombre: document.getElementById('estado-mas-cn-secundaria-nombre'),
                estadoMasCNSecundariaCantidad: document.getElementById('estado-mas-cn-secundaria-cantidad'),
                top5InicialList: document.getElementById('cn-top5-inicial-list'),
                top5PrimariaList: document.getElementById('cn-top5-primaria-list'),
                top5SecundariaList: document.getElementById('cn-top5-secundaria-list'),
                resumenCards: document.getElementById('cn-resumen-cards'),
                estadosTable: document.getElementById('cn-estados-table'),
                estadosTbody: document.getElementById('cn-estados-tbody')
            }
        },
        estados: {
            grid: document.getElementById('estados-grid'),
            searchInput: document.getElementById('estados-search-input')
        },
        plazas: {
            title: document.getElementById('plazas-por-estado-title'),
            container: document.getElementById('plazas-list-container'),
            searchInput: document.getElementById('plazas-search-input')
        },
        progress: {
            fill: document.getElementById('progress-fill'),
            steps: document.querySelectorAll('.progress-step')
        },
        ui: {
            resultsContent: document.getElementById('results-content'),
            alertContainer: document.getElementById('alert-container'),
            backToSearchButton: document.getElementById('back-to-search-button'),
            filterLoader: document.getElementById('filter-loader'),
            searchFilterButton: document.getElementById('search-filter-button'),
            themeLight: document.getElementById('theme-light'),
            themeDark: document.getElementById('theme-dark')
        }
    };

    // ==== VARIABLES DE ESTADO ====
    const state = {
        lastView: 'welcome-screen',
        estadisticasData: null,
        todosEstadosData: [],
        estadoSeleccionado: '',
        plazasDelEstado: [],
        cnResumenData: null,
        cnPorEstadoData: null,
        cnTopEstadosData: null,
        cnEstadosDestacadosData: null,
        cnTop5TodosData: null,
        modalOpenFunction: null,
        cache: new Map()
    };

    // ==== UTILIDADES GENERALES ====
    const utils = {
        safeText: (text) => text ?? 'N/A',
        
        debounce: (func, wait) => {
            let timeout;
            return function executedFunction(...args) {
                const later = () => {
                    clearTimeout(timeout);
                    func(...args);
                };
                clearTimeout(timeout);
                timeout = setTimeout(later, wait);
            };
        },
        
        setLoaderVisible: (loader, visible) => loader?.classList?.toggle('hidden', !visible),
        
        fetchData: async (url) => {
            if (state.cache.has(url)) {
                return state.cache.get(url);
            }
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`Error ${response.status}`);
            }
            const result = await response.json();
            state.cache.set(url, result);
            return result;
        },
        
        createFragment: () => document.createDocumentFragment()
    };

    // ==== SISTEMA DE TEMA ====
    const themeSystem = {
        applyTheme: (theme) => {
            document.documentElement.setAttribute('data-theme', theme);
            localStorage.setItem('theme', theme);
            
            document.body.style.backgroundImage = `url('/static/${theme === 'dark' ? 'noche' : 'claro'}.jpg')`;
            document.body.style.backgroundSize = 'cover';
            
            // Actualizar vistas si están visibles
            setTimeout(() => {
                if (!refs.views['estados-view'].classList.contains('hidden') && state.todosEstadosData.length > 0) {
                    estadosSystem.renderEstadosConPlazas(state.todosEstadosData);
                }
                if (!refs.views['plazas-por-estado-view'].classList.contains('hidden') && state.plazasDelEstado.length > 0) {
                    plazasSystem.renderPlazasList(state.plazasDelEstado, state.estadoSeleccionado);
                }
            }, 100);
        },
        
        init: () => {
            const savedTheme = localStorage.getItem('theme');
            themeSystem.applyTheme(savedTheme === 'dark' ? 'dark' : 'light');
            
            refs.ui.themeLight.addEventListener('click', () => themeSystem.applyTheme('light'));
            refs.ui.themeDark.addEventListener('click', () => themeSystem.applyTheme('dark'));
        }
    };

    // ==== SISTEMA DE NOTIFICACIONES ====
    const notificationSystem = {
        mostrar: (mensaje, tipo = 'info', duracion = 3000) => {
            // Eliminar notificaciones existentes
            document.querySelectorAll('.system-notification').forEach(n => n.remove());
            
            const notificacion = document.createElement('div');
            notificacion.className = `system-notification ${tipo}`;
            notificacion.innerHTML = `
                <div class="notification-content">
                    <span class="notification-icon">${notificationSystem.getIcon(tipo)}</span>
                    <span class="notification-text">${mensaje}</span>
                </div>
            `;
            
            document.body.appendChild(notificacion);
            
            // Mostrar con animación
            setTimeout(() => notificacion.classList.add('show'), 10);
            
            // Auto-eliminar
            setTimeout(() => {
                notificacion.classList.remove('show');
                setTimeout(() => notificacion.remove(), 300);
            }, duracion);
        },
        
        getIcon: (tipo) => ({
            success: '✅',
            warning: '⚠️',
            error: '❌',
            info: 'ℹ️'
        })[tipo] || 'ℹ️'
    };

    // ==== SISTEMA DE INDEXACIÓN DE IMÁGENES ====
    const imageIndexSystem = {
        construirIndiceImagenes: async (forzarRecarga = false) => {
            try {
                console.log('🔨 Iniciando construcción de índice...');
                
                // PASO 1: Verificar versión actual
                let versionActual = 'unknown';
                try {
                    const versionResponse = await fetch('/api/drive-tree-version');
                    if (versionResponse.ok) {
                        const versionData = await versionResponse.json();
                        versionActual = versionData.version || versionData.lastModified || 'unknown';
                        console.log(`📋 Versión detectada: ${versionActual}`);
                    }
                } catch (e) {
                    console.warn('⚠️ No se pudo obtener versión:', e);
                }
                
                // PASO 2: Verificar si podemos usar caché
                if (!forzarRecarga && driveTreeVersion === versionActual && imagenIndex.size > 0) {
                    console.log('✅ Índice ya está actualizado');
                    return { success: true, fromCache: true };
                }
                
                // PASO 3: Intentar cargar desde localStorage
                if (!forzarRecarga && versionActual !== 'unknown') {
                    try {
                        const cachedIndex = localStorage.getItem(`imagenIndex_v${versionActual}`);
                        const cachedMeta = localStorage.getItem(`imagenIndex_meta_v${versionActual}`);
                        
                        if (cachedIndex && cachedMeta) {
                            const meta = JSON.parse(cachedMeta);
                            const ahora = Date.now();
                            const edad = ahora - meta.timestamp;
                            
                            // Usar caché si tiene menos de 2 horas
                            if (edad < 2 * 60 * 60 * 1000) {
                                console.log('📂 Cargando índice desde caché local...');
                                const parsedIndex = JSON.parse(cachedIndex);
                                imagenIndex = new Map(parsedIndex);
                                driveTreeVersion = versionActual;
                                console.log(`✅ Índice cargado desde caché: ${imagenIndex.size} carpetas`);
                                
                                notificationSystem.mostrar('Índice de imágenes cargado desde caché', 'info');
                                return { success: true, fromCache: true };
                            }
                        }
                    } catch (e) {
                        console.warn('⚠️ Error cargando caché:', e);
                    }
                }
                
                // PASO 4: Construir nuevo índice desde el servidor
                console.log('⏬ Descargando árbol de Drive...');
                
                const driveTreeResponse = await fetch('/api/drive-tree');
                if (!driveTreeResponse.ok) {
                    throw new Error(`Error ${driveTreeResponse.status} al cargar árbol`);
                }
                
                const driveData = await driveTreeResponse.json();
                
                // Limpiar índice anterior
                imagenIndex.clear();
                let totalCarpetas = 0;
                let totalArchivos = 0;
                
                // Función recursiva optimizada para indexar
                const indexarCarpeta = (nodo, rutaPadre = '') => {
                    if (!nodo || nodo.type !== 'folder') return;
                    
                    const rutaActual = rutaPadre ? `${rutaPadre}/${nodo.name}` : nodo.name;
                    const claveCarpeta = rutaActual.toLowerCase().trim();
                    
                    // Procesar archivos en esta carpeta
                    if (nodo.children && nodo.children.length > 0) {
                        const archivos = [];
                        
                        for (const item of nodo.children) {
                            if (item.type === 'file') {
                                totalArchivos++;
                                archivos.push({
                                    name: item.name,
                                    url: item.mediumUrl || item.thumbnailUrl || item.directUrl,
                                    size: item.size,
                                    folder: rutaActual,
                                    mimeType: item.mimeType || 'image/jpeg'
                                });
                            }
                        }
                        
                        if (archivos.length > 0) {
                            imagenIndex.set(claveCarpeta, archivos);
                            totalCarpetas++;
                        }
                        
                        // Procesar subcarpetas
                        for (const item of nodo.children) {
                            if (item.type === 'folder') {
                                indexarCarpeta(item, rutaActual);
                            }
                        }
                    }
                };
                
                // Construir índice
                console.log('🏗️ Construyendo índice...');
                indexarCarpeta(driveData.structure);
                driveTreeVersion = versionActual;
                
                // PASO 5: Guardar en caché local
                if (versionActual !== 'unknown') {
                    try {
                        const serializableIndex = Array.from(imagenIndex.entries());
                        const meta = {
                            timestamp: Date.now(),
                            version: versionActual,
                            carpetas: totalCarpetas,
                            archivos: totalArchivos
                        };
                        
                        localStorage.setItem(`imagenIndex_v${versionActual}`, JSON.stringify(serializableIndex));
                        localStorage.setItem(`imagenIndex_meta_v${versionActual}`, JSON.stringify(meta));
                        
                        // Limpiar cachés antiguos (más de 7 días)
                        const ahora = Date.now();
                        for (let i = 0; i < localStorage.length; i++) {
                            const key = localStorage.key(i);
                            if (key.startsWith('imagenIndex_meta_v')) {
                                try {
                                    const oldMeta = JSON.parse(localStorage.getItem(key));
                                    if (ahora - oldMeta.timestamp > 7 * 24 * 60 * 60 * 1000) {
                                        const version = key.replace('imagenIndex_meta_v', '');
                                        localStorage.removeItem(`imagenIndex_v${version}`);
                                        localStorage.removeItem(key);
                                    }
                                } catch (e) {
                                    // Ignorar errores al limpiar
                                }
                            }
                        }
                        
                        console.log(`💾 Índice guardado en caché (versión: ${versionActual})`);
                    } catch (e) {
                        console.warn('⚠️ No se pudo guardar en localStorage:', e.message);
                    }
                }
                
                console.log(`✅ Índice construido: ${totalCarpetas} carpetas, ${totalArchivos} archivos`);
                
                notificationSystem.mostrar(
                    `Índice actualizado: ${totalCarpetas} carpetas, ${totalArchivos} imágenes`,
                    'success'
                );
                
                return { 
                    success: true, 
                    fromCache: false,
                    stats: { carpetas: totalCarpetas, archivos: totalArchivos }
                };
                
            } catch (error) {
                console.error('❌ Error construyendo índice:', error);
                
                // Intentar cargar última versión válida como fallback
                try {
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        if (key.startsWith('imagenIndex_v')) {
                            const version = key.replace('imagenIndex_v', '');
                            const cachedIndex = localStorage.getItem(key);
                            const cachedMeta = localStorage.getItem(`imagenIndex_meta_v${version}`);
                            
                            if (cachedIndex && cachedMeta) {
                                const meta = JSON.parse(cachedMeta);
                                console.log(`🔄 Usando caché de respaldo (versión: ${version})`);
                                
                                const parsedIndex = JSON.parse(cachedIndex);
                                imagenIndex = new Map(parsedIndex);
                                driveTreeVersion = version;
                                
                                notificationSystem.mostrar(
                                    `Usando índice en caché: ${meta.carpetas || '?'} carpetas`,
                                    'warning'
                                );
                                
                                return { success: true, fromCache: true, isFallback: true };
                            }
                        }
                    }
                } catch (fallbackError) {
                    console.warn('⚠️ Fallback también falló:', fallbackError);
                }
                
                notificationSystem.mostrar('Error al cargar índice de imágenes', 'error');
                return { success: false, error: error.message };
            }
        },
        
        verificarActualizacionIndice: async () => {
            try {
                const response = await fetch('/api/drive-tree-version');
                if (!response.ok) return false;
                
                const data = await response.json();
                const nuevaVersion = data.version || data.lastModified;
                
                if (nuevaVersion && nuevaVersion !== driveTreeVersion) {
                    console.log(`🔄 Nueva versión detectada: ${nuevaVersion} (actual: ${driveTreeVersion})`);
                    return { necesitaActualizar: true, nuevaVersion };
                }
                
                return { necesitaActualizar: false };
            } catch (error) {
                console.warn('⚠️ Error verificando actualización:', error);
                return { necesitaActualizar: false, error: error.message };
            }
        },
        
        find_image_urls_optimized: async (clave_original) => {
            try {
                const clave_lower = clave_original.trim().toLowerCase();
                if (!clave_lower) {
                    console.log('🔍 Clave vacía, buscando imágenes locales...');
                    return await imageIndexSystem.buscarImagenesLocales(clave_lower);
                }
                
                console.log(`🔍 Buscando imágenes para: "${clave_lower}"`);
                
                // PASO 1: Verificar si el índice está disponible
                if (imagenIndex.size === 0) {
                    console.log('📦 Índice vacío, construyendo...');
                    const resultado = await imageIndexSystem.construirIndiceImagenes();
                    if (!resultado.success) {
                        console.warn('⚠️ No se pudo construir índice, usando búsqueda local');
                        return await imageIndexSystem.buscarImagenesLocales(clave_lower);
                    }
                }
                
                // PASO 2: Búsqueda exacta en el índice
                let imagenesEncontradas = imagenIndex.get(clave_lower);
                
                if (imagenesEncontradas && imagenesEncontradas.length > 0) {
                    console.log(`✅ ${imagenesEncontradas.length} imágenes encontradas (búsqueda exacta)`);
                    return imagenesEncontradas.map(img => img.url);
                }
                
                // PASO 3: Búsqueda parcial (coincidencias)
                console.log(`🤔 No encontrado exactamente, buscando coincidencias...`);
                const coincidencias = [];
                
                for (const [carpeta, archivos] of imagenIndex.entries()) {
                    // Verificar si la clave está contenida en la carpeta o viceversa
                    if (carpeta.includes(clave_lower) || clave_lower.includes(carpeta)) {
                        coincidencias.push({ carpeta, archivos, score: Math.abs(carpeta.length - clave_lower.length) });
                    }
                }
                
                // Ordenar por mejor coincidencia (menor diferencia de longitud)
                coincidencias.sort((a, b) => a.score - b.score);
                
                if (coincidencias.length > 0) {
                    const mejorCoincidencia = coincidencias[0];
                    console.log(`🎯 Mejor coincidencia: "${mejorCoincidencia.carpeta}" (score: ${mejorCoincidencia.score})`);
                    return mejorCoincidencia.archivos.map(img => img.url);
                }
                
                // PASO 4: Verificar si el índice podría estar desactualizado
                console.log(`🔄 Verificando si el índice necesita actualización...`);
                const actualizacion = await imageIndexSystem.verificarActualizacionIndice();
                
                if (actualizacion.necesitaActualizar) {
                    console.log(`🔄 Índice desactualizado, reconstruyendo...`);
                    await imageIndexSystem.construirIndiceImagenes(true);
                    
                    // Intentar de nuevo después de actualizar
                    imagenesEncontradas = imagenIndex.get(clave_lower);
                    if (imagenesEncontradas && imagenesEncontradas.length > 0) {
                        console.log(`✅ Encontrado después de actualizar índice`);
                        return imagenesEncontradas.map(img => img.url);
                    }
                }
                
                // PASO 5: Fallback a búsqueda local
                console.log(`🔍 Último recurso: búsqueda local`);
                return await imageIndexSystem.buscarImagenesLocales(clave_lower);
                
            } catch (error) {
                console.error('❌ Error en búsqueda optimizada:', error);
                return await imageIndexSystem.buscarImagenesLocales(clave_original.trim().toLowerCase());
            }
        },
        
        buscarImagenesLocales: async (clave_lower) => {
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
                
                console.log('📭 No hay imágenes locales disponibles');
                return [];
                
            } catch (error) {
                console.error('❌ Error en búsqueda local de imágenes:', error);
                return [];
            }
        },
        
        iniciarActualizacionAutomatica: () => {
            // Detener intervalo anterior si existe
            if (actualizacionInterval) {
                clearInterval(actualizacionInterval);
            }
            
            // Verificar actualizaciones cada 15 minutos
            actualizacionInterval = setInterval(async () => {
                try {
                    console.log('🕐 Verificando actualizaciones del índice...');
                    const actualizacion = await imageIndexSystem.verificarActualizacionIndice();
                    
                    if (actualizacion.necesitaActualizar) {
                        console.log(`🔄 Actualización detectada, reconstruyendo índice...`);
                        const resultado = await imageIndexSystem.construirIndiceImagenes(true);
                        
                        if (resultado.success) {
                            notificationSystem.mostrar(
                                `Índice actualizado a versión ${actualizacion.nuevaVersion?.substring(0, 8)}...`,
                                'success'
                            );
                        }
                    }
                } catch (error) {
                    console.warn('⚠️ Error en verificación automática:', error);
                }
            }, 15 * 60 * 1000);
            
            console.log('🔄 Sistema de actualización automática iniciado (cada 15min)');
        },
        
        crearBotonActualizacion: () => {
            const boton = document.createElement('button');
            boton.id = 'indice-update-button';
            boton.className = 'indice-update-button hidden';
            boton.title = 'Actualizar índice de imágenes';
            boton.innerHTML = '🔄';
            
            boton.addEventListener('click', async () => {
                boton.classList.add('updating');
                boton.title = 'Actualizando...';
                
                const resultado = await imageIndexSystem.construirIndiceImagenes(true);
                
                boton.classList.remove('updating');
                boton.title = 'Índice actualizado';
                
                setTimeout(() => {
                    boton.title = 'Actualizar índice de imágenes';
                }, 2000);
            });
            
            document.body.appendChild(boton);
            return boton;
        }
    };

    // ==== SISTEMA DE PANTALLA DE CARGA ====
    const loaderSystem = {
        show: (message = null, type = 'default') => {
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
        },
        
        hide: () => {
            const loader = document.getElementById('global-loader');
            loader.classList.add('hidden');
            
            setTimeout(() => {
                loader.className = 'loader-overlay hidden';
            }, 500);
        }
    };

    // ==== SISTEMA DE NAVEGACIÓN ====
    const navigationSystem = {
        showView: (viewId) => {
            const currentView = Object.keys(refs.views).find(key => !refs.views[key].classList.contains('hidden'));
            if (currentView && currentView !== viewId) {
                state.lastView = currentView;
            }
            if (!refs.views[viewId]) viewId = 'welcome-screen';
            Object.values(refs.views).forEach(v => v.classList.add('hidden'));
            refs.views[viewId].classList.remove('hidden');

            if (viewId === 'stats-view') {
                setTimeout(() => {
                    statsSystem.initStatsNavigation();
                    
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
                
                if (!state.estadisticasData) {
                    statsSystem.cargarEstadisticas();
                } else if (!state.cnResumenData) {
                    statsSystem.cargarEstadisticasCompletasCN();
                }
            }
            
            if (viewId === 'estados-view' && state.todosEstadosData.length === 0) {
                estadosSystem.cargarEstadosConPlazas();
            }
        },
        
        handleNavigation: () => {
            const viewId = window.location.hash.substring(1) || 'welcome-screen';
            
            if (!refs.views[viewId]) {
                console.warn(`Vista no encontrada: ${viewId}`);
                navigationSystem.showView('welcome-screen');
                return;
            }
            
            navigationSystem.showView(viewId);
        },
        
        init: () => {
            window.addEventListener('popstate', navigationSystem.handleNavigation);
            document.body.addEventListener('click', (e) => {
                const link = e.target.closest('a[href^="#"]');
                if (link) {
                    e.preventDefault();
                    const viewId = link.getAttribute('href').substring(1);
                    if (window.location.hash !== `#${viewId}`) {
                        history.pushState({ view: viewId }, '', `#${viewId}`);
                    }
                    navigationSystem.handleNavigation();
                }
            });
            
            refs.ui.backToSearchButton.addEventListener('click', () => {
                history.back();
            });
        }
    };

    // ==== SISTEMA DE ALERTAS ====
    const alertSystem = {
        show: (message, type = 'info') => {
            const alertDiv = document.createElement('div');
            alertDiv.className = type === 'error' ? 'alert' :
                                type === 'success' ? 'alert success' :
                                type === 'warning' ? 'alert warning' :
                                'alert info';
            alertDiv.textContent = message;
            
            refs.ui.alertContainer.innerHTML = '';
            refs.ui.alertContainer.appendChild(alertDiv);
            
            setTimeout(() => {
                refs.ui.alertContainer.innerHTML = '';
            }, 5000);
        }
    };

    // ==== SISTEMA DE RENDERIZADO DE RESULTADOS ====
    const resultsSystem = {
        renderPlazaResultados: (data) => {
            const { excel_info, images, google_maps_url, direccion_completa, historial } = data;
            
            const template = document.getElementById('plaza-results-template');
            const clone = template.content.cloneNode(true);
            
            // 1. Cabecera y Dirección
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

            // 2. Definir nombres personalizados
            const nombresPersonalizados = {
                'CN_Inicial_Acum': 'CN Inicial',
                'CN_Prim_Acum': 'CN Primaria',
                'CN_Sec_Acum': 'CN Secundaria',
                'CN_Tot_Acum': 'CN Total',
                'Arq_Discap.': 'Arquitectura para Discap.'
            };

            // 3. Función para renderizar grids con DocumentFragment
            const renderizarGridAislado = (container, info, columns) => {
                container.innerHTML = '';
                
                const fragment = utils.createFragment();
                const wrapper = document.createElement('div');
                wrapper.className = 'custom-plaza-table';
                
                const grid = document.createElement('div');
                grid.className = 'section-grid';
                
                columns.forEach(key => {
                    const value = info[key];
                    const displayValue = utils.safeText(value);
                    const displayKey = nombresPersonalizados[key] || key.replace(/_/g, ' ');
                    
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
                fragment.appendChild(wrapper);
                container.appendChild(fragment);
            };

            // 4. Renderizar tablas estáticas
            const gridUbicacionEl = clone.querySelector('[data-bind="grid_ubicacion"]');
            const gridInfraEl = clone.querySelector('[data-bind="grid_infraestructura"]');
            const gridInventarioEl = clone.querySelector('[data-bind="grid_inventario"]');
            const gridPersonalEl = clone.querySelector('[data-bind="grid_personal"]');

            if (gridUbicacionEl) renderizarGridAislado(gridUbicacionEl, excel_info, config.columns.ubicacion);
            if (gridInfraEl) renderizarGridAislado(gridInfraEl, excel_info, config.columns.infraestructura);
            if (gridInventarioEl) renderizarGridAislado(gridInventarioEl, excel_info, config.columns.inventario);
            if (gridPersonalEl) renderizarGridAislado(gridPersonalEl, excel_info, config.columns.personal);

            // 5. Tabla interactiva (Atención)
            const gridAtencionEl = clone.querySelector('[data-bind="grid_atencion"]');
            let selectAtencion = clone.querySelector('#atencion-periodo-select');
            
            if (gridAtencionEl) {
                if (historial?.length > 0 && selectAtencion) {
                    const mesesNombres = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                                         "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"];
                    
                    historial.forEach((item, index) => {
                        const option = document.createElement('option');
                        option.value = index;
                        
                        const mesNum = parseInt(item['Cve-mes'] || item['Mes'] || 0);
                        const nombreMes = !isNaN(mesNum) && mesNum > 0 ? mesesNombres[mesNum] : (item['Mes'] || 'Mes ' + mesNum);
                        const anio = item['Año'] || '';
                        
                        option.textContent = `${nombreMes} ${anio}`;
                        selectAtencion.appendChild(option);
                    });

                    selectAtencion.addEventListener('change', (e) => {
                        const idx = e.target.value;
                        const datosMes = historial[idx];
                        renderizarGridAislado(gridAtencionEl, datosMes, config.columns.atencion);
                    });

                    renderizarGridAislado(gridAtencionEl, historial[0], config.columns.atencion);
                } else {
                    if (selectAtencion) selectAtencion.style.display = 'none';
                    renderizarGridAislado(gridAtencionEl, excel_info, config.columns.atencion);
                }
            }

            // 6. Imágenes con DocumentFragment
            const imagesContainer = clone.querySelector('[data-bind="images_grid"]');
            if (imagesContainer) {
                imagesContainer.innerHTML = '';
                
                if (images?.length > 0) {
                    const fragment = utils.createFragment();
                    const imageTemplate = document.getElementById('image-item-template');
                    
                    images.forEach((url, index) => {
                        const imageClone = imageTemplate.content.cloneNode(true);
                        const img = imageClone.querySelector('img');
                        img.src = url;
                        img.alt = `Imagen ${index + 1}`;
                        img.loading = "lazy";
                        fragment.appendChild(imageClone);
                    });
                    
                    imagesContainer.appendChild(fragment);
                } else {
                    const noImagesTemplate = document.getElementById('no-images-template');
                    if(noImagesTemplate) imagesContainer.appendChild(noImagesTemplate.content.cloneNode(true));
                }
            }
            
            // 7. Insertar en el DOM
            refs.ui.resultsContent.innerHTML = '';
            refs.ui.resultsContent.appendChild(clone);
            
            // 8. Configurar modal de imágenes
            setTimeout(() => {
                const imageContainers = document.querySelectorAll('.image-container');
                const allImages = images || [];
                
                imageContainers.forEach((container, index) => {
                    container.style.cursor = 'pointer';
                    container.addEventListener('click', () => {
                        if (allImages.length > 0 && state.modalOpenFunction) {
                            state.modalOpenFunction(allImages, index);
                        }
                    });
                });
            }, 100);
        }
    };

    // ==== SISTEMA DE BÚSQUEDA ====
    const searchSystem = {
        buscarYMostrarClave: async (clave, loader) => {
            if (!clave) {
                alertSystem.show('Por favor, introduce una clave válida', 'warning');
                return;
            }
            
            loaderSystem.show(`Buscando plaza con clave: ${clave}`);
            
            try {
                // 1. Usar el endpoint de búsqueda principal
                const data = await utils.fetchData(`/api/search?clave=${encodeURIComponent(clave)}`);
                
                if (!data || (!data.excel_info && !data.datos_organizados)) {
                    throw new Error('No se encontraron datos para esta clave');
                }

                // 2. Obtener historial
                let historial = [];
                try {
                    historial = await utils.fetchData(`/api/plaza-historial?clave=${encodeURIComponent(clave)}`);
                } catch (e) {
                    console.warn("No se pudo cargar el historial", e);
                }
                
                console.log(`🔄 Buscando imágenes para: ${clave}`);
                const imagenesDrive = await imageIndexSystem.find_image_urls_optimized(clave);
                
                // 3. Preparar el objeto de datos completo
                let datosCompletos;
                
                if (data.datos_organizados) {
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
                        excel_info: infoAplanada,
                        historial: historial
                    };
                } else {
                    datosCompletos = {
                        ...data,
                        images: imagenesDrive.length > 0 ? imagenesDrive : (data.images || []),
                        historial: historial
                    };
                }
                
                // 4. Renderizar resultados
                resultsSystem.renderPlazaResultados(datosCompletos);
                
                history.pushState({ view: 'results-view' }, '', '#results-view');
                navigationSystem.handleNavigation();
                
            } catch (error) {
                console.error('Error en búsqueda:', error);
                alertSystem.show(`Error al buscar la clave: ${error.message}`, 'error');
            } finally {
                loaderSystem.hide();
                utils.setLoaderVisible(loader, false);
            }
        }
    };

    // ==== SISTEMA DE FILTROS ====
    const filterSystem = {
        updateStepIndicator: (stepName, status) => {
            const indicator = refs.filterSteps[stepName]?.querySelector('.step-indicator');
            if(indicator) {
                indicator.classList.remove('active', 'completed');
                if (status === 'active') indicator.classList.add('active');
                if (status === 'completed') indicator.classList.add('completed');
            }
        },
        
        populateSelect: async (selectElement, url, placeholder) => {
            selectElement.disabled = true;
            
            const loadingOption = document.createElement('option');
            loadingOption.value = '';
            loadingOption.textContent = 'Cargando...';
            selectElement.innerHTML = '';
            selectElement.appendChild(loadingOption);
            
            utils.setLoaderVisible(refs.ui.filterLoader, true);
            try {
                const options = await utils.fetchData(url);
                selectElement.innerHTML = '';
                
                const defaultOption = document.createElement('option');
                defaultOption.value = '';
                defaultOption.textContent = `-- ${placeholder} --`;
                selectElement.appendChild(defaultOption);
                
                if (options?.length > 0) {
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
                alertSystem.show(`Error al cargar: ${error.message}`, 'error');
                const errorOption = document.createElement('option');
                errorOption.value = '';
                errorOption.textContent = 'Error';
                selectElement.innerHTML = '';
                selectElement.appendChild(errorOption);
            } finally {
                utils.setLoaderVisible(refs.ui.filterLoader, false);
            }
        },
        
        resetSteps: (fromStepName) => {
            const startIndex = config.stepOrder.indexOf(fromStepName);
            if (startIndex === -1) return;
            
            for (let i = startIndex; i < config.stepOrder.length; i++) {
                const stepName = config.stepOrder[i];
                if(refs.filterSteps[stepName]) refs.filterSteps[stepName].classList.add('hidden');
                filterSystem.updateStepIndicator(stepName, 'default');
                if(refs.selects[stepName]) refs.selects[stepName].disabled = true;
            }
        },
        
        navigateToNextStep: (currentStepName) => {
            const currentStepIndex = config.stepOrder.indexOf(currentStepName);
            
            if (currentStepIndex === -1 || currentStepIndex >= config.stepOrder.length - 1) return;
            
            const nextStepName = config.stepOrder[currentStepIndex + 1];
            
            if (refs.filterSteps[nextStepName]) {
                refs.filterSteps[nextStepName].classList.remove('hidden');
                filterSystem.updateStepIndicator(nextStepName, 'active');
            }
            
            for (let i = currentStepIndex + 2; i < config.stepOrder.length; i++) {
                const step = config.stepOrder[i];
                if (refs.filterSteps[step]) {
                    refs.filterSteps[step].classList.add('hidden');
                    filterSystem.updateStepIndicator(step, 'default');
                }
            }
            
            filterSystem.actualizarProgreso();
            
            setTimeout(() => {
                if (refs.filterSteps[nextStepName]) {
                    const elementPosition = refs.filterSteps[nextStepName].getBoundingClientRect().top + window.pageYOffset;
                    const offsetPosition = elementPosition - 100;
                    
                    window.scrollTo({
                        top: offsetPosition,
                        behavior: 'smooth'
                    });
                    
                    if (refs.selects[nextStepName] && !refs.selects[nextStepName].disabled) {
                        refs.selects[nextStepName].focus();
                    }
                }
            }, 300);
        },
        
        actualizarProgreso: () => {
            let completedSteps = 0;
            let activeStepIndex = -1;
            
            config.stepOrder.forEach((stepName, index) => {
                if (refs.selects[stepName]?.value) {
                    completedSteps++;
                }
                if (refs.filterSteps[stepName] && !refs.filterSteps[stepName].classList.contains('hidden')) {
                    activeStepIndex = index;
                }
            });
            
            const progressPercentage = Math.max(20, (completedSteps / config.stepOrder.length) * 100);
            if (refs.progress.fill) {
                refs.progress.fill.style.width = `${progressPercentage}%`;
            }
            
            refs.progress.steps.forEach((stepElement, index) => {
                const stepName = stepElement.getAttribute('data-step');
                const stepIndex = config.stepOrder.indexOf(stepName);
                
                stepElement.classList.remove('active', 'completed');
                
                if (stepIndex < completedSteps) {
                    stepElement.classList.add('completed');
                } else if (stepIndex === completedSteps && activeStepIndex >= stepIndex) {
                    stepElement.classList.add('active');
                }
            });
        },
        
        resetSearch: () => {
            loaderSystem.show('Reiniciando búsqueda...', 'compact');
            
            setTimeout(() => {
                Object.values(refs.selects).forEach(select => {
                    if (select) {
                        select.selectedIndex = 0;
                        select.disabled = select.id !== 'state-select';
                    }
                });
                
                Object.keys(refs.filterSteps).forEach(stepName => {
                    if (refs.filterSteps[stepName]) {
                        refs.filterSteps[stepName].classList.toggle('hidden', stepName !== 'estado');
                        filterSystem.updateStepIndicator(stepName, stepName === 'estado' ? 'active' : 'default');
                    }
                });
                
                filterSystem.actualizarProgreso();
                
                const filterSection = document.querySelector('.search-container');
                if (filterSection) {
                    const elementPosition = filterSection.getBoundingClientRect().top + window.pageYOffset;
                    window.scrollTo({
                        top: elementPosition - 80,
                        behavior: 'smooth'
                    });
                }
                
                if (refs.selects.estado) {
                    setTimeout(() => refs.selects.estado.focus(), 500);
                }
                
                loaderSystem.hide();
                alertSystem.show('Búsqueda reiniciada correctamente', 'success');
            }, 800);
        },
        
        setupProgressBarNavigation: () => {
            refs.progress.steps.forEach(step => {
                step.addEventListener('click', () => {
                    const stepName = step.getAttribute('data-step');
                    const targetIndex = config.stepOrder.indexOf(stepName);
                    
                    if (targetIndex > 0) {
                        const prevStepName = config.stepOrder[targetIndex - 1];
                        if (!refs.selects[prevStepName]?.value) {
                            alertSystem.show(`Por favor, completa primero el paso de '${prevStepName}'.`, 'warning');
                            return;
                        }
                    }

                    if (refs.filterSteps[stepName]) {
                        const elementPosition = refs.filterSteps[stepName].getBoundingClientRect().top + window.pageYOffset;
                        const offsetPosition = elementPosition - 100;
                        window.scrollTo({
                            top: offsetPosition,
                            behavior: 'smooth'
                        });
                        if (refs.selects[stepName] && !refs.selects[stepName].disabled) {
                            refs.selects[stepName].focus();
                        }
                    }
                });
            });
        },
        
        handleFilterSearch: () => {
            const clave = refs.selects.clave.value;
            if (clave) {
                searchSystem.buscarYMostrarClave(clave, refs.ui.filterLoader);
            } else {
                alertSystem.show('Por favor, completa todos los filtros hasta seleccionar una clave de plaza.', 'warning');
            }
        },
        
        handleSearchByKey: () => {
            const clave = refs.search.claveInput.value.trim();
            if (clave) {
                searchSystem.buscarYMostrarClave(clave, refs.search.keyLoader);
            } else {
                alertSystem.show('Por favor, introduce una clave para buscar.', 'warning');
            }
        },
        
        addAutoSearchToggle: () => {
            if (!refs.ui.searchFilterButton) return;
            
            const toggleContainer = document.createElement('div');
            toggleContainer.className = 'auto-search-toggle';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = 'auto-search-toggle';
            
            const label = document.createElement('label');
            label.htmlFor = 'auto-search-toggle';
            label.textContent = 'Búsqueda automática al seleccionar clave';
            
            toggleContainer.appendChild(checkbox);
            toggleContainer.appendChild(label);
            refs.ui.searchFilterButton.insertAdjacentElement('beforebegin', toggleContainer);
        },
        
        setupKeyboardNavigation: () => {
            // Navegación con Enter en selects
            Object.values(refs.selects).forEach((select) => {
                if (select) {
                    select.addEventListener('keydown', (e) => {
                        if (e.key === 'Enter') {
                            e.preventDefault();
                            const selectArray = Object.values(refs.selects);
                            const currentIndex = selectArray.indexOf(select);
                            
                            if (currentIndex < selectArray.length - 1) {
                                const nextSelect = selectArray[currentIndex + 1];
                                if (nextSelect && !nextSelect.disabled) {
                                    nextSelect.focus();
                                }
                            } else {
                                filterSystem.handleFilterSearch();
                            }
                        }
                    });
                }
            });

            // Atajos de teclado globales
            document.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') {
                    history.back();
                }
                
                if (!refs.views['welcome-screen'].classList.contains('hidden')) {
                    const shortcuts = {
                        '1': 'a[href="#key-search-view"]',
                        '2': 'a[href="#filter-search-view"]',
                        '3': 'a[href="#stats-view"]',
                        '4': 'a[href="#estados-view"]'
                    };
                    
                    if (shortcuts[e.key]) {
                        const link = document.querySelector(shortcuts[e.key]);
                        if (link) link.click();
                    }
                }
            });
        },
        
        initFilterListeners: () => {
            // Configuración DRY de listeners para selects en cascada
            const filterConfig = [
                {
                    select: 'estado',
                    nextSelect: 'zona',
                    url: (estado) => `/api/zonas?estado=${encodeURIComponent(estado)}`,
                    placeholder: 'Selecciona una Zona'
                },
                {
                    select: 'zona',
                    nextSelect: 'municipio',
                    url: (zona) => `/api/municipios?estado=${encodeURIComponent(refs.selects.estado.value)}&zona=${encodeURIComponent(zona)}`,
                    placeholder: 'Selecciona un Municipio'
                },
                {
                    select: 'municipio',
                    nextSelect: 'localidad',
                    url: (municipio) => `/api/localidades?estado=${encodeURIComponent(refs.selects.estado.value)}&zona=${encodeURIComponent(refs.selects.zona.value)}&municipio=${encodeURIComponent(municipio)}`,
                    placeholder: 'Selecciona una Localidad'
                },
                {
                    select: 'localidad',
                    nextSelect: 'clave',
                    url: (localidad) => `/api/claves_plaza?estado=${encodeURIComponent(refs.selects.estado.value)}&zona=${encodeURIComponent(refs.selects.zona.value)}&municipio=${encodeURIComponent(refs.selects.municipio.value)}&localidad=${encodeURIComponent(localidad)}`,
                    placeholder: 'Selecciona la Clave'
                }
            ];

            filterConfig.forEach((configItem, index) => {
                refs.selects[configItem.select].addEventListener('change', () => {
                    const value = refs.selects[configItem.select].value;
                    filterSystem.resetSteps(configItem.nextSelect);
                    filterSystem.updateStepIndicator(configItem.select, value ? 'completed' : 'active');
                    
                    if (value) {
                        filterSystem.populateSelect(
                            refs.selects[configItem.nextSelect],
                            configItem.url(value),
                            configItem.placeholder
                        );
                        setTimeout(() => filterSystem.navigateToNextStep(configItem.select), 100);
                    }
                    
                    filterSystem.actualizarProgreso();
                });
            });

            // Listener para clave (búsqueda automática)
            refs.selects.clave.addEventListener('change', () => {
                const clave = refs.selects.clave.value;
                filterSystem.updateStepIndicator('clave', clave ? 'completed' : 'active');
                filterSystem.actualizarProgreso();
                
                if (clave && document.getElementById('auto-search-toggle')?.checked) {
                    setTimeout(() => filterSystem.handleFilterSearch(), 500);
                }
            });

            // Event listeners para botones
            if (refs.ui.searchFilterButton) {
                refs.ui.searchFilterButton.addEventListener('click', filterSystem.handleFilterSearch);
            }
            
            refs.search.searchByKeyButton.addEventListener('click', filterSystem.handleSearchByKey);
            refs.search.claveInput.addEventListener('keyup', (e) => e.key === 'Enter' && filterSystem.handleSearchByKey());
        }
    };

    // ==== SISTEMA DE ESTADÍSTICAS - CON EL DISEÑO ORIGINAL DE LA TABLA ====
    const statsSystem = {
        renderResumenCN: () => {
            if (!refs.stats.cn.resumenCards || !state.cnResumenData?.resumen_nacional) return;
            
            const { resumen_nacional, top5_estados_por_CN_Total } = state.cnResumenData;
            
            refs.stats.cn.resumenCards.innerHTML = '';
            
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
            refs.stats.cn.resumenCards.appendChild(resumenCard);
            
            // Top 5 estados
            if (top5_estados_por_CN_Total?.length > 0) {
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
                refs.stats.cn.resumenCards.appendChild(top5Card);
            }
        },
        
        // ===== DISEÑO ORIGINAL DE "DATOS DETALLADOS POR ESTADO" =====
        renderTablaEstadosCN: () => {
            if (!refs.stats.cn.estadosTable || !state.cnPorEstadoData?.estados) return;

            const { estados } = state.cnPorEstadoData;
            let currentData = [...estados];

            // ---------------------------------------------------------
            // 1. LIMPIAR CONTENEDOR Y APLICAR DISEÑO ESPECÍFICO
            // ---------------------------------------------------------
            refs.stats.cn.estadosTable.innerHTML = '';
            refs.stats.cn.estadosTable.className = 'cn-table-container';
            
            // Aplicar estilos específicos solo para esta tabla
            refs.stats.cn.estadosTable.style.background = 'var(--surface-color)';
            refs.stats.cn.estadosTable.style.border = '1px solid var(--border-color)';
            refs.stats.cn.estadosTable.style.borderRadius = 'var(--border-radius-lg)';
            refs.stats.cn.estadosTable.style.overflowX = 'auto';
            refs.stats.cn.estadosTable.style.maxHeight = '650px';
            refs.stats.cn.estadosTable.style.boxShadow = 'var(--shadow)';
            refs.stats.cn.estadosTable.style.marginBottom = '2rem';
            refs.stats.cn.estadosTable.style.position = 'relative';

            const table = document.createElement('table');
            table.className = 'cn-table';
            
            // Aplicar estilos específicos a la tabla
            table.style.width = '100%';
            table.style.borderCollapse = 'separate';
            table.style.borderSpacing = '0';
            table.style.position = 'relative';

            // ---------------------------------------------------------
            // 2. CALCULAR TOTALES (FOOTER)
            // ---------------------------------------------------------
            const totales = estados.reduce((acc, curr) => ({
                total_plazas: acc.total_plazas + (curr.total_plazas || 0),
                plazas_operacion: acc.plazas_operacion + (curr.plazas_operacion || 0),
                conectados_actual: acc.conectados_actual + (curr.conectados_actual || 0),
                cn_inicial: acc.cn_inicial + (curr.suma_CN_Inicial_Acum || 0),
                cn_primaria: acc.cn_primaria + (curr.suma_CN_Prim_Acum || 0),
                cn_secundaria: acc.cn_secundaria + (curr.suma_CN_Sec_Acum || 0),
                cn_total: acc.cn_total + (curr.suma_CN_Total || 0)
            }), {
                total_plazas: 0, plazas_operacion: 0, conectados_actual: 0,
                cn_inicial: 0, cn_primaria: 0, cn_secundaria: 0, cn_total: 0
            });

            const pctGlobalConectividad = totales.total_plazas > 0
                ? ((totales.conectados_actual / totales.total_plazas) * 100).toFixed(1)
                : 0;

            // ---------------------------------------------------------
            // 3. HEADER CON DISEÑO ESPECIAL
            // ---------------------------------------------------------
            const thead = document.createElement('thead');
            const headerRow = document.createElement('tr');

            const headers = [
                { text: 'Estado', sort: 'estado', order: 'asc' },
                { text: 'Total Plazas', sort: 'total_plazas', order: 'desc' },
                { text: 'Plazas Operación', sort: 'plazas_operacion', order: 'desc' },
                { text: '% Conectividad', sort: 'pct_conectividad', order: 'desc' },
                { text: 'CN Inicial', sort: 'cn_inicial', order: 'desc' },
                { text: 'CN Primaria', sort: 'cn_primaria', order: 'desc' },
                { text: 'CN Secundaria', sort: 'cn_secundaria', order: 'desc' },
                { text: 'CN Total', sort: 'cn_total', order: 'desc' },
                { text: '% Sobre Nacional', sort: 'pct_nacional', order: 'desc' }
            ];

            headers.forEach(h => {
                const th = document.createElement('th');
                
                // Estilos específicos para header
                th.style.background = 'linear-gradient(135deg, var(--primary-color), var(--primary-dark))';
                th.style.color = 'var(--surface-color)';
                th.style.padding = '1rem';
                th.style.textAlign = 'left';
                th.style.fontWeight = '600';
                th.style.fontSize = 'var(--font-size-sm)';
                th.style.position = 'sticky';
                th.style.top = '0';
                th.style.zIndex = '5';
                
                const btn = document.createElement('button');

                btn.className = 'sort-btn';
                btn.textContent = `${h.text} ${h.order === 'asc' ? '▲' : '▼'}`;
                btn.dataset.sort = h.sort;
                btn.dataset.order = h.order;
                
                // Estilos específicos para botones de ordenamiento
                btn.style.background = 'transparent';
                btn.style.border = 'none';
                btn.style.color = 'inherit';
                btn.style.cursor = 'pointer';
                btn.style.fontWeight = 'inherit';
                btn.style.fontSize = 'inherit';
                btn.style.display = 'flex';
                btn.style.alignItems = 'center';
                btn.style.gap = '0.25rem';

                th.appendChild(btn);
                headerRow.appendChild(th);
            });

            thead.appendChild(headerRow);
            table.appendChild(thead);

            // ---------------------------------------------------------
            // 4. BODY CON ESTILOS ESPECÍFICOS Y DOCUMENTFRAGMENT
            // ---------------------------------------------------------
            const tbody = document.createElement('tbody');
            table.appendChild(tbody);

            const crearCeldaTexto = (texto, strong = false) => {
                const td = document.createElement('td');
                td.style.padding = '0.875rem 1rem';
                td.style.borderBottom = '1px solid var(--border-color)';
                td.style.fontSize = 'var(--font-size-sm)';
                
                if (strong) {
                    const s = document.createElement('strong');
                    s.textContent = texto;
                    td.appendChild(s);
                } else {
                    td.textContent = texto;
                }
                return td;
            };

            const crearCeldaBadge = (texto, badgeClass, tooltip = '') => {
                const td = document.createElement('td');
                td.style.padding = '0.875rem 1rem';
                td.style.borderBottom = '1px solid var(--border-color)';
                td.style.fontSize = 'var(--font-size-sm)';
                
                const badge = document.createElement('span');
                badge.className = `cn-badge ${badgeClass}`;
                badge.textContent = texto;
                
                // Estilos para badge
                badge.style.padding = '0.25rem 0.5rem';
                badge.style.borderRadius = '1rem';
                badge.style.fontSize = 'var(--font-size-xs)';
                badge.style.fontWeight = '600';
                badge.style.display = 'inline-block';
                
                if (tooltip) {
                    td.title = tooltip;
                    td.style.cursor = 'help';
                }
                td.appendChild(badge);
                return td;
            };

            // FUNCIÓN PARA CREAR CELDA DE PORCENTAJE VERDE
            const crearCeldaPorcentajeVerde = (porcentaje) => {
                const td = document.createElement('td');
                td.style.padding = '0.875rem 1rem';
                td.style.borderBottom = '1px solid var(--border-color)';
                td.style.fontSize = 'var(--font-size-sm)';
                td.style.textAlign = 'center';
                
                const porcentajeNum = parseFloat(porcentaje) || 0;
                
                // Crear píldora verde
                const pill = document.createElement('span');
                pill.textContent = `${porcentajeNum}%`;
                pill.style.display = 'inline-flex';
                pill.style.alignItems = 'center';
                pill.style.justifyContent = 'center';
                pill.style.minWidth = '70px';
                pill.style.padding = '6px 16px';
                pill.style.borderRadius = '50px';
                pill.style.fontSize = '0.9rem';
                pill.style.fontWeight = '700';
                pill.style.lineHeight = '1';
                pill.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.1)';
                pill.style.color = '#059669'; // Verde
                pill.style.background = 'rgba(5, 150, 105, 0.1)';
                pill.style.border = '1px solid rgba(5, 150, 105, 0.2)';
                
                // Gradiente de intensidad según el porcentaje
                if (porcentajeNum > 50) {
                    pill.style.background = 'rgba(5, 150, 105, 0.15)';
                    pill.style.boxShadow = '0 2px 8px rgba(5, 150, 105, 0.2)';
                } else if (porcentajeNum > 20) {
                    pill.style.background = 'rgba(5, 150, 105, 0.1)';
                }
                
                td.appendChild(pill);
                td.title = `Contribución del estado al total nacional: ${porcentajeNum}%`;
                td.style.cursor = 'help';
                
                return td;
            };

            const renderRows = (data) => {
                tbody.innerHTML = '';
                const fragment = utils.createFragment();

                data.forEach(estado => {
                    const row = document.createElement('tr');
                    
                    // Efecto hover para filas
                    row.style.transition = 'background var(--transition-fast)';
                    row.addEventListener('mouseenter', () => {
                        row.style.background = 'var(--light-color)';
                    });
                    row.addEventListener('mouseleave', () => {
                        row.style.background = 'transparent';
                    });

                    const pct = estado.pct_conectividad || 0;
                    const conectados = estado.conectados_actual || 0;
                    const plazasOperacion = estado.plazas_operacion || 0;
                    const pctSobreNacional = estado.pct_sobre_nacional || 0;

                    // Estado
                    const tdEstado = crearCeldaTexto(estado.estado, true);
                    tdEstado.style.fontWeight = '700';
                    tdEstado.style.color = 'var(--primary-color)';
                    row.appendChild(tdEstado);

                    // Total Plazas
                    row.appendChild(crearCeldaTexto(estado.total_plazas?.toLocaleString() || '0'));

                    // Plazas Operación (badge verde + tooltip)
                    const tdOperacion = crearCeldaBadge(
                        plazasOperacion.toLocaleString(),
                        'badge-success',
                        'Plazas en operación del último mes'
                    );
                    tdOperacion.querySelector('.cn-badge').style.background = 'rgba(5, 150, 105, 0.1)';
                    tdOperacion.querySelector('.cn-badge').style.color = 'var(--secondary-color)';
                    row.appendChild(tdOperacion);

                    // % Conectividad (píldora)
                    let badgeColor = '';
                    let badgeBg = '';
                    if (pct < 50) {
                        badgeColor = '#dc2626';
                        badgeBg = 'rgba(220, 38, 38, 0.1)';
                    } else if (pct < 70) {
                        badgeColor = '#f59e0b';
                        badgeBg = 'rgba(245, 158, 11, 0.1)';
                    } else {
                        badgeColor = '#059669';
                        badgeBg = 'rgba(5, 150, 105, 0.1)';
                    }

                    const tdPct = document.createElement('td');
                    tdPct.style.padding = '0.875rem 1rem';
                    tdPct.style.borderBottom = '1px solid var(--border-color)';
                    
                    const pill = document.createElement('span');
                    pill.textContent = `${pct}%`;
                    pill.style.display = 'inline-flex';
                    pill.style.alignItems = 'center';
                    pill.style.justifyContent = 'center';
                    pill.style.minWidth = '70px';
                    pill.style.padding = '6px 16px';
                    pill.style.borderRadius = '50px';
                    pill.style.fontSize = '0.9rem';
                    pill.style.fontWeight = '700';
                    pill.style.lineHeight = '1';
                    pill.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.1)';
                    pill.style.color = badgeColor;
                    pill.style.background = badgeBg;
                    pill.style.border = `1px solid ${badgeColor}20`;
                    
                    tdPct.appendChild(pill);
                    tdPct.title = `${conectados} de ${estado.total_plazas || 0} plazas conectadas`;
                    tdPct.style.cursor = 'help';
                    row.appendChild(tdPct);

                    // Resto de columnas CN
                    row.appendChild(crearCeldaTexto(estado.suma_CN_Inicial_Acum?.toLocaleString() || '0'));
                    row.appendChild(crearCeldaTexto(estado.suma_CN_Prim_Acum?.toLocaleString() || '0'));
                    row.appendChild(crearCeldaTexto(estado.suma_CN_Sec_Acum?.toLocaleString() || '0'));
                    row.appendChild(crearCeldaTexto(estado.suma_CN_Total?.toLocaleString() || '0'));
                    
                    // % Sobre Nacional EN VERDE
                    row.appendChild(crearCeldaPorcentajeVerde(pctSobreNacional));

                    fragment.appendChild(row);
                });

                tbody.appendChild(fragment);
            };

            renderRows(currentData);

            // ---------------------------------------------------------
            // 5. FOOTER FIJADO CON ESTILOS ESPECIALES
            // ---------------------------------------------------------
            const tfoot = document.createElement('tfoot');
            const fr = document.createElement('tr');
            
            // Estilo para fila de totales FIJADA
            fr.style.background = 'var(--light-color)';
            fr.style.fontWeight = '700';
            fr.style.position = 'sticky';
            fr.style.bottom = '0';
            fr.style.zIndex = '4';
            
            // Footer badge para conectividad global
            let footerBadgeColor = '';
            let footerBadgeBg = '';
            if (pctGlobalConectividad < 50) {
                footerBadgeColor = '#dc2626';
                footerBadgeBg = 'rgba(220, 38, 38, 0.1)';
            } else if (pctGlobalConectividad < 70) {
                footerBadgeColor = '#f59e0b';
                footerBadgeBg = 'rgba(245, 158, 11, 0.1)';
            } else {
                footerBadgeColor = '#059669';
                footerBadgeBg = 'rgba(5, 150, 105, 0.1)';
            }

            // FUNCIÓN PARA CREAR CELDA DE FOOTER CON ESTILO
            const crearCeldaFooter = (contenido, esPorcentaje = false, esPorcentajeVerde = false) => {
                const td = document.createElement('td');
                td.style.padding = '0.875rem 1rem';
                td.style.borderTop = '2px solid var(--border-color)';
                td.style.fontWeight = '600';
                td.style.textAlign = 'center';
                
                if (esPorcentaje) {
                    const pill = document.createElement('span');
                    pill.textContent = contenido;
                    pill.style.display = 'inline-flex';
                    pill.style.alignItems = 'center';
                    pill.style.justifyContent = 'center';
                    pill.style.minWidth = '70px';
                    pill.style.padding = '6px 16px';
                    pill.style.borderRadius = '50px';
                    pill.style.fontSize = '0.9rem';
                    pill.style.fontWeight = '700';
                    pill.style.lineHeight = '1';
                    pill.style.boxShadow = '0 2px 8px rgba(0, 0, 0, 0.15)';
                    
                    if (esPorcentajeVerde) {
                        // Píldora verde para % Sobre Nacional
                        pill.style.color = '#059669';
                        pill.style.background = 'rgba(5, 150, 105, 0.15)';
                        pill.style.border = '1px solid rgba(5, 150, 105, 0.3)';
                    } else {
                        // Píldora para % Conectividad
                        pill.style.color = footerBadgeColor;
                        pill.style.background = footerBadgeBg;
                        pill.style.border = `1px solid ${footerBadgeColor}30`;
                    }
                    
                    td.appendChild(pill);
                } else {
                    td.textContent = contenido;
                }
                
                return td;
            };

            const footerCells = [
                { contenido: 'TOTALES', esPorcentaje: false, esPorcentajeVerde: false },
                { contenido: totales.total_plazas.toLocaleString(), esPorcentaje: false, esPorcentajeVerde: false },
                { contenido: totales.plazas_operacion.toLocaleString(), esPorcentaje: false, esPorcentajeVerde: false },
                { contenido: `${pctGlobalConectividad}%`, esPorcentaje: true, esPorcentajeVerde: false },
                { contenido: totales.cn_inicial.toLocaleString(), esPorcentaje: false, esPorcentajeVerde: false },
                { contenido: totales.cn_primaria.toLocaleString(), esPorcentaje: false, esPorcentajeVerde: false },
                { contenido: totales.cn_secundaria.toLocaleString(), esPorcentaje: false, esPorcentajeVerde: false },
                { contenido: totales.cn_total.toLocaleString(), esPorcentaje: false, esPorcentajeVerde: false },
                { contenido: '100%', esPorcentaje: true, esPorcentajeVerde: true }
            ];

            footerCells.forEach(cell => {
                const td = crearCeldaFooter(cell.contenido, cell.esPorcentaje, cell.esPorcentajeVerde);
                fr.appendChild(td);
            });

            tfoot.appendChild(fr);
            table.appendChild(tfoot);
            refs.stats.cn.estadosTable.appendChild(table);

            // ---------------------------------------------------------
            // 6. SORTING CON EFECTOS VISUALES Y DELEGACIÓN DE EVENTOS
            // ---------------------------------------------------------
            const sortTableData = (field, order) => {
                currentData.sort((a, b) => {
                    if (field === 'estado') {
                        return order === 'asc'
                            ? a.estado.localeCompare(b.estado)
                            : b.estado.localeCompare(a.estado);
                    }

                    const map = {
                        total_plazas: 'total_plazas',
                        plazas_operacion: 'plazas_operacion',
                        pct_conectividad: 'pct_conectividad',
                        cn_inicial: 'suma_CN_Inicial_Acum',
                        cn_primaria: 'suma_CN_Prim_Acum',
                        cn_secundaria: 'suma_CN_Sec_Acum',
                        cn_total: 'suma_CN_Total',
                        pct_nacional: 'pct_sobre_nacional'
                    };

                    const A = a[map[field]] || 0;
                    const B = b[map[field]] || 0;
                    return order === 'asc' ? A - B : B - A;
                });

                renderRows(currentData);
                
                // Efecto visual al ordenar
                refs.stats.cn.estadosTable.style.animation = 'highlight-pulse 2s ease-in-out';
                setTimeout(() => {
                    refs.stats.cn.estadosTable.style.animation = '';
                }, 2000);
            };

            // Delegación de eventos para sorting
            thead.addEventListener('click', (e) => {
                const btn = e.target.closest('.sort-btn');
                if (!btn) return;

                const field = btn.dataset.sort;
                const order = btn.dataset.order === 'asc' ? 'desc' : 'asc';

                // Actualizar todos los botones
                thead.querySelectorAll('.sort-btn').forEach(b => {
                    b.dataset.order = 'desc';
                    b.textContent = b.textContent.replace(/[▲▼]/g, '') + ' ▼';
                });

                // Actualizar botón actual
                btn.dataset.order = order;
                btn.textContent = btn.textContent.replace(/[▲▼]/g, '') + (order === 'asc' ? ' ▲' : ' ▼');
                
                // Efecto visual en el botón
                btn.style.transform = 'scale(0.95)';
                setTimeout(() => {
                    btn.style.transform = 'scale(1)';
                }, 150);

                sortTableData(field, order);
            });
            
            // Efecto hover para botones (usando delegación)
            thead.addEventListener('mouseover', (e) => {
                const btn = e.target.closest('.sort-btn');
                if (btn) btn.style.opacity = '0.8';
            });
            
            thead.addEventListener('mouseout', (e) => {
                const btn = e.target.closest('.sort-btn');
                if (btn) btn.style.opacity = '1';
            });
        },
        
        renderEstadisticasCN: () => {
            if (!state.cnResumenData || !state.cnPorEstadoData || !state.cnTopEstadosData) return;
            statsSystem.renderResumenCN();
            statsSystem.renderTablaEstadosCN();
        },
        
        renderEstadosDestacadosCN: (estadosDestacados) => {
            if (!estadosDestacados) return;
            
            const setStat = (nombreElement, cantidadElement, data) => {
                if (data) {
                    nombreElement.textContent = data.estado;
                    cantidadElement.textContent = data.valor.toLocaleString();
                }
            };
            
            setStat(refs.stats.cn.estadoMasCNInicialNombre, refs.stats.cn.estadoMasCNInicialCantidad, estadosDestacados.CN_Inicial_Acum);
            setStat(refs.stats.cn.estadoMasCNPrimariaNombre, refs.stats.cn.estadoMasCNPrimariaCantidad, estadosDestacados.CN_Prim_Acum);
            setStat(refs.stats.cn.estadoMasCNSecundariaNombre, refs.stats.cn.estadoMasCNSecundariaCantidad, estadosDestacados.CN_Sec_Acum);
        },
        
        renderTop5TodosCN: (top5Todos) => {
            if (!top5Todos) return;
            
            const renderList = (listElement, data) => {
                if (!listElement || !data) return;
                listElement.innerHTML = '';
                
                const fragment = utils.createFragment();
                
                data.forEach((item, index) => {
                    const itemDiv = document.createElement('div');
                    itemDiv.className = 'top10-item';
                    
                    const rank = document.createElement('span');
                    rank.className = 'top10-rank';
                    rank.textContent = `#${index + 1}`;
                    
                    const stateSpan = document.createElement('span');
                    stateSpan.className = 'top10-state';
                    stateSpan.textContent = item.estado;
                    
                    const value = document.createElement('span');
                    value.className = 'top10-value';
                    value.textContent = item.valor.toLocaleString();
                    
                    itemDiv.appendChild(rank);
                    itemDiv.appendChild(stateSpan);
                    itemDiv.appendChild(value);
                    fragment.appendChild(itemDiv);
                });
                
                listElement.appendChild(fragment);
            };
            
            renderList(refs.stats.cn.top5InicialList, top5Todos.inicial);
            renderList(refs.stats.cn.top5PrimariaList, top5Todos.primaria);
            renderList(refs.stats.cn.top5SecundariaList, top5Todos.secundaria);
        },
        
        cargarEstadisticasCompletasCN: async () => {
            try {
                loaderSystem.show('Cargando estadísticas ...', 'compact');
                
                const [resumen, porEstado, topEstados, estadosDestacados, top5Todos] = await Promise.all([
                    utils.fetchData('/api/cn_resumen'),
                    utils.fetchData('/api/cn_por_estado'),
                    utils.fetchData('/api/cn_top_estados?metric=inicial&n=10'),
                    utils.fetchData('/api/cn_estados_destacados'),
                    utils.fetchData('/api/cn_top5_todos')
                ]);
                
                state.cnResumenData = resumen;
                state.cnPorEstadoData = porEstado;
                state.cnTopEstadosData = topEstados;
                state.cnEstadosDestacadosData = estadosDestacados;
                state.cnTop5TodosData = top5Todos;
                
                statsSystem.renderEstadisticasCN();
                statsSystem.renderEstadosDestacadosCN(estadosDestacados);
                statsSystem.renderTop5TodosCN(top5Todos);
                
            } catch (error) {
                console.error('Error cargando estadísticas completas CN:', error);
                alertSystem.show('Error al cargar las estadísticas', 'error');
            } finally {
                loaderSystem.hide();
            }
        },
        
        cargarEstadisticas: async () => {
            try {
                loaderSystem.show('Cargando estadísticas del sistema...', 'compact');
                const statsContainer = refs.views['stats-view'];
                if (statsContainer) statsContainer.classList.add('shimmer');
                
                const stats = await utils.fetchData('/api/estadisticas');
                state.estadisticasData = stats;

                // Actualizar estadísticas generales
                const generalStats = refs.stats.general;
                Object.entries({
                    totalPlazas: stats.totalPlazas?.toLocaleString() || '0',
                    plazasOperacion: stats.plazasOperacion?.toLocaleString() || '0',
                    totalEstados: stats.totalEstados?.toLocaleString() || '0',
                    estadoMasPlazasNombre: stats.estadoMasPlazas?.nombre || 'N/A',
                    estadoMasPlazasCantidad: stats.estadoMasPlazas?.cantidad?.toLocaleString() || '0',
                    estadoMayorConectividadNombre: stats.estadoMayorConectividad?.nombre || 'N/A',
                    estadoMayorConectividadPorcentaje: `${stats.estadoMayorConectividad?.porcentaje || 0}%`,
                    estadoMasOperacionNombre: stats.estadoMasOperacion?.nombre || 'N/A',
                    estadoMasOperacionPorcentaje: `${stats.estadoMasOperacion?.porcentaje || 0}%`,
                    estadoMasSuspensionNombre: stats.estadoMasSuspension?.nombre || 'N/A',
                    estadoMasSuspensionPorcentaje: `${stats.estadoMasSuspension?.porcentaje || 0}%`
                }).forEach(([key, value]) => {
                    if (generalStats[key]) generalStats[key].textContent = value;
                });

                await statsSystem.cargarEstadisticasCompletasCN();

            } catch (error) {
                console.error('Error cargando estadísticas:', error);
                alertSystem.show('Error al cargar las estadísticas desde el servidor.', 'error');
            } finally {
                loaderSystem.hide();
                const statsContainer = refs.views['stats-view'];
                if (statsContainer) statsContainer.classList.remove('shimmer');
            }
        },
        
       initStatsNavigation: () => {
            // 1. CORRECCIÓN CLAVE: Seleccionamos el contenedor de los botones (.stats-nav), no el de las vistas
            const navContainer = document.querySelector('.stats-nav');
            
            if (!navContainer) {
                console.warn('⚠️ Elemento .stats-nav no encontrado en el DOM');
                return;
            }
            
            // 2. Delegación de eventos (Event Delegation)
            navContainer.addEventListener('click', (e) => {
                // Buscamos el botón más cercano (por si el usuario hace clic en el ícono <i> o el <span>)
                const btn = e.target.closest('.stats-nav-btn');
                
                // Si no se hizo clic en un botón, salimos
                if (!btn) return; 
                
                const targetSubview = btn.getAttribute('data-subview');
                
                // 3. Actualización de la UI (Pestañas)
                // Removemos 'active' de todos los botones usando las referencias cacheadas
                if (refs.stats.navBtns) {
                    refs.stats.navBtns.forEach(b => b.classList.remove('active'));
                }
                btn.classList.add('active');
                
                // 4. Mostrar/Ocultar Vistas
                if (refs.stats.subviews) {
                    refs.stats.subviews.forEach(view => {
                        // Comparamos el ID de la vista con el target del botón
                        // Ejemplo: 'comparativas-stats' -> 'comparativas-stats-view'
                        if (view.id === `${targetSubview}-view`) {
                            view.classList.remove('hidden');
                        } else {
                            view.classList.add('hidden');
                        }
                    });
                }
                
                // 5. Inicialización Diferida (Lazy Load) del Módulo Comparativas
                if (targetSubview === 'comparativas-stats') {
                    console.log('🔄 Inicializando módulo de comparativas...');
                    
                    // Usamos un pequeño timeout para asegurar que el cambio de clase 'hidden' ya se procesó en el DOM
                    setTimeout(() => {
                        // Verificamos si la instancia global existe (creada en comparativas.js)
                        if (window.sistemaComparativas && typeof window.sistemaComparativas.init === 'function') {
                            window.sistemaComparativas.init();
                        } else {
                            console.error('❌ Error: window.sistemaComparativas no está definido. Revisa que comparativas.js se cargue correctamente.');
                            alertSystem.show('No se pudo inicializar el módulo de comparativas.', 'error');
                        }
                    }, 50);
                }
            });
        }
    };

    // ==== SISTEMA DE ESTADOS Y PLAZAS ====
    const estadosSystem = {
        cargarEstadosConPlazas: async () => {
            try {
                loaderSystem.show('Obteniendo lista de estados y plazas...');
                
                const estados = await utils.fetchData('/api/estados_con_conteo');
                estados.sort((a, b) => b.cantidad - a.cantidad);
                
                state.todosEstadosData = estados;
                estadosSystem.renderEstadosConPlazas(estados);
                estadosSystem.setupBusquedaEstadosView(estados);
                
            } catch (error) {
                console.error('Error cargando estados con plazas:', error);
                alertSystem.show('Error al cargar los estados', 'error');
            } finally {
                loaderSystem.hide();
            }
        },
        
        renderEstadosConPlazas: (estados) => {
            if (!refs.estados.grid) return;
            refs.estados.grid.innerHTML = '';
            
            if (estados.length === 0) {
                const noResults = document.createElement('p');
                noResults.className = 'no-results';
                noResults.textContent = 'No se encontraron estados.';
                refs.estados.grid.appendChild(noResults);
                return;
            }
            
            const fragment = utils.createFragment();
            
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
                fragment.appendChild(item);
            });
            
            refs.estados.grid.appendChild(fragment);
            
            // Delegación de eventos para los items de estado
            refs.estados.grid.addEventListener('click', (e) => {
                const item = e.target.closest('.state-menu-item');
                if (!item) return;
                
                const estadoNombre = item.querySelector('.state-menu-name').textContent;
                const estadoData = state.todosEstadosData.find(e => e.nombre === estadoNombre);
                
                if (estadoData) {
                    state.estadoSeleccionado = estadoData.nombre;
                    history.pushState({ view: 'plazas-por-estado-view' }, '', '#plazas-por-estado-view');
                    navigationSystem.handleNavigation();
                    plazasSystem.cargarPlazasPorEstado(estadoData.nombre);
                }
            });
        },
        
        setupBusquedaEstadosView: (estadosData) => {
            if (!refs.estados.searchInput) return;
            
            const debouncedSearch = utils.debounce((query) => {
                const queryLower = query.toLowerCase().trim();
                if (!queryLower) {
                    estadosSystem.renderEstadosConPlazas(estadosData);
                    return;
                }
                const estadosFiltrados = estadosData.filter(estado =>
                    estado.nombre.toLowerCase().includes(queryLower)
                );
                estadosSystem.renderEstadosConPlazas(estadosFiltrados);
            }, 300);
            
            refs.estados.searchInput.addEventListener('input', (e) => debouncedSearch(e.target.value));
        }
    };

    const plazasSystem = {
        cargarPlazasPorEstado: async (estado) => {
            try {
                loaderSystem.show(`Cargando plazas del estado: ${estado}`);
                const plazas = await utils.fetchData(`/api/plazas_por_estado/${encodeURIComponent(estado)}`);
                state.plazasDelEstado = plazas;
                plazasSystem.renderPlazasList(plazas, estado);
                
                if (refs.plazas.title) {
                    refs.plazas.title.textContent = `Plazas de ${estado} (${plazas.length})`;
                }
            } catch (error) {
                console.error('Error cargando plazas por estado:', error);
                alertSystem.show('Error al cargar las plazas del estado', 'error');
            } finally {
                loaderSystem.hide();
            }
        },
        
        renderPlazasList: (plazas, estado) => {
            if (!refs.plazas.container) return;
            refs.plazas.container.innerHTML = '';
            
            if (plazas.length === 0) {
                const noResults = document.createElement('p');
                noResults.className = 'no-results';
                noResults.textContent = 'No se encontraron plazas para este estado.';
                refs.plazas.container.appendChild(noResults);
                return;
            }
            
            const fragment = utils.createFragment();
            
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
                fragment.appendChild(item);
            });
            
            refs.plazas.container.appendChild(fragment);
            plazasSystem.setupBusquedaPlazasView();
        },
        
        setupBusquedaPlazasView: () => {
            if (!refs.plazas.searchInput) return;
            
            const debouncedSearch = utils.debounce((query) => {
                const queryLower = query.toLowerCase().trim();
                if (!queryLower) {
                    plazasSystem.renderPlazasList(state.plazasDelEstado, state.estadoSeleccionado);
                    return;
                }
                const plazasFiltradas = state.plazasDelEstado.filter(plaza =>
                    plaza.clave.toLowerCase().includes(queryLower) ||
                    plaza.municipio.toLowerCase().includes(queryLower) ||
                    plaza.localidad.toLowerCase().includes(queryLower) ||
                    (plaza.direccion && plaza.direccion.toLowerCase().includes(queryLower))
                );
                plazasSystem.renderPlazasList(plazasFiltradas, state.estadoSeleccionado);
            }, 300);
            
            refs.plazas.searchInput.addEventListener('input', (e) => debouncedSearch(e.target.value));
            
            // Delegación de eventos para items de plaza
            refs.plazas.container.addEventListener('click', (e) => {
                const item = e.target.closest('.plaza-list-item');
                if (!item) return;
                
                const clave = item.querySelector('.plaza-clave').textContent;
                searchSystem.buscarYMostrarClave(clave, refs.ui.filterLoader);
            });
        }
    };

    // ==== SISTEMA DE MODAL PARA IMÁGENES ====
    const modalSystem = {
        init: () => {
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

            const openModal = (images, startIndex = 0) => {
                currentImages = images;
                currentIndex = startIndex;
                
                modalTotal.textContent = images.length;
                updateModalImage();
                modal.classList.add('active');
                document.body.style.overflow = 'hidden';
            };

            const closeModal = () => {
                modal.classList.remove('active');
                document.body.style.overflow = '';
                currentImages = [];
                currentIndex = 0;
            };

            const updateModalImage = () => {
                if (currentImages.length === 0) return;
                
                const imageUrl = currentImages[currentIndex];
                modalImage.src = imageUrl;
                modalCurrent.textContent = currentIndex + 1;
                
                const filename = imageUrl.split('/').pop() || 'imagen.jpg';
                modalFilename.textContent = decodeURIComponent(filename);
                
                modalPrev.style.display = currentIndex > 0 ? 'flex' : 'none';
                modalNext.style.display = currentIndex < currentImages.length - 1 ? 'flex' : 'none';
            };

            const nextImage = () => {
                if (currentIndex < currentImages.length - 1) {
                    currentIndex++;
                    updateModalImage();
                }
            };

            const prevImage = () => {
                if (currentIndex > 0) {
                    currentIndex--;
                    updateModalImage();
                }
            };

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
    };

    // ==== SISTEMA DE FECHA DE ACTUALIZACIÓN ====
    const updateSystem = {
        loadExcelLastUpdate: () => {
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
        },
        
        setupUpdateBadgeInteractions: () => {
            const badge = document.getElementById('excel-update-info');
            if (badge) {
                badge.addEventListener('click', function() {
                    this.classList.toggle('minimal');
                });
            }
        }
    };

    // ==== AGREGAR ESTILOS PARA EL SISTEMA ====
    const styleSystem = {
        agregarEstilos: () => {
            const estilos = document.createElement('style');
            estilos.textContent = `
                .system-notification {
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    background: var(--card-bg);
                    color: var(--text-color);
                    border: 1px solid var(--border-color);
                    border-radius: 8px;
                    padding: 12px 16px;
                    max-width: 300px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                    z-index: 10000;
                    transform: translateX(120%);
                    transition: transform 0.3s ease;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }
                
                .system-notification.show {
                    transform: translateX(0);
                }
                
                .system-notification.success {
                    border-left: 4px solid var(--success-color);
                    background: color-mix(in srgb, var(--success-color) 10%, transparent);
                }
                
                .system-notification.warning {
                    border-left: 4px solid var(--warning-color);
                    background: color-mix(in srgb, var(--warning-color) 10%, transparent);
                }
                
                .system-notification.error {
                    border-left: 4px solid var(--error-color);
                    background: color-mix(in srgb, var(--error-color) 10%, transparent);
                }
                
                .system-notification.info {
                    border-left: 4px solid var(--info-color);
                    background: color-mix(in srgb, var(--info-color) 10%, transparent);
                }
                
                .notification-content {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }
                
                .notification-icon {
                    font-size: 1.2em;
                }
                
                .notification-text {
                    font-size: 0.9rem;
                    line-height: 1.4;
                }
                
                .indice-update-button {
                    position: fixed;
                    bottom: 20px;
                    right: 20px;
                    width: 40px;
                    height: 40px;
                    border-radius: 50%;
                    background: var(--primary-color);
                    color: white;
                    border: none;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 1.2rem;
                    z-index: 999;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.2);
                    transition: all 0.3s ease;
                    opacity: 0.7;
                }
                
                .indice-update-button:hover {
                    opacity: 1;
                    transform: scale(1.1);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                }
                
                .indice-update-button.updating {
                    animation: spin 1s linear infinite;
                }
                
                @keyframes spin {
                    from { transform: rotate(0deg); }
                    to { transform: rotate(360deg); }
                }
                
                .indice-status {
                    position: fixed;
                    bottom: 70px;
                    right: 20px;
                    background: var(--card-bg);
                    border: 1px solid var(--border-color);
                    border-radius: 6px;
                    padding: 8px 12px;
                    font-size: 0.8rem;
                    opacity: 0;
                    transition: opacity 0.3s ease;
                    pointer-events: none;
                    z-index: 998;
                    max-width: 200px;
                    text-align: center;
                }
                
                .indice-status.show {
                    opacity: 1;
                }
                
                /* Animación para el highlight de la tabla */
                @keyframes highlight-pulse {
                    0% { box-shadow: 0 0 0 0 rgba(59, 130, 246, 0.5); }
                    50% { box-shadow: 0 0 0 10px rgba(59, 130, 246, 0); }
                    100% { box-shadow: 0 0 0 0 rgba(59, 130, 246, 0); }
                }
            `;
            document.head.appendChild(estilos);
        }
    };

    // ==== INICIALIZACIÓN COMPLETA ====
    const initApp = () => {
        console.log('🚀 Inicializando aplicación...');
        
        // 1. Agregar estilos del sistema
        styleSystem.agregarEstilos();
        
        // 2. Configurar tema
        themeSystem.init();
        
        // 3. Inicializar navegación
        navigationSystem.init();
        
        // 4. Configurar filtros y búsqueda
        filterSystem.setupKeyboardNavigation();
        filterSystem.setupProgressBarNavigation();
        filterSystem.addAutoSearchToggle();
        filterSystem.initFilterListeners();
        filterSystem.actualizarProgreso();
        
        // 5. Configurar botón de reset
        const resetButton = document.getElementById('reset-search-button');
        if (resetButton) {
            resetButton.addEventListener('click', filterSystem.resetSearch);
        }
        
        // 6. Poblar select inicial
        filterSystem.populateSelect(refs.selects.estado, '/api/estados', 'Selecciona un Estado');
        
        // 7. Inicializar modal de imágenes
        const { openModal } = modalSystem.init();
        state.modalOpenFunction = openModal;
        
        // 8. Configurar fecha de actualización
        updateSystem.loadExcelLastUpdate();
        updateSystem.setupUpdateBadgeInteractions();
        
        // 9. Crear botón de actualización del índice
        const botonActualizacion = imageIndexSystem.crearBotonActualizacion();
        
        // 10. Inicializar índice de imágenes en segundo plano (no bloqueante)
        setTimeout(async () => {
            console.log('🔍 Inicializando índice de imágenes...');
            
            const estadoIndice = document.createElement('div');
            estadoIndice.className = 'indice-status';
            estadoIndice.textContent = 'Cargando índice...';
            document.body.appendChild(estadoIndice);
            
            try {
                const resultado = await imageIndexSystem.construirIndiceImagenes();
                
                if (resultado.success) {
                    estadoIndice.textContent = `Índice: ${imagenIndex.size} carpetas`;
                    estadoIndice.classList.add('show');
                    
                    botonActualizacion.classList.remove('hidden');
                    imageIndexSystem.iniciarActualizacionAutomatica();
                    
                    setTimeout(() => {
                        estadoIndice.classList.remove('show');
                    }, 3000);
                } else {
                    estadoIndice.textContent = 'Error cargando índice';
                    estadoIndice.classList.add('show');
                    estadoIndice.style.background = 'var(--error-color)';
                    estadoIndice.style.color = 'white';
                }
            } catch (error) {
                console.error('Error inicializando índice:', error);
                estadoIndice.textContent = 'Error en índice';
                estadoIndice.classList.add('show');
            }
        }, 1000);
        
        // 11. Manejar navegación inicial
        navigationSystem.handleNavigation();
        
        // 12. Registrar eventos para debugging
        window.addEventListener('error', (e) => {
            console.error('Error global:', e.error);
        });
        
        console.log('✅ Aplicación inicializada y optimizada');
    };

    // Iniciar la aplicación
    initApp();
});