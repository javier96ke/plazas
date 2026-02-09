document.addEventListener("DOMContentLoaded", function() {
    
    // =========================================================
    // VARIABLES GLOBALES Y CONFIGURACIÓN
    // =========================================================
    let userLocation = null;
    let userLocationMarker = null;
    let userAccuracyCircle = null;
    let currentPolyline = null;
    
    const KEY_DATA = 'mapa_datos_plazas';
    const KEY_VER = 'mapa_version_timestamp';
    let mapInstance = null;
    let markersGroup = null;
    let mapInitialized = false;
    let datosGlobales = [];

    window.navigationContext = {
        cameFromMap: false,
        lastMapView: null,
        lastClickedPlaza: null
    };

    let watchId = null;
    let isFollowing = false;
    let lastManualInteraction = null;

    let searchCache = new Map();
    const SEARCH_CACHE_TTL = 300000;

    // =========================================================
    // ENDPOINTS DEL BACKEND
    // =========================================================
    const ENDPOINT_MAPA_SEGURO = '/api/mapa/seguro';
    const ENDPOINT_COORDENADAS_SEGURAS = '/api/mapa/coordenadas-completas';
    const ENDPOINT_UBICAR_CERCANA = '/api/mapa/ubicar-plaza-cercana';
    const ENDPOINT_LINEA_RUTA = '/api/mapa/generar-linea-ruta';

    // =========================================================
    // SISTEMA DE CARGA (LOADER) PARA RUTAS
    // =========================================================
    function mostrarLoaderRuta(texto) {
        const loader = document.createElement('div');
        loader.id = 'route-loader';
        loader.className = 'route-loader-overlay';
        loader.innerHTML = `
            <div class="route-spinner"></div>
            <h3>${texto}</h3>
            <small>Esto puede tomar unos segundos...</small>
        `;
        document.body.appendChild(loader);
    }

    function ocultarLoaderRuta() {
        const loader = document.getElementById('route-loader');
        if (loader) loader.remove();
    }

    // =========================================================
    // FUNCIONES DE UTILIDAD PARA HTML/JS
    // =========================================================
    function escapeHTML(text) {
        if (!text) return '';
        return text.toString()
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function escapeJS(text) {
        if (!text) return '';
        return text.toString()
            .replace(/\\/g, '\\\\')
            .replace(/'/g, "\\'")
            .replace(/"/g, '\\"')
            .replace(/\n/g, '\\n')
            .replace(/\r/g, '\\r')
            .replace(/\t/g, '\\t');
    }

    // =========================================================
    // FUNCIONES PARA GENERAR HTML EN FRONTEND
    // =========================================================
    function generarPopupHTML(plaza) {
        const clave = plaza.Clave_Plaza || plaza.clave || 'Sin clave';
        const nombre = plaza.Nombre || plaza.NOMBRE_PC || plaza.nombre || 'Sin nombre';
        const estado = plaza.Estado || plaza.estado || 'Sin estado';
        const municipio = plaza.Municipio || plaza.municipio || '';
        const lat = plaza.Latitud || plaza.lat || null;
        const lng = plaza.Longitud || plaza.lng || null;
        const situacion = plaza.situacion || plaza.SITUACION || '';
        
        const claveEscapada = escapeJS(clave);
        const claveHTML = escapeHTML(clave);
        const nombreHTML = escapeHTML(nombre);
        const estadoHTML = escapeHTML(estado);
        const municipioHTML = escapeHTML(municipio);
        const situacionHTML = escapeHTML(situacion);
        
        const esMovil = window.innerWidth <= 768;
        
        if (!esMovil) {
            return `
            <div class="popup-container">
                <div class="popup-header">
                    <h4>${claveHTML}</h4>
                    <div class="popup-subtitle">
                        <span>${estadoHTML}</span>
                        ${municipioHTML ? `<span>, ${municipioHTML}</span>` : ''}
                    </div>
                </div>
                
                <div class="popup-content">
                    <p class="popup-nombre"><strong>${nombreHTML}</strong></p>
                    ${situacionHTML ? `<p class="popup-situacion">${situacionHTML}</p>` : ''}
                    
                    <div class="popup-actions">
                        <button onclick="window.navigationContext.cameFromMap = true; window.irADetallePlaza('${claveEscapada}')" 
                                class="btn-map-popup btn-map-popup-details">
                            <span class="popup-icon">📋</span>
                            <span class="popup-text">Ver Detalles</span>
                        </button>
                        
                        <button onclick="window.zoomAPlaza(${lat}, ${lng}, '${claveEscapada}')" 
                                class="btn-map-popup btn-map-popup-zoom">
                            <span class="popup-icon">📍</span>
                            <span class="popup-text">Centrar</span>
                        </button>
                        
                        <div class="popup-buttons-grid">
                            <button onclick="window.mostrarOpcionesNavegacion(${lat}, ${lng}, '${claveEscapada}', false)" 
                                    class="btn-map-popup btn-map-popup-gmaps">
                                <span class="popup-icon">🗺️</span>
                                <span class="popup-text">Maps</span>
                            </button>
                            
                            <button onclick="window.solicitarUbicacionParaRuta(${lat}, ${lng}, '${claveEscapada}')" 
                                    class="btn-map-popup btn-map-popup-route">
                                <span class="popup-icon">🚗</span>
                                <span class="popup-text">Ruta</span>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            `;
        }
        
        return `
        <div class="popup-container mobile">
            <div class="popup-header mobile">
                <h4>${claveHTML}</h4>
                <p class="popup-nombre-mobile"><strong>${nombreHTML}</strong></p>
                <p class="popup-location">${estadoHTML}${municipioHTML ? ', ' + municipioHTML : ''}</p>
            </div>
            
            <div class="popup-content mobile">
                <button onclick="window.navigationContext.cameFromMap = true; window.irADetallePlaza('${claveEscapada}')" 
                        class="btn-map-popup btn-map-popup-details mobile">
                    📋 Detalles
                </button>
                
                <div class="popup-buttons-grid mobile">
                    <button onclick="window.mostrarOpcionesNavegacion(${lat}, ${lng}, '${claveEscapada}', false)" 
                            class="btn-map-popup btn-map-popup-gmaps mobile">
                        🗺️ Maps
                    </button>
                    
                    <button onclick="window.solicitarUbicacionParaRuta(${lat}, ${lng}, '${claveEscapada}')" 
                            class="btn-map-popup btn-map-popup-route mobile">
                        🚗 Ruta
                    </button>
                </div>
            </div>
        </div>
        `;
    }

    function generarResultadoBusquedaHTML(resultado) {
        const clave = resultado.clave || 'Sin clave';
        const nombre = resultado.nombre || 'Sin nombre';
        const estado = resultado.estado || '';
        const municipio = resultado.municipio || '';
        const lat = resultado.lat;
        const lng = resultado.lng;
        
        const claveEscapada = escapeJS(clave);
        const claveHTML = escapeHTML(clave);
        const nombreHTML = escapeHTML(nombre);
        const estadoHTML = escapeHTML(estado);
        const municipioHTML = escapeHTML(municipio);
        
        const icono = resultado.tipo_coincidencia === 'exacta' ? '🎯' : '📍';
        const ubicacion = municipioHTML ? `${estadoHTML}, ${municipioHTML}` : estadoHTML;
        
        return `
        <div onclick="window.zoomAPlaza(${lat}, ${lng}, '${claveEscapada}'); 
                      document.getElementById('search-results').style.display='none';
                      document.getElementById('map-search-input').value='${claveEscapada}';" 
             class="search-result-item">
            <div class="search-result-icon">${icono}</div>
            <div class="search-result-content">
                <div class="search-result-clave"><strong>${claveHTML}</strong></div>
                <div class="search-result-nombre">${nombreHTML}</div>
                <div class="search-result-ubicacion">${ubicacion}</div>
            </div>
        </div>
        `;
    }

    function generarPlazaCercanaHTML(plaza, userLocation) {
        const clave = plaza.clave || 'Sin clave';
        const nombre = plaza.nombre || 'Sin nombre';
        const distancia = plaza.distancia_formateada || '0 km';
        const lat = plaza.lat;
        const lng = plaza.lng;
        
        const claveEscapada = escapeJS(clave);
        const claveHTML = escapeHTML(clave);
        const nombreHTML = escapeHTML(nombre);
        
        const userLat = userLocation ? userLocation.lat : null;
        const userLng = userLocation ? userLocation.lon : null;
        
        return `
        <div class="gps-item">
            <div class="gps-item-header">
                <div class="gps-item-clave"><strong>${claveHTML}</strong></div>
                <div class="gps-dist">📍 A ${distancia}</div>
            </div>
            
            <div class="gps-item-nombre">${nombreHTML}</div>
            
            <div class="gps-buttons-grid">
                <button onclick="window.zoomAPlaza(${lat}, ${lng}, '${claveEscapada}')" 
                        class="btn-map-popup btn-map-popup-zoom">
                    Ver en Mapa
                </button>
                <button onclick="window.navigationContext.cameFromMap = true; window.irADetallePlaza('${claveEscapada}')" 
                        class="btn-map-popup btn-map-popup-details">
                    Detalles
                </button>
            </div>
            
            <div class="route-buttons-grid">
                <button onclick="window.mostrarOpcionesNavegacion(${lat}, ${lng}, '${claveEscapada}', false)" 
                        class="btn-map-popup btn-map-popup-gmaps">
                    🗺️ Ver en Maps
                </button>
                <button onclick="window.solicitarUbicacionParaRuta(${lat}, ${lng}, '${claveEscapada}', ${userLat}, ${userLng})" 
                        class="btn-map-popup btn-map-popup-route">
                    🚗 Cómo Llegar
                </button>
            </div>
        </div>
        `;
    }

    // =========================================================
    // DETECCIÓN DE VISTA
    // =========================================================
    function isMapVisible() {
        const mapView = document.getElementById('map-view');
        return mapView && !mapView.classList.contains('hidden');
    }

    function observeViewChanges() {
        const observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                if (mutation.attributeName === 'class') {
                    if (isMapVisible() && !mapInitialized) {
                        initMap();
                    }
                }
            });
        });

        const mapView = document.getElementById('map-view');
        if (mapView) {
            observer.observe(mapView, { attributes: true });
        }

        window.addEventListener('hashchange', function() {
            if (window.location.hash === '#map-view' && !mapInitialized) {
                initMap();
            }
        });

        if (window.location.hash === '#map-view' || isMapVisible()) {
            setTimeout(initMap, 300);
        }
    }

    // =========================================================
    // 1. INICIALIZACIÓN DEL MAPA (OPTIMIZADA)
    // =========================================================
    function initMap() {
        if (typeof L === 'undefined') {
            setTimeout(initMap, 1000);
            return;
        }

        const mapContainer = document.getElementById('map');
        if (!mapContainer || mapInitialized) return;

        console.log("🗺️ Inicializando mapa...");

        const calle = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap', maxZoom: 18
        });

        const satelite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            attribution: '© Esri', maxZoom: 18
        });

        mapInstance = L.map('map', {
            center: [23.6345, -102.5528],
            zoom: 5,
            layers: [calle],
            zoomControl: false // Lo agregamos manual si queremos, o dejamos el default
        });
        
        // Agregar controles de zoom manualmente abajo a la derecha para móviles
        L.control.zoom({ position: 'bottomright' }).addTo(mapInstance);

        configurarGestosParaMoviles(mapInstance);
        L.control.layers({ "Mapa": calle, "Satélite": satelite }).addTo(mapInstance);

        markersGroup = L.markerClusterGroup({ 
            disableClusteringAtZoom: 16,
            spiderfyOnMaxZoom: true,
            maxClusterRadius: 60,
            chunkedLoading: true 
        });

        mapInstance.addLayer(markersGroup);
        mapInitialized = true;
        
        setupBackButtonInterceptor();
        agregarResetMapButton();
        agregarControlLiveTracking(mapInstance);

        // --- CORRECCIÓN: CARGAR CONTROLES INMEDIATAMENTE ---
        agregarControlesInmediatos(mapInstance);
        
        // Cargar datos en segundo plano
        gestionarDatosMapa();
    }

    // Nueva función para forzar la aparición de botones
    function agregarControlesInmediatos(map) {
        // 1. Botón de Plazas Cercanas (GPS)
        const GpsControl = L.Control.extend({
            options: { position: 'topleft' },
            onAdd: function() {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control leaflet-control-gps');
                container.innerHTML = '<span class="gps-icon">🧭</span> <span class="gps-text">Cercanas</span>';
                container.style.cursor = 'pointer';
                container.onclick = function(e) {
                    L.DomEvent.stopPropagation(e);
                    mostrarModalConfirmacionGPS();
                };
                return container;
            }
        });
        map.addControl(new GpsControl());

        // 2. Buscador
        agregarBuscador(map);
    }

    // =========================================================
    // GESTOS PARA MÓVILES
    // =========================================================
    function configurarGestosParaMoviles(map) {
        if (!L.Browser.mobile) return;
        
        let mapLocked = false;
        let lockTimeout = null;
        
        const lockControl = L.Control.extend({
            options: { position: 'topleft' },
            
            onAdd: function() {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control leaflet-control-lock');
                container.innerHTML = '<span class="lock-icon">🔒</span><span class="lock-text">Scroll</span>';
                container.title = "Bloquear/Desbloquear desplazamiento del mapa";
                container.style.marginTop = '90px';
                
                container.onclick = function(e) {
                    L.DomEvent.stopPropagation(e);
                    mapLocked = !mapLocked;
                    
                    if (mapLocked) {
                        map.dragging.disable();
                        map.touchZoom.disable();
                        map.doubleClickZoom.disable();
                        container.innerHTML = '<span class="lock-icon">🔓</span><span class="lock-text">Mover</span>';
                        container.classList.add('control-active');
                        mostrarNotificacion("Mapa bloqueado - Puedes hacer scroll libremente");
                    } else {
                        map.dragging.enable();
                        map.touchZoom.enable();
                        map.doubleClickZoom.enable();
                        container.innerHTML = '<span class="lock-icon">🔒</span><span class="lock-text">Scroll</span>';
                        container.classList.remove('control-active');
                        mostrarNotificacion("Mapa desbloqueado - Puedes moverlo");
                    }
                    
                    if (lockTimeout) clearTimeout(lockTimeout);
                    if (mapLocked) {
                        lockTimeout = setTimeout(() => {
                            if (mapLocked) container.click();
                        }, 30000);
                    }
                };
                
                return container;
            }
        });
        
        map.addControl(new lockControl());
        
        let scrollStart = 0;
        
        map.getContainer().addEventListener('touchstart', function(e) {
            if (e.touches.length === 1) {
                scrollStart = e.touches[0].clientY;
            }
        }, { passive: true });
        
        map.getContainer().addEventListener('touchmove', function(e) {
            if (e.touches.length === 1 && !mapLocked) {
                const currentY = e.touches[0].clientY;
                const diff = Math.abs(currentY - scrollStart);
                
                if (diff > 50 && e.cancelable) {
                    const hint = document.createElement('div');
                    hint.className = 'lock-hint';
                    hint.innerHTML = '🔒 Toca el candado para bloquear scroll';
                    map.getContainer().appendChild(hint);
                    
                    setTimeout(() => {
                        if (hint.parentNode) hint.remove();
                    }, 3000);
                }
            }
        }, { passive: true });
    }

    // =========================================================
    // BUSCADOR INTEGRADO
    // =========================================================
    function agregarBuscador(map) {
        const SearchControl = L.Control.extend({
            options: { position: 'topright' },
            
            onAdd: function() {
                const container = L.DomUtil.create('div', 'leaflet-bar search-container');
                L.DomEvent.disableClickPropagation(container);
                L.DomEvent.disableScrollPropagation(container);

                container.innerHTML = `
                    <input type="text" id="map-search-input" class="search-input" 
                           placeholder="Buscar por clave (I-31-009-02) o nombre...">
                    <div id="search-results" class="search-results" style="display: none;"></div>
                `;
                
                const input = container.querySelector('#map-search-input');
                const resultsDiv = container.querySelector('#search-results');

                async function buscarEnBackend(query) {
                    try {
                        const cacheKey = `search_${query.toLowerCase()}`;
                        const cached = searchCache.get(cacheKey);
                        
                        if (cached && (Date.now() - cached.timestamp) < SEARCH_CACHE_TTL) {
                            return cached.data;
                        }
                        
                        const params = new URLSearchParams({
                            action: 'buscar',
                            q: query,
                            tipo: 'todas',
                            limite: 15
                        });
                        
                        const response = await fetch(`${ENDPOINT_MAPA_SEGURO}?${params}`);
                        const data = await response.json();
                        
                        if (data.status === 'success') {
                            searchCache.set(cacheKey, {
                                data: data.resultados,
                                timestamp: Date.now()
                            });
                            
                            limpiarCacheViejo();
                            return data.resultados;
                        }
                        return [];
                    } catch (error) {
                        console.error("Error en búsqueda:", error);
                        return [];
                    }
                }

                function mostrarResultados(resultados) {
                    if (resultados.length === 0) {
                        resultsDiv.innerHTML = `
                            <div class="search-no-results">
                                <div class="search-no-results-icon">🔍</div>
                                <div class="search-no-results-text">No se encontraron resultados</div>
                            </div>
                        `;
                        resultsDiv.style.display = 'block';
                        return;
                    }

                    resultsDiv.innerHTML = resultados.map(p => generarResultadoBusquedaHTML(p)).join('');
                    resultsDiv.style.display = 'block';
                }

                function limpiarCacheViejo() {
                    const ahora = Date.now();
                    for (const [key, value] of searchCache.entries()) {
                        if (ahora - value.timestamp > SEARCH_CACHE_TTL) {
                            searchCache.delete(key);
                        }
                    }
                }

                let timeoutId;
                input.addEventListener('input', function(e) {
                    clearTimeout(timeoutId);
                    
                    timeoutId = setTimeout(async () => {
                        const query = e.target.value.trim();
                        
                        if (query.length < 2) {
                            resultsDiv.style.display = 'none';
                            return;
                        }

                        if (datosGlobales.length > 0) {
                            const queryLower = query.toLowerCase();
                            const resultadosLocales = datosGlobales.filter(p => {
                                const clave = (p.Clave_Plaza || p.clave || '').toLowerCase();
                                const nombre = (p.Nombre || '').toLowerCase();
                                const estado = (p.Estado || '').toLowerCase();
                                const municipio = (p.Municipio || '').toLowerCase();
                                
                                return clave.includes(queryLower) ||
                                       nombre.includes(queryLower) ||
                                       estado.includes(queryLower) ||
                                       municipio.includes(queryLower);
                            }).slice(0, 10);
                            
                            if (resultadosLocales.length > 0) {
                                const resultadosConvertidos = resultadosLocales.map(p => ({
                                    clave: p.Clave_Plaza || p.clave,
                                    nombre: p.Nombre || '',
                                    estado: p.Estado || '',
                                    municipio: p.Municipio || '',
                                    lat: p.Latitud || p.lat,
                                    lng: p.Longitud || p.lng,
                                    tipo_coincidencia: 'local'
                                }));
                                
                                mostrarResultados(resultadosConvertidos);
                                
                                buscarEnBackend(query).then(resultadosBackend => {
                                    const clavesLocales = new Set(resultadosLocales.map(r => r.Clave_Plaza || r.clave));
                                    const nuevosResultados = resultadosBackend.filter(r => 
                                        !clavesLocales.has(r.clave)
                                    );
                                    
                                    if (nuevosResultados.length > 0) {
                                        const todosResultados = [...resultadosConvertidos, ...nuevosResultados];
                                        mostrarResultados(todosResultados.slice(0, 15));
                                    }
                                });
                                
                                return;
                            }
                        }
                        
                        const resultados = await buscarEnBackend(query);
                        mostrarResultados(resultados);
                        
                    }, 300);
                });

                document.addEventListener('click', function(e) {
                    if (!container.contains(e.target)) {
                        resultsDiv.style.display = 'none';
                    }
                });

                return container;
            }
        });
        
        map.addControl(new SearchControl());
    }

    // =========================================================
    // SEGUIMIENTO EN VIVO
    // =========================================================
    function agregarControlLiveTracking(map) {
        const LiveTrackingControl = L.Control.extend({
            options: { position: 'topleft' },
            
            onAdd: function() {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control leaflet-control-gps-live');
                container.innerHTML = '<span class="gps-icon">📍</span><span class="gps-text">Live OFF</span>';
                container.title = "Activar/Desactivar seguimiento en vivo de ubicación";
                container.style.marginTop = '50px';
                
                container.onclick = function(e) {
                    L.DomEvent.stopPropagation(e);
                    toggleLiveTracking();
                };
                
                return container;
            }
        });
        
        map.addControl(new LiveTrackingControl());
    }

    function toggleLiveTracking() {
        if (isFollowing) {
            if (watchId) navigator.geolocation.clearWatch(watchId);
            isFollowing = false;
            mostrarNotificacion("Seguimiento detenido");
            const gpsBtn = document.querySelector('.leaflet-control-gps-live');
            if (gpsBtn) {
                gpsBtn.innerHTML = '<span class="gps-icon">📍</span><span class="gps-text">Live OFF</span>';
                gpsBtn.classList.remove('control-active');
            }
        } else {
            if (!navigator.geolocation) {
                mostrarNotificacion("Tu navegador no soporta seguimiento en vivo", "error");
                return;
            }
            
            const options = {
                enableHighAccuracy: true,
                maximumAge: 0,
                timeout: 5000
            };

            watchId = navigator.geolocation.watchPosition((position) => {
                const { latitude, longitude, accuracy } = position.coords;
                
                actualizarMarcadorUsuario(latitude, longitude, accuracy);
                
                if (!lastManualInteraction || (Date.now() - lastManualInteraction > 3000)) {
                    mapInstance.panTo([latitude, longitude], {
                        animate: true,
                        duration: 1,
                        easeLinearity: 0.25
                    });
                }
                
            }, (error) => {
                console.error("Error en seguimiento en vivo:", error);
                mostrarNotificacion("Error en seguimiento GPS", "error");
                toggleLiveTracking();
            }, options);

            isFollowing = true;
            mostrarNotificacion("Seguimiento activo 🚶");
            
            const gpsBtn = document.querySelector('.leaflet-control-gps-live');
            if (gpsBtn) {
                gpsBtn.innerHTML = '<span class="gps-icon">🎯</span><span class="gps-text">Live ON</span>';
                gpsBtn.classList.add('control-active');
            }
        }
    }

    function actualizarMarcadorUsuario(lat, lon, acc) {
        if (!userLocationMarker) {
            userLocationMarker = L.marker([lat, lon], {
                icon: L.divIcon({
                    className: 'user-location-icon-live',
                    html: '<div class="user-location-pulse"></div>',
                    iconSize: [26, 26],
                    iconAnchor: [13, 13]
                })
            }).addTo(mapInstance);
        } else {
            userLocationMarker.setLatLng([lat, lon]);
        }
        
        if (!userAccuracyCircle) {
            userAccuracyCircle = L.circle([lat, lon], {
                radius: acc,
                fillColor: '#007bff',
                fillOpacity: 0.15,
                color: '#007bff',
                weight: 1,
                dashArray: '5, 5'
            }).addTo(mapInstance);
        } else {
            userAccuracyCircle.setLatLng([lat, lon]);
            userAccuracyCircle.setRadius(acc);
        }
    }

    // =========================================================
    // GPS Y CERCANÍA
    // =========================================================
    function agregarControlGPS(map) {
        const GpsControl = L.Control.extend({
            options: { position: 'topleft' },
            
            onAdd: function() {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control leaflet-control-gps');
                container.innerHTML = '<span class="gps-icon">🧭</span> <span class="gps-text">Plazas Cercanas</span>';
                container.title = "Buscar las 5 plazas más cercanas a mi ubicación";
                
                container.onclick = function(e) {
                    L.DomEvent.stopPropagation(e);
                    mostrarModalConfirmacionGPS();
                };
                
                return container;
            }
        });
        
        map.addControl(new GpsControl());

        const resultsDiv = document.createElement('div');
        resultsDiv.id = 'gps-results-modal';
        document.getElementById('map').appendChild(resultsDiv);
    }

    // =========================================================
    // FUNCIONES DE UBICACIÓN
    // =========================================================
    function obtenerUbicacionUsuario(mostrarEnMapa = true) {
        return new Promise((resolve, reject) => {
            if (!navigator.geolocation) {
                reject("Tu navegador no soporta geolocalización.");
                return;
            }

            const options = {
                enableHighAccuracy: true,
                timeout: 10000,
                maximumAge: 0
            };

            navigator.geolocation.getCurrentPosition(
                (position) => {
                    const userLat = position.coords.latitude;
                    const userLon = position.coords.longitude;
                    const accuracy = position.coords.accuracy;
                    
                    userLocation = {
                        lat: userLat,
                        lon: userLon,
                        accuracy: accuracy,
                        timestamp: new Date().toISOString()
                    };
                    
                    if (mostrarEnMapa && mapInstance) {
                        if (userLocationMarker) {
                            mapInstance.removeLayer(userLocationMarker);
                        }
                        if (userAccuracyCircle) {
                            mapInstance.removeLayer(userAccuracyCircle);
                        }
                        
                        userLocationMarker = L.marker([userLat, userLon], {
                            icon: L.divIcon({
                                className: 'user-location-icon',
                                html: '<div class="user-location-dot"></div>',
                                iconSize: [22, 22],
                                iconAnchor: [11, 11]
                            })
                        }).addTo(mapInstance);
                        
                        userLocationMarker.bindPopup(`
                            <div class="user-location-popup">
                                <strong>📍 Tu ubicación</strong><br>
                                <small>Precisión: ${Math.round(accuracy)} metros</small><br>
                                <small>${new Date().toLocaleTimeString()}</small>
                            </div>
                        `).openPopup();
                        
                        if (accuracy < 1000) {
                            userAccuracyCircle = L.circle([userLat, userLon], {
                                radius: accuracy,
                                fillColor: '#007bff',
                                fillOpacity: 0.1,
                                color: '#007bff',
                                weight: 1,
                                dashArray: '5, 5'
                            }).addTo(mapInstance);
                        }
                        
                        mapInstance.setView([userLat, userLon], 15);
                    }
                    
                    resolve(userLocation);
                },
                (error) => {
                    let mensajeError = "Error al obtener la ubicación.";
                    
                    switch(error.code) {
                        case error.PERMISSION_DENIED:
                            mensajeError = "Permiso denegado. Activa la ubicación en tu navegador.";
                            break;
                        case error.POSITION_UNAVAILABLE:
                            mensajeError = "Ubicación no disponible. Verifica tu GPS.";
                            break;
                        case error.TIMEOUT:
                            mensajeError = "Tiempo agotado. Intenta nuevamente.";
                            break;
                    }
                    
                    reject(mensajeError);
                },
                options
            );
        });
    }

    function mostrarModalConfirmacionGPS() {
        const overlay = document.createElement('div');
        overlay.className = 'gps-confirm-overlay';
        
        const modal = document.createElement('div');
        modal.className = 'gps-confirm-modal';
        modal.innerHTML = `
            <h3>📍 Buscar Plazas Cercanas</h3>
            <p>Para encontrar las plazas más cercanas, necesitamos acceder a tu ubicación.</p>
            <p class="gps-confirm-note">
                Tu ubicación solo se usará para calcular distancias y no se almacenará.
            </p>
            <div class="gps-confirm-buttons">
                <button class="gps-confirm-btn gps-confirm-yes" id="gps-confirm-yes">
                    Sí, usar mi ubicación
                </button>
                <button class="gps-confirm-btn gps-confirm-no" id="gps-confirm-no">
                    Cancelar
                </button>
            </div>
        `;
        
        overlay.appendChild(modal);
        document.getElementById('map').appendChild(overlay);
        
        document.getElementById('gps-confirm-yes').addEventListener('click', function() {
            document.getElementById('map').removeChild(overlay);
            buscarCercanos();
        });
        
        document.getElementById('gps-confirm-no').addEventListener('click', function() {
            document.getElementById('map').removeChild(overlay);
        });
        
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) {
                document.getElementById('map').removeChild(overlay);
            }
        });
    }

    // =========================================================
    // MAPAS EXTERNOS (CORREGIDO Y OPTIMIZADO)
    // =========================================================
    window.abrirGoogleMaps = function(lat, lon, nombre) {
        if (!lat || !lon) {
            alert("No hay coordenadas disponibles para esta plaza.");
            return;
        }
        // URL universal de Google Maps
        const url = `https://www.google.com/maps/search/?api=1&query=${lat},${lon}`;
        window.open(url, '_blank');
    };

    window.abrirWaze = function(lat, lon, nombre) {
        if (!lat || !lon) {
            alert("No hay coordenadas disponibles para esta plaza.");
            return;
        }
        // URL universal de Waze
        const url = `https://www.waze.com/ul?ll=${lat},${lon}&navigate=yes`;
        window.open(url, '_blank');
    };

    window.crearRutaGoogleMaps = function(originLat, originLon, destLat, destLon, destinoNombre) {
        if (!originLat || !originLon || !destLat || !destLon) {
            alert("No hay coordenadas suficientes para crear la ruta.");
            return;
        }
        // URL universal de Google Maps para direcciones
        const url = `https://www.google.com/maps/dir/?api=1&origin=${originLat},${originLon}&destination=${destLat},${destLon}&travelmode=driving`;
        
        window.open(url, '_blank');
        mostrarNotificacionRuta(originLat, originLon, destLat, destLon, 'Google Maps');
    };

    window.crearRutaWaze = function(originLat, originLon, destLat, destLon, destinoNombre) {
        if (!destLat || !destLon) {
            alert("No hay coordenadas de destino.");
            return;
        }
        // Waze toma la ubicación actual automáticamente
        const url = `https://www.waze.com/ul?ll=${destLat},${destLon}&navigate=yes`;
        
        window.open(url, '_blank');
        mostrarNotificacionRuta(originLat, originLon, destLat, destLon, 'Waze');
    };

    window.mostrarOpcionesNavegacion = function(lat, lon, nombre, esRuta = false, userLat = null, userLon = null) {
        const overlay = document.createElement('div');
        overlay.className = 'maps-choice-overlay';
        
        const modal = document.createElement('div');
        modal.className = 'maps-choice-modal';
        
        const titulo = esRuta ? 'Crear Ruta' : 'Ver en Navegación';
        const mensaje = esRuta 
            ? `¿Cómo quieres crear la ruta hacia "${nombre || 'la plaza'}"?`
            : `¿Cómo quieres ver "${nombre || 'esta ubicación'}"?`;
        
        modal.innerHTML = `
            <h3>${titulo}</h3>
            <p>${mensaje}</p>
            <div class="maps-choice-buttons">
                <button class="maps-choice-btn maps-choice-gmaps" id="btn-gmaps">
                    <span class="maps-choice-icon">🗺️</span>
                    <span class="maps-choice-text">Google Maps</span>
                </button>
                <button class="maps-choice-btn maps-choice-waze" id="btn-waze">
                    <span class="maps-choice-icon">🚗</span>
                    <span class="maps-choice-text">Waze</span>
                </button>
            </div>
            <button class="maps-choice-cancel" id="btn-cancelar">
                Cancelar
            </button>
        `;
        
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        
        document.getElementById('btn-gmaps').addEventListener('click', function() {
            document.body.removeChild(overlay);
            if (esRuta && userLat && userLon) {
                window.crearRutaGoogleMaps(userLat, userLon, lat, lon, nombre);
            } else {
                window.abrirGoogleMaps(lat, lon, nombre);
            }
        });
        
        document.getElementById('btn-waze').addEventListener('click', function() {
            document.body.removeChild(overlay);
            if (esRuta) {
                // Waze usa GPS propio
                window.crearRutaWaze(userLat, userLon, lat, lon, nombre);
            } else {
                window.abrirWaze(lat, lon, nombre);
            }
        });
        
        document.getElementById('btn-cancelar').addEventListener('click', function() {
            document.body.removeChild(overlay);
        });
        
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) {
                document.body.removeChild(overlay);
            }
        });
    };

    function mostrarNotificacionRuta(originLat, originLon, destLat, destLon, appNombre) {
        const distancia = getDistanceFromLatLonInKm(originLat, originLon, destLat, destLon);
        
        const notificacion = document.createElement('div');
        notificacion.className = 'route-notification';
        
        notificacion.innerHTML = `
            <div class="route-notification-content">
                <div class="route-notification-icon">🚗</div>
                <div class="route-notification-text">
                    <strong>Ruta creada en ${appNombre}</strong><br>
                    <small>Distancia aprox: ${distancia.toFixed(1)} km</small><br>
                    <small>Se abrió en una nueva pestaña.</small>
                </div>
            </div>
            <button onclick="this.parentElement.remove()" class="route-notification-close">
                ×
            </button>
        `;
        
        document.body.appendChild(notificacion);
        
        setTimeout(() => {
            if (notificacion.parentNode) notificacion.remove();
        }, 5000);
    }

    // =========================================================
    // FUNCIONES DE NAVEGACIÓN
    // =========================================================
    window.zoomAPlaza = async function(lat, lon, clave) {
        if (!mapInstance) {
            console.error("Mapa no inicializado");
            return;
        }
        
        try {
            lastManualInteraction = Date.now();
            
            // 🔥 CENTRADO AUTOMÁTICO EN LA PANTALLA
            mapInstance.flyTo([lat, lon], 16, {
                animate: true,
                duration: 1.0 // Animación suave de 1 segundo
            });
            
            if (userLocation) {
                if (currentPolyline) mapInstance.removeLayer(currentPolyline);
                
                try {
                    const params = new URLSearchParams({
                        origen_lat: userLocation.lat,
                        origen_lng: userLocation.lon,
                        destino_lat: lat,
                        destino_lng: lon
                    });
                    
                    const response = await fetch(`${ENDPOINT_LINEA_RUTA}?${params}`);
                    const data = await response.json();
                    
                    if (data.status === 'success') {
                        currentPolyline = L.polyline(data.puntos_ruta, {
                            color: data.estilo_linea.color,
                            weight: data.estilo_linea.weight,
                            opacity: data.estilo_linea.opacity,
                            dashArray: data.estilo_linea.dashArray
                        }).addTo(mapInstance);
                        
                        currentPolyline.bindPopup(`
                            <div class="route-popup">
                                <strong>📍 Ruta a ${clave}</strong><br>
                                <small>Distancia: ${data.distancia_km} km</small><br>
                                <small>Desde tu ubicación actual</small>
                            </div>
                        `).openPopup();
                    }
                } catch (error) {
                    console.error("Error obteniendo línea de ruta:", error);
                    const latlngs = [
                        [userLocation.lat, userLocation.lon],
                        [lat, lon]
                    ];

                    currentPolyline = L.polyline(latlngs, {
                        color: 'red',
                        weight: 3,
                        opacity: 0.6,
                        dashArray: '10, 10'
                    }).addTo(mapInstance);
                }
            }
            
            setTimeout(() => {
                markersGroup.getLayers().forEach(layer => {
                    if (layer.getLatLng().lat === lat && layer.getLatLng().lng === lon) {
                        layer.openPopup();
                        
                        const icon = layer.getElement();
                        if (icon) {
                            icon.style.animation = 'pulse 0.5s 3';
                            icon.style.boxShadow = '0 0 0 5px rgba(0,123,255,0.3)';
                            
                            setTimeout(() => {
                                icon.style.animation = '';
                                icon.style.boxShadow = '';
                            }, 1500);
                        }
                    }
                });
            }, 1100); // Esperar a que termine flyTo
            
            // Cerrar modal de cercanos si existe (versión móvil)
            if (window.innerWidth < 600) {
                cerrarModalGPS();
            }
        } catch (error) {
            console.error("Error en zoomAPlaza:", error);
        }
    };

    window.ubicarPlazaMasCercana = async function() {
        try {
            const location = await obtenerUbicacionUsuario(true);
            
            const params = new URLSearchParams({
                lat: location.lat,
                lng: location.lon
            });
            
            const response = await fetch(`${ENDPOINT_UBICAR_CERCANA}?${params}`);
            const data = await response.json();
            
            if (data.status === 'success') {
                const plaza = data.plaza_mas_cercana;
                mostrarNotificacion(`Plaza más cercana encontrada: ${plaza.clave} (${plaza.distancia_formateada})`);
                window.zoomAPlaza(plaza.lat, plaza.lng, plaza.clave);
                return plaza;
            } else {
                throw new Error(data.message);
            }
        } catch (error) {
            console.error("Error ubicando plaza más cercana:", error);
            alert(`Error: ${error}`);
            return null;
        }
    };

    // =========================================================
    // BUSQUEDA CERCANA (CON GESTIÓN DE BOTÓN ATRÁS)
    // =========================================================
    async function buscarCercanos() {
        const modal = document.getElementById('gps-results-modal');
        if (!modal) return;
        
        // 🔥 AGREGAR ESTADO AL HISTORIAL PARA MANEJAR BOTÓN ATRÁS
        window.history.pushState({ modalAbierto: 'gps' }, '', '#gps-results');
        
        modal.style.display = 'block';
        modal.innerHTML = '<div class="gps-loading">🛰️ Obteniendo tu ubicación...</div>';

        try {
            const location = await obtenerUbicacionUsuario(true);
            
            const params = new URLSearchParams({
                action: 'cercanos',
                lat: location.lat,
                lng: location.lon,
                distancia_max: 50,
                limite: 10
            });
            
            const response = await fetch(`${ENDPOINT_MAPA_SEGURO}?${params}`);
            const data = await response.json();
            
            if (data.status === 'success') {
                mostrarListaCercanas(data.plazas_cercanas, location);
            } else {
                throw new Error(data.message);
            }
            
        } catch (error) {
            console.error("Error obteniendo plazas cercanas:", error);
            modal.innerHTML = `
                <span class="btn-close-gps" onclick="window.history.back()">×</span>
                <div class="gps-error">
                    ⚠️ ${error}
                </div>
            `;
            setTimeout(() => { 
                if (modal.style.display === 'block') window.history.back(); 
            }, 4000);
        }
    }

    // Función auxiliar para cerrar el modal limpiamente
    function cerrarModalGPS() {
        const modal = document.getElementById('gps-results-modal');
        if (modal) {
            modal.style.display = 'none';
            // Si estamos en el estado del modal, volvemos atrás
            if (history.state && history.state.modalAbierto === 'gps') {
                history.back();
            }
        }
    }

    // ESCUCHADOR PARA EL BOTÓN ATRÁS DEL NAVEGADOR
    window.addEventListener('popstate', function(event) {
        const modal = document.getElementById('gps-results-modal');
        // Si el modal está visible y se presiona atrás, lo ocultamos
        if (modal && modal.style.display === 'block') {
            modal.style.display = 'none';
            // Prevenir otras acciones si es necesario
        }
    });

    function mostrarListaCercanas(plazas, userLocation) {
        const modal = document.getElementById('gps-results-modal');
        if (!modal) return;
        
        // Botón de cierre ahora ejecuta history.back() para ser consistente
        let html = `
            <span class="btn-close-gps" onclick="window.history.back()">×</span>
            <h4 class="gps-title">🏢 Las 5 más cercanas</h4>
        `;

        if (userLocation) {
            html += `
                <div class="gps-location-info">
                    <span>📍 Ubicación obtenida</span>
                    <small class="gps-accuracy">
                        Precisión: ${Math.round(userLocation.accuracy)}m
                    </small>
                </div>
            `;
        }

        if (plazas.length === 0) {
            html += '<p class="gps-no-results">No se encontraron plazas cercanas.</p>';
        } else {
            html += plazas.map(p => generarPlazaCercanaHTML(p, userLocation)).join('');
        }

        modal.innerHTML = html;
        
        if (window.innerWidth <= 768) {
            configurarSwipeToClose(modal);
        }
    }

    function configurarSwipeToClose(modal) {
        let startY = 0;
        let currentY = 0;
        
        modal.addEventListener('touchstart', function(e) {
            startY = e.touches[0].clientY;
        }, { passive: true });
        
        modal.addEventListener('touchmove', function(e) {
            currentY = e.touches[0].clientY;
            const diff = currentY - startY;
            
            if (diff > 0) {
                modal.style.transform = `translateY(${diff}px)`;
            }
        }, { passive: true });
        
        modal.addEventListener('touchend', function(e) {
            const diff = currentY - startY;
            const threshold = 100;
            
            if (diff > threshold) {
                modal.classList.add('closing');
                setTimeout(() => {
                    // Usar history back para cerrar consistente
                    window.history.back();
                    modal.classList.remove('closing');
                    modal.style.transform = 'translateY(0)';
                }, 300);
            } else {
                modal.style.transform = 'translateY(0)';
            }
        }, { passive: true });
    }

    // =========================================================
    // GESTIÓN DE DATOS
    // =========================================================
    async function gestionarDatosMapa() {
        const loadingOverlay = document.getElementById('map-loading-overlay');
        const loadingText = document.getElementById('map-loading-text');

        try {
            if (loadingOverlay) loadingOverlay.style.display = 'flex';
            if (loadingText) loadingText.innerText = "Verificando actualizaciones...";

            const cachedData = localStorage.getItem(KEY_DATA);
            const cachedVersion = localStorage.getItem(KEY_VER);
            
            let usarCache = false;
            let serverVer = null;

            try {
                const respVer = await fetch('/api/version-coordenadas');
                if (respVer.ok) {
                    const jsonVer = await respVer.json();
                    serverVer = jsonVer.version;
                    
                    if (cachedData && cachedVersion === serverVer) {
                        usarCache = true;
                    }
                } else {
                    if (cachedData) usarCache = true;
                }
            } catch (e) {
                console.warn("No se pudo verificar versión:", e);
                if (cachedData) usarCache = true;
            }

            if (usarCache) {
                try {
                    const data = JSON.parse(cachedData);
                    if (Array.isArray(data) && data.length > 0) {
                        console.log(`✅ Usando caché (${data.length} plazas)`);
                        datosGlobales = data;
                        procesarPuntosMapa(data);
                        agregarControlesDespuesDeDatos();
                        return;
                    }
                } catch (e) {
                    console.warn("Caché corrupto:", e);
                }
            }

            if (loadingText) loadingText.innerText = "Actualizando mapa...";
            
            const response = await fetch(ENDPOINT_COORDENADAS_SEGURAS);
            
            if (!response.ok) {
                throw new Error(`Error HTTP ${response.status}`);
            }
            
            const data = await response.json();

            if (data.status === 'error') throw new Error(data.error);

            if (!data.plazas || !Array.isArray(data.plazas)) {
                throw new Error("Datos no válidos");
            }

            console.log(`⬇️ Datos descargados: ${data.plazas.length} plazas`);

            try {
                localStorage.setItem(KEY_DATA, JSON.stringify(data.plazas));
                localStorage.setItem(KEY_VER, serverVer || Date.now().toString());
            } catch(e) { 
                console.warn("No se pudo guardar en caché:", e); 
            }

            datosGlobales = data.plazas;
            procesarPuntosMapa(data.plazas);
            agregarControlesDespuesDeDatos();
            
        } catch (error) {
            console.error("Error cargando datos:", error);
            
            try {
                const cachedData = localStorage.getItem(KEY_DATA);
                if (cachedData) {
                    const data = JSON.parse(cachedData);
                    if (Array.isArray(data) && data.length > 0) {
                        console.log(`⚠️ Usando datos en caché (modo offline): ${data.length} plazas`);
                        datosGlobales = data;
                        procesarPuntosMapa(data);
                        agregarControlesDespuesDeDatos();
                        return;
                    }
                }
            } catch(e) {
                console.error("No se pudieron cargar datos del caché:", e);
            }
            
            if (loadingText) {
                loadingText.innerText = "Error cargando datos";
                loadingText.style.color = "red";
                loadingText.innerHTML += `<br><small>${error.message}</small>`;
            }
            
            setTimeout(() => {
                if (loadingOverlay) loadingOverlay.style.display = 'none';
            }, 3000);
        } finally {
            if (loadingOverlay) {
                setTimeout(() => {
                    loadingOverlay.style.display = 'none';
                }, 500);
            }
        }
    }

    // =========================================================
    // PROCESAR PUNTOS EN EL MAPA (MODIFICADA)
    // =========================================================
    function procesarPuntosMapa(datos) {
        if (!mapInstance || !markersGroup) return;
        
        markersGroup.clearLayers();
        
        const popupCache = new Map();
        
        datos.forEach(plaza => {
            const lat = parseFloat(plaza.Latitud || plaza.lat);
            const lon = parseFloat(plaza.Longitud || plaza.lng);
            
            if (isNaN(lat) || isNaN(lon)) return;
            
            const clave = plaza.Clave_Plaza || plaza.clave || 'Sin clave';
            
            const marker = L.marker([lat, lon], {
                icon: L.divIcon({
                    className: 'custom-pin-container',
                    html: `
                        <div class="modern-pin">
                            <span class="pin-icon">🏢</span> 
                        </div>
                        <div class="pin-shadow"></div>
                    `,
                    iconSize: [40, 50],
                    iconAnchor: [20, 50],
                    popupAnchor: [0, -50]
                })
            });
            
            let popupHTML;
            if (popupCache.has(clave)) {
                popupHTML = popupCache.get(clave);
            } else {
                popupHTML = generarPopupHTML(plaza);
                popupCache.set(clave, popupHTML);
            }
            
            marker.bindPopup(popupHTML, {
                maxWidth: window.innerWidth <= 768 ? 250 : 300
            });
            
            marker.on('click', function() {
                // 🔥 CENTRAR MAPA EN LA PLAZA AL HACER CLIC
                mapInstance.flyTo(this.getLatLng(), 16, {
                    animate: true,
                    duration: 0.5
                });
                
                this.openPopup();
                lastManualInteraction = Date.now();
            });
            
            markersGroup.addLayer(marker);
        });
        
        mapInstance.addLayer(markersGroup);
        
        if (datos.length > 0) {
            const bounds = markersGroup.getBounds();
            if (bounds.isValid()) {
                mapInstance.fitBounds(bounds, { 
                    padding: window.innerWidth <= 768 ? [30, 30] : [50, 50] 
                });
            }
        }
    }

    // =========================================================
    // CONTROLES DESPUÉS DE DATOS
    // =========================================================
    function agregarControlesDespuesDeDatos() {
        if (!mapInstance || datosGlobales.length === 0) return;
        
        const searchInput = document.getElementById('map-search-input');
        if (searchInput) {
            searchInput.disabled = false;
            searchInput.placeholder = `Buscar entre ${datosGlobales.length} plazas...`;
        }
        
        mostrarNotificacion(`✅ ${datosGlobales.length} plazas cargadas`, 'success');
    }

    // =========================================================
    // NAVEGACIÓN ENTRE VISTAS
    // =========================================================
    window.irADetallePlaza = function(clave, volverAlMapa = false) {
        try {
            window.navigationContext.cameFromMap = true;
            window.navigationContext.lastClickedPlaza = clave;
            
            if (volverAlMapa) {
                window.volverAlMapa();
                return;
            }
            
            if (mapInstance) {
                window.navigationContext.lastMapView = {
                    center: mapInstance.getCenter(),
                    zoom: mapInstance.getZoom(),
                    bounds: mapInstance.getBounds()
                };
            }
            
            document.querySelectorAll('.view').forEach(v => {
                if (v && v.classList) v.classList.add('hidden');
            });
            
            const keySearchView = document.getElementById('key-search-view');
            if (keySearchView && keySearchView.classList) {
                keySearchView.classList.remove('hidden');
                window.location.hash = '#key-search-view';
                const input = document.getElementById('clave-input');
                const btnBuscar = document.getElementById('search-by-key-button');
                if (input && btnBuscar) {
                    input.value = clave || '';
                    setTimeout(() => {
                        if (btnBuscar && btnBuscar.click) btnBuscar.click();
                    }, 200);
                }
            }
        } catch (error) {
            console.error("Error en irADetallePlaza:", error);
        }
    };

    window.volverAlMapa = function() {
        try {
            document.querySelectorAll('.view').forEach(v => {
                if (v && v.classList) v.classList.add('hidden');
            });
            
            const mapView = document.getElementById('map-view');
            if (mapView && mapView.classList) {
                mapView.classList.remove('hidden');
                window.location.hash = '#map-view';
                
                setTimeout(() => {
                    if (mapInstance) {
                        mapInstance.invalidateSize();
                        if (window.navigationContext.lastMapView) {
                            mapInstance.setView(
                                window.navigationContext.lastMapView.center,
                                window.navigationContext.lastMapView.zoom
                            );
                        }
                    }
                }, 150);
            }
            
            window.navigationContext.cameFromMap = false;
            window.navigationContext.lastMapView = null;
            
        } catch (error) {
            console.error("Error en volverAlMapa:", error);
        }
    };

    // =========================================================
    // INTERCEPTOR DE BOTÓN DE VOLVER
    // =========================================================
    function setupBackButtonInterceptor() {
        const observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                if (mutation.type === 'childList') {
                    const backButton = document.getElementById('back-to-search-button');
                    if (backButton && window.navigationContext.cameFromMap) {
                        backButton.onclick = function(e) {
                            e.preventDefault();
                            e.stopPropagation();
                            window.volverAlMapa();
                            return false;
                        };
                        
                        backButton.innerHTML = '<i class="fa-solid fa-map"></i> Volver al Mapa';
                        backButton.classList.add('back-to-map-button');
                    }
                }
            });
        });
        
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
        
        window.addEventListener('popstate', function(event) {
            if (window.navigationContext.cameFromMap && window.location.hash !== '#map-view') {
                window.history.pushState(null, '', '#map-view');
                window.volverAlMapa();
            }
        });
    }

    // =========================================================
    // BOTÓN DE RESTABLECER MAPA
    // =========================================================
    function agregarResetMapButton() {
        if (!document.getElementById('reset-map-button')) {
            const button = document.createElement('button');
            button.id = 'reset-map-button';
            button.className = 'leaflet-bar leaflet-control leaflet-control-gps';
            button.innerHTML = '<span class="reset-icon">🔄</span> <span class="reset-text">Restablecer Vista</span>';
            button.title = "Restablecer el mapa a vista inicial";
            button.style.marginTop = '10px';
            
            button.onclick = function() {
                if (mapInstance) {
                    mapInstance.setView([23.6345, -102.5528], 5);
                    mapInstance.invalidateSize();
                    
                    if (currentPolyline) {
                        mapInstance.removeLayer(currentPolyline);
                        currentPolyline = null;
                    }
                    
                    const estadoSelect = document.getElementById('filtro-estado');
                    if (estadoSelect) estadoSelect.value = '';
                    
                    if (datosGlobales.length > 0) {
                        markersGroup.clearLayers();
                        procesarPuntosMapa(datosGlobales);
                    }
                    
                    mostrarNotificacion('Mapa restablecido a vista inicial');
                }
            };
            
            setTimeout(() => {
                const mapContainer = document.querySelector('.leaflet-control-container');
                if (mapContainer) {
                    const topRight = mapContainer.querySelector('.leaflet-top.leaflet-right');
                    if (topRight) {
                        const controls = topRight.querySelector('.leaflet-control');
                        if (controls) {
                            controls.appendChild(button);
                        } else {
                            topRight.appendChild(button);
                        }
                    }
                }
            }, 1000);
        }
    }

    // =========================================================
    // FUNCIONES DE UTILIDAD
    // =========================================================
    function getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2) {
        const R = 6371;
        const dLat = deg2rad(lat2 - lat1);
        const dLon = deg2rad(lon2 - lon1);
        const a = 
            Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLon/2) * Math.sin(dLon/2); 
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
        return R * c;
    }

    function deg2rad(deg) {
        return deg * (Math.PI/180);
    }

    function mostrarNotificacion(mensaje, tipo = 'info') {
        const notificacion = document.createElement('div');
        notificacion.className = `notification notification-${tipo}`;
        
        const icon = tipo === 'error' ? '❌' : tipo === 'warning' ? '⚠️' : '✅';
        
        notificacion.innerHTML = `
            <div class="notification-icon">${icon}</div>
            <div class="notification-text">${mensaje}</div>
            <button onclick="this.parentElement.remove()" class="notification-close">
                ×
            </button>
        `;
        
        document.body.appendChild(notificacion);
        
        setTimeout(() => {
            if (notificacion.parentNode) document.body.removeChild(notificacion);
        }, 3000);
    }

    // =========================================================
    // LÓGICA DE RUTA MEJORADA
    // =========================================================
    window.solicitarUbicacionParaRuta = function(destLat, destLon, destinoNombre, userLat = null, userLon = null) {
        // Si ya tenemos ubicación, vamos directo
        if (userLat && userLon) {
            window.mostrarOpcionesNavegacion(destLat, destLon, destinoNombre, true, userLat, userLon);
        } else {
            // Si no, pedimos permiso con el modal
            mostrarModalConfirmacionRuta(destLat, destLon, destinoNombre);
        }
    };

    function mostrarModalConfirmacionRuta(destLat, destLon, destinoNombre) {
        const overlay = document.createElement('div');
        overlay.className = 'gps-confirm-overlay';
        
        const modal = document.createElement('div');
        modal.className = 'gps-confirm-modal';
        modal.innerHTML = `
            <h3>🚗 Crear Ruta</h3>
            <p>Necesitamos tu ubicación actual para trazar la ruta hacia <strong>${destinoNombre}</strong>.</p>
            <div class="gps-confirm-buttons">
                <button class="gps-confirm-btn gps-confirm-yes" id="ruta-confirm-yes">
                    📍 Obtener Ubicación y Crear Ruta
                </button>
                <button class="gps-confirm-btn gps-confirm-no" id="ruta-confirm-no">
                    Cancelar
                </button>
            </div>
        `;
        
        overlay.appendChild(modal);
        document.body.appendChild(overlay); // Usar body es más seguro que map
        
        // EVENTO: SI (Crear Ruta)
        document.getElementById('ruta-confirm-yes').addEventListener('click', function() {
            // 1. Cerrar modal
            document.body.removeChild(overlay);
            
            // 2. Mostrar pantalla de carga
            mostrarLoaderRuta("Obteniendo tu ubicación GPS...");
            
            // 3. Obtener ubicación
            obtenerUbicacionUsuario(false)
                .then(location => {
                    // Éxito
                    ocultarLoaderRuta();
                    window.mostrarOpcionesNavegacion(destLat, destLon, destinoNombre, true, location.lat, location.lon);
                })
                .catch(error => {
                    // Error
                    ocultarLoaderRuta();
                    alert(`⚠️ No pudimos obtener tu ubicación: ${error}`);
                });
        });
        
        // EVENTO: NO (Cancelar)
        document.getElementById('ruta-confirm-no').addEventListener('click', function() {
            document.body.removeChild(overlay);
        });
    }

    // =========================================================
    // LIMPIEZA Y EXPORTS
    // =========================================================
    function limpiarMapa() {
        if (markersGroup) markersGroup.clearLayers();
        if (mapInstance) { 
            mapInstance.remove(); 
            mapInstance = null; 
        }
        mapInitialized = false;
        datosGlobales = [];
        userLocation = null;
        
        if (userLocationMarker) userLocationMarker.remove();
        if (userAccuracyCircle) userAccuracyCircle.remove();
        if (currentPolyline) currentPolyline.remove();
        
        if (isFollowing && watchId) {
            navigator.geolocation.clearWatch(watchId);
            isFollowing = false;
        }
        
        userLocationMarker = null;
        userAccuracyCircle = null;
        currentPolyline = null;
    }

    window.MapaManager = {
        init: initMap,
        limpiar: limpiarMapa,
        recargar: gestionarDatosMapa,
        obtenerUbicacion: obtenerUbicacionUsuario,
        volverAlMapa: window.volverAlMapa,
        ubicarPlazaMasCercana: window.ubicarPlazaMasCercana,
        activarSeguimiento: toggleLiveTracking,
        buscar: function(termino) {
            const input = document.getElementById('map-search-input');
            if (input) {
                input.value = termino;
                input.dispatchEvent(new Event('input', { bubbles: true }));
            }
        }
    };

    // =========================================================
    // INICIALIZACIÓN
    // =========================================================
    window.navigationContext = {
        cameFromMap: false,
        lastMapView: null,
        lastClickedPlaza: null
    };
    
    console.log('✅ Mapa optimizado para móviles inicializado');
    observeViewChanges();
});
