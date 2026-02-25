document.addEventListener("DOMContentLoaded", function() {

    // =========================================================
    // VARIABLES GLOBALES Y CONFIGURACIÓN
    // =========================================================
    let userLocation = null;
    let userLocationMarker = null;
    let userAccuracyCircle = null;
    let currentPolyline = null;

    const KEY_DATA = 'mapa_datos_plazas';
    const KEY_VER  = 'mapa_version_timestamp';
    let mapInstance    = null;
    let markersGroup   = null;
    let mapInitialized = false;
    let datosGlobales  = [];

    window.navigationContext = {
        cameFromMap:      false,
        lastMapView:      null,
        lastClickedPlaza: null
    };

    let watchId              = null;
    let isFollowing          = false;
    let lastManualInteraction = null;

    let searchCache = new Map();
    const SEARCH_CACHE_TTL = 300000;

    let todosLosMarkers = [];
    const popupCache    = new Map();
    let indiceLocal     = null;

    // Limpiar searchCache cada 10 minutos
    setInterval(() => {
        const ahora = Date.now();
        for (const [key, val] of searchCache.entries()) {
            if (ahora - val.timestamp > SEARCH_CACHE_TTL) searchCache.delete(key);
        }
    }, 600_000);

    // =========================================================
    // ENDPOINTS DEL BACKEND
    // =========================================================
    const ENDPOINT_MAPA_SEGURO          = '/api/mapa/seguro';
    const ENDPOINT_COORDENADAS_SEGURAS  = '/api/mapa/coordenadas-completas';
    const ENDPOINT_UBICAR_CERCANA       = '/api/mapa/ubicar-plaza-cercana';
    const ENDPOINT_LINEA_RUTA           = '/api/mapa/generar-linea-ruta';

    // =========================================================
    // HELPER: EVENTOS TÁCTILES UNIFICADOS
    // Evita doble-disparo click+touchend en móvil
    // =========================================================
   function addTapListener(element, handler) {
    if (!element) return;
    let touchStartX = 0;
    let touchStartY = 0;
    let touchMoved = false;

    element.addEventListener('touchstart', (e) => {
        touchMoved = false;
        touchStartX = e.touches[0].clientX;
        touchStartY = e.touches[0].clientY;
    }, { passive: true });

    element.addEventListener('touchmove', (e) => {
        // Si el usuario mueve el dedo más de 10px, consideramos que es SCROLL, no CLICK
        const moveX = Math.abs(e.touches[0].clientX - touchStartX);
        const moveY = Math.abs(e.touches[0].clientY - touchStartY);
        
        if (moveX > 10 || moveY > 10) {
            touchMoved = true;
        }
    }, { passive: true });

    element.addEventListener('touchend', function(e) {
        // Solo ejecuta si el dedo NO se movió (fue un toque limpio)
        if (!touchMoved) {
            // Prevenimos comportamientos extraños pero permitimos el handler
            handler(e);
        }
    }, { passive: false });

    // Mantenemos el click para escritorio, pero el touchend arriba 
    // ya filtró el movimiento en móviles.
    element.addEventListener('click', function(e) {
        // Si es un dispositivo táctil, el touchend ya se encargó. 
        // El click solo debe dispararse si no hubo eventos touch previos.
        if (e.pointerType === 'mouse' || e.pointerType === '') {
            handler(e);
        }
    });
}
    // =========================================================
    // HELPER: FETCH CON TIMEOUT
    // =========================================================
    async function fetchConTimeout(url, opciones = {}, timeoutMs = 8000) {
        const controller = new AbortController();
        const timeoutId  = setTimeout(() => controller.abort(), timeoutMs);
        try {
            const response = await fetch(url, { ...opciones, signal: controller.signal });
            clearTimeout(timeoutId);
            return response;
        } catch (error) {
            clearTimeout(timeoutId);
            if (error.name === 'AbortError') throw new Error('Tiempo de espera agotado.');
            throw error;
        }
    }

    // =========================================================
    // HELPER: ESCAPE HTML / JS
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
            .replace(/'/g,  "\\'")
            .replace(/"/g,  '\\"')
            .replace(/\n/g, '\\n')
            .replace(/\r/g, '\\r')
            .replace(/\t/g, '\\t');
    }

    // =========================================================
    // LOADER DE RUTAS
    // =========================================================
    function mostrarLoaderRuta(texto) {
        const loader = document.createElement('div');
        loader.id        = 'route-loader';
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
    // GENERADORES DE HTML
    // =========================================================
    function generarPopupHTML(plaza) {
        const clave     = plaza.Clave_Plaza || plaza.clave    || 'Sin clave';
        const nombre    = plaza.Nombre      || plaza.NOMBRE_PC || plaza.nombre    || 'Sin nombre';
        const estado    = plaza.Estado      || plaza.estado    || 'Sin estado';
        const municipio = plaza.Municipio   || plaza.municipio || '';
        const lat       = plaza.Latitud     || plaza.lat       || null;
        const lng       = plaza.Longitud    || plaza.lng       || null;
        const situacion = plaza.situacion   || plaza.SITUACION || '';

        const cE = escapeJS(clave);
        const cH = escapeHTML(clave);
        const nH = escapeHTML(nombre);
        const eH = escapeHTML(estado);
        const mH = escapeHTML(municipio);
        const sH = escapeHTML(situacion);

        const esMovil = window.innerWidth <= 768;

        if (!esMovil) {
            return `
            <div class="popup-container">
                <div class="popup-header">
                    <h4>${cH}</h4>
                    <div class="popup-subtitle">
                        <span>${eH}</span>${mH ? `<span>, ${mH}</span>` : ''}
                    </div>
                </div>
                <div class="popup-content">
                    <p class="popup-nombre"><strong>${nH}</strong></p>
                    ${sH ? `<p class="popup-situacion">${sH}</p>` : ''}
                    <div class="popup-actions">
                        <button type="button" data-action="detalle" data-clave="${cE}"
                            class="btn-map-popup btn-map-popup-details">
                            <span class="popup-icon">📋</span>
                            <span class="popup-text">Ver Detalles</span>
                        </button>
                        <button type="button" data-action="zoom" data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                            class="btn-map-popup btn-map-popup-zoom">
                            <span class="popup-icon">📍</span>
                            <span class="popup-text">Centrar</span>
                        </button>
                        <div class="popup-buttons-grid">
                            <button type="button" data-action="maps" data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                                class="btn-map-popup btn-map-popup-gmaps">
                                <span class="popup-icon">🗺️</span>
                                <span class="popup-text">Maps</span>
                            </button>
                            <button type="button" data-action="ruta" data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                                class="btn-map-popup btn-map-popup-route">
                                <span class="popup-icon">🚗</span>
                                <span class="popup-text">Ruta</span>
                            </button>
                        </div>
                    </div>
                </div>
            </div>`;
        }

        return `
        <div class="popup-container mobile">
            <div class="popup-header mobile">
                <h4>${cH}</h4>
                <p class="popup-nombre-mobile"><strong>${nH}</strong></p>
                <p class="popup-location">${eH}${mH ? ', ' + mH : ''}</p>
            </div>
            <div class="popup-content mobile">
                <button type="button" data-action="detalle" data-clave="${cE}"
                    class="btn-map-popup btn-map-popup-details mobile">
                    📋 Detalles
                </button>
                <div class="popup-buttons-grid mobile">
                    <button type="button" data-action="maps" data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                        class="btn-map-popup btn-map-popup-gmaps mobile">
                        🗺️ Maps
                    </button>
                    <button type="button" data-action="ruta" data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                        class="btn-map-popup btn-map-popup-route mobile">
                        🚗 Ruta
                    </button>
                </div>
            </div>
        </div>`;
    }

    function aplicarEventosPopup(popupElement) {
        if (!popupElement) return;
        if (window.L) L.DomEvent.disableClickPropagation(popupElement);
    }

    function generarResultadoBusquedaHTML(resultado) {
        const clave     = resultado.clave     || 'Sin clave';
        const nombre    = resultado.nombre    || 'Sin nombre';
        const estado    = resultado.estado    || '';
        const municipio = resultado.municipio || '';
        const lat       = resultado.lat;
        const lng       = resultado.lng;

        const cE = escapeJS(clave);
        const cH = escapeHTML(clave);
        const nH = escapeHTML(nombre);
        const eH = escapeHTML(estado);
        const mH = escapeHTML(municipio);

        const icono    = resultado.tipo_coincidencia === 'exacta' ? '🎯' : '📍';
        const ubicacion = mH ? `${eH}, ${mH}` : eH;

        return `
        <button type="button" data-action="search-select"
           data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
           class="search-result-item">
            <div class="search-result-icon">${icono}</div>
            <div class="search-result-content">
                <div class="search-result-clave"><strong>${cH}</strong></div>
                <div class="search-result-nombre">${nH}</div>
                <div class="search-result-ubicacion">${ubicacion}</div>
            </div>
        </button>`;
    }

    function generarPlazaCercanaHTML(plaza, userLoc) {
        const clave     = plaza.clave    || 'Sin clave';
        const nombre    = plaza.nombre   || 'Sin nombre';
        const distancia = plaza.distancia_formateada || '0 km';
        const lat       = plaza.lat;
        const lng       = plaza.lng;
        const uLat      = userLoc ? userLoc.lat : null;
        const uLng      = userLoc ? userLoc.lon : null;

        const cE = escapeJS(clave);
        const cH = escapeHTML(clave);
        const nH = escapeHTML(nombre);

        return `
        <div class="gps-item">
            <div class="gps-item-header">
                <div class="gps-item-clave"><strong>${cH}</strong></div>
                <div class="gps-dist">📍 A ${distancia}</div>
            </div>
            <div class="gps-item-nombre">${nH}</div>
            <div class="gps-buttons-grid">
                <button type="button" data-action="zoom"
                   data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                   class="btn-map-popup btn-map-popup-zoom">
                    Ver en Mapa
                </button>
                <button type="button" data-action="detalle" data-clave="${cE}"
                   class="btn-map-popup btn-map-popup-details">
                    Detalles
                </button>
            </div>
            <div class="route-buttons-grid">
                <button type="button" data-action="maps"
                   data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                   class="btn-map-popup btn-map-popup-gmaps">
                    🗺️ Ver en Maps
                </button>
                <button type="button" data-action="ruta-con-origen"
                   data-lat="${lat}" data-lng="${lng}" data-clave="${cE}"
                   data-user-lat="${uLat}" data-user-lng="${uLng}"
                   class="btn-map-popup btn-map-popup-route">
                    🚗 Cómo Llegar
                </button>
            </div>
        </div>`;
    }

    // =========================================================
    // HELPER CENTRALIZADO DE VISTAS
    // =========================================================
    function _mostrarVista(vistaId) {
        document.querySelectorAll('.view').forEach(v => v.classList.add('hidden'));
        const target = document.getElementById(vistaId);
        if (!target) return;
        target.classList.remove('hidden');

        if (vistaId === 'map-view' && mapInstance) {
            setTimeout(() => {
                mapInstance.invalidateSize();
                if (window.navigationContext.lastMapView) {
                    mapInstance.setView(
                        window.navigationContext.lastMapView.center,
                        window.navigationContext.lastMapView.zoom,
                        { animate: false }
                    );
                    window.navigationContext.lastMapView = null;
                }
            }, 150);
        }
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
                    if (isMapVisible() && !mapInitialized) initMap();
                }
            });
        });

        const mapView = document.getElementById('map-view');
        if (mapView) observer.observe(mapView, { attributes: true });

        window.addEventListener('hashchange', function() {
            if (window.location.hash === '#map-view' && !mapInitialized) initMap();
        });

        if (window.location.hash === '#map-view' || isMapVisible()) {
            setTimeout(initMap, 300);
        }
    }

    // =========================================================
    // INICIALIZACIÓN DEL MAPA
    // =========================================================
    function initMap() {
        if (typeof L === 'undefined') { setTimeout(initMap, 1000); return; }
        const mapContainer = document.getElementById('map');
        if (!mapContainer || mapInitialized) return;

        console.log('🗺️ Inicializando mapa TURBO v3.0...');

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
            zoomControl: false,
            preferCanvas: true,
            fadeAnimation:   !L.Browser.mobile,
            zoomAnimation:   !L.Browser.mobile,
            markerZoomAnimation: false,
            tap: false
        });

        L.control.zoom({ position: 'bottomright' }).addTo(mapInstance);
        mapInstance.on('dragstart zoomstart', () => { lastManualInteraction = Date.now(); });

        configurarGestosParaMoviles(mapInstance);
        L.control.layers({ 'Mapa': calle, 'Satélite': satelite }).addTo(mapInstance);

        markersGroup = L.markerClusterGroup({
            disableClusteringAtZoom: 16,
            spiderfyOnMaxZoom: true,
            maxClusterRadius: 70,
            chunkedLoading: true,
            chunkInterval: 200,
            chunkDelay: 50,
            showCoverageOnHover: false
        });

        mapInstance.addLayer(markersGroup);
        mapInitialized = true;

        // Delegación global de eventos en el mapa
        const mapEl = document.getElementById('map');
        if (mapEl) {
            mapEl.addEventListener('click', manejarEventoMapa);
            mapEl.addEventListener('touchend', function(e) {
                const touch    = e.changedTouches[0];
                const target   = document.elementFromPoint(touch.clientX, touch.clientY);
                const actionEl = target && target.closest('[data-action]');
                if (actionEl) {
                    e.preventDefault();
                    e.customTarget = actionEl;
                    manejarEventoMapa(e);
                }
            }, { passive: false });
        }

        // Delegación global en el BODY para el panel GPS (botones fuera del #map)
        document.body.addEventListener('click', manejarEventoBody);
        document.body.addEventListener('touchend', function(e) {
            const touch    = e.changedTouches[0];
            const target   = document.elementFromPoint(touch.clientX, touch.clientY);
            const actionEl = target && target.closest('[data-action]');
            // Solo manejar si está FUERA del mapa
            const mapEl = document.getElementById('map');
            if (actionEl && mapEl && !mapEl.contains(actionEl)) {
                e.preventDefault();
                e.customTarget = actionEl;
                manejarEventoBody(e);
            }
        }, { passive: false });

        setupBackButtonInterceptor();
        agregarResetMapButton();
        agregarControlLiveTracking(mapInstance);
        agregarControlesInmediatos(mapInstance);
        gestionarDatosMapa();
    }

    // =========================================================
    // MANEJADORES DE EVENTOS DELEGADOS
    // =========================================================
    function _resolverAccion(e) {
        return e.customTarget
            || (e.target && e.target.closest('[data-action]'))
            || (e.changedTouches && document.elementFromPoint(
                e.changedTouches[0].clientX, e.changedTouches[0].clientY
            )?.closest('[data-action]'));
    }

    function _ejecutarAccion(btn) {
        if (!btn) return;
        const action = btn.dataset.action;
        const lat    = parseFloat(btn.dataset.lat);
        const lng    = parseFloat(btn.dataset.lng);
        const clave  = btn.dataset.clave;

        switch (action) {
            case 'detalle':
                window.irADetallePlaza(clave);
                break;

            case 'zoom':
                window.zoomAPlaza(lat, lng, clave);
                const sr = document.getElementById('search-results');
                if (sr) sr.style.display = 'none';
                const si = document.getElementById('map-search-input');
                if (si) si.value = clave;
                break;

            case 'maps':
                window.mostrarOpcionesNavegacion(lat, lng, clave, false);
                break;

            case 'ruta':
                window.solicitarUbicacionParaRuta(lat, lng, clave);
                break;

            case 'ruta-con-origen':
                const uLat = parseFloat(btn.dataset.userLat);
                const uLng = parseFloat(btn.dataset.userLng);
                window.solicitarUbicacionParaRuta(
                    lat, lng, clave,
                    isNaN(uLat) ? null : uLat,
                    isNaN(uLng) ? null : uLng
                );
                break;

            case 'search-select':
                window.zoomAPlaza(lat, lng, clave);
                const sr2 = document.getElementById('search-results');
                if (sr2) sr2.style.display = 'none';
                const si2 = document.getElementById('map-search-input');
                if (si2) si2.value = clave;
                break;
        }
    }

    // Eventos DENTRO del mapa Leaflet
    function manejarEventoMapa(e) {
        const btn = _resolverAccion(e);
        if (!btn) return;
        e.stopPropagation();
        _ejecutarAccion(btn);
    }

    // Eventos FUERA del mapa (panel GPS, modales, etc.)
    function manejarEventoBody(e) {
        const btn = _resolverAccion(e);
        if (!btn) return;
        // Evitar duplicado si el clic viene del mapa
        const mapEl = document.getElementById('map');
        if (mapEl && mapEl.contains(btn)) return;
        e.stopPropagation();
        _ejecutarAccion(btn);
    }

    // =========================================================
    // CONTROLES INMEDIATOS (GPS Cercanas + Buscador + Panel GPS)
    // =========================================================
    function agregarControlesInmediatos(map) {
        const GpsControl = L.Control.extend({
            options: { position: 'topleft' },
            onAdd: function() {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control leaflet-control-gps');
                container.innerHTML = '<span class="gps-icon">🧭</span> <span class="gps-text">Cercanas</span>';
                container.style.cursor = 'pointer';
                addTapListener(container, mostrarModalConfirmacionGPS);
                L.DomEvent.disableClickPropagation(container);
                return container;
            }
        });
        map.addControl(new GpsControl());
        agregarBuscador(map);

        // Panel GPS: se añade al MAP-VIEW (no al body) para que el CSS lo posicione bien
        if (!document.getElementById('gps-results-modal')) {
            const resultsDiv = document.createElement('div');
            resultsDiv.id = 'gps-results-modal';
            resultsDiv.style.display = 'none';
            // En móvil el CSS lo convierte en fixed bottom-sheet;
            // en escritorio es absolute dentro de #map-view
            const mapView = document.getElementById('map-view') || document.getElementById('map');
            if (mapView) mapView.appendChild(resultsDiv);
        }
    }

    // =========================================================
    // GESTOS PARA MÓVILES (Candado de scroll)
    // =========================================================
    function configurarGestosParaMoviles(map) {
        if (!L.Browser.mobile) return;

        let mapLocked  = false;
        let lockTimeout = null;

        const lockControl = L.Control.extend({
            options: { position: 'topleft' },
            onAdd: function() {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control leaflet-control-lock');
                container.innerHTML = '<span class="lock-icon">🔒</span><span class="lock-text">Scroll</span>';
                container.style.marginTop = '90px';
                L.DomEvent.disableClickPropagation(container);

                addTapListener(container, function() {
                    mapLocked = !mapLocked;
                    if (mapLocked) {
                        map.dragging.disable();
                        map.touchZoom.disable();
                        map.doubleClickZoom.disable();
                        container.innerHTML = '<span class="lock-icon">🔓</span><span class="lock-text">Mover</span>';
                        container.classList.add('control-active');
                        mostrarNotificacion('Mapa bloqueado — Puedes hacer scroll libremente');
                        lockTimeout = setTimeout(() => {
                            if (mapLocked) {
                                mapLocked = false;
                                map.dragging.enable();
                                map.touchZoom.enable();
                                map.doubleClickZoom.enable();
                                container.innerHTML = '<span class="lock-icon">🔒</span><span class="lock-text">Scroll</span>';
                                container.classList.remove('control-active');
                            }
                        }, 30000);
                    } else {
                        if (lockTimeout) clearTimeout(lockTimeout);
                        map.dragging.enable();
                        map.touchZoom.enable();
                        map.doubleClickZoom.enable();
                        container.innerHTML = '<span class="lock-icon">🔒</span><span class="lock-text">Scroll</span>';
                        container.classList.remove('control-active');
                        mostrarNotificacion('Mapa desbloqueado — Puedes moverlo');
                    }
                });
                return container;
            }
        });
        map.addControl(new lockControl());
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
                           placeholder="Buscar por clave o nombre...">
                    <div id="search-results" class="search-results" style="display:none;"></div>
                `;

                const input      = container.querySelector('#map-search-input');
                const resultsDiv = container.querySelector('#search-results');

                async function buscarEnBackend(query) {
                    try {
                        const cacheKey = `search_${query.toLowerCase()}`;
                        const cached   = searchCache.get(cacheKey);
                        if (cached && (Date.now() - cached.timestamp) < SEARCH_CACHE_TTL) return cached.data;

                        const params = new URLSearchParams({ action: 'buscar', q: query, tipo: 'todas', limite: 15 });
                        const response = await fetchConTimeout(`${ENDPOINT_MAPA_SEGURO}?${params}`);
                        const data     = await response.json();

                        if (data.status === 'success') {
                            searchCache.set(cacheKey, { data: data.resultados, timestamp: Date.now() });
                            return data.resultados;
                        }
                        return [];
                    } catch (error) {
                        console.error('Error en búsqueda:', error);
                        return [];
                    }
                }

                function mostrarResultados(resultados) {
                    if (resultados.length === 0) {
                        resultsDiv.innerHTML = `
                            <div class="search-no-results">
                                <div class="search-no-results-icon">🔍</div>
                                <div>No se encontraron resultados</div>
                            </div>`;
                    } else {
                        resultsDiv.innerHTML = resultados.map(p => generarResultadoBusquedaHTML(p)).join('');
                    }
                    resultsDiv.style.display = 'block';
                }

                let timeoutId;
                input.addEventListener('input', function(e) {
                    clearTimeout(timeoutId);
                    timeoutId = setTimeout(async () => {
                        const query = e.target.value.trim();
                        if (query.length < 2) { resultsDiv.style.display = 'none'; return; }

                        if (datosGlobales.length > 0) {
                            const locales = buscarEnIndice(query);
                            if (locales.length > 0) {
                                const convertidos = locales.map(p => ({
                                    clave:     p.clave      || p.Clave_Plaza || '',
                                    nombre:    p.nombre     || p.Nombre      || '',
                                    estado:    p.estado     || p.Estado      || '',
                                    municipio: p.municipio  || p.Municipio   || '',
                                    lat:       p.lat        || p.Latitud,
                                    lng:       p.lng        || p.Longitud,
                                    tipo_coincidencia: 'local'
                                }));
                                mostrarResultados(convertidos);

                                buscarEnBackend(query).then(backend => {
                                    const claves = new Set(locales.map(r => r.Clave_Plaza || r.clave));
                                    const nuevos = backend.filter(r => !claves.has(r.clave));
                                    if (nuevos.length > 0) mostrarResultados([...convertidos, ...nuevos].slice(0, 15));
                                });
                                return;
                            }
                        }
                        mostrarResultados(await buscarEnBackend(query));
                    }, 300);
                });

                document.addEventListener('click',    e => { if (!container.contains(e.target)) resultsDiv.style.display = 'none'; });
                document.addEventListener('touchend', e => { if (!container.contains(e.target)) resultsDiv.style.display = 'none'; }, { passive: true });

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
                container.style.marginTop = '50px';
                L.DomEvent.disableClickPropagation(container);
                addTapListener(container, toggleLiveTracking);
                return container;
            }
        });
        map.addControl(new LiveTrackingControl());
    }

    function toggleLiveTracking() {
        if (isFollowing) {
            if (watchId) navigator.geolocation.clearWatch(watchId);
            isFollowing = false;
            mostrarNotificacion('Seguimiento detenido');
            const btn = document.querySelector('.leaflet-control-gps-live');
            if (btn) { btn.innerHTML = '<span class="gps-icon">📍</span><span class="gps-text">Live OFF</span>'; btn.classList.remove('control-active'); }
        } else {
            if (!navigator.geolocation) { mostrarNotificacion('GPS no soportado', 'error'); return; }

            watchId = navigator.geolocation.watchPosition(
                ({ coords: { latitude, longitude, accuracy } }) => {
                    actualizarMarcadorUsuario(latitude, longitude, accuracy);
                    if (!lastManualInteraction || (Date.now() - lastManualInteraction > 3000)) {
                        L.Browser.mobile
                            ? mapInstance.setView([latitude, longitude], mapInstance.getZoom(), { animate: false })
                            : mapInstance.panTo([latitude, longitude], { animate: true, duration: 1, easeLinearity: 0.25 });
                    }
                },
                (error) => { console.error('Error GPS live:', error); mostrarNotificacion('Error GPS', 'error'); toggleLiveTracking(); },
                { enableHighAccuracy: true, maximumAge: 30000, timeout: 7000 }
            );

            isFollowing = true;
            mostrarNotificacion('Seguimiento activo 🚶');
            const btn = document.querySelector('.leaflet-control-gps-live');
            if (btn) { btn.innerHTML = '<span class="gps-icon">🎯</span><span class="gps-text">Live ON</span>'; btn.classList.add('control-active'); }
        }
    }

    function actualizarMarcadorUsuario(lat, lon, acc) {
        if (!userLocationMarker) {
            userLocationMarker = L.marker([lat, lon], {
                icon: L.divIcon({ className: 'user-location-icon-live', html: '<div class="user-location-pulse"></div>', iconSize: [26, 26], iconAnchor: [13, 13] })
            }).addTo(mapInstance);
        } else {
            userLocationMarker.setLatLng([lat, lon]);
        }

        if (!userAccuracyCircle) {
            userAccuracyCircle = L.circle([lat, lon], {
                radius: acc, fillColor: '#007bff', fillOpacity: 0.15, color: '#007bff', weight: 1, dashArray: '5, 5'
            }).addTo(mapInstance);
        } else {
            userAccuracyCircle.setLatLng([lat, lon]);
            userAccuracyCircle.setRadius(acc);
        }
    }

    // =========================================================
    // OBTENER UBICACIÓN (con reintentos progresivos)
    // =========================================================
    function obtenerUbicacionUsuario(mostrarEnMapa = true) {
        return new Promise((resolve, reject) => {
            if (!navigator.geolocation) { reject('GPS no soportado'); return; }

            function intentar(num) {
                const timeouts = [7000, 15000, 25000];
                navigator.geolocation.getCurrentPosition(
                    (position) => {
                        const { latitude: uLat, longitude: uLon, accuracy } = position.coords;
                        userLocation = { lat: uLat, lon: uLon, accuracy, timestamp: new Date().toISOString() };

                        if (mostrarEnMapa && mapInstance) {
                            if (userLocationMarker) mapInstance.removeLayer(userLocationMarker);
                            if (userAccuracyCircle) mapInstance.removeLayer(userAccuracyCircle);

                            userLocationMarker = L.marker([uLat, uLon], {
                                icon: L.divIcon({ className: 'user-location-icon', html: '<div class="user-location-dot"></div>', iconSize: [22, 22], iconAnchor: [11, 11] })
                            }).addTo(mapInstance);

                            userLocationMarker.bindPopup(`
                                <div class="user-location-popup">
                                    <strong>📍 Tu ubicación</strong><br>
                                    <small>Precisión: ${Math.round(accuracy)} metros</small><br>
                                    <small>${new Date().toLocaleTimeString()}</small>
                                </div>`);

                            if (accuracy < 1000) {
                                userAccuracyCircle = L.circle([uLat, uLon], {
                                    radius: accuracy, fillColor: '#007bff', fillOpacity: 0.1, color: '#007bff', weight: 1, dashArray: '5, 5'
                                }).addTo(mapInstance);
                            }

                            L.Browser.mobile
                                ? mapInstance.setView([uLat, uLon], 15, { animate: false })
                                : mapInstance.setView([uLat, uLon], 15);
                        }
                        resolve(userLocation);
                    },
                    (error) => {
                        console.warn(`GPS intento ${num + 1} fallido:`, error.code);
                        if (error.code === error.TIMEOUT && num < 2) {
                            mostrarNotificacion(`GPS lento, reintentando... (${num + 2}/3)`, 'warning');
                            intentar(num + 1);
                            return;
                        }
                        const msgs = {
                            [error.PERMISSION_DENIED]:     'Permiso denegado. Activa la ubicación en tu navegador.',
                            [error.POSITION_UNAVAILABLE]: 'Ubicación no disponible. Verifica tu GPS.',
                            [error.TIMEOUT]:               'Tiempo agotado tras 3 intentos.'
                        };
                        reject(msgs[error.code] || 'Error al obtener la ubicación.');
                    },
                    { enableHighAccuracy: num !== 0, timeout: timeouts[num] || 25000, maximumAge: num === 0 ? 60000 : 0 }
                );
            }
            intentar(0);
        });
    }

    // =========================================================
    // MODAL CONFIRMACIÓN GPS
    // =========================================================
    function mostrarModalConfirmacionGPS() {
        const overlay = document.createElement('div');
        overlay.className = 'gps-confirm-overlay';
        overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);display:flex;justify-content:center;align-items:center;z-index:100001;padding:20px;';

        const modal = document.createElement('div');
        modal.className = 'gps-confirm-modal';
        modal.style.cssText = 'background:white;border-radius:12px;padding:24px;max-width:400px;width:100%;box-shadow:0 10px 40px rgba(0,0,0,0.3);position:relative;z-index:100002;';
        modal.innerHTML = `
            <h3>📍 Buscar Plazas Cercanas</h3>
            <p>Para encontrar las plazas más cercanas, necesitamos acceder a tu ubicación.</p>
            <p style="font-size:13px;color:#666;">Tu ubicación solo se usará para calcular distancias y no se almacenará.</p>
            <div class="gps-confirm-buttons">
                <button class="gps-confirm-btn gps-confirm-yes" id="gps-confirm-yes" type="button">Sí, usar mi ubicación</button>
                <button class="gps-confirm-btn gps-confirm-no"  id="gps-confirm-no"  type="button">Cancelar</button>
            </div>`;

        overlay.appendChild(modal);
        document.body.appendChild(overlay);

        addTapListener(document.getElementById('gps-confirm-yes'), () => { overlay.remove(); buscarCercanos(); });
        addTapListener(document.getElementById('gps-confirm-no'),  () => overlay.remove());
        addTapListener(overlay, e => { if (e.target === overlay) overlay.remove(); });
    }

    // =========================================================
    // PANEL GPS — BUSCAR CERCANOS
    // =========================================================
    async function buscarCercanos() {
        const modal = document.getElementById('gps-results-modal');
        if (!modal) return;

        // Mostrar panel con estado de carga
        modal.style.display = 'block';
        modal.style.transform = '';
        modal.innerHTML = `
            <div class="gps-drag-handle" aria-hidden="true"></div>
            <div style="padding:30px 20px;text-align:center;color:#555;font-size:15px;">
                🛰️ Obteniendo tu ubicación...
            </div>`;

        try {
            const location = await obtenerUbicacionUsuario(true);
            const params   = new URLSearchParams({
                action: 'cercanos', lat: location.lat, lng: location.lon, distancia_max: 50, limite: 10
            });
            const response = await fetchConTimeout(`${ENDPOINT_MAPA_SEGURO}?${params}`, {}, 10000);
            const data     = await response.json();

            if (data.status === 'success') {
                mostrarListaCercanas(data.plazas_cercanas, location);
            } else {
                throw new Error(data.message);
            }
        } catch (error) {
            console.error('Error plazas cercanas:', error);
            modal.innerHTML = `
                <div class="gps-drag-handle" aria-hidden="true"></div>
                <div class="gps-header-sticky">
                    <h4 class="gps-title" style="margin:0;">Error</h4>
                    <button class="btn-close-gps" id="btn-close-error" type="button">✕</button>
                </div>
                <div class="gps-body">
                    <div class="gps-error">⚠️ ${error}</div>
                </div>`;
            addTapListener(document.getElementById('btn-close-error'), cerrarModalGPS);
            setTimeout(() => { if (modal.style.display === 'block') cerrarModalGPS(); }, 4000);
        }
    }

    function cerrarModalGPS() {
        const modal = document.getElementById('gps-results-modal');
        if (!modal || modal.style.display === 'none') return;
        modal.classList.add('closing');
        setTimeout(() => {
            modal.classList.remove('closing');
            modal.style.display   = 'none';
            modal.style.transform = '';
        }, 280);
    }

    function mostrarListaCercanas(plazas, userLoc) {
        const modal = document.getElementById('gps-results-modal');
        if (!modal) return;

        const precisionHTML = userLoc
            ? `<span class="gps-accuracy">📡 Precisión: ${Math.round(userLoc.accuracy)} m</span>`
            : '';

        const itemsHTML = plazas.length === 0
            ? '<p class="gps-no-results">No se encontraron plazas cercanas en un radio de 50 km.</p>'
            : plazas.map(p => generarPlazaCercanaHTML(p, userLoc)).join('');

        modal.innerHTML = `
            <div class="gps-drag-handle" id="gps-drag-handle" aria-hidden="true"></div>
            <div class="gps-header-sticky">
                <div>
                    <h4 class="gps-title" style="margin:0;font-size:16px;">🏢 Plazas cercanas</h4>
                    ${precisionHTML}
                </div>
                <button class="btn-close-gps" id="btn-close-gps-main" type="button" aria-label="Cerrar">✕</button>
            </div>
            <div class="gps-body">${itemsHTML}</div>`;

        addTapListener(document.getElementById('btn-close-gps-main'), cerrarModalGPS);

        if (window.innerWidth <= 768) configurarSwipeToClose(modal);
    }

    function configurarSwipeToClose(modal) {
        // El swipe solo se activa desde el drag handle — no interfiere con el scroll de la lista
        const handle = modal.querySelector('.gps-drag-handle');
        if (!handle) return;

        let startY = 0;
        let diff   = 0;

        handle.addEventListener('touchstart', e => {
            startY = e.touches[0].clientY;
            diff   = 0;
            modal.style.transition = 'none';
        }, { passive: true });

        handle.addEventListener('touchmove', e => {
            diff = e.touches[0].clientY - startY;
            if (diff > 0) modal.style.transform = `translateY(${diff}px)`;
        }, { passive: true });

        handle.addEventListener('touchend', () => {
            modal.style.transition = '';
            if (diff > 90) {
                cerrarModalGPS();
            } else {
                modal.style.transform = 'translateY(0)';
            }
        }, { passive: true });
    }

    // =========================================================
    // NAVEGACIÓN EXTERNA (Google Maps / Waze)
    // =========================================================
    window.abrirGoogleMaps = function(lat, lon) {
        if (!lat || !lon) { alert('No hay coordenadas.'); return; }
        window.open(`https://www.google.com/maps/search/?api=1&query=${lat},${lon}`, '_blank');
    };

    window.abrirWaze = function(lat, lon) {
        if (!lat || !lon) { alert('No hay coordenadas.'); return; }
        window.open(`https://www.waze.com/ul?ll=${lat},${lon}&navigate=yes`, '_blank');
    };

    window.crearRutaGoogleMaps = function(oLat, oLon, dLat, dLon) {
        if (!oLat || !oLon || !dLat || !dLon) { alert('Coordenadas insuficientes.'); return; }
        window.open(`https://www.google.com/maps/dir/?api=1&origin=${oLat},${oLon}&destination=${dLat},${dLon}&travelmode=driving`, '_blank');
        mostrarNotificacionRuta(oLat, oLon, dLat, dLon, 'Google Maps');
    };

    window.crearRutaWaze = function(oLat, oLon, dLat, dLon) {
        if (!dLat || !dLon) { alert('Sin coordenadas de destino.'); return; }
        window.open(`https://www.waze.com/ul?ll=${dLat},${dLon}&navigate=yes`, '_blank');
        mostrarNotificacionRuta(oLat, oLon, dLat, dLon, 'Waze');
    };

    window.mostrarOpcionesNavegacion = function(lat, lon, nombre, esRuta = false, userLat = null, userLon = null) {
        const overlay = document.createElement('div');
        overlay.className = 'maps-choice-overlay';
        overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);display:flex;justify-content:center;align-items:center;z-index:100001;padding:20px;';

        const modal = document.createElement('div');
        modal.className = 'maps-choice-modal';
        modal.style.cssText = 'background:white;border-radius:12px;padding:24px;max-width:400px;width:100%;box-shadow:0 10px 40px rgba(0,0,0,0.3);position:relative;z-index:100002;';
        modal.innerHTML = `
            <h3>${esRuta ? 'Crear Ruta' : 'Ver en Navegación'}</h3>
            <p>${esRuta ? `¿Cómo quieres crear la ruta hacia "${nombre || 'la plaza'}"?` : `¿Cómo quieres ver "${nombre || 'esta ubicación'}"?`}</p>
            <div class="maps-choice-buttons">
                <button class="maps-choice-btn maps-choice-gmaps" id="btn-gmaps" type="button">
                    <span class="maps-choice-icon">🗺️</span>
                    <span class="maps-choice-text">Google Maps</span>
                </button>
                <button class="maps-choice-btn maps-choice-waze" id="btn-waze" type="button">
                    <span class="maps-choice-icon">🚗</span>
                    <span class="maps-choice-text">Waze</span>
                </button>
            </div>
            <button class="maps-choice-cancel" id="btn-cancelar" type="button">Cancelar</button>`;

        overlay.appendChild(modal);
        document.body.appendChild(overlay);

        function remove() {
            overlay.style.opacity = '0';
            setTimeout(() => { if (overlay.parentNode) overlay.parentNode.removeChild(overlay); }, 100);
        }

        addTapListener(document.getElementById('btn-gmaps'), () => {
            overlay.parentNode && overlay.parentNode.removeChild(overlay);
            setTimeout(() => {
                esRuta && userLat && userLon
                    ? window.crearRutaGoogleMaps(userLat, userLon, lat, lon)
                    : window.abrirGoogleMaps(lat, lon);
            }, 50);
        });

        addTapListener(document.getElementById('btn-waze'), () => {
            overlay.parentNode && overlay.parentNode.removeChild(overlay);
            setTimeout(() => {
                esRuta
                    ? window.crearRutaWaze(userLat, userLon, lat, lon)
                    : window.abrirWaze(lat, lon);
            }, 50);
        });

        addTapListener(document.getElementById('btn-cancelar'), remove);
        addTapListener(overlay, e => { if (e.target === overlay) remove(); });
    };

    function mostrarNotificacionRuta(oLat, oLon, dLat, dLon, app) {
        const distancia = getDistanceFromLatLonInKm(oLat, oLon, dLat, dLon);
        const n = document.createElement('div');
        n.className = 'route-notification';
        n.innerHTML = `
            <div class="route-notification-content">
                <div class="route-notification-icon">🚗</div>
                <div class="route-notification-text">
                    <strong>Ruta creada en ${app}</strong><br>
                    <small>Distancia aprox: ${distancia.toFixed(1)} km</small>
                </div>
            </div>
            <button class="route-notification-close" type="button">×</button>`;
        document.body.appendChild(n);
        addTapListener(n.querySelector('.route-notification-close'), () => n.remove());
        setTimeout(() => { if (n.parentNode) n.remove(); }, 5000);
    }

    // =========================================================
    // ZOOM A PLAZA
    // =========================================================
    window.zoomAPlaza = async function(lat, lon, clave) {
        if (!mapInstance) return;
        try {
            lastManualInteraction = Date.now();
            L.Browser.mobile
                ? mapInstance.setView([lat, lon], 16, { animate: false })
                : mapInstance.flyTo([lat, lon], 16, { animate: true, duration: 1.0 });

            if (userLocation) {
                if (currentPolyline) mapInstance.removeLayer(currentPolyline);
                try {
                    const params   = new URLSearchParams({ origen_lat: userLocation.lat, origen_lng: userLocation.lon, destino_lat: lat, destino_lng: lon });
                    const response = await fetchConTimeout(`${ENDPOINT_LINEA_RUTA}?${params}`, {}, 6000);
                    const data     = await response.json();
                    if (data.status === 'success') {
                        currentPolyline = L.polyline(data.puntos_ruta, data.estilo_linea).addTo(mapInstance);
                    }
                } catch {
                    currentPolyline = L.polyline([[userLocation.lat, userLocation.lon], [lat, lon]], {
                        color: 'red', weight: 3, opacity: 0.6, dashArray: '10, 10'
                    }).addTo(mapInstance);
                }
            }

            const delay = L.Browser.mobile ? 100 : 1100;
            setTimeout(() => {
                markersGroup.getLayers().forEach(layer => {
                    const ll = layer.getLatLng();
                    if (Math.abs(ll.lat - lat) < 0.0001 && Math.abs(ll.lng - lon) < 0.0001) layer.openPopup();
                });
            }, delay);

            if (window.innerWidth < 600) cerrarModalGPS();
        } catch (error) {
            console.error('Error en zoomAPlaza:', error);
        }
    };

    window.ubicarPlazaMasCercana = async function() {
        try {
            const location = await obtenerUbicacionUsuario(true);
            const params   = new URLSearchParams({ lat: location.lat, lng: location.lon });
            const response = await fetchConTimeout(`${ENDPOINT_UBICAR_CERCANA}?${params}`);
            const data     = await response.json();
            if (data.status === 'success') {
                const plaza = data.plaza_mas_cercana;
                mostrarNotificacion(`Plaza más cercana: ${plaza.clave} (${plaza.distancia_formateada})`);
                window.zoomAPlaza(plaza.lat, plaza.lng, plaza.clave);
                return plaza;
            }
            throw new Error(data.message);
        } catch (error) {
            mostrarNotificacion(`Error: ${error}`, 'error');
            return null;
        }
    };

    // =========================================================
    // SOLICITAR UBICACIÓN PARA RUTA
    // =========================================================
    window.solicitarUbicacionParaRuta = function(destLat, destLon, nombre, userLat = null, userLon = null) {
        if (userLat && userLon) {
            window.mostrarOpcionesNavegacion(destLat, destLon, nombre, true, userLat, userLon);
        } else {
            mostrarModalConfirmacionRuta(destLat, destLon, nombre);
        }
    };

    function mostrarModalConfirmacionRuta(destLat, destLon, nombre) {
        const overlay = document.createElement('div');
        overlay.className = 'gps-confirm-overlay';
        overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);display:flex;justify-content:center;align-items:center;z-index:100001;padding:20px;';

        const modal = document.createElement('div');
        modal.className = 'gps-confirm-modal';
        modal.style.cssText = 'background:white;border-radius:12px;padding:24px;max-width:400px;width:100%;box-shadow:0 10px 40px rgba(0,0,0,0.3);position:relative;z-index:100002;';
        modal.innerHTML = `
            <h3>🚗 Crear Ruta</h3>
            <p>Necesitamos tu ubicación para trazar la ruta hacia <strong>${escapeHTML(nombre)}</strong>.</p>
            <div class="gps-confirm-buttons">
                <button class="gps-confirm-btn gps-confirm-yes" id="ruta-confirm-yes" type="button">
                    📍 Obtener Ubicación y Crear Ruta
                </button>
                <button class="gps-confirm-btn gps-confirm-no" id="ruta-confirm-no" type="button">Cancelar</button>
            </div>`;

        overlay.appendChild(modal);
        document.body.appendChild(overlay);

        function handleYes(e) {
            e && e.preventDefault && e.preventDefault();
            e && e.stopPropagation && e.stopPropagation();
            overlay.remove();
            mostrarLoaderRuta('Obteniendo tu ubicación GPS...');
            obtenerUbicacionUsuario(false)
                .then(loc => { ocultarLoaderRuta(); window.mostrarOpcionesNavegacion(destLat, destLon, nombre, true, loc.lat, loc.lon); })
                .catch(err => { ocultarLoaderRuta(); mostrarNotificacion(`⚠️ ${err}`, 'error'); });
        }

        function handleNo(e) {
            e && e.preventDefault && e.preventDefault();
            overlay.remove();
        }

        const yesBtn = document.getElementById('ruta-confirm-yes');
        const noBtn  = document.getElementById('ruta-confirm-no');

        if (yesBtn) { yesBtn.onclick = handleYes; yesBtn.ontouchend = e => { e.preventDefault(); handleYes(e); }; }
        if (noBtn)  { noBtn.onclick  = handleNo;  noBtn.ontouchend  = e => { e.preventDefault(); handleNo(e); }; }

        overlay.onclick    = e => { if (e.target === overlay) overlay.remove(); };
        overlay.ontouchend = e => { if (e.target === overlay) { e.preventDefault(); overlay.remove(); } };
    }

    // =========================================================
    // SISTEMA DE NAVEGACIÓN SPA — BOTÓN ATRÁS DEL TELÉFONO
    // =========================================================
    function setupBackButtonInterceptor() {

        // Establecer entrada base del mapa en el historial
        // replaceState no agrega entrada, solo etiqueta la actual
        history.replaceState({ vista: 'mapa' }, '', '#map-view');

        window.addEventListener('popstate', function(event) {
            const state = event.state;

            // Prioridad 1: cerrar panel GPS si está abierto
            const modal = document.getElementById('gps-results-modal');
            if (modal && modal.style.display === 'block') {
                cerrarModalGPS();
                // Restaurar la entrada del mapa para que el próximo "atrás" salga de la app
                history.replaceState({ vista: 'mapa' }, '', '#map-view');
                return;
            }

            // Prioridad 2: volver al mapa si venimos de detalle
            if (state && state.vista === 'mapa') {
                window.navigationContext.cameFromMap = false;
                _mostrarVista('map-view');
                return;
            }

            // Fallback: si cameFromMap está activo pero el state no coincide
            if (window.navigationContext.cameFromMap) {
                window.navigationContext.cameFromMap = false;
                _mostrarVista('map-view');
                history.replaceState({ vista: 'mapa' }, '', '#map-view');
            }
        });

        // Observar el botón dinámico "Volver" en la vista de detalle
        const observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                if (mutation.type !== 'childList') return;
                const backButton = document.getElementById('back-to-search-button');
                if (backButton && !backButton._mapaListenerAttached) {
                    backButton._mapaListenerAttached = true;

                    if (window.navigationContext.cameFromMap) {
                        backButton.innerHTML = '← Volver al Mapa';
                        backButton.style.cssText += ';background:#007bff;color:white;border-color:#007bff;';

                        // capture:true — dispara ANTES que el handler original del botón
                        backButton.addEventListener('click', function(e) {
                            e.preventDefault();
                            e.stopImmediatePropagation();
                            window.volverAlMapa();
                        }, true);

                        backButton.addEventListener('touchend', function(e) {
                            e.preventDefault();
                            e.stopImmediatePropagation();
                            window.volverAlMapa();
                        }, { capture: true, passive: false });
                    }
                }
            });
        });
        observer.observe(document.body, { childList: true, subtree: true });
    }

    // =========================================================
    // NAVEGACIÓN ENTRE VISTAS
    // =========================================================
    window.irADetallePlaza = function(clave) {
        try {
            window.navigationContext.cameFromMap      = true;
            window.navigationContext.lastClickedPlaza = clave;

            // Guardar posición del mapa
            if (mapInstance) {
                window.navigationContext.lastMapView = {
                    center: mapInstance.getCenter(),
                    zoom:   mapInstance.getZoom()
                };
            }

            // CRÍTICO: pushState PRIMERO → stack: [{vista:'mapa'}] → [{vista:'detalle'}]
            // El botón atrás del teléfono dispara popstate con state={vista:'mapa'}
            history.pushState({ vista: 'detalle', clave }, '', '#detalle-plaza');

            // Ahora cambiar la vista
            _mostrarVista('key-search-view');

            // Lanzar búsqueda automática
            const input     = document.getElementById('clave-input');
            const btnBuscar = document.getElementById('search-by-key-button');
            if (input && btnBuscar) {
                input.value = clave;
                setTimeout(() => btnBuscar.click(), 200);
            }
        } catch (error) {
            console.error('Error en irADetallePlaza:', error);
        }
    };

    window.volverAlMapa = function() {
        try {
            window.navigationContext.cameFromMap      = false;
            window.navigationContext.lastClickedPlaza = null;

            // Si el estado actual es 'detalle', history.back() limpia el stack
            // y dispara popstate → que llama _mostrarVista('map-view')
            if (history.state && history.state.vista === 'detalle') {
                history.back();
                return;
            }

            // Fallback directo
            history.replaceState({ vista: 'mapa' }, '', '#map-view');
            _mostrarVista('map-view');
        } catch (error) {
            console.error('Error en volverAlMapa:', error);
            _mostrarVista('map-view');
        }
    };

    // =========================================================
    // GESTIÓN DE DATOS Y CACHÉ
    // =========================================================
    async function gestionarDatosMapa() {
        const loadingOverlay = document.getElementById('map-loading-overlay');
        const loadingText    = document.getElementById('map-loading-text');

        try {
            if (loadingOverlay) loadingOverlay.style.display = 'flex';
            if (loadingText)    loadingText.innerText = 'Verificando actualizaciones...';

            const cachedData    = localStorage.getItem(KEY_DATA);
            const cachedVersion = localStorage.getItem(KEY_VER);
            let usarCache = false;
            let serverVer = null;

            try {
                const respVer = await fetchConTimeout('/api/version-coordenadas', {}, 5000);
                if (respVer.ok) {
                    const jsonVer = await respVer.json();
                    serverVer = jsonVer.version;
                    if (cachedData && cachedVersion === serverVer) usarCache = true;
                } else {
                    if (cachedData) usarCache = true;
                }
            } catch (e) {
                console.warn('No se pudo verificar versión:', e);
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
                } catch (e) { console.warn('Caché corrupto:', e); }
            }

            if (loadingText) loadingText.innerText = 'Actualizando mapa...';
            const response = await fetchConTimeout(ENDPOINT_COORDENADAS_SEGURAS, {}, 25000);
            if (!response.ok) throw new Error(`Error HTTP ${response.status}`);

            const data = await response.json();
            if (data.status === 'error') throw new Error(data.error);
            if (!data.plazas || !Array.isArray(data.plazas)) throw new Error('Datos no válidos');

            console.log(`⬇️ Datos descargados: ${data.plazas.length} plazas`);

            try {
                const serialized = JSON.stringify(data.plazas);
                const kb = (serialized.length * 2) / 1024;
                if (kb < 4500) {
                    localStorage.setItem(KEY_DATA, serialized);
                    localStorage.setItem(KEY_VER, serverVer || Date.now().toString());
                    console.log(`💾 Caché guardado (${kb.toFixed(0)} KB)`);
                } else {
                    console.warn(`⚠️ Datos muy grandes (${kb.toFixed(0)} KB), omitiendo caché`);
                }
            } catch (e) { console.warn('No se pudo guardar caché:', e); }

            datosGlobales = data.plazas;
            procesarPuntosMapa(data.plazas);
            agregarControlesDespuesDeDatos();

        } catch (error) {
            console.error('Error cargando datos:', error);
            try {
                const cachedData = localStorage.getItem(KEY_DATA);
                if (cachedData) {
                    const data = JSON.parse(cachedData);
                    if (Array.isArray(data) && data.length > 0) {
                        datosGlobales = data;
                        procesarPuntosMapa(data);
                        agregarControlesDespuesDeDatos();
                        mostrarNotificacion('Usando datos guardados (sin conexión)', 'warning');
                        return;
                    }
                }
            } catch (e) { console.error('Error caché fallback:', e); }

            if (loadingText) {
                loadingText.innerText = 'Error cargando datos';
                loadingText.style.color = 'red';
                loadingText.innerHTML += `<br><small>${error.message}</small>`;
            }
            setTimeout(() => { if (loadingOverlay) loadingOverlay.style.display = 'none'; }, 3000);

        } finally {
            if (loadingOverlay) setTimeout(() => { loadingOverlay.style.display = 'none'; }, 500);
        }
    }

    // =========================================================
    // ÍNDICE DE BÚSQUEDA LOCAL
    // =========================================================
    function construirIndiceLocal(datos) {
        indiceLocal = datos.map(p => ({
            ref:   p,
            texto: [
                p.clave     || p.Clave_Plaza || '',
                p.nombre    || p.Nombre      || '',
                p.estado    || p.Estado      || '',
                p.municipio || p.Municipio   || '',
                p.localidad || p.Localidad   || ''
            ].join('|').toLowerCase()
        }));
        console.log(`🔍 Índice construido: ${indiceLocal.length} entradas`);
    }

    function buscarEnIndice(query) {
        if (!indiceLocal) return [];
        const q = query.toLowerCase();
        return indiceLocal.filter(item => item.texto.includes(q)).slice(0, 10).map(item => item.ref);
    }

    // =========================================================
    // PROCESAR PUNTOS EN EL MAPA
    // =========================================================
    function procesarPuntosMapa(datos) {
        if (!mapInstance || !markersGroup) return;

        if (todosLosMarkers.length > 0) {
            markersGroup.clearLayers();
            markersGroup.addLayers(todosLosMarkers);
            mapInstance.addLayer(markersGroup);
            _ajustarBounds();
            return;
        }

        markersGroup.clearLayers();
        const popupOpts     = { maxWidth: window.innerWidth <= 768 ? 250 : 300, autoPan: !L.Browser.mobile };
        const nuevosMarkers = [];

        datos.forEach(plaza => {
            const lat = parseFloat(plaza.Latitud  || plaza.lat);
            const lon = parseFloat(plaza.Longitud || plaza.lng);
            if (isNaN(lat) || isNaN(lon)) return;

            const clave = plaza.Clave_Plaza || plaza.clave || 'Sin clave';

            const marker = L.marker([lat, lon], {
                icon: L.divIcon({
                    className: 'custom-pin-container',
                    html: `<div class="modern-pin"><span class="pin-icon">🏢</span></div><div class="pin-shadow"></div>`,
                    iconSize: [40, 50], iconAnchor: [20, 50], popupAnchor: [0, -50]
                })
            });

            if (!popupCache.has(clave)) popupCache.set(clave, generarPopupHTML(plaza));
            marker.bindPopup(L.popup(popupOpts).setContent(popupCache.get(clave)));

            marker.on('popupopen', function() {
                const el = this.getPopup().getElement();
                if (el) aplicarEventosPopup(el);
            });

            marker.on('click', function() {
                if (L.Browser.mobile) mapInstance.setView(this.getLatLng(), 16, { animate: false });
                this.openPopup();
                lastManualInteraction = Date.now();
            });

            nuevosMarkers.push(marker);
        });

        todosLosMarkers = nuevosMarkers;
        markersGroup.addLayers(todosLosMarkers);
        mapInstance.addLayer(markersGroup);
        _ajustarBounds();
    }

    function _ajustarBounds() {
        if (todosLosMarkers.length === 0) return;
        const bounds = markersGroup.getBounds();
        if (bounds.isValid()) {
            mapInstance.fitBounds(bounds, {
                padding: window.innerWidth <= 768 ? [30, 30] : [50, 50],
                animate: false
            });
        }
    }

    function filtrarMarcadores(predicado) {
        if (!markersGroup || todosLosMarkers.length === 0) return;
        markersGroup.clearLayers();
        markersGroup.addLayers(predicado ? todosLosMarkers.filter(predicado) : todosLosMarkers);
    }

    // =========================================================
    // CONTROLES DESPUÉS DE DATOS
    // =========================================================
    function agregarControlesDespuesDeDatos() {
        if (!mapInstance || datosGlobales.length === 0) return;
        construirIndiceLocal(datosGlobales);

        const searchInput = document.getElementById('map-search-input');
        if (searchInput) {
            searchInput.disabled     = false;
            searchInput.placeholder  = `Buscar entre ${datosGlobales.length} plazas...`;
        }
        mostrarNotificacion(`✅ ${datosGlobales.length} plazas cargadas`, 'success');
    }

    // =========================================================
    // BOTÓN RESTABLECER MAPA
    // =========================================================
    function agregarResetMapButton() {
        if (document.getElementById('reset-map-button')) return;
        const button = document.createElement('button');
        button.id        = 'reset-map-button';
        button.className = 'leaflet-bar leaflet-control leaflet-control-gps';
        button.innerHTML = '<span class="reset-icon">🔄</span> <span class="reset-text">Restablecer Vista</span>';
        button.style.marginTop = '10px';
        button.type = 'button';

        addTapListener(button, function() {
            if (!mapInstance) return;
            mapInstance.setView([23.6345, -102.5528], 5, { animate: false });
            mapInstance.invalidateSize();
            if (currentPolyline) { mapInstance.removeLayer(currentPolyline); currentPolyline = null; }
            const estadoSelect = document.getElementById('filtro-estado');
            if (estadoSelect) estadoSelect.value = '';
            if (datosGlobales.length > 0) { markersGroup.clearLayers(); procesarPuntosMapa(datosGlobales); }
            mostrarNotificacion('Mapa restablecido a vista inicial');
        });

        setTimeout(() => {
            const topRight = document.querySelector('.leaflet-top.leaflet-right');
            if (topRight) topRight.appendChild(button);
        }, 1000);
    }

    // =========================================================
    // NOTIFICACIONES
    // =========================================================
    function mostrarNotificacion(mensaje, tipo = 'info') {
        const n = document.createElement('div');
        n.className = `notification notification-${tipo}`;
        const icon = tipo === 'error' ? '❌' : tipo === 'warning' ? '⚠️' : '✅';
        n.innerHTML = `
            <div class="notification-icon">${icon}</div>
            <div class="notification-text">${mensaje}</div>
            <button class="notification-close" type="button">×</button>`;
        document.body.appendChild(n);
        addTapListener(n.querySelector('.notification-close'), () => { if (n.parentNode) n.remove(); });
        setTimeout(() => { if (n.parentNode) n.remove(); }, 3000);
    }

    // =========================================================
    // UTILIDADES
    // =========================================================
    function getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2) {
        const R    = 6371;
        const dLat = deg2rad(lat2 - lat1);
        const dLon = deg2rad(lon2 - lon1);
        const a    = Math.sin(dLat/2)**2 + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLon/2)**2;
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    function deg2rad(deg) { return deg * (Math.PI / 180); }

    // =========================================================
    // LIMPIEZA
    // =========================================================
    function limpiarMapa() {
        if (markersGroup) markersGroup.clearLayers();
        if (mapInstance)  { mapInstance.remove(); mapInstance = null; }
        mapInitialized = false;
        datosGlobales  = [];
        userLocation   = null;
        todosLosMarkers = [];
        indiceLocal     = null;
        popupCache.clear();
        if (userLocationMarker) userLocationMarker.remove();
        if (userAccuracyCircle) userAccuracyCircle.remove();
        if (currentPolyline)    currentPolyline.remove();
        if (isFollowing && watchId) { navigator.geolocation.clearWatch(watchId); isFollowing = false; }
        userLocationMarker = userAccuracyCircle = currentPolyline = null;
    }

    // =========================================================
    // API PÚBLICA
    // =========================================================
    window.MapaManager = {
        init:                  initMap,
        limpiar:               limpiarMapa,
        recargar:              gestionarDatosMapa,
        obtenerUbicacion:      obtenerUbicacionUsuario,
        volverAlMapa:          window.volverAlMapa,
        ubicarPlazaMasCercana: window.ubicarPlazaMasCercana,
        activarSeguimiento:    toggleLiveTracking,
        filtrar:               filtrarMarcadores,
        buscar: function(termino) {
            const input = document.getElementById('map-search-input');
            if (input) { input.value = termino; input.dispatchEvent(new Event('input', { bubbles: true })); }
        }
    };

    // =========================================================
    // ARRANQUE
    // =========================================================
    window.navigationContext = { cameFromMap: false, lastMapView: null, lastClickedPlaza: null };
    console.log('✅ Mapa TURBO v3.0 — navegación SPA + panel GPS mejorado');
    observeViewChanges();
});