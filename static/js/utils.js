// ============================================================
// utils.js — Utilidades compartidas
// ============================================================
// Contiene: cache, fetchData, debounce, showAlert, loader,
//           tema (light/dark), sistema de notificaciones,
//           estilos del sistema (inyectados en <head>)
// ============================================================

'use strict';

// --- CACHE ---
export const cache = new Map();
export const cacheTimers = new Map();
export const CACHE_TTL = 5 * 60 * 1000; // 5 minutos

export const fetchData = async (url) => {
    const now = Date.now();
    const cached = cache.get(url);
    const cachedTime = cacheTimers.get(url);

    if (cached && cachedTime && (now - cachedTime) < CACHE_TTL) {
        return cached;
    }

    const response = await fetch(url);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ error: `Error ${response.status}` }));
        throw new Error(error.error || `Error ${response.status}`);
    }
    const data = await response.json();
    cache.set(url, data);
    cacheTimers.set(url, now);
    return data;
};

// --- DEBOUNCE ---
export const debounce = (func, wait) => {
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

// --- LOADER GLOBAL ---
export function showLoader(message = null, type = 'default') {
    const loader = document.getElementById('global-loader');
    if (!loader) return;
    const loaderMessage = loader.querySelector('.loader-message');
    if (message && loaderMessage) loaderMessage.textContent = message;
    loader.className = 'loader-overlay';
    if (type !== 'default') loader.classList.add(type);
    loader.classList.remove('hidden');
    loader.style.zIndex = '9999';
}

export function hideLoader() {
    const loader = document.getElementById('global-loader');
    if (!loader) return;
    loader.classList.add('hidden');
    setTimeout(() => { loader.className = 'loader-overlay hidden'; }, 500);
}

// Exponer globalmente (compatibilidad con código legacy si existe)
window.showLoader = showLoader;
window.hideLoader = hideLoader;

// --- ALERTAS EN CONTENEDOR DOM ---
export const showAlert = (message, type = 'info') => {
    const alertContainer = document.getElementById('alert-container');
    if (!alertContainer) return;

    const alertDiv = document.createElement('div');
    alertDiv.className =
        type === 'error'   ? 'alert' :
        type === 'success' ? 'alert success' :
        type === 'warning' ? 'alert warning' :
                             'alert info';
    alertDiv.textContent = message;

    alertContainer.innerHTML = '';
    alertContainer.appendChild(alertDiv);
    setTimeout(() => { alertContainer.innerHTML = ''; }, 5000);
};

// --- NOTIFICACIONES FLOTANTES ---
export const mostrarNotificacion = (mensaje, tipo = 'info', duracion = 3000) => {
    document.querySelectorAll('.system-notification').forEach(n => n.remove());

    const iconos = { success: '✅', warning: '⚠️', error: '❌', info: 'ℹ️' };
    const notificacion = document.createElement('div');
    notificacion.className = `system-notification ${tipo}`;
    notificacion.innerHTML = `
        <div class="notification-content">
            <span class="notification-icon">${iconos[tipo] ?? 'ℹ️'}</span>
            <span class="notification-text">${mensaje}</span>
        </div>`;

    document.body.appendChild(notificacion);
    setTimeout(() => notificacion.classList.add('show'), 10);
    setTimeout(() => {
        notificacion.classList.remove('show');
        setTimeout(() => notificacion.remove(), 300);
    }, duracion);
};

// --- TEMA LIGHT / DARK ---
export const applyTheme = (theme, callbacks = {}) => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
    document.body.style.backgroundImage = theme === 'dark'
        ? "url('/static/img/noche.jpg')"
        : "url('/static/img/claro.jpg')";
    document.body.style.backgroundSize = 'cover';

    // Llamar callbacks opcionales para re-renderizar vistas dependientes del tema
    setTimeout(() => {
        if (typeof callbacks.onThemeChange === 'function') callbacks.onThemeChange(theme);
    }, 100);
};

export const initTheme = (callbacks = {}) => {
    const savedTheme = localStorage.getItem('theme');
    applyTheme(savedTheme === 'dark' ? 'dark' : 'light', callbacks);

    const themeLight = document.getElementById('theme-light');
    const themeDark  = document.getElementById('theme-dark');
    if (themeLight) themeLight.addEventListener('click', () => applyTheme('light', callbacks));
    if (themeDark)  themeDark.addEventListener('click',  () => applyTheme('dark',  callbacks));
};

// --- LOADER INLINE DE SELECTS ---
export const setLoaderVisible = (loader, visible) => {
    if (loader) loader.classList.toggle('hidden', !visible);
};

// --- ESTILOS DEL SISTEMA (inyectados una vez) ---
export const agregarEstilosSistema = () => {
    if (document.getElementById('system-styles')) return; // evitar duplicados
    const estilos = document.createElement('style');
    estilos.id = 'system-styles';
    estilos.textContent = `
        /* ===== NOTIFICACIONES FLOTANTES ===== */
        .system-notification {
            position: fixed; top: 20px; right: 20px;
            background: var(--card-bg); color: var(--text-color);
            border: 1px solid var(--border-color); border-radius: 8px;
            padding: 12px 16px; max-width: 300px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15); z-index: 10000;
            transform: translateX(120%); transition: transform 0.3s ease;
        }
        .system-notification.show { transform: translateX(0); }
        .system-notification.success { border-left: 4px solid var(--success-color); background: color-mix(in srgb, var(--success-color) 10%, transparent); }
        .system-notification.warning { border-left: 4px solid var(--warning-color); background: color-mix(in srgb, var(--warning-color) 10%, transparent); }
        .system-notification.error   { border-left: 4px solid var(--error-color);   background: color-mix(in srgb, var(--error-color)   10%, transparent); }
        .system-notification.info    { border-left: 4px solid var(--info-color);    background: color-mix(in srgb, var(--info-color)    10%, transparent); }
        .notification-content { display: flex; align-items: center; gap: 8px; }
        .notification-icon { font-size: 1.2em; }
        .notification-text { font-size: 0.9rem; line-height: 1.4; }

        /* ===== BOTÓN ACTUALIZAR ÍNDICE ===== */
        .indice-update-button {
            position: fixed; bottom: 20px; right: 20px;
            width: 40px; height: 40px; border-radius: 50%;
            background: var(--primary-color); color: white; border: none;
            cursor: pointer; display: flex; align-items: center; justify-content: center;
            font-size: 1.2rem; z-index: 999;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
            transition: all 0.3s ease; opacity: 0.7;
        }
        .indice-update-button:hover { opacity: 1; transform: scale(1.1); box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
        .indice-update-button.updating { animation: spin 1s linear infinite; }
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }

        .indice-status {
            position: fixed; bottom: 70px; right: 20px;
            background: var(--card-bg); border: 1px solid var(--border-color);
            border-radius: 6px; padding: 8px 12px; font-size: 0.8rem;
            opacity: 0; transition: opacity 0.3s ease; pointer-events: none;
            z-index: 998; max-width: 200px; text-align: center;
        }
        .indice-status.show { opacity: 1; }

        /* ===== BADGES CN ===== */
        .cn-badge { padding: 4px 8px; border-radius: 1rem; font-weight: 600; display: inline-block; text-align: center; min-width: 60px; }
        .badge-success  { background: rgba(5,150,105,0.1);  color: var(--secondary-color); }
        .badge-warning  { background: rgba(245,158,11,0.1); color: #f59e0b; }
        .badge-danger   { background: rgba(220,38,38,0.1);  color: #dc2626; }
        .badge-info     { background: rgba(59,130,246,0.1); color: #3b82f6; }

        /* ===== CSS AISLADO PARA TABLAS DE PLAZA ===== */
        .plaza-data-isolated { all: initial; display: block; width: 100%; font-family: var(--font-family, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif); }
        .plaza-data-isolated .custom-plaza-table { width: 100%; background: var(--surface-color, #ffffff); padding: 1.25rem; border-radius: var(--border-radius-lg, 12px); border: 1px solid var(--border-color, #e2e8f0); box-sizing: border-box; margin: 0; }
        .plaza-data-isolated .section-title { font-size: var(--font-size-lg, 1.125rem); font-weight: 700; color: var(--primary-color, #2563eb); margin: 0 0 1.25rem 0; padding: 0 0 0.5rem 0; border-bottom: 1px solid var(--border-color, #e2e8f0); line-height: 1.3; }
        .plaza-data-isolated .section-grid { display: grid; grid-template-columns: repeat(4, minmax(240px, 1fr)); gap: 1rem; margin: 0; padding: 0; }
        .plaza-data-isolated .data-item { background: linear-gradient(180deg, var(--surface-color,#fff), var(--light-color,#f8fafc)); border: 1px solid var(--border-color,#e2e8f0); border-radius: var(--border-radius,8px); padding: 1.25rem 1rem 1rem; box-shadow: var(--shadow,0 1px 3px rgba(0,0,0,0.1)); transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease; position: relative; min-height: 90px; height: auto; display: flex; flex-direction: column; align-items: stretch; box-sizing: border-box; margin: 0; list-style: none; }
        .plaza-data-isolated .data-item:hover { transform: translateY(-2px); border-color: var(--border-hover,#94a3b8); box-shadow: var(--shadow-lg,0 10px 15px -3px rgba(0,0,0,0.1)); }
        .plaza-data-isolated .data-item::before { content: ""; position: absolute; top: 0; left: 0; bottom: 0; width: 4px; background: linear-gradient(180deg, var(--primary-color,#2563eb), var(--secondary-color,#10b981)); border-radius: 8px 0 0 8px; pointer-events: none; }
        .plaza-data-isolated .data-label { font-size: var(--font-size-xs,0.75rem); font-weight: 700; color: var(--text-muted,#64748b); margin: 0 0 0.5rem 0; padding: 0; text-transform: uppercase; letter-spacing: 0.04em; line-height: 1.3; height: auto; white-space: normal; display: block; opacity: 0.9; border: none; background: none; }
        .plaza-data-isolated .data-value { font-size: var(--font-size-base,1rem); font-weight: 600; color: var(--dark-color,#0f172a); line-height: 1.4; margin: 0; padding: 0; width: 100%; word-break: break-word; overflow-wrap: anywhere; hyphens: auto; display: flex; align-items: center; min-height: 2.5rem; box-sizing: border-box; border: none; background: none; }
        .plaza-data-isolated .data-value.highlight { color: var(--secondary-color,#10b981); font-size: var(--font-size-lg,1.125rem); font-weight: 700; }
        .plaza-data-isolated .data-value.coord { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .plaza-data-isolated .data-value.empty { color: var(--text-muted,#64748b); font-style: italic; font-weight: 400; }
        [data-theme="dark"] .plaza-data-isolated .custom-plaza-table { background: var(--surface-color,#1e293b); border-color: var(--border-color,#334155); }
        [data-theme="dark"] .plaza-data-isolated .data-item { background: linear-gradient(180deg, var(--surface-color,#1e293b) 0%, color-mix(in srgb, var(--surface-color,#1e293b) 90%, var(--light-color,#f8fafc) 10%) 100%); border-color: var(--border-color,#334155); }
        [data-theme="dark"] .plaza-data-isolated .data-value { color: var(--dark-color,#f1f5f9); opacity: 0.95; }
        [data-theme="dark"] .plaza-data-isolated .data-value.highlight { color: var(--secondary-color,#34d399); }

        @media (max-width: 1024px) {
            .plaza-data-isolated .section-grid { grid-template-columns: repeat(2,1fr); gap: 0.875rem; }
            .plaza-data-isolated .data-item { min-height: 85px; padding: 1.125rem 0.875rem 0.875rem; }
            .plaza-data-isolated .data-value { min-height: 2.25rem; font-size: 0.95rem; }
            .plaza-data-isolated .data-label { font-size: 0.7rem; }
        }
        @media (max-width: 480px) {
            .plaza-data-isolated .custom-plaza-table { padding: 1rem; }
            .plaza-data-isolated .section-grid { grid-template-columns: 1fr; gap: 0.75rem; }
            .plaza-data-isolated .data-item { min-height: 80px; padding: 1rem 0.75rem 0.75rem; }
            .plaza-data-isolated .data-label { font-size: 0.8rem; margin-bottom: 0.375rem; }
            .plaza-data-isolated .data-value { min-height: 2rem; font-size: 0.95rem; line-height: 1.3; }
        }
        @media (prefers-reduced-motion: reduce) {
            .plaza-data-isolated .data-item { transition: none; }
            .plaza-data-isolated .data-item:hover { transform: none; }
        }
        @media print {
            .plaza-data-isolated .custom-plaza-table { border: 1px solid #000; box-shadow: none; padding: 0.5rem; background: white !important; }
            .plaza-data-isolated .data-item { break-inside: avoid; box-shadow: none; border: 1px solid #ccc; background: white !important; }
            .plaza-data-isolated .data-item:hover { transform: none; }
            .plaza-data-isolated .data-item::before { display: none; }
            .plaza-data-isolated .data-label,
            .plaza-data-isolated .data-value { color: #000 !important; }
        }
    `;
    document.head.appendChild(estilos);
};