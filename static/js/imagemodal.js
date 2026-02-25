// ============================================================
// imageModal.js — Modal / lightbox de imágenes
// ============================================================
// Responsabilidades:
//   - Crear el overlay del modal (una sola vez en el DOM)
//   - Abrir/cerrar el modal
//   - Navegar entre imágenes (anterior / siguiente / teclado)
//   - Exponer openModal para que renderizado.js lo llame
// ============================================================

'use strict';

/**
 * Inicializa el modal de imágenes (crea el DOM si no existe).
 * @returns {{ openModal: (images: string[], startIndex?: number) => void }}
 */
export function initImageModal() {
    if (!document.getElementById('image-modal')) {
        _crearDOM();
    }

    const modal        = document.getElementById('image-modal');
    const modalImage   = modal.querySelector('.modal-image');
    const modalClose   = modal.querySelector('.modal-close');
    const modalPrev    = modal.querySelector('.modal-prev');
    const modalNext    = modal.querySelector('.modal-next');
    const modalCurrent = document.getElementById('modal-current');
    const modalTotal   = document.getElementById('modal-total');
    const modalFilename= document.getElementById('modal-filename');

    let currentImages = [];
    let currentIndex  = 0;

    // --- Abrir modal ---
    function openModal(images, startIndex = 0) {
        if (!images?.length) return;
        currentImages = images;
        currentIndex  = startIndex;
        if (modalTotal) modalTotal.textContent = images.length;
        _updateImage();
        modal.classList.add('active');
        document.body.style.overflow = 'hidden';
    }

    // --- Cerrar modal ---
    function closeModal() {
        modal.classList.remove('active');
        document.body.style.overflow = '';
        currentImages = [];
        currentIndex  = 0;
    }

    // --- Actualizar imagen mostrada ---
    function _updateImage() {
        if (!currentImages.length) return;
        const url = currentImages[currentIndex];
        modalImage.src = url;
        if (modalCurrent) modalCurrent.textContent = currentIndex + 1;

        const filename = url.split('/').pop() || 'imagen.jpg';
        if (modalFilename) modalFilename.textContent = decodeURIComponent(filename);

        if (modalPrev) modalPrev.style.display = currentIndex > 0 ? 'flex' : 'none';
        if (modalNext) modalNext.style.display = currentIndex < currentImages.length - 1 ? 'flex' : 'none';
    }

    function nextImage() {
        if (currentIndex < currentImages.length - 1) { currentIndex++; _updateImage(); }
    }

    function prevImage() {
        if (currentIndex > 0) { currentIndex--; _updateImage(); }
    }

    // --- Eventos ---
    if (modalClose) modalClose.addEventListener('click', closeModal);
    if (modalPrev)  modalPrev.addEventListener('click', prevImage);
    if (modalNext)  modalNext.addEventListener('click', nextImage);

    modal.addEventListener('click', (e) => { if (e.target === modal) closeModal(); });

    document.addEventListener('keydown', (e) => {
        if (!modal.classList.contains('active')) return;
        if (e.key === 'Escape')     closeModal();
        if (e.key === 'ArrowLeft')  prevImage();
        if (e.key === 'ArrowRight') nextImage();
    });

    return { openModal };
}

// --- Construcción del DOM del modal ---
function _crearDOM() {
    const overlay = document.createElement('div');
    overlay.id        = 'image-modal';
    overlay.className = 'modal-overlay';

    overlay.innerHTML = `
        <div class="modal-content">
            <div class="modal-counter">
                <span id="modal-current">1</span> / <span id="modal-total">1</span>
            </div>
            <div class="modal-controls">
                <button class="modal-btn modal-close" title="Cerrar (Esc)">×</button>
            </div>
            <button class="modal-nav modal-prev" title="Anterior (←)">‹</button>
            <button class="modal-nav modal-next" title="Siguiente (→)">›</button>
            <img class="modal-image" src="" alt="" />
            <div class="modal-info">
                <div id="modal-filename" class="modal-filename">Imagen</div>
                <div id="modal-source"   class="modal-source">Desde Google Drive</div>
            </div>
        </div>`;

    document.body.appendChild(overlay);
}