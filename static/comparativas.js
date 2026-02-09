class SistemaComparativas {
    constructor() {
        this.añosDisponibles = [];
        this.mesesPorAño = {};
        this.datosComparativa = null;
        this.elementos = {};
        this.inicializado = false;
        this.modoComparacion = 'periodo';
    }

    // Inicializar el sistema
    init() {
        if (this.inicializado) return;
        
        this.obtenerElementosDOM();
        if (!this.verificarVistaComparativas()) return;
        
        this.setupEventListeners();
        this.cargarAñosDisponibles();
        this.configurarModoComparacion();
        this.inicializado = true;
    }

    // Obtener elementos DOM
    obtenerElementosDOM() {
        this.elementos = {
            yearSelect: document.getElementById('comparativa-year'),
            periodo1Select: document.getElementById('comparativa-periodo1'),
            periodo2Select: document.getElementById('comparativa-periodo2'),
            compararBtn: document.getElementById('comparar-periodos-btn'),
            resultadosContainer: document.getElementById('comparativa-resultados')
        };
    }

    // Verificar si la vista de comparativas existe
    verificarVistaComparativas() {
        return !!this.elementos.yearSelect;
    }

    // Configurar event listeners
    setupEventListeners() {
        if (this.elementos.yearSelect) {
            this.elementos.yearSelect.addEventListener('change', () => this.onYearChange());
        }
        if (this.elementos.periodo1Select) {
            this.elementos.periodo1Select.addEventListener('change', () => this.onPeriodo1Change());
        }
        if (this.elementos.periodo2Select) {
            this.elementos.periodo2Select.addEventListener('change', () => this.onPeriodo2Change());
        }
        if (this.elementos.compararBtn) {
            this.elementos.compararBtn.addEventListener('click', () => this.ejecutarComparativa());
        }
    }

    // Configurar modo de comparación
    configurarModoComparacion() {
        const periodoSelector = document.querySelector('.periodo-selector');
        if (!periodoSelector) return;
        
        const modoSelectorHTML = `
            <div class="modo-comparacion-group">
                <label for="modo-comparacion">Tipo de Comparación</label>
                <select id="modo-comparacion" class="modo-comparacion-select">
                    <option value="periodo">Período con Período</option>
                    <option value="anual">Año con Año</option>
                </select>
            </div>
        `;
        
        const titulo = periodoSelector.querySelector('h3');
        if (titulo) {
            titulo.insertAdjacentHTML('afterend', modoSelectorHTML);
        }
        
        const modoSelect = document.getElementById('modo-comparacion');
        if (modoSelect) {
            modoSelect.addEventListener('change', (e) => {
                this.modoComparacion = e.target.value;
                this.onModoComparacionChange();
            });
        }
    }

    // Manejar cambio de modo de comparación
    onModoComparacionChange() {
        if (this.modoComparacion === 'anual') {
            this.configurarComparacionAnual();
        } else {
            this.configurarComparacionPeriodo();
        }
        this.limpiarResultados();
    }

    // Configurar interfaz para comparación anual
    configurarComparacionAnual() {
        const periodo1Select = this.elementos.periodo1Select;
        const periodo2Select = this.elementos.periodo2Select;
        
        if (!periodo1Select || !periodo2Select) return;
        
        this.crearSelectoresAnuales();
        
        const labels = document.querySelectorAll('.selector-group label');
        if (labels.length >= 3) {
            labels[1].textContent = 'Año 1';
            labels[2].textContent = 'Año 2';
        }
        
        this.elementos.yearSelect.disabled = true;
        this.elementos.yearSelect.value = '';
    }

    // Configurar interfaz para comparación por período
    configurarComparacionPeriodo() {
        const periodo1Select = this.elementos.periodo1Select;
        const periodo2Select = this.elementos.periodo2Select;
        
        if (!periodo1Select || !periodo2Select) return;
        
        periodo1Select.style.display = '';
        periodo2Select.style.display = '';
        
        const año1Select = document.getElementById('comparativa-año1');
        const año2Select = document.getElementById('comparativa-año2');
        if (año1Select) año1Select.remove();
        if (año2Select) año2Select.remove();
        
        const labels = document.querySelectorAll('.selector-group label');
        if (labels.length >= 3) {
            labels[1].textContent = 'Período 1';
            labels[2].textContent = 'Período 2';
        }
        
        this.elementos.yearSelect.disabled = false;
        this.resetMonthSelects();
        this.elementos.compararBtn.disabled = true;
    }

    // Crear selectores para comparación anual
    crearSelectoresAnuales() {
        const periodo1Container = this.elementos.periodo1Select.parentElement;
        const periodo2Container = this.elementos.periodo2Select.parentElement;
        
        this.elementos.periodo1Select.style.display = 'none';
        this.elementos.periodo2Select.style.display = 'none';
        
        const año1Existente = document.getElementById('comparativa-año1');
        const año2Existente = document.getElementById('comparativa-año2');
        if (año1Existente) año1Existente.remove();
        if (año2Existente) año2Existente.remove();
        
        const año1Select = document.createElement('select');
        año1Select.id = 'comparativa-año1';
        año1Select.innerHTML = '<option value="">Selecciona año 1</option>';
        
        const año2Select = document.createElement('select');
        año2Select.id = 'comparativa-año2';
        año2Select.innerHTML = '<option value="">Selecciona año 2</option>';
        
        // Llenar con años disponibles
        this.añosDisponibles.forEach(year => {
            const option1 = document.createElement('option');
            option1.value = year;
            option1.textContent = year;
            año1Select.appendChild(option1);
            
            const option2 = document.createElement('option');
            option2.value = year;
            option2.textContent = year;
            año2Select.appendChild(option2);
        });
        
        año1Select.addEventListener('change', () => this.onAño1Change());
        año2Select.addEventListener('change', () => this.onAño2Change());
        
        periodo1Container.appendChild(año1Select);
        periodo2Container.appendChild(año2Select);
        
        this.updateCompararButtonAnual();
    }

    // Manejar cambio de año 1
    onAño1Change() {
        this.updateCompararButtonAnual();
    }

    // Manejar cambio de año 2
    onAño2Change() {
        this.updateCompararButtonAnual();
    }

    // Actualizar botón comparar para modo anual
    updateCompararButtonAnual() {
        const año1Select = document.getElementById('comparativa-año1');
        const año2Select = document.getElementById('comparativa-año2');
        
        if (!año1Select || !año2Select) return;
        
        const año1 = año1Select.value;
        const año2 = año2Select.value;
        const mismosAños = año1 && año2 && año1 === año2;
        
        // Verificar que ambos años tengan 12 meses completos
        const año1Completo = this.tiene12MesesCompletos(this.mesesPorAño[año1] || []);
        const año2Completo = this.tiene12MesesCompletos(this.mesesPorAño[año2] || []);
        const añosCompletos = año1Completo && año2Completo;
        
        this.elementos.compararBtn.disabled = !año1 || !año2 || mismosAños || !añosCompletos;
        
        if (mismosAños) {
            this.elementos.compararBtn.title = 'Selecciona años diferentes';
        } else if (!añosCompletos) {
            this.elementos.compararBtn.title = 'Ambos años deben tener los 12 meses completos';
        } else {
            this.elementos.compararBtn.title = '';
        }
    }

    // Cargar años disponibles
    async cargarAñosDisponibles() {
        try {
            this.mostrarLoader('Cargando años disponibles...');
            
            let response = await fetch('/api/drive-comparativas/periodos');
            
            if (!response.ok) {
                throw new Error(`Error ${response.status} al cargar años`);
            }
            
            const data = await response.json();
            
            if (data.status === 'success') {
                this.añosDisponibles = data.years || [];
                this.mesesPorAño = {};
                
                if (data.meses_por_anio && typeof data.meses_por_anio === 'object') {
                    Object.keys(data.meses_por_anio).forEach(año => {
                        const añoStr = año.toString();
                        const meses = Array.isArray(data.meses_por_anio[año]) 
                            ? data.meses_por_anio[año].sort() 
                            : [];
                        this.mesesPorAño[añoStr] = meses;
                    });
                }
                
                this.añosDisponibles.forEach(año => {
                    const añoStr = año.toString();
                    if (!this.mesesPorAño[añoStr]) {
                        this.mesesPorAño[añoStr] = [];
                    }
                });
                
                this.populateYearSelect();
            } else {
                throw new Error(data.message || 'Error en la respuesta del servidor');
            }
            
        } catch (error) {
            this.mostrarAlerta(`Error al cargar años: ${error.message}`, 'error');
        } finally {
            this.ocultarLoader();
        }
    }

    // Llenar select de años
    populateYearSelect() {
        if (!this.elementos.yearSelect) return;
        
        this.elementos.yearSelect.innerHTML = '<option value="">Selecciona un año</option>';
        
        if (this.añosDisponibles.length === 0) {
            this.elementos.yearSelect.innerHTML = '<option value="">No hay años disponibles</option>';
            return;
        }
        
        this.añosDisponibles.forEach(year => {
            const option = document.createElement('option');
            option.value = year;
            option.textContent = year;
            this.elementos.yearSelect.appendChild(option);
        });
        
        this.elementos.yearSelect.disabled = false;
    }

    // Cuando cambia el año
    onYearChange() {
        const year = this.elementos.yearSelect.value;
        
        this.elementos.periodo1Select.value = '';
        this.elementos.periodo2Select.value = '';
        this.elementos.compararBtn.disabled = true;
        
        if (year) {
            this.populateMonthSelects(year);
        } else {
            this.resetMonthSelects();
        }
    }

    // Cuando cambia período 1
    onPeriodo1Change() {
        this.validarSeleccionPeriodos();
    }

    // Cuando cambia período 2
    onPeriodo2Change() {
        this.validarSeleccionPeriodos();
    }

    // Validar selección de períodos
    validarSeleccionPeriodos() {
        const periodo1 = this.elementos.periodo1Select.value;
        const periodo2 = this.elementos.periodo2Select.value;
        
        const periodosValidos = periodo1 && periodo2;
        const periodosDiferentes = periodo1 !== periodo2;
        
        this.elementos.compararBtn.disabled = !periodosValidos || !periodosDiferentes;
        
        if (periodosValidos && !periodosDiferentes) {
            this.elementos.compararBtn.title = 'Selecciona períodos diferentes';
        } else {
            this.elementos.compararBtn.title = '';
        }
    }

    // Llenar selects de meses 
    populateMonthSelects(year) {
        if (!year) {
            this.resetMonthSelects();
            return;
        }
        
        const yearStr = year.toString();
        const meses = this.mesesPorAño[yearStr] || [];
        
        const populateSelect = (selectElement, defaultValue = '') => {
            selectElement.innerHTML = defaultValue ? 
                `<option value="">${defaultValue}</option>` : 
                '<option value="">Selecciona un mes</option>';
            
            if (meses.length === 0) {
                selectElement.innerHTML = '<option value="">No hay meses disponibles para este año</option>';
                selectElement.disabled = true;
                return;
            }
            
            meses.forEach(mes => {
                const mesFormateado = mes.toString().padStart(2, '0');
                const option = document.createElement('option');
                option.value = mesFormateado;
                option.textContent = this.getMonthName(mesFormateado);
                selectElement.appendChild(option);
            });
            
            selectElement.disabled = false;
        };
        
        populateSelect(this.elementos.periodo1Select, 'Selecciona el primer período');
        populateSelect(this.elementos.periodo2Select, 'Selecciona el segundo período');
        
        this.elementos.periodo1Select.value = '';
        this.elementos.periodo2Select.value = '';
        this.elementos.compararBtn.disabled = true;
    }

    // Resetear selects de meses
    resetMonthSelects() {
        this.elementos.periodo1Select.innerHTML = '<option value="">Selecciona año primero</option>';
        this.elementos.periodo2Select.innerHTML = '<option value="">Selecciona año primero</option>';
        this.elementos.periodo1Select.disabled = true;
        this.elementos.periodo2Select.disabled = true;
    }

    // Obtener nombre del mes
    getMonthName(monthNumber) {
        const meses = {
            '01': 'Enero', '02': 'Febrero', '03': 'Marzo', '04': 'Abril',
            '05': 'Mayo', '06': 'Junio', '07': 'Julio', '08': 'Agosto',
            '09': 'Septiembre', '10': 'Octubre', '11': 'Noviembre', '12': 'Diciembre'
        };
        return meses[monthNumber] || `Mes ${monthNumber}`;
    }

    // Ejecutar comparativa
    async ejecutarComparativa() {
        if (this.modoComparacion === 'anual') {
            await this.ejecutarComparativaAnual();
        } else {
            await this.ejecutarComparativaPeriodo();
        }
    }

    // Función para comparación anual
    async ejecutarComparativaAnual() {
        const año1Select = document.getElementById('comparativa-año1');
        const año2Select = document.getElementById('comparativa-año2');
        
        if (!año1Select || !año2Select) return;
        
        const año1 = año1Select.value;
        const año2 = año2Select.value;
        
        if (!año1 || !año2) return;
        
        try {
            this.mostrarLoader('Comparando años...');
            
            // ✅ VERIFICAR SI AMBOS AÑOS TIENEN LOS 12 MESES COMPLETOS
            const mesesAño1 = this.mesesPorAño[año1] || [];
            const mesesAño2 = this.mesesPorAño[año2] || [];
            
            const año1Completo = this.tiene12MesesCompletos(mesesAño1);
            const año2Completo = this.tiene12MesesCompletos(mesesAño2);
            
            // Si algún año no tiene los 12 meses, mostrar mensaje simple
            if (!año1Completo || !año2Completo) {
                let mensajeError = 'No se puede realizar la comparación anual. ';
                
                if (!año1Completo && !año2Completo) {
                    mensajeError += `Los años ${año1} y ${año2} no tienen los 12 meses completos.`;
                } else if (!año1Completo) {
                    mensajeError += `El año ${año1} no tiene los 12 meses completos.`;
                } else {
                    mensajeError += `El año ${año2} no tiene los 12 meses completos.`;
                }
                
                throw new Error(mensajeError);
            }
            
            // Usar diciembre (12) como período acumulado para ambos años
            const comparativaData = await this.fetchComparativaAvanzada(año1, '12', año2, '12');
            
            this.datosComparativa = {
                año1,
                año2,
                periodo1: '12',
                periodo2: '12',
                data: comparativaData,
                modo: 'anual'
            };
            
            this.renderResultadosComparativa();
            
        } catch (error) {
            this.mostrarAlerta(`❌ ${error.message}`, 'error');
        } finally {
            this.ocultarLoader();
        }
    }

    // ✅ FUNCIÓN: Verificar si tiene los 12 meses completos
    tiene12MesesCompletos(mesesDisponibles) {
        if (!mesesDisponibles || mesesDisponibles.length < 12) {
            return false;
        }
        
        const mesesNum = mesesDisponibles.map(mes => parseInt(mes));
        for (let mes = 1; mes <= 12; mes++) {
            if (!mesesNum.includes(mes)) {
                return false;
            }
        }
        
        return true;
    }

    // Función para comparación por período
    async ejecutarComparativaPeriodo() {
        const year = this.elementos.yearSelect.value;
        const periodo1 = this.elementos.periodo1Select.value;
        const periodo2 = this.elementos.periodo2Select.value;
        
        if (!year || !periodo1 || !periodo2) return;
        
        try {
            this.mostrarLoader('Comparando períodos...');
            
            // ✅ VERIFICAR DISPONIBILIDAD DE MESES
            const mesesDisponibles = this.mesesPorAño[year] || [];
            const periodo1Num = parseInt(periodo1);
            const periodo2Num = parseInt(periodo2);
            
            const periodo1Existe = mesesDisponibles.includes(periodo1Num) || mesesDisponibles.includes(periodo1);
            const periodo2Existe = mesesDisponibles.includes(periodo2Num) || mesesDisponibles.includes(periodo2);
            
            if (!periodo1Existe || !periodo2Existe) {
                let mensajeError = 'Meses no disponibles: ';
                
                if (!periodo1Existe && !periodo2Existe) {
                    mensajeError += `${this.getMonthName(periodo1)} y ${this.getMonthName(periodo2)} no están disponibles`;
                } else if (!periodo1Existe) {
                    mensajeError += `${this.getMonthName(periodo1)} no está disponible`;
                } else {
                    mensajeError += `${this.getMonthName(periodo2)} no está disponible`;
                }
                
                throw new Error(mensajeError);
            }
            
            const comparativaData = await this.fetchComparativaAvanzada(year, periodo1, year, periodo2);
            
            this.datosComparativa = {
                year,
                periodo1,
                periodo2,
                data: comparativaData,
                modo: 'periodo'
            };
            
            this.renderResultadosComparativa();
            
        } catch (error) {
            this.mostrarAlerta(`❌ ${error.message}`, 'error');
        } finally {
            this.ocultarLoader();
        }
    }

    // Fetch para comparación avanzada
    async fetchComparativaAvanzada(year1, periodo1, year2, periodo2) {
        try {
            if (!year1 || !periodo1 || !year2 || !periodo2) {
                throw new Error('Parámetros incompletos para la comparativa');
            }

            let response = await fetch(
                `/api/drive-comparativas/comparar-avanzado?year1=${year1}&periodo1=${periodo1}&year2=${year2}&periodo2=${periodo2}`
            );

            if (!response.ok) {
                const errorText = await response.text();
                try {
                    const errorData = JSON.parse(errorText);
                    throw new Error(errorData.message || `Error ${response.status}: ${response.statusText}`);
                } catch {
                    throw new Error(`Error ${response.status}: ${response.statusText}`);
                }
            }

            const data = await response.json();
            
            if (data.status === 'error') {
                throw new Error(data.message || 'Error en la comparativa');
            }
            
            return data;
            
        } catch (error) {
            throw error;
        }
    }

    // Renderizar resultados de comparativa
    renderResultadosComparativa() {
        if (!this.datosComparativa || !this.elementos.resultadosContainer) return;
        
        const { modo } = this.datosComparativa;
        
        if (modo === 'anual') {
            this.renderResultadosComparativaAnual();
        } else {
            this.renderResultadosComparativaPeriodo();
        }
    }

    // Método para renderizar resultados anuales
    renderResultadosComparativaAnual() {
        const { año1, año2, data } = this.datosComparativa;
        
        if (data.status !== 'success') {
            this.mostrarAlerta(data.error || 'Error en la comparativa', 'error');
            return;
        }

        const metricasPrincipales = data.metricas_principales || {};
        const comparacion = data.comparacion || {};
        const analisisPlazas = comparacion.analisis_plazas || {};
        const metricasGlobales = comparacion.metricas_globales || {};
        const analisisPorEstado = comparacion.analisis_por_estado || {};

        const template = document.getElementById('comparativa-resultados-template');
        if (!template) return;

        const clone = template.content.cloneNode(true);
        
        this.actualizarTemplateComparativaAnual(clone, año1, año2, {
            metricasPrincipales,
            analisisPlazas,
            metricasGlobales,
            analisisPorEstado
        });

        this.elementos.resultadosContainer.innerHTML = '';
        this.elementos.resultadosContainer.appendChild(clone);
        this.elementos.resultadosContainer.classList.remove('hidden');

        this.generarTablaEstados(analisisPorEstado);
        this.actualizarAnalisisAvanzado(comparacion, analisisPorEstado);
    }

    // Actualizar template para comparación anual
    actualizarTemplateComparativaAnual(clone, año1, año2, datos) {
        const { metricasPrincipales, analisisPlazas, metricasGlobales, analisisPorEstado } = datos;
        
        this.actualizarResumenGeneralAnual(clone, analisisPlazas, año1, año2);
        this.actualizarTablaComparativaAnual(clone, metricasGlobales);
        
        const periodo1Elements = clone.querySelectorAll('[data-bind="periodo1_nombre"]');
        const periodo2Elements = clone.querySelectorAll('[data-bind="periodo2_nombre"]');
        
        periodo1Elements.forEach(el => {
            el.textContent = `Año ${año1} (Acumulado)`;
        });
        
        periodo2Elements.forEach(el => {
            el.textContent = `Año ${año2} (Acumulado)`;
        });
        
        this.actualizarMetricasPrincipales(clone, metricasPrincipales);
    }

    // Actualizar métricas principales
    actualizarMetricasPrincipales(clone, metricasPrincipales) {
        const plazasNuevasElement = clone.querySelector('[data-bind="plazas_nuevas"]');
        const plazasEliminadasElement = clone.querySelector('[data-bind="plazas_eliminadas"]');
        const incrementoCNElement = clone.querySelector('[data-bind="incremento_cn_total"]');
        const resumenElement = clone.querySelector('[data-bind="resumen_cambios"]');
        
        if (plazasNuevasElement) plazasNuevasElement.textContent = this.formatNumber(metricasPrincipales.plazas_nuevas || 0);
        if (plazasEliminadasElement) plazasEliminadasElement.textContent = this.formatNumber(metricasPrincipales.plazas_eliminadas || 0);
        if (incrementoCNElement) incrementoCNElement.textContent = this.formatNumber(metricasPrincipales.incremento_cn_total || 0);
        if (resumenElement) resumenElement.textContent = metricasPrincipales.resumen_cambios || 'Sin cambios significativos';
    }

    // Actualizar resumen general para modo anual
    actualizarResumenGeneralAnual(clone, analisisPlazas, año1, año2) {
        const totalPeriodo1 = analisisPlazas.total_plazas_periodo1 || 0;
        const totalPeriodo2 = analisisPlazas.total_plazas_periodo2 || 0;
        const diferencia = totalPeriodo2 - totalPeriodo1;
        const tendencia = diferencia > 0 ? '📈 Aumento' : diferencia < 0 ? '📉 Disminución' : '➡️ Sin cambio';

        const totalPeriodo1Element = clone.querySelector('[data-bind="total_periodo1"]');
        const totalPeriodo2Element = clone.querySelector('[data-bind="total_periodo2"]');
        const diferenciaElement = clone.querySelector('[data-bind="diferencia_total"]');
        const tendenciaElement = clone.querySelector('[data-bind="tendencia_diferencia"]');

        if (totalPeriodo1Element) totalPeriodo1Element.textContent = this.formatNumber(totalPeriodo1);
        if (totalPeriodo2Element) totalPeriodo2Element.textContent = this.formatNumber(totalPeriodo2);
        if (diferenciaElement) diferenciaElement.textContent = `${diferencia > 0 ? '+' : ''}${this.formatNumber(diferencia)}`;
        if (tendenciaElement) tendenciaElement.textContent = tendencia;
    }

    // Actualizar tabla comparativa para modo anual
    actualizarTablaComparativaAnual(clone, metricasGlobales) {
        const tbody = clone.getElementById('comparativa-tbody');
        if (!tbody) return;

        tbody.innerHTML = '';

        if (!metricasGlobales || Object.keys(metricasGlobales).length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 2rem;">No hay datos disponibles para mostrar</td></tr>';
            return;
        }

        const metricasOrden = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum'];
        
        metricasOrden.forEach(metrica => {
            if (metricasGlobales[metrica]) {
                const datos = metricasGlobales[metrica];
                const diferencia = datos.incremento || 0;
                const porcentajeCambio = datos.porcentaje_cambio || 0;
                
                const tendencia = diferencia > 0 ? '📈' : diferencia < 0 ? '📉' : '➡️';
                const claseTendencia = diferencia > 0 ? 'positivo' : diferencia < 0 ? 'negativo' : '';
                
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td><strong>${this.getNombreMetrica(metrica)}</strong></td>
                    <td>${this.formatNumber(datos.periodo1 || 0)}</td>
                    <td>${this.formatNumber(datos.periodo2 || 0)}</td>
                    <td class="${claseTendencia}">${diferencia > 0 ? '+' : ''}${this.formatNumber(diferencia)}</td>
                    <td class="${claseTendencia}">${tendencia}</td>
                    <td class="${claseTendencia}">${porcentajeCambio > 0 ? '+' : ''}${this.formatPorcentaje(porcentajeCambio)}</td>
                `;
                tbody.appendChild(row);
            }
        });
    }

    // Método para renderizar resultados por período
    renderResultadosComparativaPeriodo() {
        const { year, periodo1, periodo2, data } = this.datosComparativa;
        
        if (data.status !== 'success') {
            this.mostrarAlerta(data.error || 'Error en la comparativa', 'error');
            return;
        }

        const metricasPrincipales = data.metricas_principales || {};
        const comparacion = data.comparacion || {};
        const analisisPlazas = comparacion.analisis_plazas || {};
        const metricasGlobales = comparacion.metricas_globales || {};
        const analisisPorEstado = comparacion.analisis_por_estado || {};

        const template = document.getElementById('comparativa-resultados-template');
        if (!template) return;

        const clone = template.content.cloneNode(true);
        
        this.actualizarTemplateComparativa(clone, year, periodo1, periodo2, {
            metricasPrincipales,
            analisisPlazas,
            metricasGlobales,
            analisisPorEstado
        });

        this.elementos.resultadosContainer.innerHTML = '';
        this.elementos.resultadosContainer.appendChild(clone);
        this.elementos.resultadosContainer.classList.remove('hidden');

        this.generarTablaEstados(analisisPorEstado);
        this.actualizarAnalisisAvanzado(comparacion, analisisPorEstado);
    }

    // Actualizar template con datos reales para modo período
    actualizarTemplateComparativa(clone, year, periodo1, periodo2, datos) {
        const { metricasPrincipales, analisisPlazas, metricasGlobales, analisisPorEstado } = datos;
        
        this.actualizarResumenGeneral(clone, analisisPlazas, periodo1, periodo2);
        this.actualizarTablaComparativa(clone, metricasGlobales);
        
        const periodo1Elements = clone.querySelectorAll('[data-bind="periodo1_nombre"]');
        const periodo2Elements = clone.querySelectorAll('[data-bind="periodo2_nombre"]');
        
        periodo1Elements.forEach(el => {
            el.textContent = `${this.getMonthName(periodo1)} ${year}`;
        });
        
        periodo2Elements.forEach(el => {
            el.textContent = `${this.getMonthName(periodo2)} ${year}`;
        });
        
        this.actualizarMetricasPrincipales(clone, metricasPrincipales);
    }

    // Actualizar resumen general en cards para modo período
    actualizarResumenGeneral(clone, analisisPlazas, periodo1, periodo2) {
        const totalPeriodo1 = analisisPlazas.total_plazas_periodo1 || 0;
        const totalPeriodo2 = analisisPlazas.total_plazas_periodo2 || 0;
        const diferencia = totalPeriodo2 - totalPeriodo1;
        const tendencia = diferencia > 0 ? '📈 Aumento' : diferencia < 0 ? '📉 Disminución' : '➡️ Sin cambio';

        const totalPeriodo1Element = clone.querySelector('[data-bind="total_periodo1"]');
        const totalPeriodo2Element = clone.querySelector('[data-bind="total_periodo2"]');
        const diferenciaElement = clone.querySelector('[data-bind="diferencia_total"]');
        const tendenciaElement = clone.querySelector('[data-bind="tendencia_diferencia"]');

        if (totalPeriodo1Element) totalPeriodo1Element.textContent = this.formatNumber(totalPeriodo1);
        if (totalPeriodo2Element) totalPeriodo2Element.textContent = this.formatNumber(totalPeriodo2);
        if (diferenciaElement) diferenciaElement.textContent = `${diferencia > 0 ? '+' : ''}${this.formatNumber(diferencia)}`;
        if (tendenciaElement) tendenciaElement.textContent = tendencia;
    }

    // Actualizar tabla comparativa con datos reales para modo período
    actualizarTablaComparativa(clone, metricasGlobales) {
        const tbody = clone.getElementById('comparativa-tbody');
        if (!tbody) return;

        tbody.innerHTML = '';

        if (!metricasGlobales || Object.keys(metricasGlobales).length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 2rem;">No hay datos disponibles para mostrar</td></tr>';
            return;
        }

        const metricasOrden = ['CN_Inicial_Acum', 'CN_Prim_Acum', 'CN_Sec_Acum', 'CN_Tot_Acum'];
        
        metricasOrden.forEach(metrica => {
            if (metricasGlobales[metrica]) {
                const datos = metricasGlobales[metrica];
                const diferencia = datos.incremento || 0;
                const porcentajeCambio = datos.porcentaje_cambio || 0;
                
                const tendencia = diferencia > 0 ? '📈' : diferencia < 0 ? '📉' : '➡️';
                const claseTendencia = diferencia > 0 ? 'positivo' : diferencia < 0 ? 'negativo' : '';
                
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td><strong>${this.getNombreMetrica(metrica)}</strong></td>
                    <td>${this.formatNumber(datos.periodo1 || 0)}</td>
                    <td>${this.formatNumber(datos.periodo2 || 0)}</td>
                    <td class="${claseTendencia}">${diferencia > 0 ? '+' : ''}${this.formatNumber(diferencia)}</td>
                    <td class="${claseTendencia}">${tendencia}</td>
                    <td class="${claseTendencia}">${porcentajeCambio > 0 ? '+' : ''}${this.formatPorcentaje(porcentajeCambio)}</td>
                `;
                tbody.appendChild(row);
            }
        });
    }

    // Generar tabla de estados
    generarTablaEstados(analisisPorEstado) {
        const tbody = document.getElementById('estados-comparativa-tbody');
        if (!tbody) return;

        tbody.innerHTML = "";

        if (!analisisPorEstado || Object.keys(analisisPorEstado).length === 0) {
            this.mostrarMensajeSinDatosEstados();
            return;
        }

        const estados = Object.keys(analisisPorEstado).sort();
        const fragment = document.createDocumentFragment();

        for (const estado of estados) {
            const datos = analisisPorEstado[estado];
            const metricas = datos.metricas || {};

            const tr = this.crearFilaEstado(estado, datos, metricas);
            if (tr) {
                fragment.appendChild(tr);
            }
        }

        tbody.appendChild(fragment);

        const totalElement = document.getElementById('estados-total');
        if (totalElement) {
            totalElement.textContent = `${estados.length} estados`;
        }

        this.configurarBusquedaEstados();
        this.configurarModalEstados();
    }

    // Crear fila de estado
    crearFilaEstado(estado, datos, metricas) {
        try {
            const plazasP1 = datos.total_plazas_periodo1 || 0;
            const plazasP2 = datos.total_plazas_periodo2 || 0;

            const getM = (key) => {
                const m = metricas[key] || {};
                return {
                    p1: m.periodo1 || 0,
                    p2: m.periodo2 || 0,
                    cambio: m.cambio || 0,
                    porc: m.porcentaje_cambio || 0
                };
            };

            const mIni = getM('CN_Inicial_Acum');
            const mPri = getM('CN_Prim_Acum');
            const mSec = getM('CN_Sec_Acum');
            const mTot = getM('CN_Tot_Acum');

            const tr = document.createElement('tr');
            tr.className = 'estado-row';
            tr.dataset.estado = estado;
            
            tr.innerHTML = `
                <td><strong>${estado}</strong></td>
                <td>${this.formatNumber(plazasP1)}</td>
                <td>${this.formatNumber(plazasP2)}</td>
                <td class="${this.getClassCambio(plazasP2 - plazasP1)}">${this.formatCambio(plazasP2 - plazasP1)}</td>
                <td>${this.formatNumber(mIni.p1)}</td>
                <td>${this.formatNumber(mIni.p2)}</td>
                <td class="${this.getClassCambio(mIni.porc)}">${this.formatPorcentaje(mIni.porc)}</td>
                <td>${this.formatNumber(mPri.p1)}</td>
                <td>${this.formatNumber(mPri.p2)}</td>
                <td class="${this.getClassCambio(mPri.porc)}">${this.formatPorcentaje(mPri.porc)}</td>
                <td>${this.formatNumber(mSec.p1)}</td>
                <td>${this.formatNumber(mSec.p2)}</td>
                <td class="${this.getClassCambio(mSec.porc)}">${this.formatPorcentaje(mSec.porc)}</td>
                <td>${this.formatNumber(mTot.p1)}</td>
                <td>${this.formatNumber(mTot.p2)}</td>
                <td class="${this.getClassCambio(mTot.porc)}">${this.formatPorcentaje(mTot.porc)}</td>
            `;

            tr.addEventListener('click', () => {
                this.mostrarModalDetalleEstado(estado, datos);
            });

            return tr;

        } catch (e) {
            return null;
        }
    }

    // Configurar búsqueda de estados
    configurarBusquedaEstados() {
        const searchInput = document.getElementById('estados-search');
        if (!searchInput) return;
        
        searchInput.addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase();
            const rows = document.querySelectorAll('.estado-row');
            
            rows.forEach(row => {
                const estado = row.dataset.estado.toLowerCase();
                if (estado.includes(searchTerm)) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            });
        });
    }

    // Mostrar modal de detalle de estado
    mostrarModalDetalleEstado(estado, datosEstado) {
        const modal = document.getElementById("modal-detalle-estado");
        const titulo = document.getElementById("modal-estado-titulo");
        const contenido = document.getElementById("modal-estado-contenido");

        if (!modal || !titulo || !contenido) return;

        titulo.textContent = "📊 Comparativa Detallada - " + estado;
        contenido.innerHTML = this.generarContenidoModal(estado, datosEstado);

        modal.classList.remove("hidden");
    }

    // Generar contenido del modal
    generarContenidoModal(estado, datosEstado) {
        const metricas = datosEstado.metricas || {};
        const plazasP1 = datosEstado.total_plazas_periodo1 || 0;
        const plazasP2 = datosEstado.total_plazas_periodo2 || 0;
        const cambioPlazas = plazasP2 - plazasP1;
        const porc = plazasP1 ? (cambioPlazas / plazasP1) * 100 : 0;

        let metricasHTML = '';
        
        const metricasList = [
            { key: 'CN_Inicial_Acum', nombre: 'CN Inicial Acumulado', icono: '👶' },
            { key: 'CN_Prim_Acum', nombre: 'CN Primaria Acumulada', icono: '🎒' },
            { key: 'CN_Sec_Acum', nombre: 'CN Secundaria Acumulada', icono: '📚' },
            { key: 'CN_Tot_Acum', nombre: 'CN Total Acumulado', icono: '📊' }
        ];

        metricasList.forEach(metrica => {
            const data = metricas[metrica.key] || {};
            if (data.periodo1 !== undefined || data.periodo2 !== undefined) {
                metricasHTML += `
                    <div class="metrica-item">
                        <div class="metrica-header">
                            <span class="metrica-icon">${metrica.icono}</span>
                            <span class="metrica-name">${metrica.nombre}</span>
                        </div>
                        <div class="metrica-values">
                            <div class="value-periodo1">${this.formatNumber(data.periodo1 || 0)}</div>
                            <div class="value-periodo2">${this.formatNumber(data.periodo2 || 0)}</div>
                            <div class="value-cambio ${this.getClassCambio(data.cambio || 0)}">
                                ${(data.cambio > 0 ? '+' : '') + this.formatNumber(data.cambio || 0)}
                            </div>
                            <div class="value-porcentaje ${this.getClassCambio(data.porcentaje_cambio || 0)}">
                                ${this.formatPorcentaje(data.porcentaje_cambio || 0)}
                            </div>
                        </div>
                    </div>
                `;
            }
        });

        return `
            <div class="estado-resumen">
                <h4>Resumen de Plazas</h4>
                <div class="plazas-comparison">
                    <div class="plaza-value">
                        <span class="label">Período 1:</span>
                        <span class="value">${this.formatNumber(plazasP1)}</span>
                    </div>
                    <div class="plaza-value">
                        <span class="label">Período 2:</span>
                        <span class="value">${this.formatNumber(plazasP2)}</span>
                    </div>
                    <div class="plaza-value ${this.getClassCambio(cambioPlazas)}">
                        <span class="label">Cambio:</span>
                        <span class="value">${cambioPlazas > 0 ? '+' : ''}${this.formatNumber(cambioPlazas)}</span>
                    </div>
                    <div class="plaza-value ${this.getClassCambio(porc)}">
                        <span class="label">% Cambio:</span>
                        <span class="value">${this.formatPorcentaje(porc)}</span>
                    </div>
                </div>
            </div>
            <div class="estado-metricas">
                <h4>Métricas Detalladas</h4>
                ${metricasHTML || '<p>No hay métricas disponibles para este estado.</p>'}
            </div>
        `;
    }

    // Mostrar mensaje sin datos de estados
    mostrarMensajeSinDatosEstados() {
        const tbody = document.getElementById("estados-comparativa-tbody");
        if (!tbody) return;
        
        tbody.innerHTML = `
            <tr>
                <td colspan="16" style="text-align: center; padding: 2rem; color: #666;">
                    No hay datos de estados disponibles para esta comparativa
                </td>
            </tr>
        `;
    }

    // ANÁLISIS ESTADÍSTICO AVANZADO
    actualizarAnalisisAvanzado(comparacion, analisisPorEstado) {
        const metricasAvanzadas = this.calcularMetricasAvanzadas(comparacion, analisisPorEstado);
        this.actualizarMetricasAvanzadas(metricasAvanzadas);
        this.actualizarRankingEstados(analisisPorEstado, metricasAvanzadas);
        this.actualizarTendencias(analisisPorEstado);
        this.actualizarResumenEjecutivo(metricasAvanzadas, analisisPorEstado);
    }

    // Calcular métricas avanzadas
    calcularMetricasAvanzadas(comparacion, analisisPorEstado) {
        const metricasGlobales = comparacion.metricas_globales || {};
        const analisisPlazas = comparacion.analisis_plazas || {};
        
        const plazasOperacionP2 = analisisPlazas.plazas_operacion_periodo2 || 
                                 analisisPlazas.total_plazas_periodo2 || 1;
        
        const cnTotal = metricasGlobales.CN_Tot_Acum || {};
        const tasaCrecimiento = cnTotal.porcentaje_cambio || 0;
        
        const cnTotalP2 = cnTotal.periodo2 || 0;
        const eficienciaOperativa = plazasOperacionP2 > 0 ? (cnTotalP2 / plazasOperacionP2) : 0;
        
        const estadoMasDinamico = this.calcularEstadoMasDinamico(analisisPorEstado);
        const distribucionCN = this.calcularDistribucionCN(metricasGlobales);
        
        return {
            tasaCrecimiento,
            eficienciaOperativa,
            estadoMasDinamico,
            distribucionCN,
            plazasOperacionP2
        };
    }

    // Calcular estado más dinámico
    calcularEstadoMasDinamico(analisisPorEstado) {
        let estadoMax = { nombre: 'N/A', cambio: 0 };
        
        Object.entries(analisisPorEstado).forEach(([estado, datos]) => {
            const metricas = datos.metricas || {};
            const cnTotal = metricas.CN_Tot_Acum || {};
            const cambioAbsoluto = Math.abs(cnTotal.cambio || 0);
            
            if (cambioAbsoluto > estadoMax.cambio) {
                estadoMax = { nombre: estado, cambio: cambioAbsoluto };
            }
        });
        
        return estadoMax;
    }

    // Calcular distribución CN
    calcularDistribucionCN(metricasGlobales) {
        const inicial = metricasGlobales.CN_Inicial_Acum || {};
        const primaria = metricasGlobales.CN_Prim_Acum || {};
        const secundaria = metricasGlobales.CN_Sec_Acum || {};
        
        const totalP2 = (inicial.periodo2 || 0) + (primaria.periodo2 || 0) + (secundaria.periodo2 || 0);
        
        if (totalP2 === 0) return { inicial: 33, primaria: 33, secundaria: 34 };
        
        return {
            inicial: ((inicial.periodo2 || 0) / totalP2) * 100,
            primaria: ((primaria.periodo2 || 0) / totalP2) * 100,
            secundaria: ((secundaria.periodo2 || 0) / totalP2) * 100
        };
    }

    calcularDistribucionCNPeriodo1(metricasGlobales) {
        const inicial = metricasGlobales.CN_Inicial_Acum || {};
        const primaria = metricasGlobales.CN_Prim_Acum || {};
        const secundaria = metricasGlobales.CN_Sec_Acum || {};

        const totalP1 = (inicial.periodo1 || 0) + (primaria.periodo1 || 0) + (secundaria.periodo1 || 0);

        if (totalP1 === 0) {
            return { inicial: 33, primaria: 33, secundaria: 34 };
        }

        return {
            inicial: ((inicial.periodo1 || 0) / totalP1) * 100,
            primaria: ((primaria.periodo1 || 0) / totalP1) * 100,
            secundaria: ((secundaria.periodo1 || 0) / totalP1) * 100
        };
    }

    // Actualizar métricas avanzadas en las tarjetas
    actualizarMetricasAvanzadas(metricasAvanzadas) {
        const tasaCrecimientoElement = document.querySelector('[data-bind="tasa_crecimiento"]');
        if (tasaCrecimientoElement) {
            tasaCrecimientoElement.textContent = this.formatPorcentaje(metricasAvanzadas.tasaCrecimiento);
            tasaCrecimientoElement.className = `metrica-valor ${this.getClassCambio(metricasAvanzadas.tasaCrecimiento)}`;
        }
        
        const detalleCrecimientoElement = document.querySelector('[data-bind="detalle_crecimiento"]');
        if (detalleCrecimientoElement) {
            detalleCrecimientoElement.textContent = 
                metricasAvanzadas.tasaCrecimiento > 0 ? 'Crecimiento positivo' :
                metricasAvanzadas.tasaCrecimiento < 0 ? 'Crecimiento negativo' : 'Sin cambios';
        }
        
        const eficienciaElement = document.querySelector('[data-bind="eficiencia_operativa"]');
        if (eficienciaElement) {
            eficienciaElement.textContent = Math.round(metricasAvanzadas.eficienciaOperativa).toLocaleString();
        }
        
        const detalleEficienciaElement = document.querySelector('[data-bind="detalle_eficiencia"]');
        if (detalleEficienciaElement) {
            detalleEficienciaElement.textContent = 
                `CN Total / Plazas en Operación = ${Math.round(metricasAvanzadas.eficienciaOperativa)}`;
        }
        
        const estadoDinamicoElement = document.querySelector('[data-bind="estado_mas_dinamico"]');
        if (estadoDinamicoElement) {
            estadoDinamicoElement.textContent = metricasAvanzadas.estadoMasDinamico.nombre;
        }
        
        const detalleDinamicoElement = document.querySelector('[data-bind="detalle_dinamico"]');
        if (detalleDinamicoElement) {
            detalleDinamicoElement.textContent = 
                `Cambio absoluto: ${this.formatNumber(metricasAvanzadas.estadoMasDinamico.cambio)}`;
        }
        
        this.renderizarDistribucion(metricasAvanzadas);
    }

    // Actualizar gráfica de distribución CN
    actualizarDistribucionCN(distribucionP1, distribucionP2) {
        const container = document.querySelector('[data-bind="distribucion_cn_comparativa"]');
        if (!container) return;

        container.innerHTML = `
            <div class="grid-distribucion">
                <div class="distribucion-card">
                    <h4 class="dist-title">Período 1</h4>
                    <div class="distribucion-bar">
                        <div class="distribucion-segmento inicial" style="width:${distribucionP1.inicial}%">
                            <span>Inicial ${distribucionP1.inicial.toFixed(1)}%</span>
                        </div>
                        <div class="distribucion-segmento primaria" style="width:${distribucionP1.primaria}%">
                            <span>Primaria ${distribucionP1.primaria.toFixed(1)}%</span>
                        </div>
                        <div class="distribucion-segmento secundaria" style="width:${distribucionP1.secundaria}%">
                            <span>Secundaria ${distribucionP1.secundaria.toFixed(1)}%</span>
                        </div>
                    </div>
                </div>

                <div class="distribucion-card">
                    <h4 class="dist-title">Período 2</h4>
                    <div class="distribucion-bar">
                        <div class="distribucion-segmento inicial" style="width:${distribucionP2.inicial}%">
                            <span>Inicial ${distribucionP2.inicial.toFixed(1)}%</span>
                        </div>
                        <div class="distribucion-segmento primaria" style="width:${distribucionP2.primaria}%">
                            <span>Primaria ${distribucionP2.primaria.toFixed(1)}%</span>
                        </div>
                        <div class="distribucion-segmento secundaria" style="width:${distribucionP2.secundaria}%">
                            <span>Secundaria ${distribucionP2.secundaria.toFixed(1)}%</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }


    renderizarDistribucion(metricasAvanzadas) {
        if (!this.datosComparativa || !metricasAvanzadas) return;
        
        // 1. Obtener los datos con seguridad (evita el error de undefined)
        const comparacion = this.datosComparativa.data.comparacion || {};
        const metricasGlobales = comparacion.metricas_globales || {}; 
        
        // 2. Ahora sí es seguro llamar a la función, porque metricasGlobales nunca será undefined
        const distribucionP1 = this.calcularDistribucionCNPeriodo1(metricasGlobales);
        const distribucionP2 = metricasAvanzadas.distribucionCN;
        
        this.actualizarDistribucionCN(distribucionP1, distribucionP2); 
    }

    // Actualizar ranking de estados
    actualizarRankingEstados(analisisPorEstado, metricasAvanzadas) {
        const tbody = document.getElementById('ranking-estados-tbody');
        const totalElement = document.getElementById('ranking-total');
        
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        if (!analisisPorEstado || Object.keys(analisisPorEstado).length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" style="text-align: center; padding: 1rem; color: #666;">
                        No hay datos suficientes para el ranking
                    </td>
                </tr>
            `;
            if (totalElement) totalElement.textContent = '0 estados evaluados';
            return;
        }
        
        const estadosConPuntaje = this.calcularPuntajesEstados(analisisPorEstado);
        estadosConPuntaje.sort((a, b) => b.porcentajeCrecimiento - a.porcentajeCrecimiento);
        
        estadosConPuntaje.forEach((estadoData, index) => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${index + 1}</td>
                <td><strong>${estadoData.estado}</strong></td>
                <td>
                    <span class="puntaje-badge ${this.getClassPuntaje(estadoData.puntaje)}">
                        ${estadoData.puntaje.toFixed(1)}
                    </span>
                </td>
                <td class="${this.getClassCambio(estadoData.cambioCN)}">
                    ${estadoData.cambioCN > 0 ? '+' : ''}${this.formatNumber(estadoData.cambioCN)}
                </td>
                <td class="${this.getClassCambio(estadoData.porcentajeCrecimiento)}">
                    ${this.formatPorcentaje(estadoData.porcentajeCrecimiento)}
                </td>
                <td>${this.formatNumber(estadoData.plazasOperacion)}</td>
                <td>${Math.round(estadoData.eficiencia).toLocaleString()}</td>
            `;
            tbody.appendChild(row);
        });
        
        if (totalElement) {
            totalElement.textContent = `${estadosConPuntaje.length} estados evaluados`;
        }
    }

    // Calcular puntajes compuestos para estados
    calcularPuntajesEstados(analisisPorEstado) {
        return Object.entries(analisisPorEstado).map(([estado, datos]) => {
            const metricas = datos.metricas || {};
            const cnTotal = metricas.CN_Tot_Acum || {};
            
            const cambioCN = cnTotal.cambio || 0;
            const porcentajeCrecimiento = cnTotal.porcentaje_cambio || 0;
            
            const plazasOperacion = datos.plazas_operacion_periodo2 || datos.total_plazas_periodo2 || 1;
            const cnTotalP2 = cnTotal.periodo2 || 0;
            const eficiencia = plazasOperacion > 0 ? (cnTotalP2 / plazasOperacion) : 0;
            
            const crecimientoNormalizado = Math.min(porcentajeCrecimiento / 100, 1);
            const cambioNormalizado = Math.min(cambioCN / 1000, 1);
            const eficienciaNormalizada = Math.min(eficiencia / 10, 1);
            
            const puntaje = 
                (crecimientoNormalizado * 4) +
                (cambioNormalizado * 3) +  
                (eficienciaNormalizada * 3);
            
            return {
                estado,
                cambioCN,
                porcentajeCrecimiento,
                totalPlazas: datos.total_plazas_periodo2 || 0,
                plazasOperacion,
                eficiencia,
                puntaje: Number(puntaje.toFixed(1))
            };
        });
    }

    // Clase para puntaje
    getClassPuntaje(puntaje) {
        if (puntaje >= 8) return 'puntaje-excelente';
        if (puntaje >= 6) return 'puntaje-bueno';
        if (puntaje >= 4) return 'puntaje-regular';
        return 'puntaje-bajo';
    }

    // Actualizar análisis de tendencias
    actualizarTendencias(analisisPorEstado) {
        if (!analisisPorEstado) return;
        
        const tendencias = this.calcularTendenciasPorCategoria(analisisPorEstado);
        
        this.actualizarTendenciaCategoria('inicial', tendencias.inicial, '👶', 'CN Inicial');
        this.actualizarTendenciaCategoria('primaria', tendencias.primaria, '🎒', 'CN Primaria');
        this.actualizarTendenciaCategoria('secundaria', tendencias.secundaria, '📚', 'CN Secundaria');
    }

    // Calcular tendencias por categoría
    calcularTendenciasPorCategoria(analisisPorEstado) {
        const tendencias = {
            inicial: { crecimientoTotal: 0, estados: [], count: 0 },
            primaria: { crecimientoTotal: 0, estados: [], count: 0 },
            secundaria: { crecimientoTotal: 0, estados: [], count: 0 }
        };
        
        Object.entries(analisisPorEstado).forEach(([estado, datos]) => {
            const metricas = datos.metricas || {};
            
            const inicial = metricas.CN_Inicial_Acum || {};
            if (inicial.porcentaje_cambio !== undefined) {
                tendencias.inicial.crecimientoTotal += inicial.porcentaje_cambio;
                tendencias.inicial.estados.push({ estado, crecimiento: inicial.porcentaje_cambio, cambio: inicial.cambio || 0 });
                tendencias.inicial.count++;
            }
            
            const primaria = metricas.CN_Prim_Acum || {};
            if (primaria.porcentaje_cambio !== undefined) {
                tendencias.primaria.crecimientoTotal += primaria.porcentaje_cambio;
                tendencias.primaria.estados.push({ estado, crecimiento: primaria.porcentaje_cambio, cambio: primaria.cambio || 0 });
                tendencias.primaria.count++;
            }
            
            const secundaria = metricas.CN_Sec_Acum || {};
            if (secundaria.porcentaje_cambio !== undefined) {
                tendencias.secundaria.crecimientoTotal += secundaria.porcentaje_cambio;
                tendencias.secundaria.estados.push({ estado, crecimiento: secundaria.porcentaje_cambio, cambio: secundaria.cambio || 0 });
                tendencias.secundaria.count++;
            }
        });
        
        Object.keys(tendencias).forEach(categoria => {
            const data = tendencias[categoria];
            data.promedioCrecimiento = data.count > 0 ? data.crecimientoTotal / data.count : 0;
            data.estados.sort((a, b) => b.crecimiento - a.crecimiento);
            data.topEstados = data.estados.slice(0, 3);
        });
        
        return tendencias;
    }

    // Actualizar una categoría específica de tendencia
    actualizarTendenciaCategoria(sufijo, datos, icono, nombre) {
        const valorElement = document.querySelector(`[data-bind="tendencia_${sufijo}_valor"]`);
        const descElement = document.querySelector(`[data-bind="tendencia_${sufijo}_desc"]`);
        const detalleElement = document.querySelector(`[data-bind="tendencia_${sufijo}_detalle"]`);
        
        if (valorElement) {
            valorElement.textContent = this.formatPorcentaje(datos.promedioCrecimiento);
            valorElement.className = `tendencia-valor ${this.getClassCambio(datos.promedioCrecimiento)}`;
        }
        
        if (descElement) {
            descElement.textContent = datos.promedioCrecimiento > 0 ? 'Crecimiento' : 
                                    datos.promedioCrecimiento < 0 ? 'Decrecimiento' : 'Estable';
        }
        
        if (detalleElement) {
            if (datos.topEstados && datos.topEstados.length > 0) {
                const topEstados = datos.topEstados.map(e => 
                    `${e.estado} (${this.formatPorcentaje(e.crecimiento)})`
                ).join(', ');
                detalleElement.textContent = `Estados con mayor crecimiento: ${topEstados}`;
            } else {
                detalleElement.textContent = 'No hay datos suficientes de estados';
            }
        }
    }

    // Actualizar resumen ejecutivo
    actualizarResumenEjecutivo(metricasAvanzadas, analisisPorEstado) {
        if (!analisisPorEstado) return;
        
        const stats = this.calcularEstadisticasGenerales(analisisPorEstado);
        
        const periodoElement = document.querySelector('[data-bind="resumen_periodo"]');
        if (periodoElement && this.datosComparativa) {
            if (this.datosComparativa.modo === 'anual') {
                periodoElement.textContent = `Año ${this.datosComparativa.año1} vs Año ${this.datosComparativa.año2}`;
            } else {
                periodoElement.textContent = 
                    `${this.getMonthName(this.datosComparativa.periodo1)} vs ${this.getMonthName(this.datosComparativa.periodo2)} ${this.datosComparativa.year}`;
            }
        }
        
        const tendenciaElement = document.querySelector('[data-bind="resumen_tendencia"]');
        if (tendenciaElement) {
            const tendencia = metricasAvanzadas.tasaCrecimiento > 5 ? '📈 Fuertemente Positiva' :
                             metricasAvanzadas.tasaCrecimiento > 0 ? '📈 Positiva' :
                             metricasAvanzadas.tasaCrecimiento < -5 ? '📉 Fuertemente Negativa' :
                             metricasAvanzadas.tasaCrecimiento < 0 ? '📉 Negativa' : '➡️ Estable';
            tendenciaElement.textContent = tendencia;
        }
        
        const estadosCrecimientoElement = document.querySelector('[data-bind="resumen_estados_crecimiento"]');
        const estadosDecrecimientoElement = document.querySelector('[data-bind="resumen_estados_decrecimiento"]');
        
        if (estadosCrecimientoElement) estadosCrecimientoElement.textContent = stats.estadosCrecimiento;
        if (estadosDecrecimientoElement) estadosDecrecimientoElement.textContent = stats.estadosDecrecimiento;
        
        const recomendacionElement = document.querySelector('[data-bind="resumen_recomendacion"]');
        if (recomendacionElement) {
            recomendacionElement.textContent = this.generarRecomendacion(metricasAvanzadas, stats);
        }
    }

    // Calcular estadísticas generales
    calcularEstadisticasGenerales(analisisPorEstado) {
        let estadosCrecimiento = 0;
        let estadosDecrecimiento = 0;
        let estadosEstables = 0;
        
        Object.values(analisisPorEstado).forEach(datos => {
            const metricas = datos.metricas || {};
            const cnTotal = metricas.CN_Tot_Acum || {};
            const crecimiento = cnTotal.porcentaje_cambio || 0;
            
            if (crecimiento > 1) estadosCrecimiento++;
            else if (crecimiento < -1) estadosDecrecimiento++;
            else estadosEstables++;
        });
        
        return {
            estadosCrecimiento,
            estadosDecrecimiento,
            estadosEstables,
            totalEstados: Object.keys(analisisPorEstado).length
        };
    }

    // Generar recomendación ejecutiva
    generarRecomendacion(metricasAvanzadas, stats) {
        const { tasaCrecimiento } = metricasAvanzadas;
        const { estadosCrecimiento, totalEstados } = stats;
        
        const porcentajeCrecimiento = (estadosCrecimiento / totalEstados) * 100;
        
        if (tasaCrecimiento > 10 && porcentajeCrecimiento > 70) {
            return 'Excelente desempeño general. Mantener estrategias actuales.';
        } else if (tasaCrecimiento > 5 && porcentajeCrecimiento > 50) {
            return 'Buen desempeño. Considerar expansión en estados con mayor crecimiento.';
        } else if (tasaCrecimiento > 0) {
            return 'Crecimiento moderado. Revisar estrategia en estados con decrecimiento.';
        } else if (tasaCrecimiento === 0) {
            return 'Situación estable. Evaluar oportunidades de mejora.';
        } else {
            return 'Desempeño negativo. Se requiere análisis detallado y plan de acción.';
        }
    }

    // Helper methods
    getClassCambio(valor) {
        if (valor > 0) return "cambio-positivo";
        if (valor < 0) return "cambio-negativo";
        return "cambio-neutral";
    }

    formatNumber(num) {
        if (!num || isNaN(num)) return "0";
        return new Intl.NumberFormat("es-MX").format(Math.round(num));
    }

    formatCambio(valor) {
        if (valor === 0) return "0";
        return `${valor > 0 ? '+' : ''}${this.formatNumber(valor)}`;
    }

    formatPorcentaje(porcentaje) {
        if (porcentaje === null || porcentaje === undefined) return "0.00%";
        const signo = porcentaje > 0 ? "+" : "";
        return `${signo}${parseFloat(porcentaje).toFixed(2)}%`;
    }

    getNombreMetrica(key) {
        const nombres = {
            CN_Inicial_Acum: "CN Inicial Acumulado",
            CN_Prim_Acum: "CN Primaria Acumulada",
            CN_Sec_Acum: "CN Secundaria Acumulada",
            CN_Tot_Acum: "CN Total Acumulado",
        };
        return nombres[key] || key;
    }

    // Configurar modal de estados
    configurarModalEstados() {
        const modal = document.getElementById('modal-detalle-estado');
        if (!modal) return;
        
        const closeBtn = modal.querySelector('.modal-close');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => {
                modal.classList.add('hidden');
            });
        }
        
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.add('hidden');
            }
        });
    }

    // Mostrar loader
    mostrarLoader(mensaje = 'Cargando...') {
        const loader = document.getElementById('global-loader');
        if (!loader) return;
        
        const loaderText = loader.querySelector('.loader-text');
        if (loaderText) {
            loaderText.textContent = mensaje;
        }
        loader.classList.remove('hidden');
    }

    // Ocultar loader
    ocultarLoader() {
        const loader = document.getElementById('global-loader');
        if (loader) loader.classList.add('hidden');
    }

    // Mostrar alerta
    mostrarAlerta(mensaje, tipo = 'info') {
        const alertContainer = document.getElementById('alert-container');
        if (!alertContainer) return;
        
        const alertDiv = document.createElement('div');
        alertDiv.className = tipo === 'error' ? 'alert error' :
                           tipo === 'success' ? 'alert success' :
                           tipo === 'warning' ? 'alert warning' :
                           'alert info';
        alertDiv.textContent = mensaje;
        
        alertContainer.innerHTML = '';
        alertContainer.appendChild(alertDiv);
        
        setTimeout(() => {
            alertContainer.innerHTML = '';
        }, 5000);
    }

    // Limpiar resultados
    limpiarResultados() {
        if (this.elementos.resultadosContainer) {
            this.elementos.resultadosContainer.innerHTML = '';
            this.elementos.resultadosContainer.classList.add('hidden');
        }
        this.datosComparativa = null;
    }

    // Reiniciar formulario
    reiniciarFormulario() {
        this.elementos.yearSelect.selectedIndex = 0;
        this.resetMonthSelects();
        this.elementos.compararBtn.disabled = true;
        this.limpiarResultados();
        
        const modoSelect = document.getElementById('modo-comparacion');
        if (modoSelect) {
            modoSelect.value = 'periodo';
            this.onModoComparacionChange();
        }
    }
}

// Instancia global
const sistemaComparativas = new SistemaComparativas();

// Inicializar cuando el DOM esté listo
document.addEventListener('DOMContentLoaded', function() {
    sistemaComparativas.init();
});

// También exportar para uso modular
if (typeof module !== 'undefined' && module.exports) {
    module.exports = sistemaComparativas;
}