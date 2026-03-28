// ============================================
// Script Principal - Índice Onomástico AHPC
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    console.log('📚 Sistema de Consulta Índice Onomástico AHPC');
    
    // Animación de entrada para elementos
    animateOnScroll();
    
    // Mejorar inputs de formularios
    enhanceFormInputs();
    
    // Inicializar autocompletado de apellidos
    initAutocomplete();
    
    // Navegación con Enter en formulario de búsqueda avanzada
    initFormNavigation();
});

// ============================================
// NAVEGACIÓN CON ENTER EN FORMULARIO
// ============================================
function initFormNavigation() {
    const searchForm = document.getElementById('searchForm');
    if (!searchForm) return;
    
    // Obtener todos los campos del formulario en orden
    const fields = [
        document.getElementById('apellido'),
        document.getElementById('nombre'),
        document.getElementById('año_desde'),
        document.getElementById('año_hasta'),
        document.getElementById('escribano'),
        document.getElementById('tipo_acto'),
        document.getElementById('texto_libre')
    ].filter(field => field !== null);
    
    // Agregar event listener a cada campo
    fields.forEach((field, index) => {
        field.addEventListener('keydown', function(e) {
            // Si presiona Enter
            if (e.keyCode === 13) {
                e.preventDefault();
                
                // Si es el último campo, hacer submit
                if (index === fields.length - 1) {
                    searchForm.dispatchEvent(new Event('submit'));
                } else {
                    // Ir al siguiente campo
                    fields[index + 1].focus();
                }
            }
        });
    });
}

// ============================================
// AUTOCOMPLETADO DE APELLIDOS
// ============================================
function initAutocomplete() {
    // Buscar campos de apellido en página de búsqueda avanzada Y página de inicio
    const apellidoInputs = [
        document.getElementById('apellido'),      // Búsqueda avanzada
        document.getElementById('apellido-home')  // Página de inicio
    ].filter(input => input !== null);
    
    // Aplicar autocompletar a cada campo encontrado
    apellidoInputs.forEach(apellidoInput => {
        setupAutocomplete(apellidoInput);
    });
}

function setupAutocomplete(apellidoInput) {
    // Crear contenedor de sugerencias
    const suggestionsDiv = document.createElement('div');
    suggestionsDiv.className = 'autocomplete-suggestions';
    suggestionsDiv.style.cssText = `
        position: absolute;
        background: white;
        border: 1px solid #D4C5A9;
        border-top: none;
        max-height: 200px;
        overflow-y: auto;
        display: none;
        z-index: 1000;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    `;
    
    apellidoInput.parentElement.style.position = 'relative';
    apellidoInput.parentElement.appendChild(suggestionsDiv);
    
    let currentFocus = -1;
    let debounceTimer;
    
    // Event listener para input
    apellidoInput.addEventListener('input', function() {
        const val = this.value.trim();
        currentFocus = -1;
        
        clearTimeout(debounceTimer);
        
        if (val.length < 1) {  // Con 1 letra ya muestra sugerencias
            suggestionsDiv.style.display = 'none';
            return;
        }
        
        // Debounce para no hacer demasiadas peticiones
        debounceTimer = setTimeout(() => {
            fetch(`/api/apellidos?q=${encodeURIComponent(val)}`)
                .then(response => response.json())
                .then(apellidos => {
                    suggestionsDiv.innerHTML = '';
                    
                    if (apellidos.length === 0) {
                        suggestionsDiv.style.display = 'none';
                        return;
                    }
                    
                    apellidos.forEach(apellido => {
                        const div = document.createElement('div');
                        div.className = 'autocomplete-item';
                        div.textContent = apellido;
                        div.style.cssText = `
                            padding: 10px 15px;
                            cursor: pointer;
                            border-bottom: 1px solid #f0f0f0;
                            font-family: 'Courier Prime', monospace;
                        `;
                        
                        div.addEventListener('mouseenter', function() {
                            this.style.background = '#8B6F47';
                            this.style.color = 'white';
                        });
                        
                        div.addEventListener('mouseleave', function() {
                            this.style.background = 'white';
                            this.style.color = 'black';
                        });
                        
                        div.addEventListener('click', function() {
                            apellidoInput.value = this.textContent;
                            suggestionsDiv.style.display = 'none';
                        });
                        
                        suggestionsDiv.appendChild(div);
                    });
                    
                    suggestionsDiv.style.display = 'block';
                    suggestionsDiv.style.width = apellidoInput.offsetWidth + 'px';
                });
        }, 300);
    });
    
    // Navegación con teclado
    apellidoInput.addEventListener('keydown', function(e) {
        const items = suggestionsDiv.getElementsByClassName('autocomplete-item');
        
        if (e.keyCode === 40) { // Arrow Down
            e.preventDefault();
            currentFocus++;
            addActive(items);
        } else if (e.keyCode === 38) { // Arrow Up
            e.preventDefault();
            currentFocus--;
            addActive(items);
        } else if (e.keyCode === 13) { // Enter
            // Si hay sugerencias abiertas, seleccionar
            if (suggestionsDiv.style.display === 'block' && currentFocus > -1 && items[currentFocus]) {
                e.preventDefault();
                items[currentFocus].click();
            }
            // Si no, dejar que el formulario se envíe normalmente
        } else if (e.keyCode === 27) { // Escape
            suggestionsDiv.style.display = 'none';
        }
    });
    
    function addActive(items) {
        if (!items || items.length === 0) return;
        removeActive(items);
        if (currentFocus >= items.length) currentFocus = 0;
        if (currentFocus < 0) currentFocus = items.length - 1;
        items[currentFocus].style.background = '#8B6F47';
        items[currentFocus].style.color = 'white';
    }
    
    function removeActive(items) {
        for (let i = 0; i < items.length; i++) {
            items[i].style.background = 'white';
            items[i].style.color = 'black';
        }
    }
    
    // Cerrar sugerencias al hacer click fuera
    document.addEventListener('click', function(e) {
        if (e.target !== apellidoInput) {
            suggestionsDiv.style.display = 'none';
        }
    });
}

// Animación de elementos al hacer scroll
function animateOnScroll() {
    const observerOptions = {
        threshold: 0.1,
        rootMargin: '0px 0px -50px 0px'
    };
    
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.style.opacity = '1';
                entry.target.style.transform = 'translateY(0)';
            }
        });
    }, observerOptions);
    
    // Observar cards y secciones
    const elements = document.querySelectorAll('.stat-card, .info-card, .result-card');
    elements.forEach(el => {
        el.style.opacity = '0';
        el.style.transform = 'translateY(20px)';
        el.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
        observer.observe(el);
    });
}

// Mejorar experiencia de inputs
function enhanceFormInputs() {
    const inputs = document.querySelectorAll('.form-input, .search-input');
    
    inputs.forEach(input => {
        // Auto-capitalizar apellidos
        if (input.name === 'apellido') {
            input.addEventListener('input', (e) => {
                e.target.value = e.target.value.toUpperCase();
            });
        }
        
        // Highlight al enfocar
        input.addEventListener('focus', function() {
            this.parentElement.style.boxShadow = '0 0 0 3px rgba(139, 111, 71, 0.1)';
        });
        
        input.addEventListener('blur', function() {
            this.parentElement.style.boxShadow = 'none';
        });
    });
}

// Utilidad: Formatear números
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

// Utilidad: Scroll suave
function smoothScroll(target) {
    const element = document.querySelector(target);
    if (element) {
        element.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });
    }
}
