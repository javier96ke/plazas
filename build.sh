#!/usr/bin/env bash
set -e

echo "🐍 Actualizando pip e instalando dependencias de Python..."
python -m pip install --upgrade pip

# Instalar patchelf ANTES de maturin para que el .so tenga rpath correcto en Linux
echo "🔧 Instalando patchelf para linking correcto del .so..."
pip install patchelf
pip install "maturin[patchelf]"

pip install -r requirements.txt

echo "⚙️ Compilando módulo Rust (plaza_rust)..."

# Redirigir cargo a una carpeta con permisos de escritura
export CARGO_HOME="$HOME/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"

if [ -d "plaza_rust" ]; then
    cd plaza_rust
    export CARGO_BUILD_JOBS=1
    maturin develop --release
    cd ..
else
    echo "❌ Error: No se encontró la carpeta 'plaza_rust'"
    exit 1
fi

echo "✅ Build completado con éxito."
