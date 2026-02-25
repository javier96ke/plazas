#!/usr/bin/env python3
# ==============================================================================
# build_rust.py
# Compila plaza_rust y copia el .so al directorio de app.py
#
# Uso:
#   python build_rust.py           # compilaciÃ³n release
#   python build_rust.py --dev     # compilaciÃ³n debug (mÃ¡s rÃ¡pida, sin optimizar)
#   python build_rust.py --check   # solo verifica dependencias, no compila
# ==============================================================================

import os
import sys
import shutil
import subprocess
import platform
import glob
import argparse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CRATE_DIR   = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "plaza_rust"))
OUTPUT_DIR  = os.path.dirname(__file__)   # donde vive app.py
LIB_NAME    = "plaza_rust"


def run(cmd: list[str], cwd: str = None) -> int:
    print(f"  â–¶ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    return result.returncode


def check_dependencies() -> bool:
    ok = True

    # Rust
    if shutil.which("cargo") is None:
        print("âŒ cargo no encontrado. Instala Rust:")
        print("   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh")
        ok = False
    else:
        r = subprocess.run(["cargo", "--version"], capture_output=True, text=True)
        print(f"âœ… {r.stdout.strip()}")

    # maturin (compilador PyO3 recomendado)
    if shutil.which("maturin") is None:
        print("âš ï¸  maturin no encontrado â€” se usarÃ¡ 'cargo build' directo")
        print("   Para instalar: pip install maturin")
    else:
        r = subprocess.run(["maturin", "--version"], capture_output=True, text=True)
        print(f"âœ… {r.stdout.strip()}")

    # polars (requerido en runtime por rust_bridge.py)
    try:
        import polars
        print(f"âœ… polars {polars.__version__}")
    except ImportError:
        print("âŒ polars no instalado: pip install polars")
        ok = False

    return ok


def find_so(target_dir: str) -> str | None:
    """Busca el .so / .pyd compilado en target/"""
    patterns = [
        f"{target_dir}/**/{LIB_NAME}*.so",
        f"{target_dir}/**/{LIB_NAME}*.pyd",
    ]
    for pat in patterns:
        matches = glob.glob(pat, recursive=True)
        if matches:
            # Preferir el mÃ¡s reciente
            return max(matches, key=os.path.getmtime)
    return None


def build_with_maturin(dev: bool) -> bool:
    """Compila usando maturin develop (mÃ¡s simple, instala directo en el env)."""
    cmd = ["maturin", "develop"]
    if not dev:
        cmd += ["--release"]
    rc = run(cmd, cwd=CRATE_DIR)
    return rc == 0


def build_with_cargo(dev: bool) -> bool:
    """Fallback: compila directamente con cargo."""
    cmd = ["cargo", "build"]
    if not dev:
        cmd += ["--release"]
    rc = run(cmd, cwd=CRATE_DIR)
    if rc != 0:
        return False

    # Buscar el .so y copiarlo al directorio de app.py
    subdir = "debug" if dev else "release"
    target_dir = os.path.join(CRATE_DIR, "target", subdir)
    so = find_so(target_dir)

    if so is None:
        # Buscar en todo target/
        so = find_so(os.path.join(CRATE_DIR, "target"))

    if so is None:
        print(f"âŒ No se encontrÃ³ el .so en {target_dir}")
        return False

    dest = os.path.join(OUTPUT_DIR, os.path.basename(so))
    shutil.copy2(so, dest)
    print(f"âœ… Copiado: {so} â†’ {dest}")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dev",   action="store_true", help="CompilaciÃ³n debug (mÃ¡s rÃ¡pida)")
    parser.add_argument("--check", action="store_true", help="Solo verificar dependencias")
    args = parser.parse_args()

    print("=" * 60)
    print("  plaza_rust â€” build script")
    print("=" * 60)

    print("\nðŸ“¦ Verificando dependenciasâ€¦")
    if not check_dependencies():
        sys.exit(1)

    if args.check:
        print("\nâœ… Dependencias OK (--check, sin compilar)")
        sys.exit(0)

    mode = "DEBUG" if args.dev else "RELEASE"
    print(f"\nðŸ”¨ Compilando en modo {mode}â€¦\n")

    # Intentar con maturin primero, fallback a cargo
    if shutil.which("maturin"):
        success = build_with_maturin(args.dev)
    else:
        print("â„¹ï¸  Usando cargo directamente (maturin no disponible)")
        success = build_with_cargo(args.dev)

    if success:
        print(f"\nâœ… CompilaciÃ³n exitosa.")
        print(f"   Ahora puedes arrancar app.py â€” rust_bridge.py cargarÃ¡ plaza_rust automÃ¡ticamente.")
    else:
        print(f"\nâŒ CompilaciÃ³n fallida. Revisa los errores arriba.")
        sys.exit(1)


if __name__ == "__main__":
    main()
