# ==============================================================================
# plaza_index.py
# Índice en memoria para la cascada de selección:
#   Estado → Zona → Municipio → Localidad → Claves
#
# Se construye UNA SOLA VEZ al arranque del servidor a partir del DataFrame
# del último mes. Cada nivel es un dict → acceso O(1) en vez de filtrar
# el DataFrame completo en cada petición.
#
# Uso en app.py:
#   from plaza_index import plaza_index as _plaza_index
#   _plaza_index.build(df_ultimo_mes, ...)   # al arranque
#   _plaza_index.get_estados()               # ["Jalisco", "CDMX", ...]
#   _plaza_index.get_zonas("Jalisco")        # ["Zona 1", ...]
#   _plaza_index.get_municipios("Jalisco", "Zona 1")
#   _plaza_index.get_localidades("Jalisco", "Zona 1", "Guadalajara")
#   _plaza_index.get_claves("Jalisco", "Zona 1", "Guadalajara", "Zapopan")
#   _plaza_index.get_estados_con_conteo()    # [{"nombre": ..., "cantidad": ...}]
#   _plaza_index.get_estados_populares(n=8)
# ==============================================================================

import logging
import threading
from typing import Optional
from unidecode import unidecode

import pandas as pd

logger = logging.getLogger(__name__)


def _norm(texto) -> str:
    """Normaliza texto para comparación: sin acentos, mayúsculas, sin espacios extra."""
    if not isinstance(texto, str):
        texto = str(texto) if texto is not None else ""
    return unidecode(texto).strip().upper()


class PlazaIndex:
    """
    Índice en memoria de la cascada Estado→Zona→Municipio→Localidad→Clave.

    Estructura interna:
        _tree: dict normalizado
        {
          "JALISCO": {
            "_display": "Jalisco",
            "_count": 120,
            "ZONA 1": {
              "_display": "Zona 1",
              "GUADALAJARA": {
                "_display": "Guadalajara",
                "ZAPOPAN": {
                  "_display": "Zapopan",
                  "_claves": ["CLAVE1", "CLAVE2"]
                }
              }
            }
          }
        }
    """

    def __init__(self):
        self._lock  = threading.RLock()
        self._tree  = {}          # árbol normalizado
        self._ready = False

        # Cache de listas ya formateadas (se construyen una sola vez)
        self._estados_lista:    list = []   # ["Jalisco", "CDMX", ...]
        self._estados_conteo:   list = []   # [{"nombre":..., "cantidad":...}]

    # ------------------------------------------------------------------
    # Construcción del índice
    # ------------------------------------------------------------------

    def build(
        self,
        df: pd.DataFrame,
        col_estado:    str = "Estado",
        col_zona:      str = "Coord. Zona",
        col_municipio: str = "Municipio",
        col_localidad: str = "Localidad",
        col_clave:     str = "Clave_Plaza",
    ) -> bool:
        """
        Construye el índice a partir del DataFrame del último mes.
        Es thread-safe: mientras reconstruye, las lecturas siguen
        devolviendo el índice anterior.

        Args:
            df            : DataFrame del último mes (ya traducido al esquema legacy)
            col_estado    : nombre de la columna Estado
            col_zona      : nombre de la columna Coord. Zona
            col_municipio : nombre de la columna Municipio
            col_localidad : nombre de la columna Localidad
            col_clave     : nombre de la columna Clave_Plaza

        Returns:
            True si la construcción fue exitosa.
        """
        if df is None or df.empty:
            logger.warning("PlazaIndex.build: DataFrame vacío, índice no construido.")
            return False

        # Verificar que las columnas necesarias existen
        cols_requeridas = [col_estado, col_zona, col_municipio, col_localidad, col_clave]
        faltantes = [c for c in cols_requeridas if c not in df.columns]
        if faltantes:
            logger.error(f"PlazaIndex.build: columnas faltantes: {faltantes}")
            return False

        logger.info(f"🔨 PlazaIndex: construyendo índice con {len(df):,} filas…")

        try:
            tree = {}

            for _, row in df.iterrows():
                estado    = row[col_estado]
                zona      = row[col_zona]
                municipio = row[col_municipio]
                localidad = row[col_localidad]
                clave     = row[col_clave]

                # Ignorar filas con datos críticos nulos
                if pd.isna(estado) or pd.isna(zona) or pd.isna(municipio) or \
                   pd.isna(localidad) or pd.isna(clave):
                    continue

                estado_str    = str(estado).strip()
                zona_str      = str(zona).strip()
                municipio_str = str(municipio).strip()
                localidad_str = str(localidad).strip()
                clave_str     = str(clave).strip()

                if not all([estado_str, zona_str, municipio_str, localidad_str, clave_str]):
                    continue

                # Claves normalizadas para lookup
                ke = _norm(estado_str)
                kz = _norm(zona_str)
                km = _norm(municipio_str)
                kl = _norm(localidad_str)

                # Nivel Estado
                if ke not in tree:
                    tree[ke] = {"_display": estado_str, "_count": 0}
                tree[ke]["_count"] += 1

                # Nivel Zona
                if kz not in tree[ke]:
                    tree[ke][kz] = {"_display": zona_str}

                # Nivel Municipio
                if km not in tree[ke][kz]:
                    tree[ke][kz][km] = {"_display": municipio_str}

                # Nivel Localidad
                if kl not in tree[ke][kz][km]:
                    tree[ke][kz][km][kl] = {"_display": localidad_str, "_claves": []}

                # Clave (sin duplicados)
                claves_list = tree[ke][kz][km][kl]["_claves"]
                if clave_str not in claves_list:
                    claves_list.append(clave_str)

            # Pre-calcular listas de estados
            estados_lista = sorted(
                [v["_display"] for v in tree.values()],
                key=str
            )
            estados_conteo = sorted(
                [{"nombre": v["_display"], "cantidad": v["_count"],
                  "codigo": _norm(v["_display"])[:10]}
                 for v in tree.values()],
                key=lambda x: x["nombre"]
            )

            # Commit atómico
            with self._lock:
                self._tree           = tree
                self._estados_lista  = estados_lista
                self._estados_conteo = estados_conteo
                self._ready          = True

            n_estados    = len(tree)
            n_claves     = sum(
                len(loc["_claves"])
                for e in tree.values()
                for z in e.values() if isinstance(z, dict) and "_display" in z
                for m in z.values() if isinstance(m, dict) and "_display" in m
                for loc in m.values() if isinstance(loc, dict) and "_claves" in loc
            )
            logger.info(
                f"✅ PlazaIndex listo: {n_estados} estados, "
                f"{len(df):,} registros, ~{n_claves:,} claves únicas"
            )
            return True

        except Exception as exc:
            logger.error(f"❌ PlazaIndex.build: {exc}", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # API de consulta — O(1) con normalización
    # ------------------------------------------------------------------

    @property
    def is_ready(self) -> bool:
        return self._ready

    def get_estados(self) -> list:
        """Lista de nombres de estados ordenados alfabéticamente."""
        with self._lock:
            return list(self._estados_lista)

    def get_estados_con_conteo(self) -> list:
        """Lista de dicts {nombre, cantidad, codigo} ordenada por nombre."""
        with self._lock:
            return list(self._estados_conteo)

    def get_estados_populares(self, n: int = 8) -> list:
        """Top-N estados por cantidad de plazas."""
        with self._lock:
            return sorted(
                self._estados_conteo,
                key=lambda x: x["cantidad"],
                reverse=True
            )[:n]

    def get_zonas(self, estado: str) -> list:
        """Zonas disponibles para un estado."""
        with self._lock:
            ke = _norm(estado)
            nodo_e = self._tree.get(ke)
            if not nodo_e:
                return []
            return sorted([
                v["_display"]
                for k, v in nodo_e.items()
                if k not in ("_display", "_count") and isinstance(v, dict)
            ])

    def get_municipios(self, estado: str, zona: str) -> list:
        """Municipios disponibles para un estado y zona."""
        with self._lock:
            ke = _norm(estado)
            kz = _norm(zona)
            nodo_e = self._tree.get(ke, {})
            nodo_z = nodo_e.get(kz)
            if not nodo_z:
                return []
            return sorted([
                v["_display"]
                for k, v in nodo_z.items()
                if k != "_display" and isinstance(v, dict)
            ])

    def get_localidades(self, estado: str, zona: str, municipio: str) -> list:
        """Localidades disponibles para estado, zona y municipio."""
        with self._lock:
            ke = _norm(estado)
            kz = _norm(zona)
            km = _norm(municipio)
            nodo_e = self._tree.get(ke, {})
            nodo_z = nodo_e.get(kz, {})
            nodo_m = nodo_z.get(km)
            if not nodo_m:
                return []
            return sorted([
                v["_display"]
                for k, v in nodo_m.items()
                if k != "_display" and isinstance(v, dict)
            ])

    def get_claves(
        self, estado: str, zona: str, municipio: str, localidad: str
    ) -> list:
        """Claves de plaza para el nivel más específico de la cascada."""
        with self._lock:
            ke = _norm(estado)
            kz = _norm(zona)
            km = _norm(municipio)
            kl = _norm(localidad)
            nodo_e = self._tree.get(ke, {})
            nodo_z = nodo_e.get(kz, {})
            nodo_m = nodo_z.get(km, {})
            nodo_l = nodo_m.get(kl)
            if not nodo_l:
                return []
            return sorted(nodo_l.get("_claves", []))

    def buscar_estado(self, texto: str) -> Optional[str]:
        """
        Devuelve el nombre display del estado que coincide con el texto,
        o None si no existe. Útil para normalizar entradas del usuario.
        """
        with self._lock:
            ke = _norm(texto)
            nodo = self._tree.get(ke)
            return nodo["_display"] if nodo else None


# ---------------------------------------------------------------------------
# Instancia global
# ---------------------------------------------------------------------------
plaza_index = PlazaIndex()