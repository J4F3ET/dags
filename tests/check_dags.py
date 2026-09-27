"""Verifica que cada archivo de DAG importe sin errores, defina al menos un DAG y parsee rápido.

Se ejecuta en CI dentro de la imagen de Airflow: mismo Python y mismas dependencias que en el host.
Uso: python tests/check_dags.py <directorio_de_dags>
"""

import runpy
import sys
import time
from pathlib import Path

from airflow.sdk import DAG

# El dag-processor re-ejecuta cada archivo completo en cada ciclo de parseo.
# Si importar un archivo tarda más que esto, casi siempre es un import pesado a nivel de módulo.
MAX_PARSE_SECONDS = 5.0


def main(dags_dir: str) -> int:
    sys.path.insert(0, dags_dir)
    errores = []

    for path in sorted(Path(dags_dir).rglob("*.py")):
        inicio = time.perf_counter()
        try:
            namespace = runpy.run_path(str(path))
        except Exception as e:
            errores.append(f"{path}: error al importar -> {e!r}")
            continue
        duracion = time.perf_counter() - inicio

        dags = [obj for obj in namespace.values() if isinstance(obj, DAG)]
        if not dags:
            errores.append(f"{path}: no define ningún DAG")
        if duracion > MAX_PARSE_SECONDS:
            errores.append(f"{path}: tardó {duracion:.1f}s en importarse (máximo {MAX_PARSE_SECONDS}s)")
        print(f"{path}: {len(dags)} DAG(s), {duracion:.2f}s")

    for error in errores:
        print(f"ERROR {error}", file=sys.stderr)
    return 1 if errores else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
