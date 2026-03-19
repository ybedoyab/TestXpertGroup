# Hospital Data Quality Pipeline (Ingeniero de Datos)

Proyecto de calidad de datos para el dataset `dataset_hospital 2.json`, con foco en:
trazabilidad (auditoría por regla y fila), limpieza conservadora, validaciones cruzadas,
métricas antes/después y entregables listos para enviar.

## Índice
1. [Qué contiene](#que-contiene)
2. [Estructura del proyecto](#estructura-del-proyecto)
3. [Requisitos](#requisitos)
4. [Preparación del dataset](#preparacion-dataset)
5. [Ejecución paso a paso](#ejecucion-paso-a-paso)
6. [Ejecutar pruebas](#ejecutar-pruebas)
7. [Archivos generados (outputs)](#archivos-generados)
8. [Bonus: DWH y PDF](#bonus-dwh-y-pdf)
9. [Compresión para entrega](#compresion-zip)
10. [Decisiones de diseño](#decisiones-de-diseno)
11. [Limitaciones y supuestos](#limitaciones-y-supuestos)

<a id="que-contiene"></a>
## Qué contiene

El dataset incluye al menos dos tablas:
`pacientes` y `citas_medicas`.

El pipeline ejecuta:
1. Ingesta determinista (JSON -> `pandas.DataFrame`).
2. Profiling exploratorio antes de limpieza.
3. Limpieza conservadora con auditoría (banderas, issues y rechazados).
4. Validaciones cruzadas (integridad referencial y catálogos).
5. Métricas globales y por tabla (antes vs después).
6. Export de datasets limpios/rechazados y reporte de calidad.
7. Bonus: simulación de migración a un modelo tipo Data Warehouse (SQLite).
8. Bonus: suite de pruebas con `pytest` y cobertura de código (`pytest-cov`).

<a id="estructura-del-proyecto"></a>
## Estructura del proyecto

- `main.py`: entrypoint CLI que ejecuta el pipeline completo.
- `config/`: configuración del pipeline.
  - `config.py`: defaults de CLI (paths, fecha, límites).
  - `settings.py`: settings de runtime (tolerancias, validaciones).
- `src/`: implementación del ETL por capas.
  - `src/core/`: tipos compartidos (`IssueRecord`, catálogos), utilidades (fechas, email, teléfono) y logging.
  - `src/extract/`: extracción (carga del JSON de entrada a `pandas.DataFrame`).
  - `src/transform/`: transformaciones y reglas:
    - `profiling_impl.py`: profiling antes de limpieza.
    - `cleaners/`: limpieza conservadora + auditoría por fila/campo.
    - `validation_impl.py`: validaciones cruzadas y orfandades.
    - `metrics/`: métricas antes/después y flatten para exports.
  - `src/load/`: carga a artefactos de salida:
    - `export_impl.py`: exports CSV/JSON.
    - `dwh/`: simulación de DWH (SQLite star schema).
  - `src/pipeline/`: orquestador de punta a punta (`pipeline_runner.py`).
  - `src/report/`: generador del informe técnico (`technical_report/`).
- `tests/`: pruebas (36 tests, ~91% cobertura) con `conftest.py` y fixtures compartidos.
- `data/processed/`: datasets limpios y rechazados generados (ignorados en git).
- `data/reports/`: métricas, resumen y SQLite DWH generado (ignorados en git).
- `docs/technical_report.md`: informe técnico generado a partir de los resultados reales.

<a id="requisitos"></a>
## Requisitos

- Python `3.11+`
- Se recomienda usar `venv` (incluido en este repo vía `.venv/` cuando lo creas localmente).
- Dependencias definidas en `pyproject.toml` (fuente de verdad única).

<a id="preparacion-dataset"></a>
## Preparación del dataset (`data/raw`)

El pipeline espera el dataset en:

- `data/raw/dataset_hospital 2.json`

Si el archivo no está ahí, muévelo antes de ejecutar.

## Ejecución paso a paso

Abre PowerShell en la raíz del repo (donde está `README.md`), para que las rutas relativas funcionen tal como están en los comandos.

<a id="ejecucion-paso-a-paso"></a>
### 1) Crear entorno virtual e instalar dependencias

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

### 2) Ejecutar el pipeline completo

Esto generará: exports limpios/rechazados, métricas antes/después, resumen, DWH (SQLite) y el informe técnico.

```powershell
.venv\Scripts\python.exe main.py
```

Notas:
- Por defecto el pipeline usa (configurable vía flags):
  - `--input "data/raw/dataset_hospital 2.json"`
  - `--output-dir "."`
  - `--reference-date "2026-03-18"`
  - `--limit 0`
  - `--age-tolerance-years 2`
- El pipeline es idempotente: sobrescribe exports y recrea métricas en las rutas estándar para que puedas re-ejecutar sin romper nada.

### Parámetros opcionales (si necesitas cambiar defaults)

Ejemplos:

```powershell
.venv\Scripts\python.exe main.py --limit 500
.venv\Scripts\python.exe main.py --reference-date "2026-01-01"
.venv\Scripts\python.exe main.py --input "data/raw/otro_dataset.json"
```

<a id="ejecutar-pruebas"></a>
### 3) Ejecutar pruebas (recomendado antes de enviar)

```powershell
.venv\Scripts\python.exe -m pytest -v
```

La configuración de cobertura ya está en `pyproject.toml`; el comando anterior
reportará automáticamente la cobertura de `src/` y `config/`.

<a id="archivos-generados"></a>
## Archivos generados (outputs)

Mínimo requerido por la prueba:

- `data/processed/pacientes_clean.csv`
- `data/processed/citas_medicas_clean.csv`
- `data/processed/pacientes_rejected.csv`
- `data/processed/citas_medicas_rejected.csv`
- `data/processed/data_quality_issues.csv`
- `data/reports/before_quality_metrics.csv`
- `data/reports/after_quality_metrics.csv`
- `data/reports/quality_summary.json`

> **Nota**: los archivos generados están en `.gitignore` porque son 100% reproducibles ejecutando `main.py`.

<a id="bonus-dwh-y-pdf"></a>
## Bonus: DWH (SQLite) y PDF del informe

### DWH (SQLite)

El pipeline carga automáticamente los datos limpios en:

- `data/reports/dwh.sqlite`

### PDF (opcional)

El informe en Markdown se genera en `docs/technical_report.md`.

Si tienes `pandoc`, puedes generar el PDF así:

```powershell
pandoc .\docs\technical_report.md -o .\docs\technical_report.pdf
```

<a id="decisiones-de-diseno"></a>
## Decisiones de diseño (por qué así)

- **Limpieza conservadora**: no se corrigen valores "clínicamente dudosos" sin base. Se normaliza a catálogos
  (por ejemplo `sexo`, `estado_cita`), se parsean fechas de forma estricta (`YYYY-MM-DD`) y lo no parseable
  se deja como `NULL` con auditoría.
- **Trazabilidad**: cada corrección/rechazo queda registrada como `IssueRecord` (regla, severidad, fila y valores).
- **Reproducibilidad**: la edad derivada usa una `REFERENCE_DATE` fija vía CLI.
- **Idempotencia**: el pipeline sobrescribe exports y recrea `dwh.sqlite` en cada corrida.
- **Sin wrappers innecesarios**: cada módulo contiene lógica real; no hay capas de indirección triviales.
- **Logging estructurado**: cada etapa del pipeline emite logs con conteos (filas procesadas, rechazadas, issues).
- **Configuración única**: `pyproject.toml` es la fuente de verdad para dependencias (no hay `requirements.txt` duplicado).

<a id="limitaciones-y-supuestos"></a>
## Limitaciones y supuestos

- Fechas: se parsean de forma conservadora esperando `YYYY-MM-DD`. Formatos mixtos no estándar se marcan y quedan como `NULL`.
- Telefonía: se normaliza a dígitos-only y se valida longitud razonable (10 a 15 dígitos).
- Emails: validación por regex (no se hace verificación de existencia de dominio).
- Integridad referencial: las filas huérfanas de `citas_medicas` se rechazan (no se inventan pacientes).
- Para el DWH: `NULL` se mapea a un miembro `UNKNOWN` (key=0) en las `dim_*`.

<a id="compresion-zip"></a>
## Cómo comprimir para entregar (ZIP)

```powershell
Compress-Archive -Path * -DestinationPath deliverable_hospital_dq.zip -Force
```
