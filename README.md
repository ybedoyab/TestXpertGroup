# Hospital Data Quality Pipeline

Pipeline de calidad de datos para el procesamiento del archivo `dataset_hospital 2.json`. El proyecto incluye trazabilidad mediante auditoría por regla y fila, limpieza de datos, validaciones cruzadas y generación de métricas.

## Índice

1. [Características](#caracteristicas)
2. [Estructura del Proyecto](#estructura-del-proyecto)
3. [Requisitos](#requisitos)
4. [Preparación de Datos](#preparacion-de-datos)
5. [Ejecución del Pipeline](#ejecucion-del-pipeline)
6. [Pruebas Automatizadas](#pruebas-automatizadas)
7. [Archivos de Salida](#archivos-de-salida)
8. [Data Warehouse y Reportes](#data-warehouse-y-reportes)
9. [Empaquetado](#empaquetado)
10. [Decisiones de Arquitectura](#decisiones-de-arquitectura)
11. [Restricciones y Supuestos](#restricciones-y-supuestos)

## Características

El pipeline ejecuta las siguientes operaciones:

1. Ingesta determinista (JSON a `pandas.DataFrame`).
2. Perfilado (profiling) exploratorio previo a la limpieza.
3. Limpieza de datos con registro de auditoría (identificación de anomalías y rechazos).
4. Validaciones cruzadas (integridad referencial y catálogos).
5. Cálculo de métricas de calidad globales y por entidad (pre y post procesamiento).
6. Exportación de datasets (limpios y rechazados) y reportes de calidad.
7. Simulación de carga hacia un modelo Data Warehouse (SQLite).
8. Ejecución de pruebas automatizadas mediante `pytest` con reporte de cobertura.

## Estructura del Proyecto

- `main.py`: Punto de entrada CLI para la ejecución del pipeline.
- `config/`: Configuración general.
  - `config.py`: Valores predeterminados del CLI (rutas, fechas, límites).
  - `settings.py`: Parámetros de ejecución (tolerancias, reglas de validación).
- `src/`: Implementación del ETL.
  - `src/core/`: Tipos compartidos (`IssueRecord`, catálogos), utilidades de transformación y configuración de logs.
  - `src/extract/`: Módulo de ingesta de datos JSON.
  - `src/transform/`: Módulos de transformación y reglas de negocio.
    - `profiling_impl.py`: Perfilado de datos.
    - `cleaners/`: Lógica de limpieza y auditoría a nivel de registro.
    - `validation_impl.py`: Validaciones de integridad referencial.
    - `metrics/`: Cálculo de métricas de calidad.
  - `src/load/`: Módulos de exportación.
    - `export_impl.py`: Generación de archivos CSV/JSON.
    - `dwh/`: Carga a modelo relacional (SQLite).
  - `src/pipeline/`: Orquestador principal (`pipeline_runner.py`).
  - `src/report/`: Generador del informe técnico.
- `tests/`: Suite de pruebas unitarias y de integración.
- `data/processed/`: Directorio de salida para datasets procesados (ignorado en control de versiones).
- `data/reports/`: Directorio de salida para métricas, resúmenes y base de datos SQLite (ignorado en control de versiones).
- `docs/technical_report.md`: Informe técnico generado automáticamente.

## Requisitos

- Python 3.11 o superior.
- Entorno virtual (`venv`).
- Dependencias gestionadas mediante `pyproject.toml`.

## Preparación de Datos

El archivo JSON de entrada debe ubicarse en la siguiente ruta antes de iniciar la ejecución:

`data/raw/dataset_hospital 2.json`

## Ejecución del Pipeline

Todos los comandos deben ejecutarse desde el directorio raíz del repositorio.

### 1. Configuración del Entorno Virtual

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

### 2. Ejecución Principal

El siguiente comando procesa el dataset completo y genera todos los artefactos de salida (exportaciones, métricas, base de datos DWH e informe técnico).

```powershell
.venv\Scripts\python.exe main.py
```

El pipeline es idempotente; ejecuciones sucesivas sobrescribirán los archivos de salida existentes.

### Parámetros de Ejecución

El punto de entrada `main.py` admite parámetros opcionales para modificar el comportamiento estándar:

```powershell
.venv\Scripts\python.exe main.py --limit 500
.venv\Scripts\python.exe main.py --reference-date "2026-01-01"
.venv\Scripts\python.exe main.py --input "data/raw/otro_dataset.json"
```

Valores predeterminados:
- `--input`: `data/raw/dataset_hospital 2.json`
- `--output-dir`: `.`
- `--reference-date`: `2026-03-18`
- `--limit`: `0` (procesamiento completo sin límite)
- `--age-tolerance-years`: `2`

## Pruebas Automatizadas

Comando para ejecutar la suite de pruebas validando la cobertura del código:

```powershell
.venv\Scripts\python.exe -m pytest -v
```

La configuración de métricas de cobertura se encuentra definida en `pyproject.toml`.

## Archivos de Salida

La ejecución exitosa del pipeline generará los siguientes archivos:

- `data/processed/pacientes_clean.csv`
- `data/processed/citas_medicas_clean.csv`
- `data/processed/pacientes_rejected.csv`
- `data/processed/citas_medicas_rejected.csv`
- `data/processed/data_quality_issues.csv`
- `data/reports/before_quality_metrics.csv`
- `data/reports/after_quality_metrics.csv`
- `data/reports/quality_summary.json`

## Data Warehouse y Reportes

### Data Warehouse (SQLite)

Los datos procesados son cargados automáticamente mediante un esquema estrella en:

- `data/reports/dwh.sqlite`

### Informe Técnico

El informe detallado de calidad de datos se genera en formato Markdown:

- `docs/technical_report.md`

Generación de formato PDF (requiere `pandoc`):

```powershell
pandoc .\docs\technical_report.md -o .\docs\technical_report.pdf
```

## Decisiones de Arquitectura

- **Limpieza Conservadora**: Los valores inválidos son normalizados mediante catálogos restringidos. Valores no interpretables se procesan como nulos (`NULL`) y se registran en la pista de auditoría.
- **Trazabilidad Continua**: Modificaciones o rechazos son categorizados mediante la estructura `IssueRecord`, indicando regla aplicada, severidad y valor original.
- **Variables Deterministas**: Cálculos dependientes del tiempo, como la edad derivada, operan sobre la base de una fecha de referencia estática (`REFERENCE_DATE`).
- **Idempotencia Transaccional**: Toda ejecución regenera los artefactos analíticos de salida y bases de datos destino para asegurar reproducibilidad.
- **Manejo de Nulos en Dimensiones**: En el modelo dimensional, los registros `NULL` son mapeados al identificador `UNKNOWN` (clave = 0).

## Reglas de calidad implementadas

| Rule ID | Tabla | Severidad | Acción |
|---|---|---|---|
| `R_SEXO_UNRECOGNIZED` | pacientes | warning | Nulifica `sexo` si no mapea al catálogo `{M, F}` |
| `R_FECHA_NAC_INVALID` | pacientes | warning | Nulifica `fecha_nacimiento` si no es parseable |
| `R_EDAD_FILLED_FROM_DERIVADA` | pacientes | info | Imputa `edad` desde `fecha_nacimiento` cuando está ausente |
| `R_EDAD_INCONSISTENT_WITH_DERIVED` | pacientes | warning | Corrige `edad` cuando difiere > tolerancia de la derivada |
| `R_EDAD_PROVIDED_OUT_OF_RANGE` | pacientes | warning | Nulifica `edad` fuera del rango `[0, 120]` |
| `R_EMAIL_INVALID` | pacientes | warning | Nulifica email que no cumple formato RFC |
| `R_TELEFONO_INVALID_LENGTH` | pacientes | warning | Nulifica teléfono con longitud fuera de `[10, 15]` dígitos |
| `R_PK_MISSING` | ambas | error | Rechaza registros sin llave primaria |
| `R_DUP_PK_EXACT` | ambas | info | Deduplica exactos conservando la primera ocurrencia |
| `R_DUP_PK_CONFLICT` | ambas | error | Rechaza PKs duplicadas con datos contradictorios |
| `R_ESTADO_CITA_UNRECOGNIZED` | citas_medicas | warning | Nulifica `estado_cita` si no mapea al catálogo |
| `R_FECHA_CITA_INVALID` | citas_medicas | warning | Nulifica `fecha_cita` no parseable |
| `R_FECHA_CITA_FUTURE` | citas_medicas | warning | Alerta si `fecha_cita` es posterior a la fecha de referencia; el registro se conserva |
| `R_COST_NON_NUMERIC` | citas_medicas | error | Rechaza registro con costo no numérico |
| `R_COST_NEGATIVE` | citas_medicas | error | Rechaza registro con costo negativo |
| `R_ORPHAN_CITA` | citas_medicas | error | Rechaza citas cuyo `id_paciente` no existe en pacientes |

## Schema de salida

Los archivos `*_clean.csv` contienen únicamente el **schema canónico** — sin columnas internas de auditoría (`*_original`, `source_row_id`, etc.). Dichas columnas se publican exclusivamente en `data_quality_issues.csv` para trazabilidad.

**pacientes_clean.csv**: `id_paciente`, `nombre`, `fecha_nacimiento`, `edad`, `edad_derivada`, `edad_inconsistente`, `sexo`, `email`, `telefono`, `ciudad`

**citas_medicas_clean.csv**: `id_cita`, `id_paciente`, `fecha_cita`, `especialidad`, `medico`, `costo`, `estado_cita`

## Restricciones y Supuestos

- Procesamiento de fechas: El sistema requiere el formato ISO (`YYYY-MM-DD`). Estructuras incompatibles resultan en asignación nula documentada.
- Procesamiento telefónico: Exclusivamente caracteres numéricos validando longitud entre 10 y 15 dígitos.
- Correos electrónicos: Validados mediante coincidencia de estructura de expresión regular.
- Integridad Referencial: Filas en la entidad de citas médicas sin correspondencia de paciente se clasifican como registros rechazados.
- Fechas futuras en citas: Se conservan con alerta (`R_FECHA_CITA_FUTURE`) ya que pueden corresponder a citas programadas. La decisión final de rechazo se delega al data owner.
- **Cuando `rejected_rows = 0`:** No indica un pipeline pasivo. Significa que el dataset no presentaba problemas estructurales (FKs huérfanas, costos inválidos, PKs conflictivas). La limpieza actuó sobre campos de valor mediante correcciones in-place, documentadas en `data_quality_issues.csv`.
