# Arquitectura (ETL por capas)

Este proyecto implementa un pipeline de calidad de datos para `dataset_hospital 2.json`
con un layout ETL explícito por responsabilidades:

- `config/`: configuración del pipeline (`PipelineDefaults`, `Settings`).
- `src/core/`: componentes compartidos (schemas, catálogos, utilidades) que no dependen de etapas del pipeline.
- `src/extract/`: extracción desde JSON a `pandas.DataFrame` con un contrato mínimo de columnas.
- `src/transform/`: transformaciones y reglas:
  - `profiling_impl.py`: profiling exploratorio antes de limpieza.
  - `cleaners/`: limpieza conservadora + auditoría (cada corrección/rechazo queda trazada).
  - `validation_impl.py`: validaciones cruzadas (integridad referencial y "safety checks" de catálogos).
  - `metrics/`: cálculo de métricas antes/después (completitud, validez, unicidad, consistencia y FK).
- `src/load/`: carga a artefactos:
  - `export_impl.py`: exports CSV y métricas.
  - `dwh/`: carga a SQLite en un esquema tipo DWH (estrella) con miembros `UNKNOWN` para `NULL`.
- `src/pipeline/`: orquestación completa de extremo a extremo (`pipeline_runner.py`).
- `src/report/`: generación del `docs/technical_report.md` con hallazgos reales.
- `tests/`: 36 tests automatizados con ~91% de cobertura (`conftest.py` con fixtures compartidos).

## Flujo de ejecución

El entrypoint es `main.py`:

1. `main.py` parsea CLI y resuelve rutas/paths por default.
2. `pipeline_runner.run_pipeline()` ejecuta:
   - Extract: `ingestion_impl.load_dataset()`
   - Profiling: `profiling_impl.profile_dataset()`
   - Cleaning: `pacientes_cleaner.clean_pacientes()` + `citas_cleaner.clean_citas_medicas()`
   - Cross-validation: `validation_impl.validate_cross_references()`
   - Metrics: `quality_metrics.compute_quality_metrics()`
   - Load exports: `export_impl.export_datasets()`
   - DWH SQLite: `dwh.loader.load_to_sqlite()`
   - Report: `technical_report.generator.generate_technical_report_md()`

## Principios de diseño

- **Sin capas de indirección innecesarias**: `pipeline_runner.py` llama directamente a cada módulo sin wrappers intermedios.
- **Logging estructurado**: cada etapa emite logs informativos con conteos.
- **Idempotencia**: la ejecución sobreescribe `data/processed/*`, recrea métricas en `data/reports/*` y regenera `docs/technical_report.md` y `data/reports/dwh.sqlite`.
- **Archivos generados en .gitignore**: `data/processed/` y `data/reports/` son 100% reproducibles y no se committean.
