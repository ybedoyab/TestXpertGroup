# Informe Técnico - Calidad de Datos Hospitalarios

## Resumen ejecutivo
- Completitud global (pct no nulos): 82.77% -> 84.27% (delta +1.50 pp).
- Integridad referencial FK (pct válido entre no-nulos): 98.09% -> 100.00%.
- Rechazos: pacientes=0 / citas=190.

## Portada
- Dataset de entrada: `dataset_hospital 2.json`
- Fecha de referencia (edad derivada): `2026-03-18`

## Objetivo
Evaluar la calidad del dataset, aplicar limpieza y validaciones con trazabilidad, y generar métricas antes/después junto con exportables listos para auditoría y migración.

## Descripción del dataset
El dataset contiene al menos dos tablas: `pacientes` y `citas_medicas`. Las llaves principales son `pacientes.id_paciente` (entero) y `citas_medicas.id_cita` (UUID). La integridad referencial se basa en `citas_medicas.id_paciente -> pacientes.id_paciente`.

## Enfoque metodológico
1. Ingesta determinista en `pandas.DataFrame`.
2. Profiling exploratorio antes de limpieza (nulos, duplicados, formatos, cardinalidad).
3. Limpieza conservadora con reglas explícitas y auditoría por registro/campo.
4. Validaciones cruzadas e identificación de huérfanos/rechazos.
5. Métricas de calidad antes y después y resumen ejecutable.
6. Bonus: simulación de carga a un modelo tipo Data Warehouse (SQLite).

<pdf:nextpage />
<div style="page-break-inside: avoid; margin-bottom: 10px;">
<h2 style="color: #2980B9; font-size: 15px; margin-top: 15px; border-bottom: 1px solid #BDC3C7; padding-bottom: 3px;">Hallazgos principales de calidad y recuperabilidad detectada</h2>
<table style="width: 100%; border: none; table-layout: fixed;">
<tr>
<td style="width: 50%; vertical-align: top; border: none; padding-right: 10px; line-height: 1.2;">
<h3>Tabla <code>pacientes</code></h3>&bull; <b>Filas:</b> 5010<br>&bull; <b>Nulos (top):</b><br>&nbsp;&nbsp;&bull; <code>email</code>: 2506<br>&nbsp;&nbsp;&bull; <code>telefono</code>: 1668<br>&nbsp;&nbsp;&bull; <code>edad</code>: 1647<br>&nbsp;&nbsp;&bull; <code>sexo</code>: 1023<br>&nbsp;&nbsp;&bull; <code>ciudad</code>: 827<br>&bull; <b>Formatos anómalos:</b><br>&nbsp;&nbsp;&bull; <code>fecha_nacimiento_invalid_raw</code>: 4<br>&nbsp;&nbsp;&bull; <code>fecha_nacimiento_recovered_by_swap</code>: 3<br>&nbsp;&nbsp;&bull; <code>fecha_nacimiento_final_null</code>: 1<br>&nbsp;&nbsp;&bull; <code>sexo_invalid</code>: 0<br>&nbsp;&nbsp;&bull; <code>email_invalid</code>: 0<br>&nbsp;&nbsp;&bull; <code>telefono_invalid</code>: 0<br>
</td>
<td style="width: 50%; vertical-align: top; border: none; padding-left: 10px; line-height: 1.2;">
<h3>Tabla <code>citas_medicas</code></h3>&bull; <b>Filas:</b> 9961<br>&bull; <b>Nulos (top):</b><br>&nbsp;&nbsp;&bull; <code>fecha_cita</code>: 3278<br>&nbsp;&nbsp;&bull; <code>estado_cita</code>: 2542<br>&nbsp;&nbsp;&bull; <code>medico</code>: 2033<br>&nbsp;&nbsp;&bull; <code>costo</code>: 1724<br>&nbsp;&nbsp;&bull; <code>especialidad</code>: 1673<br>&bull; <b>Formatos anómalos:</b><br>&nbsp;&nbsp;&bull; <code>fecha_cita_invalid_raw</code>: 3314<br>&nbsp;&nbsp;&bull; <code>fecha_cita_recovered_by_swap</code>: 3314<br>&nbsp;&nbsp;&bull; <code>fecha_cita_final_null</code>: 0<br>&nbsp;&nbsp;&bull; <code>estado_cita_invalid</code>: 0<br>&nbsp;&nbsp;&bull; <code>costo_non_numeric</code>: 0<br>&nbsp;&nbsp;&bull; <code>costo_negative</code>: 0<br>
</td>
</tr>
</table>
</div>
## Reglas de validación implementadas
- Sexo: normalización a catálogo `{M,F}`; valores no mapeables se dejan `NULL` y se registran.
- Estado de cita: normalización a `{Completada, Cancelada, Reprogramada}`; no mapeables -> `NULL`.
- Fechas: parsing conservador `YYYY-MM-DD` y recuperación controlada `YYYY-DD-MM` solo cuando falla el parse inicial (por mes/día inválidos); si ambas fallan -> `NULL`.
- Emails: formato válido vía regex; inválidos -> `NULL`.
- Teléfonos: normalización a dígitos y validación de longitud razonable; fuera de rango -> `NULL`.
- Edad: cálculo derivado desde `fecha_nacimiento` usando `REFERENCE_DATE`; se corrige `edad` solo si está `NULL` o difiere por más de la tolerancia configurada (±2 años).
- Costo: debe ser numérico y no negativo; no numérico/negativo -> registro rechazado.
- Integridad referencial: citas sin `id_paciente` existente -> registro rechazado.

## Estrategia de limpieza aplicada
La limpieza evita inferencias clínicas/dudosas: no se infiere sexo desde el nombre. Las correcciones ambiguas se documentan como `warning` y quedan en auditoría. Las reglas que no son corregibles automáticamente (por ejemplo costo inválido o huérfanos) se trasladan a datasets `*_rejected.csv`.

## Supuestos adoptados
- Fechas se interpretan inicialmente como `YYYY-MM-DD`. Cuando ese parse falla por inconsistencia de mes/día, se intenta un swap conservador `YYYY-DD-MM` y si no aplica, se deja `NULL`.
- Teléfonos se consideran plausibles si tienen entre 10 y 15 dígitos.
- Edad razonable se acota a `[0,120]` años; valores fuera se consideran inconsistentes.
- Para dimensiones en el DWH, `NULL` se mapea a un miembro `UNKNOWN` (key=0).


## Métricas de calidad (antes y después)
- Completitud global (pct de celdas no nulas): 82.77% -> 84.27%
- Integridad referencial FK valid (pct entre no-nulos): 98.09% -> 100.00%

### Cambios de completitud (campos clave)
- `pacientes.email`: 49.98% -> 49.94% (delta -0.04 pp)
- `pacientes.telefono`: 66.71% -> 66.72% (delta +0.01 pp)
- `pacientes.sexo`: 79.58% -> 79.56% (delta -0.02 pp)
- `pacientes.fecha_nacimiento`: 100.00% -> 99.98% (delta -0.02 pp)
- `citas_medicas.estado_cita`: 74.48% -> 74.54% (delta +0.06 pp)
- `citas_medicas.fecha_cita`: 67.09% -> 67.02% (delta -0.07 pp)
- `citas_medicas.costo`: 82.69% -> 82.67% (delta -0.02 pp)
- `citas_medicas.medico`: 79.59% -> 79.60% (delta +0.01 pp)
- `citas_medicas.especialidad`: 83.20% -> 83.17% (delta -0.03 pp)

## Casos no corregidos automáticamente (y por qué)
- Regla de costo no numérico/negativo: 0 rechazos en esta ejecución (sin activación en los registros procesados).
- Citas huérfanas (FK `id_paciente` no existe): se rechazan porque la tabla puente no tiene entidad padre.
- Duplicados exactos de PK: se deduplicaron conservando la primera ocurrencia y registrando el evento en auditoría (no se incluyen en `*_rejected.csv`).

## Resultados de auditoría y rechazos
- pacientes_rejected_rows: 0
- citas_medicas_rejected_rows: 190

### Top reglas por número de eventos de auditoría
- **R_EDAD_FILLED_FROM_DERIVADA**: 1647 (severidad principal: info)
- **R_EDAD_INCONSISTENT_WITH_DERIVED**: 1614 (severidad principal: warning)
- **R_ORPHAN_CITA**: 190 (severidad principal: error)
- **R_DUP_PK_EXACT**: 10 (severidad principal: info)
- **R_FECHA_NAC_INVALID**: 1 (severidad principal: warning)

## Impacto por regla de calidad
| Regla | Tabla | Campo | Acción | Severidad | Registros |
|---|---|---|---|---|---|
| **R EDAD FILLED FROM DERIVADA** | pacientes | edad | Corrección automática: rellenar edad | info | 1647 |
| **R EDAD INCONSISTENT WITH DERIVED** | pacientes | edad | Corrección automática: ajustar edad a derivada | warning | 1614 |
| **R ORPHAN CITA** | citas_medicas | id_paciente | Rechazo por FK huérfana | error | 190 |
| **R DUP PK EXACT** | pacientes | id_paciente | Deduplicación exacta (keep first) | info | 10 |
| **R FECHA NAC INVALID** | pacientes | fecha_nacimiento | Nulificación: fecha no parseable | warning | 1 |

## Evidencias (ejemplos controlados)
- **R FECHA NAC INVALID** -> pacientes (columna `fecha_nacimiento`) (row_id=84): original=1959-06-33, limpio=NULL
  - _Justificación:_ La fecha provista ('1959-06-33') tiene un formato de texto irreconocible o contiene un día/mes matemáticamente imposible en el calendario. Fue nulificada.
- **R EDAD FILLED FROM DERIVADA** -> pacientes (columna `edad`) (row_id=0): original=nan (nació: 1954-01-08), limpio=72.0
  - _Justificación:_ El paciente no tenía registrada la edad, pero sí se conocía el dato original: nan (nació: 1954-01-08). Se calculó e imputó a 72.0 comparando la fecha de nacimiento contra la fecha de referencia.
- **R EDAD INCONSISTENT WITH DERIVED** -> pacientes (columna `edad`) (row_id=1): original=58.0 (nació: 1965-01-01), limpio=61.0
  - _Justificación:_ La dupla capturada de edad y nacimiento (58.0 (nació: 1965-01-01)) se contradice matemáticamente. Se prefirió corregir la edad a 61.0 respetando la fecha de nacimiento como fuente de la verdad.
- **R DUP PK EXACT** -> pacientes (columna `id_paciente`) (row_id=5000): original=500, limpio=NULL
  - _Justificación:_ Fila descartada por ser 100% idéntica a otra ocurrencia. Se conservó la primera para no duplicar volumetría.
- **R ORPHAN CITA** -> citas_medicas (columna `id_paciente`) (row_id=183): original=6416, limpio=NULL
  - _Justificación:_ Registro rechazado íntegramente por romper integridad referencial (el paciente referenciado no existe en la dimensión de pacientes).

## Consideraciones de gobierno de datos
- Clasificación por severidad: `info` (observación/cambio no destructivo), `warning` (nulificación/normalización), `error` (rechazo por inviabilidad).
- Separación explícita entre: corrección automática, nulificación técnica y rechazados para revisión.
- Reglas reproducibles: la edad derivada depende de `REFERENCE_DATE` configurable por CLI.
- Contrato de datos sugerido: validar en ingestión catálogos (`sexo`, `estado_cita`) y formatos (`fecha_*`, `telefono`, `email`).
- Dueño de datos: definir data owner para catálogos (evitar variantes) y para decidir criterios de rejected vs NULL en producción.

## Bonus: Diseño del modelo Data Warehouse (simulado en SQLite)
Se propone un modelo en estrella con grain a nivel de fila de cita médica:
- `fact_citas`: una fila por `id_cita` (natural key preservada como `id_cita`).
- `dim_paciente`: dimensión con `id_paciente` (natural key) y atributos normalizados.
- `dim_medico` y `dim_especialidad`: catálogos derivados de `medico` y `especialidad`.
- `dim_fecha`: tabla calendario a partir de `fecha_cita` (y un miembro `UNKNOWN` para NULL).
- Manejo de NULLs: se mapea a `UNKNOWN` en dimensiones para no romper FKs.
- Evidencia de carga (conteos): pacientes=5000, médicos=5, especialidades=6, fechas=1307, fact_citas=9771.

## Recomendaciones de mejora futura
- Definir un contrato de datos (schema) y validarlo en ingestión (por ejemplo, con Pandera/Great Expectations).
- Estabilizar la generación de `estado_cita` y `sexo` en fuente (evitar múltiples variantes).
- Estandarizar el formato de fechas y teléfonos en origen para reducir `NULL` y parsing fallido.
- Añadir reglas de linaje/ID consistentes para reducir discrepancias en PK.

## Conclusión
El pipeline implementa limpieza conservadora, validaciones cruzadas y trazabilidad a nivel de registro/campo. Los datasets exportados `*_clean.csv` y `data_quality_issues.csv` quedan listos para auditoría y para una carga posterior a un modelo tipo Data Warehouse.
Las reducciones marginales de completitud en algunos campos responden a una decisión deliberada de calidad: valores inválidos o no confiables fueron nulificados para priorizar validez y consistencia sobre completitud artificial.
Se priorizó validez, consistencia y trazabilidad sobre imputaciones no verificables, dejando evidencia explícita de cada corrección, nulificación y rechazo.
