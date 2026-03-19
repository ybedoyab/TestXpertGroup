# Exportar el informe a PDF

El pipeline genera el informe técnico como `docs/technical_report.md`.

## Opción recomendada: Pandoc

1. Instala `pandoc` (según tu SO).
2. Ejecuta:

```powershell
pandoc .\docs\technical_report.md -o .\docs\technical_report.pdf
```

## Opción alternativa: conversores Markdown -> PDF

Si no tienes `pandoc`, puedes usar cualquier conversor local (por ejemplo extensiones de VS Code o herramientas en línea).
Recomendación: convertir a PDF en modo “local file” para que conserve links/formatos.

## Verificación

Antes de enviar, abre el PDF generado y verifica que:
- La estructura incluye las secciones solicitadas.
- Las cifras provienen del dataset (no son texto genérico).

