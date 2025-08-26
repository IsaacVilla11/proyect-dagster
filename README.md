# Pipeline de Datos COVID-19 (Dagster)

## Requisitos
- Python 3.10+
- `pip install -r requirements.txt`

## Estructura
- `data/`: solo artefactos ligeros (p. ej., `tabla_perfilado.csv`, `reporte_covid.xlsx`)
- `scripts/`: utilidades locales (EDA manual, etc.)

## Cómo correr EDA (Paso 1)
1) Descarga manual del CSV filtrado desde OWID.
2) Ejecuta el script de perfilado:
