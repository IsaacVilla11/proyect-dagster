from dagster import Definitions
from .assets.ingest import leer_datos
from .assets.processing import datos_procesados
from .assets.metrics import metrica_incidencia_7d, metrica_factor_crec_7d
from .assets.report import reporte_excel_covid
from .assets.checks import (
    sin_fechas_futuras,
    claves_no_nulas_y_unicidad,
    incidencia_rango_esperado,
)

defs = Definitions(
    assets=[
        leer_datos,
        datos_procesados,
        metrica_incidencia_7d,
        metrica_factor_crec_7d,
        reporte_excel_covid,   # << nuevo
    ],
    asset_checks=[
        sin_fechas_futuras,
        claves_no_nulas_y_unicidad,
        incidencia_rango_esperado,
    ],
)
