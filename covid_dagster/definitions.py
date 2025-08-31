from dagster import Definitions
from .assets.ingest import leer_datos
from .assets.checks import sin_fechas_futuras, claves_no_nulas_y_unicidad
from .assets.processing import datos_procesados

defs = Definitions(
    assets=[leer_datos, datos_procesados],
    asset_checks=[sin_fechas_futuras, claves_no_nulas_y_unicidad],
)
