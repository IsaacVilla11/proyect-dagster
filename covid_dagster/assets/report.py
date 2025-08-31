from dagster import asset
import pandas as pd
import numpy as np
from pathlib import Path

@asset(
    name="reporte_excel_covid",
    compute_kind="pandas",
    description="Exporta a Excel los resultados finales: datos_procesados, incidencia_7d y factor_crec_7d.",
)
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame,
) -> str:
    Path("data").mkdir(exist_ok=True)
    path = "data/reporte_covid.xlsx"

    # Copias de trabajo
    dp = datos_procesados.copy()
    inc = metrica_incidencia_7d.copy()
    fac = metrica_factor_crec_7d.copy()

    # ---- Limpieza/format Incidencia ----
    if "date" in inc.columns:
        inc["date"] = pd.to_datetime(inc["date"], errors="coerce")
    if "incidencia_7d" in inc.columns:
        inc["incidencia_7d"] = pd.to_numeric(inc["incidencia_7d"], errors="coerce")
        inc["incidencia_7d"] = inc["incidencia_7d"].round(3)

    # ---- Limpieza/format Factor de crecimiento ----
    if "semana_fin" in fac.columns:
        fac["semana_fin"] = pd.to_datetime(fac["semana_fin"], errors="coerce")

    # Fuerza numérico y limpia infinitos antes de redondear
    if "casos_semana" in fac.columns:
        fac["casos_semana"] = pd.to_numeric(fac["casos_semana"], errors="coerce")
        fac.replace({"casos_semana": {np.inf: pd.NA, -np.inf: pd.NA}}, inplace=True)
        # Redondea y usa entero nulo-friendly
        fac["casos_semana"] = fac["casos_semana"].round(0).astype("Int64")

    if "factor_crec_7d" in fac.columns:
        fac["factor_crec_7d"] = pd.to_numeric(fac["factor_crec_7d"], errors="coerce")
        fac.replace({"factor_crec_7d": {np.inf: pd.NA, -np.inf: pd.NA}}, inplace=True)
        fac["factor_crec_7d"] = fac["factor_crec_7d"].round(3)

    # (Opcional) Orden de columnas amigable si existen
    inc_cols = [c for c in ["date", "country", "incidencia_7d"] if c in inc.columns]
    fac_cols = [c for c in ["semana_fin", "country", "casos_semana", "factor_crec_7d"] if c in fac.columns]

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        datos_cols = [c for c in ["country","date","new_cases","people_vaccinated","population"] if c in dp.columns]
        dp[datos_cols].to_excel(xw, sheet_name="datos_procesados", index=False)
        inc[inc_cols].to_excel(xw, sheet_name="incidencia_7d", index=False)
        fac[fac_cols].to_excel(xw, sheet_name="factor_crec_7d", index=False)

    return path
