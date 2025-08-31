from dagster import asset
import pandas as pd

@asset(name="metrica_incidencia_7d", compute_kind="pandas",
       description="Incidencia acumulada a 7 días por 100k hab.")
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy().sort_values(["country", "date"])
    # Ajuste: negativos a 0 solo para la métrica (documentado)
    casos = df["new_cases"].clip(lower=0)
    df["incidencia_diaria"] = (casos / df["population"]) * 100_000
    df["incidencia_7d"] = (
        df.groupby("country")["incidencia_diaria"]
          .rolling(7, min_periods=7).mean()
          .reset_index(level=0, drop=True)
    )
    out = df[["date", "country", "incidencia_7d"]].dropna().reset_index(drop=True)
    return out

@asset(name="metrica_factor_crec_7d", compute_kind="pandas",
       description="Factor de crecimiento semanal: semana_actual / semana_previa.")
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy().sort_values(["country", "date"])
    # Suma móvil de 7 días (usamos casos ajustados a >=0 para evitar artefactos)
    casos = df["new_cases"].clip(lower=0)
    semana_actual = (
        df.assign(casos=casos)
          .groupby("country")["casos"]
          .rolling(7, min_periods=7).sum()
          .reset_index(level=0, drop=True)
          .rename("casos_semana")
    )
    semana_previa = semana_actual.shift(7).rename("casos_semana_prev")
    out = pd.concat([df[["date", "country"]], semana_actual, semana_previa], axis=1).dropna()
    out["factor_crec_7d"] = out["casos_semana"] / out["casos_semana_prev"].replace(0, pd.NA)
    # Renombramos 'date' como fin de la ventana de 7 días
    out = out.rename(columns={"date": "semana_fin"})
    out = out.dropna(subset=["factor_crec_7d"]).reset_index(drop=True)
    return out[["semana_fin", "country", "casos_semana", "factor_crec_7d"]]
