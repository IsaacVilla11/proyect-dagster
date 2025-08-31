from dagster import asset, AssetExecutionContext
import pandas as pd
import yaml

ESSENTIAL = ["country", "date", "new_cases", "people_vaccinated", "population"]

def _load_cfg():
    with open("config/params.yaml") as f:
        return yaml.safe_load(f)

@asset(
    name="datos_procesados",
    compute_kind="pandas",
    description=(
        "Limpia nulos/duplicados, filtra Ecuador + país comparativo y devuelve "
        "las columnas esenciales listas para métricas."
    ),
)
def datos_procesados(context: AssetExecutionContext, leer_datos: pd.DataFrame) -> pd.DataFrame:
    cfg = _load_cfg()
    pais_ecuador = cfg.get("ecuador", "Ecuador")
    pais_cmp = cfg.get("pais_comparativo", "Peru")

    df = leer_datos.copy()

    missing = [c for c in ESSENTIAL if c not in df.columns]
    if missing:
        raise RuntimeError(f"Faltan columnas obligatorias para el procesamiento: {missing}.")

    # Normalización de tipos
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["new_cases"] = pd.to_numeric(df["new_cases"], errors="coerce")
    df["people_vaccinated"] = pd.to_numeric(df["people_vaccinated"], errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce")

    # Filtrar fechas futuras (seguridad para métricas)
    today = pd.Timestamp.utcnow().date()
    before_fut = len(df)
    df = df[df["date"].dt.date <= today]
    fut_removed = before_fut - len(df)

    # Eliminar duplicados por (country, date)
    before = len(df)
    df = df.drop_duplicates(subset=["country", "date"], keep="last")
    dups_removed = before - len(df)

    # Eliminar filas con NA en new_cases o people_vaccinated
    before = len(df)
    df = df.dropna(subset=["new_cases", "people_vaccinated"])
    na_removed = before - len(df)

    # Filtrar países
    df = df[df["country"].isin([pais_ecuador, pais_cmp])]

    # Seleccionar columnas esenciales
    df = df[ESSENTIAL].sort_values(["country", "date"])

    context.log.info(
        f"Procesado OK | países: {pais_ecuador}, {pais_cmp} | "
        f"future_removed={fut_removed} | dup={dups_removed} | "
        f"na_removed={na_removed} | filas_finales={len(df)}"
    )

    return df
