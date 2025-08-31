from dagster import asset_check, AssetCheckResult
import pandas as pd

@asset_check(asset="leer_datos", name="sin_fechas_futuras")
def sin_fechas_futuras(leer_datos: pd.DataFrame) -> AssetCheckResult:
    if "date" not in leer_datos.columns:
        return AssetCheckResult(passed=False, metadata={"error": "No existe columna 'date'."})
    ds = pd.to_datetime(leer_datos["date"], errors="coerce")
    max_date = ds.max()
    if pd.notna(max_date) and getattr(max_date, "tzinfo", None) is None:
        max_date = max_date.tz_localize("UTC")
    today = pd.Timestamp.now(tz="UTC").normalize()
    passed = pd.notna(max_date) and (max_date <= today)
    return AssetCheckResult(
        passed=bool(passed),
        metadata={"max_date": str(max_date), "today_utc": str(today)},
    )

@asset_check(asset="leer_datos", name="claves_no_nulas_y_unicidad")
def claves_no_nulas_y_unicidad(leer_datos: pd.DataFrame) -> AssetCheckResult:
    required = ["country", "date", "population"]
    missing = [c for c in required if c not in leer_datos.columns]
    if missing:
        return AssetCheckResult(passed=False, metadata={"faltantes": ", ".join(missing)})

    df = leer_datos.copy()
    base_nulls = df[required].isna().any(axis=1).sum()
    dup = df.duplicated(["country", "date"]).sum()
    pop_le0 = (pd.to_numeric(df["population"], errors="coerce") <= 0).sum()

    passed = (base_nulls == 0) and (dup == 0) and (pop_le0 == 0)
    return AssetCheckResult(
        passed=bool(passed),
        metadata={
            "filas_con_na_en_claves": int(base_nulls),
            "duplicados_country_date": int(dup),
            "population_<=0": int(pop_le0),
        },
    )
