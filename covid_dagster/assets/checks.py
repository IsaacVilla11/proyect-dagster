from dagster import asset_check, AssetCheckResult
import pandas as pd
import yaml

def _cfg():
    with open("config/params.yaml") as f:
        return yaml.safe_load(f)

@asset_check(asset="leer_datos", name="sin_fechas_futuras")
def sin_fechas_futuras(leer_datos: pd.DataFrame) -> AssetCheckResult:
    if "date" not in leer_datos.columns:
        return AssetCheckResult(passed=False, metadata={"error": "No existe columna 'date'."})

    cfg = _cfg()
    paises = [cfg.get("ecuador", "Ecuador"), cfg.get("pais_comparativo", "Peru")]
    tol_days = int(cfg.get("allow_future_days", 0))

    df = leer_datos.copy()
    if "country" in df.columns:
        df = df[df["country"].isin(paises)]

    ds = pd.to_datetime(df["date"], errors="coerce")
    max_date = ds.max()
    today = pd.Timestamp.utcnow().date()  # comparar por fecha (día)

    if pd.isna(max_date):
        return AssetCheckResult(passed=False, metadata={"error": "No se pudo calcular max(date)."})

    delta = (max_date.date() - today).days
    passed = delta <= tol_days

    fut_rows = int((ds.dt.date > today).sum())
    return AssetCheckResult(
        passed=bool(passed),
        metadata={
            "max_date": str(max_date.date()),
            "today_utc_date": str(today),
            "delta_days": delta,
            "future_rows": fut_rows,
            "tolerance_days": tol_days,
        },
    )

@asset_check(asset="leer_datos", name="claves_no_nulas_y_unicidad")
def claves_no_nulas_y_unicidad(leer_datos: pd.DataFrame) -> AssetCheckResult:
    required = ["country", "date", "population"]
    missing = [c for c in required if c not in leer_datos.columns]
    if missing:
        return AssetCheckResult(passed=False, metadata={"faltantes": ", ".join(missing)})

    cfg = _cfg()
    paises = [cfg.get("ecuador", "Ecuador"), cfg.get("pais_comparativo", "Peru")]

    df = leer_datos.copy()
    df = df[df["country"].isin(paises)].copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce")

    base_nulls = df[["country", "date", "population"]].isna().any(axis=1).sum()
    dup = df.duplicated(["country", "date"]).sum()
    pop_le0 = (df["population"] <= 0).sum()

    passed = (base_nulls == 0) and (dup == 0) and (pop_le0 == 0)
    return AssetCheckResult(
        passed=bool(passed),
        metadata={
            "filas_con_na_en_claves": int(base_nulls),
            "duplicados_country_date": int(dup),
            "population_<=0": int(pop_le0),
            "filas_evaluadas": int(len(df)),
        },
    )

@asset_check(asset="metrica_incidencia_7d", name="incidencia_rango_esperado")
def incidencia_rango_esperado(metrica_incidencia_7d) -> AssetCheckResult:
    if not isinstance(metrica_incidencia_7d, pd.DataFrame) or "incidencia_7d" not in metrica_incidencia_7d.columns:
        return AssetCheckResult(passed=False, metadata={"error": "No hay columna 'incidencia_7d' en la métrica."})
    df = metrica_incidencia_7d
    fuera = df[(df["incidencia_7d"] < 0) | (df["incidencia_7d"] > 2000)]
    return AssetCheckResult(
        passed=fuera.empty,
        metadata={"filas_fuera_rango": int(len(fuera)), "evaluadas": int(len(df))},
    )
