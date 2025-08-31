from dagster import asset, AssetExecutionContext
import pandas as pd
import requests
from io import StringIO
import yaml

CFG = yaml.safe_load(open("config/params.yaml"))
URL = CFG["owid_url"]

@asset(name="leer_datos", compute_kind="python", description="Lee el CSV canónico de OWID (sin transformar).")
def leer_datos(context: AssetExecutionContext) -> pd.DataFrame:
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    context.log.info(f"Filas: {len(df)}, Columnas: {len(df.columns)}")
    return df
