import argparse
import pandas as pd
from pathlib import Path

def perfilado(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    res = {
        "n_filas": len(df),
        "n_columnas": len(df.columns),
        "columnas": [", ".join(df.columns)],
        "dtypes": [", ".join([f"{c}:{str(t)}" for c, t in df.dtypes.items()])],
        "min_new_cases": pd.to_numeric(df.get("new_cases"), errors="coerce").min(),
        "max_new_cases": pd.to_numeric(df.get("new_cases"), errors="coerce").max(),
        "pct_na_new_cases": df["new_cases"].isna().mean() if "new_cases" in df else None,
        "pct_na_people_vaccinated": df["people_vaccinated"].isna().mean() if "people_vaccinated" in df else None,
        "min_date": df["date"].min() if "date" in df else None,
        "max_date": df["date"].max() if "date" in df else None,
    }
    return pd.DataFrame([res])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Ruta al CSV (filtrado)")
    ap.add_argument("--output", default="data/tabla_perfilado.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    out = perfilado(df)

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(f"[OK] Perfilado guardado en: {args.output}")

if __name__ == "__main__":
    main()
