import duckdb
import pandas as pd
import numpy as np

# ---------- fungsi utama QC ----------
def run_qc_visit(csv_path, out_prefix="qc_pregvisit",
                 date_min="2015-01-01", date_max="2023-12-31",
                 export=True):

    # --- 1. baca data ---
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} rows from {csv_path}")

    # --- 2. completeness ---
    required_cols = ["PSTV01", "combined_date", "age", "age_risk", "dom", "subsid"]
    dx_cols = ["FKP14A", "FKL15A", "FKL17A", "FKL24A"]

    missing_req = df[df[required_cols].isnull().any(axis=1)]
    missing_dx = df[df[dx_cols].isnull().all(axis=1)]
    completeness_fail = pd.concat([missing_req, missing_dx]).drop_duplicates()

    # --- 3. accuracy ---
    age_oob = df[(df["age"] < 12) | (df["age"] > 55)]
    oob_patients = df[df["PSTV01"].isin(age_oob["PSTV01"])]

    # --- 4. consistency ---
    date_cols = [c for c in ["combined_date", "ref_start", "fin_g"] if c in df.columns]
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    out_of_range = df[
        ((df[date_cols] < date_min).any(axis=1)) |
        ((df[date_cols] > date_max).any(axis=1))
    ]

    # --- ringkasan sederhana ---
    summary = {
        "rows_total": len(df),
        "completeness_fail": len(completeness_fail),
        "age_out_of_range": len(age_oob),
        "temporal_out_of_range": len(out_of_range)
    }

    # --- 5. simpan bila perlu ---
    if export:
        completeness_fail.to_csv(f"{out_prefix}_completeness_fail.csv", index=False)
        age_oob.to_csv(f"{out_prefix}_age_oob_rows.csv", index=False)
        oob_patients.to_csv(f"{out_prefix}_age_oob_patients_all_rows.csv", index=False)
        out_of_range.to_csv(f"{out_prefix}_date_window_violations.csv", index=False)
        pd.DataFrame([summary]).to_markdown(f"{out_prefix}_summary.md", index=False)

    return summary


# ---------- main ----------
if __name__ == "__main__":
    summary = run_qc_visit(
        csv_path="pregnancy by visit.csv",
        out_prefix="qc_pregvisit",
        export=True
    )
    print("\nQC summary:")
    print(summary)
