import pandas as pd
import numpy as np

# ---------- QC pregnancy-level ----------
def run_qc_pregnancy(csv_path,
                     out_prefix="qc_preg",
                     export=True):
    # 1) load
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} rows from {csv_path}")

    # 2) completeness
    required_cols = ["PSTV01", "age", "age_risk", "dom", "subsid"]
    missing_req = df[df[required_cols].isnull().any(axis=1)]

    # cari kolom prefix a_, b_, c_ yang ada di file
    a_cols = [c for c in df.columns if c.startswith("a_")]
    b_cols = [c for c in df.columns if c.startswith("b_")]
    c_cols = [c for c in df.columns if c.startswith("c_")]
    abc_cols = a_cols + b_cols + c_cols

    if len(abc_cols) == 0:
        # kalau memang tidak ada kolom a_/b_/c_ di file, seluruh baris dianggap gagal kriteria "minimal satu a_/b_/c_"
        missing_abc = df.copy()
    else:
        missing_abc = df[df[abc_cols].isnull().all(axis=1)]

    completeness_fail = pd.concat([missing_req, missing_abc]).drop_duplicates()

    # 3) accuracy (umur)
    age_oob = df[(df["age"] < 12) | (df["age"] > 55)]
    oob_patients = df[df["PSTV01"].isin(age_oob["PSTV01"])]

    # 4) consistency (duplikasi PSTV01 + n_preg)
    if "n_preg" not in df.columns:
        raise KeyError("Kolom 'n_preg' tidak ditemukan di file. Pastikan 'final set.csv' punya kolom n_preg.")

    dup_mask = df.duplicated(subset=["PSTV01", "n_preg"], keep=False)
    dup_pairs = df[dup_mask].sort_values(["PSTV01", "n_preg"])

    # opsional: rekap jumlah dupe per key
    dup_summary = (dup_pairs
                   .groupby(["PSTV01", "n_preg"])
                   .size()
                   .reset_index(name="dup_count")
                   .sort_values("dup_count", ascending=False))

    # 5) validation: c_abortive co-occurrence with delivery/labor vars
    requested_conflict_cols = [
        "c_preecl", "c_ecl", "c_anh", "c_prev", "c_previa", "c_abrupt",
        "c_polyhydra", "c_abnamnio", "c_prom", "c_prolong", "c_preterm",
        "c_fail", "c_abnforce", "c_long", "c_malpres", "c_obspelvic", "c_iph",
        "c_distress", "c_umbilical", "c_laceration", "c_obstrau", "c_pph",
        "c_retained", "c_normal", "c_instrum", "c_caesar", "c_assisted",
        "c_multiple",
    ]
    present_conflict_cols = [c for c in requested_conflict_cols if c in df.columns]

    if "c_abortive" not in df.columns:
        raise KeyError("Kolom 'c_abortive' tidak ditemukan untuk validasi.")

    if present_conflict_cols:
        conflict_sum = df[present_conflict_cols].fillna(0).sum(axis=1)
        validation_mask = (df["c_abortive"] == 1) & (conflict_sum > 0)
        validation_conflicts = df[validation_mask]
    else:
        validation_conflicts = df.iloc[0:0].copy()

    # 6) ringkasan
    summary = {
        "rows_total": len(df),
        "completeness_fail": len(completeness_fail),
        "age_out_of_range": len(age_oob),
        "duplicate_pstv01_npreg_keys": dup_summary.shape[0],
        "validation_conflicts": len(validation_conflicts),
    }

    # 7) export
    if export:
        completeness_fail.to_csv(f"{out_prefix}_completeness_fail.csv", index=False)
        age_oob.to_csv(f"{out_prefix}_age_oob_rows.csv", index=False)
        oob_patients.to_csv(f"{out_prefix}_age_oob_patients_all_rows.csv", index=False)
        dup_pairs.to_csv(f"{out_prefix}_duplicates_rows.csv", index=False)
        dup_summary.to_csv(f"{out_prefix}_duplicates_summary.csv", index=False)
        # explicit file name as requested
        validation_conflicts.to_csv("qc_final_validation.csv", index=False)
        pd.DataFrame([summary]).to_markdown(f"{out_prefix}_summary.md", index=False)

    return summary


# ---------- main ----------
if __name__ == "__main__":
    summary = run_qc_pregnancy(
        csv_path="final_set.csv",
        out_prefix="qc_pregnancy",
        export=True
    )
    print("\nPregnancy-level QC summary:")
    print(summary)
