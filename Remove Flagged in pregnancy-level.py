import duckdb

"""
Filter final_set.csv with two-step dropping and report counts per step:
1) Age filter: keep age between 12 and 55
2) Validation conflicts (as defined in or_free_conflicts.py):
   - A row is conflicted if c_abortive = 1 AND any present conflict column > 0
   - Drop only the conflicted rows (row-level), not all rows for the PSTV01

Output: final_set_free_conflicts.csv
Printed counts:
- Original rows
- Dropped by age
- Rows after age filter
- Dropped by validation conflicts (row-level)
- Final rows
"""


CONFLICT_COLS = [
    'c_preecl', 'c_ecl', 'c_anh', 'c_prev', 'c_previa', 'c_abrupt',
    'c_polyhydra', 'c_abnamnio', 'c_prom', 'c_prolong', 'c_preterm',
    'c_fail', 'c_abnforce', 'c_long', 'c_malpres', 'c_obspelvic', 'c_iph',
    'c_distress', 'c_umbilical', 'c_laceration', 'c_obstrau', 'c_pph',
    'c_retained', 'c_normal', 'c_instrum', 'c_caesar', 'c_assisted',
    'c_multiple', 'c_disprop', 'c_malpresent', 'c_abnorpelv', 'c_placental'
]


def main():
    db = duckdb.connect()

    # Load CSV
    db.execute(
        """
        CREATE OR REPLACE TABLE raw_final AS
        SELECT * FROM read_csv_auto('final_set.csv')
        """
    )

    # Validate required columns
    cols = db.execute("SELECT name FROM pragma_table_info('raw_final')").fetchdf()["name"].str.lower().tolist()
    present_cols = set(cols)

    if 'pstv01' not in present_cols:
        raise RuntimeError("Kolom 'PSTV01' tidak ditemukan di final_set.csv.")
    if 'c_abortive' not in present_cols:
        raise RuntimeError("Kolom 'c_abortive' tidak ditemukan di final_set.csv.")
    if 'age' not in present_cols:
        raise RuntimeError("Kolom 'age' tidak ditemukan di final_set.csv.")

    present_conflicts = [c for c in CONFLICT_COLS if c.lower() in present_cols]

    # Counts before
    n_original = db.execute("SELECT COUNT(*) FROM raw_final").fetchone()[0]

    # Step 1: Age filter first
    db.execute(
        """
        CREATE OR REPLACE TABLE age_filtered AS
        SELECT * FROM raw_final WHERE age BETWEEN 12 AND 55
        """
    )
    n_after_age = db.execute("SELECT COUNT(*) FROM age_filtered").fetchone()[0]
    n_dropped_age = n_original - n_after_age

    # Build predicate: conflicted if c_abortive=1 and any conflict col > 0 (row-level)
    if present_conflicts:
        conflict_sum_expr = " + ".join([f"COALESCE(\"{c}\",0)" for c in present_conflicts])
        predicate = f"(COALESCE(\"c_abortive\",0)=1 AND ({conflict_sum_expr})>0)"
    else:
        predicate = "FALSE"  # no conflicts available -> no drop

    # Compute rows to drop (row-level) and create final set excluding only conflicted rows
    n_dropped = db.execute(
        f"""
        SELECT COUNT(*)
        FROM age_filtered
        WHERE {predicate}
        """
    ).fetchone()[0]

    db.execute(
        f"""
        CREATE OR REPLACE TABLE final_filtered AS
        SELECT *
        FROM age_filtered
        WHERE NOT ({predicate})
        """
    )

    n_final = db.execute("SELECT COUNT(*) FROM final_filtered").fetchone()[0]

    # Write output
    db.execute("COPY final_filtered TO 'final_set_free_conflicts.csv' (HEADER, DELIMITER ',')")

    print(f"Original rows:                 {n_original}")
    print(f"Dropped by age (12-55):        {n_dropped_age}")
    print(f"Rows after age filter:         {n_after_age}")
    print(f"Dropped by validation conflicts:{n_dropped}")
    print(f"Final rows:                    {n_final}")
    print("Output saved to final_set_free_conflicts.csv")


if __name__ == "__main__":
    main()
