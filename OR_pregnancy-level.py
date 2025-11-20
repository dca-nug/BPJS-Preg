import duckdb

# OR Abortus (c_abortive) vs pregnancy-related columns + demographics from final_set.csv
# Dataset sudah dibersihkan sehingga tidak perlu lagi drop mask conflict ataupun filter age.
# Output CSV menggabungkan:
#   - OR untuk setiap kolom b_/c_/a_ kondisi kehamilan + age_risk/dom/subsid
#   - OR untuk n_preg==1 dibanding n_preg==2..9
# Hasil akhir ditulis ke or_preg_abort_combined.csv

PREGNANCY_GROUPS = [
    'preecl', 'ecl', 'earlyhemo', 'heg', 'venpreg', 'utipreg', 'malpreg', 'multigest',
    'malpresent', 'disprop', 'abnorpelv', 'fetalprob', 'polyhydra', 'abnamnio', 'prom',
    'placental', 'previa', 'abrupt', 'anh', 'prolong', 'preterm', 'fail', 'abnforce', 'long',
    'obspelvic', 'malpres', 'iph', 'distress', 'umbilical', 'laceration', 'obstrau',
    'pph', 'retained', 'normal', 'instrum', 'caesar', 'assisted', 'multiple'
]

DEMO_COLS = ['age_risk', 'dom', 'subsid']
ABORTIVE_COMPARISONS = ['b_abortive', 'a_abortive']


def build_union_blocks(columns):
    blocks = []
    for col in columns:
        qcol = f'"{col}"'
        blocks.append(
            f"""
            SELECT
              '{col}' AS var,
              SUM(CASE WHEN "c_abortive"=1 AND {qcol}=1 THEN 1 ELSE 0 END) AS a,
              SUM(CASE WHEN "c_abortive"=1 AND {qcol}=0 THEN 1 ELSE 0 END) AS b,
              SUM(CASE WHEN "c_abortive"=0 AND {qcol}=1 THEN 1 ELSE 0 END) AS c,
              SUM(CASE WHEN "c_abortive"=0 AND {qcol}=0 THEN 1 ELSE 0 END) AS d
            FROM final
            WHERE "c_abortive" IS NOT NULL
              AND {qcol} IS NOT NULL
            """
        )
    return " UNION ALL ".join(blocks)


def main():
    db = duckdb.connect()

    db.execute(
        """
        CREATE OR REPLACE TABLE final AS
        SELECT *
        FROM read_csv_auto('final_set_free_conflicts.csv')
        """
    )

    col_df = db.execute("SELECT name FROM pragma_table_info('final') ORDER BY name").fetchdf()
    col_names = col_df["name"].tolist()
    lower_to_name = {name.lower(): name for name in col_names}
    present_cols = set(lower_to_name.keys())

    if 'c_abortive' not in present_cols:
        raise RuntimeError("Kolom 'c_abortive' tidak ditemukan di final_set.csv.")
    if 'n_preg' not in present_cols:
        raise RuntimeError("Kolom 'n_preg' tidak ditemukan di final_set.csv.")

    preg_set = set(PREGNANCY_GROUPS)
    exposure_cols = []
    for name in col_names:
        lower = name.lower()
        if lower.startswith(('b_', 'c_', 'a_')):
            suffix = lower[2:]
            if suffix == 'abortive':
                continue
            if suffix in preg_set:
                exposure_cols.append(name)

    for demo in DEMO_COLS:
        if demo in present_cols:
            exposure_cols.append(lower_to_name[demo])

    for abort_col in ABORTIVE_COMPARISONS:
        if abort_col in present_cols:
            exposure_cols.append(lower_to_name[abort_col])

    if not exposure_cols:
        raise RuntimeError("Tidak ada kolom exposure yang cocok (b_/c_/a_ + demo).")

    union_sql = build_union_blocks(exposure_cols)

    combined_sql = f"""
    WITH base AS (
      {union_sql}
    ),
    stats AS (
      SELECT
        var,
        CASE
          WHEN lower(var) LIKE 'b_%' THEN 'b_'
          WHEN lower(var) LIKE 'c_%' THEN 'c_'
          WHEN lower(var) LIKE 'a_%' THEN 'a_'
          WHEN lower(var) IN ('age_risk','dom','subsid') THEN 'demo'
          ELSE 'other'
        END AS group_prefix,
        a, b, c, d,
        CASE WHEN a>0 AND b>0 AND c>0 AND d>0 THEN (a*d)/(b*c)::DOUBLE END AS or_naive,
        ((a+0.5)*(d+0.5))/((b+0.5)*(c+0.5))::DOUBLE AS or_corr,
        (a + b) AS total_abortus,
        (c + d) AS total_non_abortus
      FROM base
    ),
    or_ci AS (
      SELECT
        var,
        group_prefix,
        a, b, c, d,
        total_abortus,
        total_non_abortus,
        COALESCE(or_naive, or_corr) AS odds_ratio,
        CASE WHEN or_naive IS NOT NULL AND a>0 AND b>0 AND c>0 AND d>0
             THEN EXP(LN(or_naive) - 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) - 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_lower,
        CASE WHEN or_naive IS NOT NULL AND a>0 AND b>0 AND c>0 AND d>0
             THEN EXP(LN(or_naive) + 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) + 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_upper,
        CASE WHEN total_abortus > 0 THEN CAST(a AS DOUBLE)/total_abortus ELSE NULL END AS prevalence_abortus,
        CASE WHEN total_non_abortus > 0 THEN CAST(c AS DOUBLE)/total_non_abortus ELSE NULL END AS prevalence_non_abortus
      FROM stats
    ),
    preg_output AS (
      SELECT
        'preg_group' AS analysis_type,
        var AS label,
        ROW_NUMBER() OVER (ORDER BY ABS(LN(odds_ratio)) DESC NULLS LAST) AS rank,
        ROUND(odds_ratio, 3) AS odds_ratio,
        ROUND(ci_lower, 3) AS ci_lower,
        ROUND(ci_upper, 3) AS ci_upper,
        CASE WHEN ci_lower IS NOT NULL AND ci_upper IS NOT NULL AND (ci_lower > 1 OR ci_upper < 1) THEN 'Significant' ELSE 'Not Significant' END AS significance,
        ROUND(prevalence_abortus * 100, 1) AS prevalence_abortus_pct,
        ROUND(prevalence_non_abortus * 100, 1) AS prevalence_non_abortus_pct,
        a AS cell_a,
        b AS cell_b,
        c AS cell_c,
        d AS cell_d,
        NULL::INT AS ref_n_preg,
        NULL::INT AS cmp_n_preg
      FROM or_ci
    ),
    n_preg_counts AS (
      SELECT
        CAST(n_preg AS INTEGER) AS n_preg,
        SUM(CASE WHEN c_abortive = 1 THEN 1 ELSE 0 END) AS abortus_count,
        SUM(CASE WHEN c_abortive = 0 THEN 1 ELSE 0 END) AS non_abortus_count
      FROM final
      WHERE n_preg BETWEEN 1 AND 7
        AND c_abortive IS NOT NULL
        AND n_preg IS NOT NULL
      GROUP BY 1
    ),
    pairs AS (
      SELECT
        ref.n_preg AS ref_n_preg,
        cmp.n_preg AS cmp_n_preg,
        ref.abortus_count AS a_ref,
        ref.non_abortus_count AS b_ref,
        cmp.abortus_count AS a_cmp,
        cmp.non_abortus_count AS b_cmp
      FROM n_preg_counts ref
      JOIN n_preg_counts cmp
        ON ref.n_preg = 1
       AND cmp.n_preg BETWEEN 2 AND 7
    ),
    n_preg_or_calc AS (
      SELECT
        ref_n_preg,
        cmp_n_preg,
        a_ref AS a,
        b_ref AS b,
        a_cmp AS c,
        b_cmp AS d,
        CASE WHEN a_ref > 0 AND b_ref > 0 AND a_cmp > 0 AND b_cmp > 0
             THEN (a_ref * b_cmp)::DOUBLE / (b_ref * a_cmp)
             ELSE NULL END AS or_naive,
        ((a_ref + 0.5) * (b_cmp + 0.5))::DOUBLE / ((b_ref + 0.5) * (a_cmp + 0.5)) AS or_corr
      FROM pairs
    ),
    n_preg_or_ci AS (
      SELECT
        ref_n_preg,
        cmp_n_preg,
        a, b, c, d,
        COALESCE(or_naive, or_corr) AS odds_ratio,
        CASE WHEN or_naive IS NOT NULL AND a>0 AND b>0 AND c>0 AND d>0
             THEN EXP(LN(or_naive) - 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) - 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_lower,
        CASE WHEN or_naive IS NOT NULL AND a>0 AND b>0 AND c>0 AND d>0
             THEN EXP(LN(or_naive) + 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) + 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_upper
      FROM n_preg_or_calc
    ),
    n_preg_output AS (
      SELECT
        'n_preg_pair' AS analysis_type,
        CONCAT('n_preg_', ref_n_preg, '_vs_', cmp_n_preg) AS label,
        ROW_NUMBER() OVER (ORDER BY cmp_n_preg) AS rank,
        ROUND(odds_ratio, 3) AS odds_ratio,
        ROUND(ci_lower, 3) AS ci_lower,
        ROUND(ci_upper, 3) AS ci_upper,
        CASE WHEN ci_lower IS NOT NULL AND ci_upper IS NOT NULL AND (ci_lower > 1 OR ci_upper < 1) THEN 'Significant' ELSE 'Not Significant' END AS significance,
        NULL::DOUBLE AS prevalence_abortus_pct,
        NULL::DOUBLE AS prevalence_non_abortus_pct,
        a AS cell_a,
        b AS cell_b,
        c AS cell_c,
        d AS cell_d,
        ref_n_preg,
        cmp_n_preg
      FROM n_preg_or_ci
    ),
    combined AS (
      SELECT * FROM preg_output
      UNION ALL
      SELECT * FROM n_preg_output
    )
    SELECT
      analysis_type,
      label,
      rank,
      odds_ratio,
      ci_lower,
      ci_upper,
      significance,
      prevalence_abortus_pct,
      prevalence_non_abortus_pct,
      cell_a,
      cell_b,
      cell_c,
      cell_d,
      ref_n_preg,
      cmp_n_preg
    FROM combined
    ORDER BY analysis_type, rank
    """

    db.execute(
        f"COPY ({combined_sql}) TO 'or_preg_abort_combined.csv' (HEADER, DELIMITER ',');"
    )
    print("Hasil disimpan ke or_preglvl_abort.csv")


if __name__ == '__main__':
    main()
