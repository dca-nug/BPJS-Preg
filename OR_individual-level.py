import duckdb

# OR Abortus (O00-O08) vs pregnancy condition groups + key demographics
# Input:  pregnancy by visit.csv (per-visit rows)
# Patient key: PSTV01
# Diagnose cols: FKP14A, FKL15A, FKL17A, FKL24A (first 3 chars)
# Output:
#   - OR_abortus_combined.csv -> pregnancy group + demographic rows and n_preg pair comparisons

db = duckdb.connect(database=':memory:')

# Cache visits so the CSV is read once
db.execute(
    """
    CREATE OR REPLACE TEMP TABLE visits AS
    SELECT *
    FROM read_csv_auto('pregnancy by visit.csv', HEADER=TRUE)
    WHERE age BETWEEN 12 AND 55
    """
)

# Normalize ICD codes per patient
db.execute(
    """
    CREATE OR REPLACE TEMP TABLE diagnoses AS
    SELECT CAST(PSTV01 AS VARCHAR) AS PSTV01,
           LEFT(UPPER(TRIM(FKP14A)), 3) AS icd_code
    FROM visits
    WHERE FKP14A IS NOT NULL AND LEFT(UPPER(TRIM(FKP14A)), 3) ~ '^[A-Z][0-9]{2}$'

    UNION ALL
    SELECT CAST(PSTV01 AS VARCHAR) AS PSTV01,
           LEFT(UPPER(TRIM(FKL15A)), 3) AS icd_code
    FROM visits
    WHERE FKL15A IS NOT NULL AND LEFT(UPPER(TRIM(FKL15A)), 3) ~ '^[A-Z][0-9]{2}$'

    UNION ALL
    SELECT CAST(PSTV01 AS VARCHAR) AS PSTV01,
           LEFT(UPPER(TRIM(FKL17A)), 3) AS icd_code
    FROM visits
    WHERE FKL17A IS NOT NULL AND LEFT(UPPER(TRIM(FKL17A)), 3) ~ '^[A-Z][0-9]{2}$'

    UNION ALL
    SELECT CAST(PSTV01 AS VARCHAR) AS PSTV01,
           LEFT(UPPER(TRIM(FKL24A)), 3) AS icd_code
    FROM visits
    WHERE FKL24A IS NOT NULL AND LEFT(UPPER(TRIM(FKL24A)), 3) ~ '^[A-Z][0-9]{2}$'
    """
)

# Abortus status per patient (1 = has ICD O00-O08 anywhere)
db.execute(
    """
    CREATE OR REPLACE TEMP TABLE patient_abortus_status AS
    SELECT PSTV01,
           CASE WHEN COUNT(CASE WHEN icd_code ~ '^O0[0-8]$' THEN 1 END) > 0 THEN 1 ELSE 0 END AS has_abortus
    FROM diagnoses
    GROUP BY PSTV01
    """
)

# Aggregate patient-level attributes for age_risk/dom/subsid/n_preg
db.execute(
    """
    CREATE OR REPLACE TEMP TABLE patient_attrs AS
    SELECT
        CAST(PSTV01 AS VARCHAR) AS PSTV01,
        MAX(TRY_CAST(age_risk AS INTEGER)) AS age_risk,
        MAX(TRY_CAST(dom AS INTEGER)) AS dom,
        MAX(TRY_CAST(subsid AS INTEGER)) AS subsid,
        MAX(TRY_CAST(n_preg AS INTEGER)) AS n_preg
    FROM visits
    GROUP BY 1
    """
)

combined_or_sql = """
WITH preg_map AS (
    SELECT * FROM (
        VALUES
        ('preecl','O11'), ('preecl','O14'),
        ('ecl','O15'),
        ('earlyhemo','O20'),
        ('heg','O21'),
        ('venpreg','O22'),
        ('utipreg','O23'),
        ('malpreg','O25'),
        ('multigest','O30'),
        ('malpresent','O32'),
        ('disprop','O33'),
        ('abnorpelv','O34'),
        ('fetalprob','O35'), ('fetalprob','O36'),
        ('polyhydra','O40'),
        ('abnamnio','O41'),
        ('prom','O42'),
        ('placental','O43'),
        ('previa','O44'),
        ('abrupt','O45'),
        ('anh','O46'),
        ('prolong','O48'),
        ('preterm','O60'),
        ('fail','O61'),
        ('abnforce','O62'),
        ('long','O63'),
        ('malpres','O64'),
        ('obspelvic','O65'), ('obspelvic','O66'),
        ('iph','O67'),
        ('distress','O68'),
        ('umbilical','O69'),
        ('laceration','O70'),
        ('obstrau','O71'),
        ('pph','O72'),
        ('retained','O73'),
        ('normal','O80'),
        ('instrum','O81'),
        ('caesar','O82'),
        ('assisted','O83'),
        ('multiple','O84'),
        ('abortive','O00'), ('abortive','O01'), ('abortive','O02'), ('abortive','O03'),
        ('abortive','O04'), ('abortive','O05'), ('abortive','O06'), ('abortive','O07'),
        ('abortive','O08')
    ) AS t(group_name, icd3)
),
icd_groups AS (
    SELECT DISTINCT d.PSTV01, pm.group_name
    FROM diagnoses d
    JOIN preg_map pm ON d.icd_code = pm.icd3
),
additional_groups AS (
    SELECT PSTV01, 'age_risk' AS group_name
    FROM patient_attrs
    WHERE age_risk = 1

    UNION ALL
    SELECT PSTV01, 'dom' AS group_name
    FROM patient_attrs
    WHERE dom = 1

    UNION ALL
    SELECT PSTV01, 'subsid' AS group_name
    FROM patient_attrs
    WHERE subsid = 1
),
patient_preg_groups AS (
    SELECT PSTV01, group_name FROM icd_groups
    UNION ALL
    SELECT PSTV01, group_name FROM additional_groups
),
totals AS (
    SELECT
        SUM(CASE WHEN has_abortus = 1 THEN 1 ELSE 0 END) AS total_abortus,
        SUM(CASE WHEN has_abortus = 0 THEN 1 ELSE 0 END) AS total_non_abortus
    FROM patient_abortus_status
),
contingency AS (
    SELECT
        ppg.group_name,
        COUNT(*) FILTER (WHERE pas.has_abortus = 1) AS a,
        COUNT(*) FILTER (WHERE pas.has_abortus = 0) AS c,
        totals.total_abortus,
        totals.total_non_abortus
    FROM patient_preg_groups ppg
    JOIN patient_abortus_status pas USING (PSTV01)
    CROSS JOIN totals
    GROUP BY ppg.group_name, totals.total_abortus, totals.total_non_abortus
),
or_calc AS (
    SELECT
        group_name,
        a,
        (total_abortus - a) AS b,
        c,
        (total_non_abortus - c) AS d,
        total_abortus,
        total_non_abortus,
        CASE WHEN a > 0 AND (total_abortus - a) > 0 AND c > 0 AND (total_non_abortus - c) > 0
             THEN (a * (total_non_abortus - c)) / ((total_abortus - a) * c)
             ELSE NULL END AS or_naive,
        ((a+0.5) * (total_non_abortus - c + 0.5)) / ((total_abortus - a + 0.5) * (c + 0.5)) AS or_corr,
        CASE WHEN total_abortus > 0 THEN CAST(a AS DOUBLE) / total_abortus ELSE 0 END AS prevalence_abortus,
        CASE WHEN total_non_abortus > 0 THEN CAST(c AS DOUBLE) / total_non_abortus ELSE 0 END AS prevalence_non_abortus
    FROM contingency
),
or_ci AS (
    SELECT
        group_name,
        a, b, c, d,
        COALESCE(or_naive, or_corr) AS odds_ratio,
        prevalence_abortus,
        prevalence_non_abortus,
        CASE WHEN or_naive IS NOT NULL AND a > 0 AND b > 0 AND c > 0 AND d > 0
             THEN EXP(LN(or_naive) - 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) - 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_lower,
        CASE WHEN or_naive IS NOT NULL AND a > 0 AND b > 0 AND c > 0 AND d > 0
             THEN EXP(LN(or_naive) + 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) + 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_upper
    FROM or_calc
),
ranked AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ABS(LN(odds_ratio)) DESC NULLS LAST) AS rank,
        group_name,
        odds_ratio,
        ci_lower,
        ci_upper,
        prevalence_abortus,
        prevalence_non_abortus,
        a, b, c, d
    FROM or_ci
),
n_preg_counts AS (
    SELECT
        pa.n_preg,
        SUM(CASE WHEN pas.has_abortus = 1 THEN 1 ELSE 0 END) AS abortus_count,
        SUM(CASE WHEN pas.has_abortus = 0 THEN 1 ELSE 0 END) AS non_abortus_count
    FROM patient_attrs pa
    JOIN patient_abortus_status pas USING (PSTV01)
    WHERE pa.n_preg BETWEEN 1 AND 9
    GROUP BY pa.n_preg
),
pairs AS (
    SELECT
        npc_ref.n_preg AS ref_n_preg,
        npc_cmp.n_preg AS cmp_n_preg,
        npc_ref.abortus_count AS a_ref,
        npc_ref.non_abortus_count AS b_ref,
        npc_cmp.abortus_count AS a_cmp,
        npc_cmp.non_abortus_count AS b_cmp
    FROM n_preg_counts npc_ref
    JOIN n_preg_counts npc_cmp
      ON npc_ref.n_preg = 1
     AND npc_cmp.n_preg BETWEEN 2 AND 9
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
        CASE WHEN or_naive IS NOT NULL AND a > 0 AND b > 0 AND c > 0 AND d > 0
             THEN EXP(LN(or_naive) - 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) - 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_lower,
        CASE WHEN or_naive IS NOT NULL AND a > 0 AND b > 0 AND c > 0 AND d > 0
             THEN EXP(LN(or_naive) + 1.96 * SQRT(1.0/a + 1.0/b + 1.0/c + 1.0/d))
             ELSE EXP(LN(or_corr) + 1.96 * SQRT(1.0/(a+0.5) + 1.0/(b+0.5) + 1.0/(c+0.5) + 1.0/(d+0.5)))
        END AS ci_upper
    FROM n_preg_or_calc
),
preg_output AS (
    SELECT
        'preg_group' AS analysis_type,
        group_name AS label,
        rank,
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
    FROM ranked
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
    f"COPY ({combined_or_sql}) TO 'OR_abortus_combined.csv' (HEADER, DELIMITER ',');"
)
