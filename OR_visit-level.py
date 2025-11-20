import duckdb

# OR Abortus (O00–O08) vs selected pregnancy condition groups only
# Visit-level computation (no patient aggregation)
# Input:  `pregnancy by visit.csv`
# Diagnose cols: FKP14A, FKL15A, FKL17A, FKL24A (first 3 chars)
# Output: `OR_abortus_preg_visitlvl.csv`

db = duckdb.connect(database=':memory:')

sql = """
COPY (
WITH base AS (
    SELECT 
        CAST(PSTV01 AS VARCHAR) AS PSTV01,
        LEFT(UPPER(TRIM(FKP14A)), 3) AS code1,
        LEFT(UPPER(TRIM(FKL15A)), 3) AS code2,
        LEFT(UPPER(TRIM(FKL17A)), 3) AS code3,
        LEFT(UPPER(TRIM(FKL24A)), 3) AS code4,
        TRY_CAST(NULLIF(TRIM(CAST(age_risk AS VARCHAR)), '') AS INTEGER) AS age_risk,
        TRY_CAST(NULLIF(TRIM(CAST(dom AS VARCHAR)), '') AS INTEGER) AS dom,
        TRY_CAST(NULLIF(TRIM(CAST(subsid AS VARCHAR)), '') AS INTEGER) AS subsid,
        TRY_CAST(NULLIF(TRIM(CAST(n_preg AS VARCHAR)), '') AS INTEGER) AS n_preg
    FROM read_csv_auto('pregnancy by visit - validation 300.csv', HEADER=TRUE)
    WHERE age BETWEEN 12 AND 55
),

visits_norm AS (
    -- Keep only visits with at least one valid ICD-10 3-char code
    SELECT 
        ROW_NUMBER() OVER () AS visit_id,
        PSTV01,
        code1, code2, code3, code4,
        age_risk,
        dom,
        subsid,
        n_preg,
        CASE WHEN 
            (code1 IS NOT NULL AND code1 ~ '^O0[0-8]$') OR
            (code2 IS NOT NULL AND code2 ~ '^O0[0-8]$') OR
            (code3 IS NOT NULL AND code3 ~ '^O0[0-8]$') OR
            (code4 IS NOT NULL AND code4 ~ '^O0[0-8]$')
        THEN 1 ELSE 0 END AS has_abortus
    FROM base
    WHERE 
        (code1 IS NOT NULL AND code1 ~ '^[A-Z][0-9]{2}$') OR
        (code2 IS NOT NULL AND code2 ~ '^[A-Z][0-9]{2}$') OR
        (code3 IS NOT NULL AND code3 ~ '^[A-Z][0-9]{2}$') OR
        (code4 IS NOT NULL AND code4 ~ '^[A-Z][0-9]{2}$')
),

overall_totals AS (
    SELECT
        COUNT(*) FILTER (WHERE has_abortus = 1) AS total_abortus,
        COUNT(*) FILTER (WHERE has_abortus = 0) AS total_non_abortus
    FROM visits_norm
),

-- Mapping of pregnancy groups to their ICD 3-char prefixes
preg_map AS (
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
        -- include abortive codes as in original mapping
        ('abortive','O00'), ('abortive','O01'), ('abortive','O02'), ('abortive','O03'),
        ('abortive','O04'), ('abortive','O05'), ('abortive','O06'), ('abortive','O07'),
        ('abortive','O08')
    ) AS t(group_name, icd3)
),

-- Presence of each selected pregnancy group per visit (row)
visit_groups AS (
    SELECT DISTINCT v.visit_id, pm.group_name
    FROM visits_norm v
    JOIN preg_map pm
      ON v.code1 = pm.icd3 OR v.code2 = pm.icd3 OR v.code3 = pm.icd3 OR v.code4 = pm.icd3
),

demographic_groups AS (
    SELECT visit_id, 'age_risk' AS group_name
    FROM visits_norm
    WHERE age_risk = 1

    UNION ALL

    SELECT visit_id, 'dom' AS group_name
    FROM visits_norm
    WHERE dom = 1

    UNION ALL

    SELECT visit_id, 'subsid' AS group_name
    FROM visits_norm
    WHERE subsid = 1
),

single_groups AS (
    SELECT visit_id, group_name FROM visit_groups
    UNION ALL
    SELECT visit_id, group_name FROM demographic_groups
),

single_counts AS (
    SELECT
        sg.group_name,
        COUNT(*) FILTER (WHERE v.has_abortus = 1) AS a,
        COUNT(*) FILTER (WHERE v.has_abortus = 0) AS c,
        ot.total_abortus,
        ot.total_non_abortus
    FROM single_groups sg
    JOIN visits_norm v USING (visit_id)
    CROSS JOIN overall_totals ot
    GROUP BY sg.group_name, ot.total_abortus, ot.total_non_abortus
),

single_contingency AS (
    SELECT
        group_name,
        a,
        (total_abortus - a) AS b,
        c,
        (total_non_abortus - c) AS d,
        total_abortus,
        total_non_abortus
    FROM single_counts
),

npreg_pairs AS (
    SELECT * FROM (
        VALUES
        (2, 'n_preg=1_vs_2'),
        (3, 'n_preg=1_vs_3'),
        (4, 'n_preg=1_vs_4'),
        (5, 'n_preg=1_vs_5'),
        (6, 'n_preg=1_vs_6')
    ) AS np(target_value, label)
),

npreg_raw AS (
    SELECT
        np.label AS group_name,
        SUM(CASE WHEN v.n_preg = 1 AND v.has_abortus = 1 THEN 1 ELSE 0 END) AS a,
        SUM(CASE WHEN v.n_preg = np.target_value AND v.has_abortus = 1 THEN 1 ELSE 0 END) AS b,
        SUM(CASE WHEN v.n_preg = 1 AND v.has_abortus = 0 THEN 1 ELSE 0 END) AS c,
        SUM(CASE WHEN v.n_preg = np.target_value AND v.has_abortus = 0 THEN 1 ELSE 0 END) AS d
    FROM npreg_pairs np
    JOIN visits_norm v
      ON v.n_preg IN (1, np.target_value)
    GROUP BY np.label
),

npreg_contingency AS (
    SELECT
        nr.group_name,
        nr.a,
        nr.b,
        nr.c,
        nr.d,
        ot.total_abortus,
        ot.total_non_abortus
    FROM npreg_raw nr
    CROSS JOIN overall_totals ot
),

contingency AS (
    SELECT * FROM single_contingency
    UNION ALL
    SELECT * FROM npreg_contingency
),

or_calc AS (
    SELECT
        group_name,
        a,
        b,
        c,
        d,
        total_abortus,
        total_non_abortus,
        CASE WHEN a > 0 AND b > 0 AND c > 0 AND d > 0
             THEN (a * d) / (b * c)
             ELSE NULL END AS or_naive,
        ((a+0.5) * (d+0.5)) / ((b+0.5) * (c+0.5)) AS or_corr,
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
)

SELECT
    rank,
    group_name,
    ROUND(odds_ratio, 3) AS odds_ratio,
    ROUND(ci_lower, 3) AS ci_lower,
    ROUND(ci_upper, 3) AS ci_upper,
    ROUND(prevalence_abortus * 100, 1) AS prevalence_abortus_pct,
    ROUND(prevalence_non_abortus * 100, 1) AS prevalence_non_abortus_pct,
    CASE WHEN ci_lower IS NOT NULL AND ci_upper IS NOT NULL AND (ci_lower > 1 OR ci_upper < 1) THEN 'Significant' ELSE 'Not Significant' END AS significance,
    a AS abortus_with_group,
    b AS abortus_without_group,
    c AS non_abortus_with_group,
    d AS non_abortus_without_group
FROM ranked
ORDER BY rank
) TO 'OR_abortus_preg_visitlvl_300.csv' (HEADER, DELIMITER ',');
"""

db.execute(sql)
