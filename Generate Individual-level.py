import duckdb

# Build individual-level table from visit-level data
# Source: 'pregnancy by visit.csv'
# Group by: PSTV01
# Keep variables: age, age_risk, dom, subsid, n_preg
# Aggregation: take the maximum value for each variable
# Output: 'pregnancy by individual.csv'

db = duckdb.connect(database=':memory:')

# 1) Save filtered visit-level rows (age 12–55) for visibility
sql_filtered_visits = """
COPY (
WITH src AS (
    SELECT 
        CAST(PSTV01 AS VARCHAR) AS PSTV01,
        TRY_CAST(age AS DOUBLE) AS age,
        TRY_CAST(age_risk AS BIGINT) AS age_risk,
        TRY_CAST(dom AS BIGINT) AS dom,
        TRY_CAST(subsid AS BIGINT) AS subsid,
        TRY_CAST(n_preg AS BIGINT) AS n_preg
    FROM read_csv_auto('pregnancy by visit - validation 300.csv', HEADER=TRUE)
    WHERE PSTV01 IS NOT NULL
),
filtered AS (
    SELECT * FROM src WHERE age BETWEEN 12 AND 55
)
SELECT PSTV01, age, age_risk, dom, subsid, n_preg
FROM filtered
ORDER BY PSTV01
) TO 'pregnancy by visit - filtered - validation 300.csv' (HEADER, DELIMITER ',');
"""

db.execute(sql_filtered_visits)

# 2) Aggregate filtered rows to individual-level (take maxima)
sql_individual = """
COPY (
WITH src AS (
    SELECT 
        CAST(PSTV01 AS VARCHAR) AS PSTV01,
        TRY_CAST(age AS DOUBLE) AS age,
        TRY_CAST(age_risk AS BIGINT) AS age_risk,
        TRY_CAST(dom AS BIGINT) AS dom,
        TRY_CAST(subsid AS BIGINT) AS subsid,
        TRY_CAST(n_preg AS BIGINT) AS n_preg
    FROM read_csv_auto('pregnancy by visit - validation 300.csv', HEADER=TRUE)
    WHERE PSTV01 IS NOT NULL
),
filtered AS (
    SELECT * FROM src WHERE age BETWEEN 12 AND 55
),
agg AS (
    SELECT 
        PSTV01,
        MAX(age) AS age,
        MAX(age_risk) AS age_risk,
        MAX(dom) AS dom,
        MAX(subsid) AS subsid,
        MAX(n_preg) AS n_preg
    FROM filtered
    GROUP BY PSTV01
)
SELECT * FROM agg ORDER BY PSTV01
) TO 'pregnancy by individual - validation 300.csv' (HEADER, DELIMITER ',');
"""

db.execute(sql_individual)
