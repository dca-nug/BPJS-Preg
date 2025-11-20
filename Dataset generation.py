# Auto-generated from Re-Re DB.ipynb
# Converted via PowerShell because jupyter/nbconvert not available

import pandas as pd
import duckdb
import numpy as np
import re

#generate duckdb connection
con = duckdb.connect('fin_maternal.db')

# Read .dta files and create tables (with convert_categoricals=False to handle duplicate labels)
# Table 1: fktp1
df_fktp = pd.read_stata('FKTP_2023.dta', convert_categoricals=False)
con.register('fktp_temp', df_fktp)
con.sql("""
CREATE TABLE fktp1 AS
SELECT PSTV01, FKP02, FKP03, FKP04, FKP05, FKP13, FKP14A
FROM fktp_temp;
""")

# Table 2: fkrtl1
df_fkrtl = pd.read_stata('FKRTL_2023.dta', convert_categoricals=False)
con.register('fkrtl_temp', df_fkrtl)
con.sql("""
CREATE TABLE fkrtl1 AS
SELECT PSTV01, FKP02, FKL02, FKL03, FKL04, FKL05, FKL09, FKL11, FKL14, FKL15A, FKL17A
FROM fkrtl_temp;
""")

# Table 3: sek1
df_sek = pd.read_stata('FKRTL_Sekunder_2023.dta', convert_categoricals=False)
con.register('sek_temp', df_sek)
con.sql("""
CREATE TABLE sek1 AS
SELECT FKL02, FKL24A
FROM sek_temp;
""")

# Table 4: peserta1
df_peserta = pd.read_stata('Kepesertaan_2023.dta', convert_categoricals=False)
con.register('peserta_temp', df_peserta)
con.sql("""
CREATE TABLE peserta1 AS
SELECT PSTV01, PSTV03, PSTV08, PSTV18
FROM peserta_temp;
""")

# Clean up temporary registrations (optional)
con.unregister('fktp_temp')
con.unregister('fkrtl_temp')
con.unregister('sek_temp')
con.unregister('peserta_temp')

#merge all visits
# This step merge Hospital visits with secondary diagnosis table
con.sql("""CREATE TABLE rs AS
SELECT p.*, s.FKL24A
FROM fkrtl1 AS p
LEFT JOIN sek1  AS s
USING (FKL02);
""")

# This step combines the rs table with the fktp1 table 
con.sql("""
CREATE TABLE klin AS
SELECT
  COALESCE(CAST(f.PSTV01 AS VARCHAR), CAST(p.PSTV01 AS VARCHAR)) AS PSTV01,
  COALESCE(CAST(f.FKP02  AS VARCHAR), CAST(p.FKP02  AS VARCHAR)) AS FKP02,
  f.* EXCLUDE (FKP02),
  p.* EXCLUDE (FKP02)
FROM rs AS f
FULL OUTER JOIN fktp1 AS p
ON CAST(f.PSTV01 AS VARCHAR) = CAST(p.PSTV01 AS VARCHAR) AND CAST(f.FKP02 AS VARCHAR) = CAST(p.FKP02 AS VARCHAR);
""")

# This step combines the total visits with the membership table  
con.sql("""
CREATE TABLE kia AS
SELECT
  k.*,
  m.PSTV03,
  m.PSTV08,
  m.PSTV18
FROM klin AS k
LEFT JOIN peserta1 AS m
USING (PSTV01);
""")

con.sql("PRAGMA table_info(kia)").df()

# Remove column duplicates
con.sql('ALTER TABLE kia DROP COLUMN "pstv01_1"')
con.sql('ALTER TABLE kia DROP COLUMN "pstv01_2"')
con.sql('ALTER TABLE kia DROP COLUMN "fkl02"') # This column is no longer needed as it is only used for merging purposes
con.sql('ALTER TABLE kia DROP COLUMN "fkp02"')

# Generate Characteristics
## Age
con.sql("alter table kia add column age int")
# Generate Age
con.sql("""
UPDATE kia 
SET age = CASE 
    WHEN fkl03 IS NOT NULL THEN EXTRACT(YEAR FROM fkl03) - EXTRACT(YEAR FROM pstv03)
    WHEN fkp03 IS NOT NULL THEN EXTRACT(YEAR FROM fkp03) - EXTRACT(YEAR FROM pstv03)
          END;
""")

# Age Group Categorization
con.sql("ALTER TABLE kia ADD COLUMN age_risk INT")
con.sql("""
UPDATE kia
SET age_risk =
  CASE
    WHEN age IS NULL THEN NULL        
    WHEN age < 20 OR age > 35 THEN 1  
    ELSE 0                            
  END;
""")

# Region of Residence
con.sql("ALTER TABLE kia ADD COLUMN dom INT")
con.sql("""
UPDATE kia
SET dom =
  CASE
    WHEN COALESCE(fkl05, fkp05) IN 
         (31, 32, 33, 34, 35, 36, 51)
      THEN 0
    WHEN fkl05 IS NULL AND fkp05 IS NULL
      THEN NULL
    ELSE 1
  END;
""")

# Subsidy Membership Status
con.sql("alter table kia add column subsid int")
con.sql("""
UPDATE kia
SET subsid =
  CASE
    WHEN (pstv08) IN (2,3) THEN 1
    WHEN pstv08 IS NULL THEN NULL  
    ELSE 0
  END;
""")

# Drop unneeded columns
con.sql('ALTER TABLE kia DROP COLUMN "pstv03"')
con.sql('ALTER TABLE kia DROP COLUMN "fkl05"')
con.sql('ALTER TABLE kia DROP COLUMN "fkp05"')
con.sql('ALTER TABLE kia DROP COLUMN "pstv08"')

# Moving to python since complex date functions works better on pandas
df = con.sql("select * from kia").df()

# Generate 'combined_date' which refers to patient's visit date
# FKP03 are prioritized than FKL03 when both column existed, since patient require referral from primary care before going to hospital
df['combined_date'] = df[['FKP03', 'FKL03']].min(axis=1)

# Generate the Date of Abortive termination 'doa'
abort_codes = ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07']
df['doa'] = pd.to_datetime(np.where(
    df['FKP14A'].isin(abort_codes) | 
    df['FKL15A'].isin(abort_codes) |  
    df['FKL17A'].isin(abort_codes) |  
    df['FKL24A'].isin(abort_codes), 
    df['combined_date'],  
    pd.NaT  
), errors='coerce')

# Generate the Date of Delivery 'dol'
delivery_codes = ['O80', 'O81', 'O82', 'O83', 'O84']
df['dol'] = pd.to_datetime(np.where(
    df['FKP14A'].isin(delivery_codes) | 
    df['FKL15A'].isin(delivery_codes) |  
    df['FKL17A'].isin(delivery_codes) |  
    df['FKL24A'].isin(delivery_codes), 
    df['combined_date'],  
    pd.NaT  
), errors='coerce')

# Generate date of pregnancy termination 'dopt'
df['dopt'] = df['doa'].fillna(df['dol'])

# 180 days rule to identify distinct pregnancy episode
df = df.sort_values(['PSTV01', 'dopt'])
prev = df.groupby('PSTV01')['dopt'].shift()
gap = (df['dopt'] - prev).dt.days
is_anchor = df['dopt'].notna() & (prev.isna() | (gap >= 180))

# fin_g = anchor episode (validated pregnancy episode timeline)
df['fin_g'] = df['dopt'].where(is_anchor)

# Generate number of pregnancy 'n_preg'
df['n_preg'] = (
    is_anchor.astype(int)
             .groupby(df['PSTV01'])
             .cumsum()
             .where(df['dopt'].notna())
).astype('Int64')

# ref_start for abortus â†’ ref - 140 days
df.loc[mask_anchor & abortus_mask, 'ref_start'] = df['fin_g'] - pd.to_timedelta(140, unit='D')

# ref_start for childbirth â†’ ref - 280 days
df.loc[mask_anchor & partus_mask, 'ref_start'] = df['fin_g'] - pd.to_timedelta(280, unit='D')

# Drop unneeded colums
df = df.drop(columns=['doa', 'dol', 'dopt'])

# 1) Pastikan tanggal
for c in ['FKP03','FKL03']:
    df[c] = pd.to_datetime(df[c], errors='coerce')

# 2) combined_date = min(FKP03, FKL03)
df['combined_date'] = df[['FKP03','FKL03']].min(axis=1)

# 3) Mask abortus/partus SEBAGAI SERIES (bukan DataFrame)
abort_codes  = ['O00','O01','O02','O03','O04','O05','O06','O07']
partus_codes = ['O80','O81','O82','O83','O84']

abortus_mask = df[['FKP14A','FKL15A','FKL17A','FKL24A']].isin(abort_codes).any(axis=1)
partus_mask  = df[['FKP14A','FKL15A','FKL17A','FKL24A']].isin(partus_codes).any(axis=1)

# 4) doa / dol / dopt
df['doa'] = df['combined_date'].where(abortus_mask)
df['dol'] = df['combined_date'].where(partus_mask)
df['dopt'] = df['doa'].fillna(df['dol'])

# 5) Episode anchor (180 hari)
df = df.sort_values(['PSTV01','dopt'])
prev = df.groupby('PSTV01', dropna=False)['dopt'].shift()
gap  = (df['dopt'] - prev).dt.days
is_anchor = df['dopt'].notna() & (prev.isna() | (gap >= 180))
df['fin_g'] = df['dopt'].where(is_anchor)

# 6) ref_start (pakai .loc di kanan juga supaya index align)
mask_anchor = df['fin_g'].notna()
df['ref_start'] = pd.NaT  # init

idx_abort = (mask_anchor & abortus_mask)
idx_part  = (mask_anchor & partus_mask)

df.loc[idx_abort, 'ref_start'] = df.loc[idx_abort, 'fin_g'] - pd.to_timedelta(140, unit='D')
df.loc[idx_part,  'ref_start'] = df.loc[idx_part,  'fin_g'] - pd.to_timedelta(280, unit='D')

# Drop unneeded colums
df = df.drop(columns=['doa', 'dol', 'dopt'])


print(df.head(20))

df[['PSTV01', 'combined_date', 'ref_start', 'fin_g']].sample(n=20, random_state=42)


print(df['n_preg'].value_counts(dropna=False).sort_index())

print(df.columns)

df.to_csv("pregnancy by visit.csv",index=False)

# Pregnancy level 

# Load the visit-level data
primi = pd.read_csv("pregnancy by visit.csv")

# or if you want to continue from the previous dataframe without saving/loading
# primi = df.copy()


# 1st Pregnancy
# Cohort filtering: only patients who had n_preg = 1
valid_pstv01 = primi.loc[primi['n_preg'] == 1, 'PSTV01'].unique()
primi = primi[primi['PSTV01'].isin(valid_pstv01)].reset_index(drop=True)

# Generate pregnancy termination date 'ref' 
ref_map = (
    primi.loc[(primi['n_preg'] == 1) & (primi['fin_g'].notna()), ['PSTV01', 'fin_g']]
      .sort_values(['PSTV01', 'fin_g'])          # prioritaskan tanggal awal
      .drop_duplicates('PSTV01', keep='first')   # pastikan 1 nilai per PSTV01
      .set_index('PSTV01')['fin_g']              # jadi Series siap untuk map()
)
primi['ref'] = primi['PSTV01'].map(ref_map)

#Generate pregnancy start date 'ref_start'
ref_start_map = (
    primi.loc[(primi['n_preg'] == 1) & (primi['ref_start'].notna()), ['PSTV01', 'ref_start']]
         .sort_values(['PSTV01', 'ref_start'])
         .drop_duplicates('PSTV01', keep='first')
         .set_index('PSTV01')['ref_start']
)
primi['ref_start'] = primi['PSTV01'].map(ref_start_map)


# Ensure 'combined_date' and 'ref' are datetime
primi['combined_date'] = pd.to_datetime(primi['combined_date'], errors='coerce')
primi['ref'] = pd.to_datetime(primi['ref'], errors='coerce')
primi['ref_start'] = pd.to_datetime(primi['ref_start'], errors='coerce')

# COMPLETE OPTIMIZATION: Before & After Conditions
import re

# All condition dictionaries
chronic_conditions = { 
    'dm': ['E10','E11','E12','E13','E14','O24'],
    'malnut': ['E40','E41','E42','E43','E44','E45','E46'],
    'nutri': ['E50','E51','E52','E53','E54','E55','E56','E57','E58','E59','E60','E61','E62','E63','E64'],
    'obese': ['E66'],
    'substance': ['F10','F11','F12','F13','F14','F15','F16','F17','F18','F19'],
    'schizo': ['F20','F21','F22','F23','F24','F25','F28','F29'],
    'neurot': ['F40','F41','F42','F43','F44','F48','F45'],
    'neu_deg': ['G10','G11','G12','G20','G21','G22','G23','G24','G25','G26','G30','G31','G32','G35','G36','G37'],
    'headache': ['G43','G44'],
    'neuropathy': ['G50','G51','G52','G53','G54','G55','G56','G57','G58','G59','G60','G61','G62','G63','G64'],
    'rhd': ['I05','I06','I07','I08','I09'],
    'ht': ['I10','I11','I12','I13','I14','I15','O10','O13','O16'],
    'isch': ['I20','I21','I22','I23','I24','I25'],
    'phd': ['I26','I27','I28'],
    'carditis': ['I30','I32','I33','I38','I39','I40','I41'],
    'cmp': ['I42','I43'],
    'arrythmia': ['I44','I45','I47','I48','I49'],
    'hf': ['I50'],
    'stroke': ['I60','I61','I62','I63','I64','I69'],
    'artery': ['I70','I71','I72','I73','I74','I77','I78','I79'],
    'vein': ['I80','I81','I82','I83','I85','I86','I87','I88','I89'],
    'chronic_res': ['J35','J37','J40','J41','J42','J43','J44','J45'],
    'pul_edema': ['J81'],
    'pleura': ['J90','J91','J92','J93','J94'],
    'oral': ['K00','K01','K02','K03','K04','K05','K06','K07','K08','K09','K10','K11','K12','K13','K14'],
    'gastritis': ['K22','K25','K26','K27','K28','K29','K30'],
    'hernia': ['K40','K41','K42','K43','K44','K45','K46'],
    'intestinal': ['K50','K51','K52','K56','K58','K59','K60','K61','K62','K63'],
    'hemorrh': ['K64'],
    'periton': ['K65'],
    'liver_fail': ['K72'],
    'liver': ['K70','K71','K73','K74','K75','K76'],
    'gallbladder': ['K80','K81','K82','K83'],
    'pancreas': ['K85','K86'],
    'bullous': ['L10','L11','L12','L13','L14'],
    'atopic': ['L20'],
    'dermatitis': ['L21','L23','L25','L26','L27','L28','L30'],
    'urticaria': ['L50'],
    'urolith': ['N20','N21','N22'],
    'endomet': ['N80'],
    'femgen': ['N81','N82','N83','N84','N85','N86','N87','N88','N89','N90'],
    'hypomen': ['N91'],
    'menorrh': ['N92'],
    'dysmen': ['N94']
}
# Infectious conditions
infectious_conditions = {
    'typhoid': ['A01'],
    'cholera': ['A00'],
    'v_age': ['A08'],
    'b_age': ['A00', 'A02', 'A03', 'A04', 'A05'],
    'p_age': ['A06', 'A07'],
    'tb': ['A15', 'A16', 'A17', 'A18', 'A19'],
    'myco': ['A30', 'A31'],
    'lepto': ['A27'],
    'std': ['A51','A52','A53','A54','A55','A56','A57','A58','A59','A63','A64'],
    'torch': ['B58','B06','B25','B00','A60'],
    'v_skin': ['B01','B02','B03','B04','B05','B07','B08','B09'],
    'hepatitis': ['B15','B16','B17','B18','B19'],
    'hiv': ['B20','B21','B22','B23','B24'],
    'sepsis': ['A40','A41'],
    'infla_cns': ['G00','G01','G02','G03','G04','G08','G05','G06','G07','G09'],
    'urti': ['J00','J01','J02','J03','J04','J05','J06','J09','J10','J11'],
    'lrti': ['J12','J13','J14','J15','J16','J17','J18','J20','J21','J22'],
    'uti': ['N30','N34','N39']
}

# Pregnancy conditions
pregnancy_conditions = {
    'abortive': ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07', 'O08'],
    'preecl': ['O11', 'O14'], 'ecl': ['O15'], 'earlyhemo': ['O20'], 'heg': ['O21'],
    'venpreg': ['O22'], 'utipreg': ['O23'], 'malpreg': ['O25'], 'multigest': ['O30'],
    'malpresent': ['O32'], 'disprop': ['O33'], 'abnorpelv': ['O34'], 'fetalprob': ['O35', 'O36'],
    'polyhydra': ['O40'], 'abnamnio': ['O41'], 'prom': ['O42'], 'placental': ['O43'],
    'previa': ['O44'], 'abrupt': ['O45'], 'anh': ['O46'], 'prolong': ['O48'],
    'preterm': ['O60'], 'fail': ['O61'], 'abnforce': ['O62'], 'long': ['O63'],
    'obspelvic': ['O65', 'O66'], 'malpres': ['O64'], 'iph': ['O67'], 'distress': ['O68'],
    'umbilical': ['O69'], 'laceration': ['O70'], 'obstrau': ['O71'], 'pph': ['O72'],
    'retained': ['O73'], 'normal': ['O80'], 'instrum': ['O81'], 'caesar': ['O82'],
    'assisted': ['O83'], 'multiple': ['O84']
}

# Regex conditions
conditions_regex = {
    'arthropathy': r'M(0[0-9]|1[0-9]|2[0-5])',
    'sysconn': r'M3[0-6]',
    'dorsopathy': r'M4[0-9]|M5[0-4]',
    'muscle_dis': r'M6[0-3]',
    'synov_dis': r'M6[5-8]',
    'soft_dis': r'M8[0-9]|M9[0-4]',
    'renal_dis': r'N0[0-9]|N1[0-6]',
    'renal_fail': r'N1[7-9]',
    'breast_dis': r'N6[0-4]',
    'pid': r'N7[0-7]',
    'poison': r'T3[6-9]|T4[0-9]|T50',
    'toxic': r'T5[1-9]|T6[0-5]'
}

# Gabungkan kondisi chronic + infectious
all_conditions = {}
all_conditions.update(chronic_conditions)
all_conditions.update(infectious_conditions)

# --- Pre-compile regex ---
compiled_patterns = {condition: re.compile(regex) for condition, regex in conditions_regex.items()}

# === DATE CONDITIONS ===
before_chronic   = (primi['combined_date'] <= primi['ref'])
before_inf_preg  = (primi['combined_date'] < primi['ref_start'])

after_chronic_inf = (primi['combined_date'] > primi['ref'])
after_preg        = (primi['combined_date'] - primi['ref']).dt.days > 30

during_inf  = (
    (primi['combined_date'] >= primi['ref_start']) &
    (primi['combined_date'] <= primi['ref'])
)
during_preg = (
    (primi['combined_date'] >= primi['ref_start']) &
    (primi['combined_date'] <= (primi['ref'] + pd.Timedelta(days=30)))
)

# === LOOPING ===

# Chronic
for cond, codes in chronic_conditions.items():
    m = (primi['FKP14A'].isin(codes) |
         primi['FKL15A'].isin(codes) |
         primi['FKL17A'].isin(codes) |
         primi['FKL24A'].isin(codes))
    primi[f'b_{cond}'] = (m & before_chronic).astype(int)
    primi[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Infectious
for cond, codes in infectious_conditions.items():
    m = (primi['FKP14A'].isin(codes) |
         primi['FKL15A'].isin(codes) |
         primi['FKL17A'].isin(codes) |
         primi['FKL24A'].isin(codes))
    primi[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    primi[f'c_{cond}'] = (m & during_inf).astype(int)
    primi[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Pregnancy
for cond, codes in pregnancy_conditions.items():
    m = (primi['FKP14A'].isin(codes) |
         primi['FKL15A'].isin(codes) |
         primi['FKL17A'].isin(codes) |
         primi['FKL24A'].isin(codes))
    primi[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    primi[f'c_{cond}'] = (m & during_preg).astype(int)
    primi[f'a_{cond}'] = (m & after_preg).astype(int)

# Regex (chronic rules)
for cond, pat in compiled_patterns.items():
    m = (primi['FKP14A'].str.contains(pat, na=False) |
         primi['FKL15A'].str.contains(pat, na=False) |
         primi['FKL17A'].str.contains(pat, na=False) |
         primi['FKL24A'].str.contains(pat, na=False))
    primi[f'b_{cond}'] = (m & before_chronic).astype(int)
    primi[f'a_{cond}'] = (m & after_chronic_inf).astype(int)


# Washout period
# Extract year from 'ref'
primi['ref_year'] = primi['ref'].dt.year

# Remove ref_year = 2015 
rows_to_drop = primi[primi['ref_year'] == 2015]
num_rows_dropped = len(rows_to_drop)

# Update DataFrame
primi_wash = primi[primi['ref_year'] != 2015]


# Remove columns that are no longer needed
columns_to_drop = ['FKP03', 'FKP04', 'FKP13', 'FKP14A', 'FKL03', 'FKL04', 'FKL09', 'FKL11', 'FKL14', 'FKL15A', 'FKL16', 'FKL17A', 'FKL18', 'FKL24A', 'PSTV18', 'fin_g', 'combined_date', 'ref', 'ref_start']
ichi = primi_wash.drop(columns=columns_to_drop, errors='ignore')

# Set all values in the 'n_preg' column to 1
ichi['n_preg'] = 1

aggregation_rules = {
    'subsid': 'max',  
    'age': 'min',     
    'dom': 'min',     
    'age_risk' : 'min',
    'subsid': 'max', 
    'n_preg' : 'min',
    'ref_year' : 'min'
}

# Add rules for columns starting with 'b_', 'a_' or 'c_' to cap their sum at 1
for col in ichi.columns:
    if col.startswith('b_') or col.startswith('a_') or col.startswith('c_'):
        aggregation_rules[col] = lambda x: min(x.sum(), 1)

# Perform the groupby and aggregation
fin_ich = ichi.groupby('PSTV01').agg(aggregation_rules).reset_index()

fin_ich.to_csv("1st_washed.csv",index=False)

#2nd Pregnancy

# Load the visit-level data
secon = pd.read_csv("pregnancy by visit.csv")

# or if you want to continue from the previous dataframe without saving/loading
# secon = df.copy()


# Cohort filtering: only patients who had n_preg = 2
valid_pstv01 = secon.loc[secon['n_preg'] == 2, 'PSTV01'].unique()
secon = secon[secon['PSTV01'].isin(valid_pstv01)].reset_index(drop=True)

# Generate pregnancy termination date 'ref' 
ref_map = (
    secon.loc[(secon['n_preg'] == 2) & (secon['fin_g'].notna()), ['PSTV01', 'fin_g']]
      .sort_values(['PSTV01', 'fin_g'])          # prioritaskan tanggal awal
      .drop_duplicates('PSTV01', keep='first')   # pastikan 1 nilai per PSTV01
      .set_index('PSTV01')['fin_g']              # jadi Series siap untuk map()
)
secon['ref'] = secon['PSTV01'].map(ref_map)

#Generate pregnancy start date 'ref_start'
ref_start_map = (
    secon.loc[(secon['n_preg'] == 2) & (secon['ref_start'].notna()), ['PSTV01', 'ref_start']]
         .sort_values(['PSTV01', 'ref_start'])
         .drop_duplicates('PSTV01', keep='first')
         .set_index('PSTV01')['ref_start']
)
secon['ref_start'] = secon['PSTV01'].map(ref_start_map)

# Ensure 'combined_date' and 'ref' are datetime
secon['combined_date'] = pd.to_datetime(secon['combined_date'], errors='coerce')
secon['ref'] = pd.to_datetime(secon['ref'], errors='coerce')
secon['ref_start'] = pd.to_datetime(secon['ref_start'], errors='coerce')

# All condition dictionaries
chronic_conditions = { 
    'dm': ['E10','E11','E12','E13','E14','O24'],
    'malnut': ['E40','E41','E42','E43','E44','E45','E46'],
    'nutri': ['E50','E51','E52','E53','E54','E55','E56','E57','E58','E59','E60','E61','E62','E63','E64'],
    'obese': ['E66'],
    'substance': ['F10','F11','F12','F13','F14','F15','F16','F17','F18','F19'],
    'schizo': ['F20','F21','F22','F23','F24','F25','F28','F29'],
    'neurot': ['F40','F41','F42','F43','F44','F48','F45'],
    'neu_deg': ['G10','G11','G12','G20','G21','G22','G23','G24','G25','G26','G30','G31','G32','G35','G36','G37'],
    'headache': ['G43','G44'],
    'neuropathy': ['G50','G51','G52','G53','G54','G55','G56','G57','G58','G59','G60','G61','G62','G63','G64'],
    'rhd': ['I05','I06','I07','I08','I09'],
    'ht': ['I10','I11','I12','I13','I14','I15','O10','O13','O16'],
    'isch': ['I20','I21','I22','I23','I24','I25'],
    'phd': ['I26','I27','I28'],
    'carditis': ['I30','I32','I33','I38','I39','I40','I41'],
    'cmp': ['I42','I43'],
    'arrythmia': ['I44','I45','I47','I48','I49'],
    'hf': ['I50'],
    'stroke': ['I60','I61','I62','I63','I64','I69'],
    'artery': ['I70','I71','I72','I73','I74','I77','I78','I79'],
    'vein': ['I80','I81','I82','I83','I85','I86','I87','I88','I89'],
    'chronic_res': ['J35','J37','J40','J41','J42','J43','J44','J45'],
    'pul_edema': ['J81'],
    'pleura': ['J90','J91','J92','J93','J94'],
    'oral': ['K00','K01','K02','K03','K04','K05','K06','K07','K08','K09','K10','K11','K12','K13','K14'],
    'gastritis': ['K22','K25','K26','K27','K28','K29','K30'],
    'hernia': ['K40','K41','K42','K43','K44','K45','K46'],
    'intestinal': ['K50','K51','K52','K56','K58','K59','K60','K61','K62','K63'],
    'hemorrh': ['K64'],
    'periton': ['K65'],
    'liver_fail': ['K72'],
    'liver': ['K70','K71','K73','K74','K75','K76'],
    'gallbladder': ['K80','K81','K82','K83'],
    'pancreas': ['K85','K86'],
    'bullous': ['L10','L11','L12','L13','L14'],
    'atopic': ['L20'],
    'dermatitis': ['L21','L23','L25','L26','L27','L28','L30'],
    'urticaria': ['L50'],
    'urolith': ['N20','N21','N22'],
    'endomet': ['N80'],
    'femgen': ['N81','N82','N83','N84','N85','N86','N87','N88','N89','N90'],
    'hypomen': ['N91'],
    'menorrh': ['N92'],
    'dysmen': ['N94']
}
# Infectious conditions
infectious_conditions = {
    'typhoid': ['A01'],
    'cholera': ['A00'],
    'v_age': ['A08'],
    'b_age': ['A00', 'A02', 'A03', 'A04', 'A05'],
    'p_age': ['A06', 'A07'],
    'tb': ['A15', 'A16', 'A17', 'A18', 'A19'],
    'myco': ['A30', 'A31'],
    'lepto': ['A27'],
    'std': ['A51','A52','A53','A54','A55','A56','A57','A58','A59','A63','A64'],
    'torch': ['B58','B06','B25','B00','A60'],
    'v_skin': ['B01','B02','B03','B04','B05','B07','B08','B09'],
    'hepatitis': ['B15','B16','B17','B18','B19'],
    'hiv': ['B20','B21','B22','B23','B24'],
    'sepsis': ['A40','A41'],
    'infla_cns': ['G00','G01','G02','G03','G04','G08','G05','G06','G07','G09'],
    'urti': ['J00','J01','J02','J03','J04','J05','J06','J09','J10','J11'],
    'lrti': ['J12','J13','J14','J15','J16','J17','J18','J20','J21','J22'],
    'uti': ['N30','N34','N39']
}

# Pregnancy conditions
pregnancy_conditions = {
    'abortive': ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07', 'O08'],
    'preecl': ['O11', 'O14'], 'ecl': ['O15'], 'earlyhemo': ['O20'], 'heg': ['O21'],
    'venpreg': ['O22'], 'utipreg': ['O23'], 'malpreg': ['O25'], 'multigest': ['O30'],
    'malpresent': ['O32'], 'disprop': ['O33'], 'abnorpelv': ['O34'], 'fetalprob': ['O35', 'O36'],
    'polyhydra': ['O40'], 'abnamnio': ['O41'], 'prom': ['O42'], 'placental': ['O43'],
    'previa': ['O44'], 'abrupt': ['O45'], 'anh': ['O46'], 'prolong': ['O48'],
    'preterm': ['O60'], 'fail': ['O61'], 'abnforce': ['O62'], 'long': ['O63'],
    'obspelvic': ['O65', 'O66'], 'malpres': ['O64'], 'iph': ['O67'], 'distress': ['O68'],
    'umbilical': ['O69'], 'laceration': ['O70'], 'obstrau': ['O71'], 'pph': ['O72'],
    'retained': ['O73'], 'normal': ['O80'], 'instrum': ['O81'], 'caesar': ['O82'],
    'assisted': ['O83'], 'multiple': ['O84']
}

# Regex conditions
conditions_regex = {
    'arthropathy': r'M(0[0-9]|1[0-9]|2[0-5])',
    'sysconn': r'M3[0-6]',
    'dorsopathy': r'M4[0-9]|M5[0-4]',
    'muscle_dis': r'M6[0-3]',
    'synov_dis': r'M6[5-8]',
    'soft_dis': r'M8[0-9]|M9[0-4]',
    'renal_dis': r'N0[0-9]|N1[0-6]',
    'renal_fail': r'N1[7-9]',
    'breast_dis': r'N6[0-4]',
    'pid': r'N7[0-7]',
    'poison': r'T3[6-9]|T4[0-9]|T50',
    'toxic': r'T5[1-9]|T6[0-5]'
}

# Gabungkan kondisi chronic + infectious
all_conditions = {}
all_conditions.update(chronic_conditions)
all_conditions.update(infectious_conditions)

# --- Pre-compile regex ---
compiled_patterns = {condition: re.compile(regex) for condition, regex in conditions_regex.items()}

# === DATE CONDITIONS ===
before_chronic   = (secon['combined_date'] <= secon['ref'])
before_inf_preg  = (secon['combined_date'] < secon['ref_start'])

after_chronic_inf = (secon['combined_date'] > secon['ref'])
after_preg        = (secon['combined_date'] - secon['ref']).dt.days > 30

during_inf  = (
    (secon['combined_date'] >= secon['ref_start']) &
    (secon['combined_date'] <= secon['ref'])
)
during_preg = (
    (secon['combined_date'] >= secon['ref_start']) &
    (secon['combined_date'] <= (secon['ref'] + pd.Timedelta(days=30)))
)

# === LOOPING ===

# Chronic
for cond, codes in chronic_conditions.items():
    m = (secon['FKP14A'].isin(codes) |
         secon['FKL15A'].isin(codes) |
         secon['FKL17A'].isin(codes) |
         secon['FKL24A'].isin(codes))
    secon[f'b_{cond}'] = (m & before_chronic).astype(int)
    secon[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Infectious
for cond, codes in infectious_conditions.items():
    m = (secon['FKP14A'].isin(codes) |
         secon['FKL15A'].isin(codes) |
         secon['FKL17A'].isin(codes) |
         secon['FKL24A'].isin(codes))
    secon[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    secon[f'c_{cond}'] = (m & during_inf).astype(int)
    secon[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Pregnancy
for cond, codes in pregnancy_conditions.items():
    m = (secon['FKP14A'].isin(codes) |
         secon['FKL15A'].isin(codes) |
         secon['FKL17A'].isin(codes) |
         secon['FKL24A'].isin(codes))
    secon[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    secon[f'c_{cond}'] = (m & during_preg).astype(int)
    secon[f'a_{cond}'] = (m & after_preg).astype(int)

# Regex (chronic rules)
for cond, pat in compiled_patterns.items():
    m = (secon['FKP14A'].str.contains(pat, na=False) |
         secon['FKL15A'].str.contains(pat, na=False) |
         secon['FKL17A'].str.contains(pat, na=False) |
         secon['FKL24A'].str.contains(pat, na=False))
    secon[f'b_{cond}'] = (m & before_chronic).astype(int)
    secon[f'a_{cond}'] = (m & after_chronic_inf).astype(int)


# Extract year from 'ref'
secon['ref_year'] = secon['ref'].dt.year

# Remove columns that are no longer needed
columns_to_drop = ['FKP03', 'FKP04', 'FKP13', 'FKP14A', 'FKL03', 'FKL04', 'FKL09', 'FKL11', 'FKL14', 'FKL15A', 'FKL16', 'FKL17A', 'FKL18', 'FKL24A', 'PSTV18', 'fin_g', 'combined_date', 'ref', 'ref_start']
nii = secon.drop(columns=columns_to_drop, errors='ignore')

# Set all values in the 'n_preg' column to 2
nii['n_preg'] = 2

aggregation_rules = {
    'subsid': 'max',  
    'age': 'min',     
    'dom': 'min',     
    'age_risk' : 'min',
    'subsid': 'max', 
    'n_preg' : 'min',
    'ref_year' : 'min'
}

# Add rules for columns starting with 'b_', 'a_' or 'c_' to cap their sum at 1
for col in nii.columns:
    if col.startswith('b_') or col.startswith('a_') or col.startswith('c_'):
        aggregation_rules[col] = lambda x: min(x.sum(), 1)

# Perform the groupby and aggregation
fin_ni = nii.groupby('PSTV01').agg(aggregation_rules).reset_index()

fin_ni.to_csv("2nd.csv",index=False)

# 3rd Pregnancy

# Load the visit-level data
trio = pd.read_csv("pregnancy by visit.csv")

# or if you want to continue from the previous dataframe without saving/loading
# trio = df.copy()

# Cohort filtering: only patients who had n_preg = 3
valid_pstv01 = trio.loc[trio['n_preg'] == 3, 'PSTV01'].unique()
trio = trio[trio['PSTV01'].isin(valid_pstv01)].reset_index(drop=True)

# Generate pregnancy termination date 'ref' 
ref_map = (
    trio.loc[(trio['n_preg'] == 3) & (trio['fin_g'].notna()), ['PSTV01', 'fin_g']]
      .sort_values(['PSTV01', 'fin_g'])          # prioritaskan tanggal awal
      .drop_duplicates('PSTV01', keep='first')   # pastikan 1 nilai per PSTV01
      .set_index('PSTV01')['fin_g']              # jadi Series siap untuk map()
)
trio['ref'] = trio['PSTV01'].map(ref_map)

#Generate pregnancy start date 'ref_start'
ref_start_map = (
    trio.loc[(trio['n_preg'] == 3) & (trio['ref_start'].notna()), ['PSTV01', 'ref_start']]
         .sort_values(['PSTV01', 'ref_start'])
         .drop_duplicates('PSTV01', keep='first')
         .set_index('PSTV01')['ref_start']
)
trio['ref_start'] = trio['PSTV01'].map(ref_start_map)


# Ensure 'combined_date' and 'ref' are datetime
trio['combined_date'] = pd.to_datetime(trio['combined_date'], errors='coerce')
trio['ref'] = pd.to_datetime(trio['ref'], errors='coerce')
trio['ref_start'] = pd.to_datetime(trio['ref_start'], errors='coerce')


# All condition dictionaries
chronic_conditions = { 
    'dm': ['E10','E11','E12','E13','E14','O24'],
    'malnut': ['E40','E41','E42','E43','E44','E45','E46'],
    'nutri': ['E50','E51','E52','E53','E54','E55','E56','E57','E58','E59','E60','E61','E62','E63','E64'],
    'obese': ['E66'],
    'substance': ['F10','F11','F12','F13','F14','F15','F16','F17','F18','F19'],
    'schizo': ['F20','F21','F22','F23','F24','F25','F28','F29'],
    'neurot': ['F40','F41','F42','F43','F44','F48','F45'],
    'neu_deg': ['G10','G11','G12','G20','G21','G22','G23','G24','G25','G26','G30','G31','G32','G35','G36','G37'],
    'headache': ['G43','G44'],
    'neuropathy': ['G50','G51','G52','G53','G54','G55','G56','G57','G58','G59','G60','G61','G62','G63','G64'],
    'rhd': ['I05','I06','I07','I08','I09'],
    'ht': ['I10','I11','I12','I13','I14','I15','O10','O13','O16'],
    'isch': ['I20','I21','I22','I23','I24','I25'],
    'phd': ['I26','I27','I28'],
    'carditis': ['I30','I32','I33','I38','I39','I40','I41'],
    'cmp': ['I42','I43'],
    'arrythmia': ['I44','I45','I47','I48','I49'],
    'hf': ['I50'],
    'stroke': ['I60','I61','I62','I63','I64','I69'],
    'artery': ['I70','I71','I72','I73','I74','I77','I78','I79'],
    'vein': ['I80','I81','I82','I83','I85','I86','I87','I88','I89'],
    'chronic_res': ['J35','J37','J40','J41','J42','J43','J44','J45'],
    'pul_edema': ['J81'],
    'pleura': ['J90','J91','J92','J93','J94'],
    'oral': ['K00','K01','K02','K03','K04','K05','K06','K07','K08','K09','K10','K11','K12','K13','K14'],
    'gastritis': ['K22','K25','K26','K27','K28','K29','K30'],
    'hernia': ['K40','K41','K42','K43','K44','K45','K46'],
    'intestinal': ['K50','K51','K52','K56','K58','K59','K60','K61','K62','K63'],
    'hemorrh': ['K64'],
    'periton': ['K65'],
    'liver_fail': ['K72'],
    'liver': ['K70','K71','K73','K74','K75','K76'],
    'gallbladder': ['K80','K81','K82','K83'],
    'pancreas': ['K85','K86'],
    'bullous': ['L10','L11','L12','L13','L14'],
    'atopic': ['L20'],
    'dermatitis': ['L21','L23','L25','L26','L27','L28','L30'],
    'urticaria': ['L50'],
    'urolith': ['N20','N21','N22'],
    'endomet': ['N80'],
    'femgen': ['N81','N82','N83','N84','N85','N86','N87','N88','N89','N90'],
    'hypomen': ['N91'],
    'menorrh': ['N92'],
    'dysmen': ['N94']
}
# Infectious conditions
infectious_conditions = {
    'typhoid': ['A01'],
    'cholera': ['A00'],
    'v_age': ['A08'],
    'b_age': ['A00', 'A02', 'A03', 'A04', 'A05'],
    'p_age': ['A06', 'A07'],
    'tb': ['A15', 'A16', 'A17', 'A18', 'A19'],
    'myco': ['A30', 'A31'],
    'lepto': ['A27'],
    'std': ['A51','A52','A53','A54','A55','A56','A57','A58','A59','A63','A64'],
    'torch': ['B58','B06','B25','B00','A60'],
    'v_skin': ['B01','B02','B03','B04','B05','B07','B08','B09'],
    'hepatitis': ['B15','B16','B17','B18','B19'],
    'hiv': ['B20','B21','B22','B23','B24'],
    'sepsis': ['A40','A41'],
    'infla_cns': ['G00','G01','G02','G03','G04','G08','G05','G06','G07','G09'],
    'urti': ['J00','J01','J02','J03','J04','J05','J06','J09','J10','J11'],
    'lrti': ['J12','J13','J14','J15','J16','J17','J18','J20','J21','J22'],
    'uti': ['N30','N34','N39']
}

# Pregnancy conditions
pregnancy_conditions = {
    'abortive': ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07', 'O08'],
    'preecl': ['O11', 'O14'], 'ecl': ['O15'], 'earlyhemo': ['O20'], 'heg': ['O21'],
    'venpreg': ['O22'], 'utipreg': ['O23'], 'malpreg': ['O25'], 'multigest': ['O30'],
    'malpresent': ['O32'], 'disprop': ['O33'], 'abnorpelv': ['O34'], 'fetalprob': ['O35', 'O36'],
    'polyhydra': ['O40'], 'abnamnio': ['O41'], 'prom': ['O42'], 'placental': ['O43'],
    'previa': ['O44'], 'abrupt': ['O45'], 'anh': ['O46'], 'prolong': ['O48'],
    'preterm': ['O60'], 'fail': ['O61'], 'abnforce': ['O62'], 'long': ['O63'],
    'obspelvic': ['O65', 'O66'], 'malpres': ['O64'], 'iph': ['O67'], 'distress': ['O68'],
    'umbilical': ['O69'], 'laceration': ['O70'], 'obstrau': ['O71'], 'pph': ['O72'],
    'retained': ['O73'], 'normal': ['O80'], 'instrum': ['O81'], 'caesar': ['O82'],
    'assisted': ['O83'], 'multiple': ['O84']
}

# Regex conditions
conditions_regex = {
    'arthropathy': r'M(0[0-9]|1[0-9]|2[0-5])',
    'sysconn': r'M3[0-6]',
    'dorsopathy': r'M4[0-9]|M5[0-4]',
    'muscle_dis': r'M6[0-3]',
    'synov_dis': r'M6[5-8]',
    'soft_dis': r'M8[0-9]|M9[0-4]',
    'renal_dis': r'N0[0-9]|N1[0-6]',
    'renal_fail': r'N1[7-9]',
    'breast_dis': r'N6[0-4]',
    'pid': r'N7[0-7]',
    'poison': r'T3[6-9]|T4[0-9]|T50',
    'toxic': r'T5[1-9]|T6[0-5]'
}

# Gabungkan kondisi chronic + infectious
all_conditions = {}
all_conditions.update(chronic_conditions)
all_conditions.update(infectious_conditions)

# --- Pre-compile regex ---
compiled_patterns = {condition: re.compile(regex) for condition, regex in conditions_regex.items()}

# === DATE CONDITIONS ===
before_chronic   = (trio['combined_date'] <= trio['ref'])
before_inf_preg  = (trio['combined_date'] < trio['ref_start'])

after_chronic_inf = (trio['combined_date'] > trio['ref'])
after_preg        = (trio['combined_date'] - trio['ref']).dt.days > 30

during_inf  = (
    (trio['combined_date'] >= trio['ref_start']) &
    (trio['combined_date'] <= trio['ref'])
)
during_preg = (
    (trio['combined_date'] >= trio['ref_start']) &
    (trio['combined_date'] <= (trio['ref'] + pd.Timedelta(days=30)))
)

# === LOOPING ===

# Chronic
for cond, codes in chronic_conditions.items():
    m = (trio['FKP14A'].isin(codes) |
         trio['FKL15A'].isin(codes) |
         trio['FKL17A'].isin(codes) |
         trio['FKL24A'].isin(codes))
    trio[f'b_{cond}'] = (m & before_chronic).astype(int)
    trio[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Infectious
for cond, codes in infectious_conditions.items():
    m = (trio['FKP14A'].isin(codes) |
         trio['FKL15A'].isin(codes) |
         trio['FKL17A'].isin(codes) |
         trio['FKL24A'].isin(codes))
    trio[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    trio[f'c_{cond}'] = (m & during_inf).astype(int)
    trio[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Pregnancy
for cond, codes in pregnancy_conditions.items():
    m = (trio['FKP14A'].isin(codes) |
         trio['FKL15A'].isin(codes) |
         trio['FKL17A'].isin(codes) |
         trio['FKL24A'].isin(codes))
    trio[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    trio[f'c_{cond}'] = (m & during_preg).astype(int)
    trio[f'a_{cond}'] = (m & after_preg).astype(int)

# Regex (chronic rules)
for cond, pat in compiled_patterns.items():
    m = (trio['FKP14A'].str.contains(pat, na=False) |
         trio['FKL15A'].str.contains(pat, na=False) |
         trio['FKL17A'].str.contains(pat, na=False) |
         trio['FKL24A'].str.contains(pat, na=False))
    trio[f'b_{cond}'] = (m & before_chronic).astype(int)
    trio[f'a_{cond}'] = (m & after_chronic_inf).astype(int)


# Extract year from 'ref'
trio['ref_year'] = trio['ref'].dt.year

# Remove columns that are no longer needed
columns_to_drop = ['FKP03', 'FKP04', 'FKP13', 'FKP14A', 'FKL03', 'FKL04', 'FKL09', 'FKL11', 'FKL14', 'FKL15A', 'FKL16', 'FKL17A', 'FKL18', 'FKL24A', 'PSTV18', 'fin_g', 'combined_date', 'ref']
san = trio.drop(columns=columns_to_drop, errors='ignore')

# Set all values in the 'n_preg' column to 3
san['n_preg'] = 3

aggregation_rules = {
    'subsid': 'max',  
    'age': 'min',     
    'dom': 'min',     
    'age_risk' : 'min',
    'subsid': 'max', 
    'n_preg' : 'min',
    'ref_year' : 'min'
}

# Add rules for columns starting with 'b_', 'a_' or 'c_' to cap their sum at 1
for col in san.columns:
    if col.startswith('b_') or col.startswith('a_') or col.startswith('c_'):
        aggregation_rules[col] = lambda x: min(x.sum(), 1)

# Perform the groupby and aggregation
fin_san = san.groupby('PSTV01').agg(aggregation_rules).reset_index()

fin_san.to_csv("3rd.csv",index=False)

#4th

# Load the visit-level data
quad = pd.read_csv("pregnancy by visit.csv")

# or if you want to continue from the previous dataframe without saving/loading
# quad = df.copy()

# 1st Pregnancy
# Cohort filtering: only patients who had n_preg = 4
valid_pstv01 = quad.loc[quad['n_preg'] == 4, 'PSTV01'].unique()
quad = quad[quad['PSTV01'].isin(valid_pstv01)].reset_index(drop=True)

# Generate pregnancy termination date 'ref' 
ref_map = (
    quad.loc[(quad['n_preg'] == 4) & (quad['fin_g'].notna()), ['PSTV01', 'fin_g']]
      .sort_values(['PSTV01', 'fin_g'])          # prioritaskan tanggal awal
      .drop_duplicates('PSTV01', keep='first')   # pastikan 1 nilai per PSTV01
      .set_index('PSTV01')['fin_g']              # jadi Series siap untuk map()
)
quad['ref'] = quad['PSTV01'].map(ref_map)

#Generate pregnancy start date 'ref_start'
ref_start_map = (
    quad.loc[(quad['n_preg'] == 4) & (quad['ref_start'].notna()), ['PSTV01', 'ref_start']]
         .sort_values(['PSTV01', 'ref_start'])
         .drop_duplicates('PSTV01', keep='first')
         .set_index('PSTV01')['ref_start']
)
quad['ref_start'] = quad['PSTV01'].map(ref_start_map)

# Ensure 'combined_date' and 'ref' are datetime
quad['combined_date'] = pd.to_datetime(quad['combined_date'], errors='coerce')
quad['ref'] = pd.to_datetime(quad['ref'], errors='coerce')
quad['ref_start'] = pd.to_datetime(quad['ref_start'], errors='coerce')

# All condition dictionaries
chronic_conditions = { 
    'dm': ['E10','E11','E12','E13','E14','O24'],
    'malnut': ['E40','E41','E42','E43','E44','E45','E46'],
    'nutri': ['E50','E51','E52','E53','E54','E55','E56','E57','E58','E59','E60','E61','E62','E63','E64'],
    'obese': ['E66'],
    'substance': ['F10','F11','F12','F13','F14','F15','F16','F17','F18','F19'],
    'schizo': ['F20','F21','F22','F23','F24','F25','F28','F29'],
    'neurot': ['F40','F41','F42','F43','F44','F48','F45'],
    'neu_deg': ['G10','G11','G12','G20','G21','G22','G23','G24','G25','G26','G30','G31','G32','G35','G36','G37'],
    'headache': ['G43','G44'],
    'neuropathy': ['G50','G51','G52','G53','G54','G55','G56','G57','G58','G59','G60','G61','G62','G63','G64'],
    'rhd': ['I05','I06','I07','I08','I09'],
    'ht': ['I10','I11','I12','I13','I14','I15','O10','O13','O16'],
    'isch': ['I20','I21','I22','I23','I24','I25'],
    'phd': ['I26','I27','I28'],
    'carditis': ['I30','I32','I33','I38','I39','I40','I41'],
    'cmp': ['I42','I43'],
    'arrythmia': ['I44','I45','I47','I48','I49'],
    'hf': ['I50'],
    'stroke': ['I60','I61','I62','I63','I64','I69'],
    'artery': ['I70','I71','I72','I73','I74','I77','I78','I79'],
    'vein': ['I80','I81','I82','I83','I85','I86','I87','I88','I89'],
    'chronic_res': ['J35','J37','J40','J41','J42','J43','J44','J45'],
    'pul_edema': ['J81'],
    'pleura': ['J90','J91','J92','J93','J94'],
    'oral': ['K00','K01','K02','K03','K04','K05','K06','K07','K08','K09','K10','K11','K12','K13','K14'],
    'gastritis': ['K22','K25','K26','K27','K28','K29','K30'],
    'hernia': ['K40','K41','K42','K43','K44','K45','K46'],
    'intestinal': ['K50','K51','K52','K56','K58','K59','K60','K61','K62','K63'],
    'hemorrh': ['K64'],
    'periton': ['K65'],
    'liver_fail': ['K72'],
    'liver': ['K70','K71','K73','K74','K75','K76'],
    'gallbladder': ['K80','K81','K82','K83'],
    'pancreas': ['K85','K86'],
    'bullous': ['L10','L11','L12','L13','L14'],
    'atopic': ['L20'],
    'dermatitis': ['L21','L23','L25','L26','L27','L28','L30'],
    'urticaria': ['L50'],
    'urolith': ['N20','N21','N22'],
    'endomet': ['N80'],
    'femgen': ['N81','N82','N83','N84','N85','N86','N87','N88','N89','N90'],
    'hypomen': ['N91'],
    'menorrh': ['N92'],
    'dysmen': ['N94']
}
# Infectious conditions
infectious_conditions = {
    'typhoid': ['A01'],
    'cholera': ['A00'],
    'v_age': ['A08'],
    'b_age': ['A00', 'A02', 'A03', 'A04', 'A05'],
    'p_age': ['A06', 'A07'],
    'tb': ['A15', 'A16', 'A17', 'A18', 'A19'],
    'myco': ['A30', 'A31'],
    'lepto': ['A27'],
    'std': ['A51','A52','A53','A54','A55','A56','A57','A58','A59','A63','A64'],
    'torch': ['B58','B06','B25','B00','A60'],
    'v_skin': ['B01','B02','B03','B04','B05','B07','B08','B09'],
    'hepatitis': ['B15','B16','B17','B18','B19'],
    'hiv': ['B20','B21','B22','B23','B24'],
    'sepsis': ['A40','A41'],
    'infla_cns': ['G00','G01','G02','G03','G04','G08','G05','G06','G07','G09'],
    'urti': ['J00','J01','J02','J03','J04','J05','J06','J09','J10','J11'],
    'lrti': ['J12','J13','J14','J15','J16','J17','J18','J20','J21','J22'],
    'uti': ['N30','N34','N39']
}

# Pregnancy conditions
pregnancy_conditions = {
    'abortive': ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07', 'O08'],
    'preecl': ['O11', 'O14'], 'ecl': ['O15'], 'earlyhemo': ['O20'], 'heg': ['O21'],
    'venpreg': ['O22'], 'utipreg': ['O23'], 'malpreg': ['O25'], 'multigest': ['O30'],
    'malpresent': ['O32'], 'disprop': ['O33'], 'abnorpelv': ['O34'], 'fetalprob': ['O35', 'O36'],
    'polyhydra': ['O40'], 'abnamnio': ['O41'], 'prom': ['O42'], 'placental': ['O43'],
    'previa': ['O44'], 'abrupt': ['O45'], 'anh': ['O46'], 'prolong': ['O48'],
    'preterm': ['O60'], 'fail': ['O61'], 'abnforce': ['O62'], 'long': ['O63'],
    'obspelvic': ['O65', 'O66'], 'malpres': ['O64'], 'iph': ['O67'], 'distress': ['O68'],
    'umbilical': ['O69'], 'laceration': ['O70'], 'obstrau': ['O71'], 'pph': ['O72'],
    'retained': ['O73'], 'normal': ['O80'], 'instrum': ['O81'], 'caesar': ['O82'],
    'assisted': ['O83'], 'multiple': ['O84']
}

# Regex conditions
conditions_regex = {
    'arthropathy': r'M(0[0-9]|1[0-9]|2[0-5])',
    'sysconn': r'M3[0-6]',
    'dorsopathy': r'M4[0-9]|M5[0-4]',
    'muscle_dis': r'M6[0-3]',
    'synov_dis': r'M6[5-8]',
    'soft_dis': r'M8[0-9]|M9[0-4]',
    'renal_dis': r'N0[0-9]|N1[0-6]',
    'renal_fail': r'N1[7-9]',
    'breast_dis': r'N6[0-4]',
    'pid': r'N7[0-7]',
    'poison': r'T3[6-9]|T4[0-9]|T50',
    'toxic': r'T5[1-9]|T6[0-5]'
}

# Gabungkan kondisi chronic + infectious
all_conditions = {}
all_conditions.update(chronic_conditions)
all_conditions.update(infectious_conditions)

# --- Pre-compile regex ---
compiled_patterns = {condition: re.compile(regex) for condition, regex in conditions_regex.items()}

# === DATE CONDITIONS ===
before_chronic   = (quad['combined_date'] <= quad['ref'])
before_inf_preg  = (quad['combined_date'] < quad['ref_start'])

after_chronic_inf = (quad['combined_date'] > quad['ref'])
after_preg        = (quad['combined_date'] - quad['ref']).dt.days > 30

during_inf  = (
    (quad['combined_date'] >= quad['ref_start']) &
    (quad['combined_date'] <= quad['ref'])
)
during_preg = (
    (quad['combined_date'] >= quad['ref_start']) &
    (quad['combined_date'] <= (quad['ref'] + pd.Timedelta(days=30)))
)

# === LOOPING ===

# Chronic
for cond, codes in chronic_conditions.items():
    m = (quad['FKP14A'].isin(codes) |
         quad['FKL15A'].isin(codes) |
         quad['FKL17A'].isin(codes) |
         quad['FKL24A'].isin(codes))
    quad[f'b_{cond}'] = (m & before_chronic).astype(int)
    quad[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Infectious
for cond, codes in infectious_conditions.items():
    m = (quad['FKP14A'].isin(codes) |
         quad['FKL15A'].isin(codes) |
         quad['FKL17A'].isin(codes) |
         quad['FKL24A'].isin(codes))
    quad[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    quad[f'c_{cond}'] = (m & during_inf).astype(int)
    quad[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Pregnancy
for cond, codes in pregnancy_conditions.items():
    m = (quad['FKP14A'].isin(codes) |
         quad['FKL15A'].isin(codes) |
         quad['FKL17A'].isin(codes) |
         quad['FKL24A'].isin(codes))
    quad[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    quad[f'c_{cond}'] = (m & during_preg).astype(int)
    quad[f'a_{cond}'] = (m & after_preg).astype(int)

# Regex (chronic rules)
for cond, pat in compiled_patterns.items():
    m = (quad['FKP14A'].str.contains(pat, na=False) |
         quad['FKL15A'].str.contains(pat, na=False) |
         quad['FKL17A'].str.contains(pat, na=False) |
         quad['FKL24A'].str.contains(pat, na=False))
    quad[f'b_{cond}'] = (m & before_chronic).astype(int)
    quad[f'a_{cond}'] = (m & after_chronic_inf).astype(int)


# Extract year from 'ref'
quad['ref_year'] = quad['ref'].dt.year

# Remove columns that are no longer needed
columns_to_drop = ['FKP03', 'FKP04', 'FKP13', 'FKP14A', 'FKL03', 'FKL04', 'FKL09', 'FKL11', 'FKL14', 'FKL15A', 'FKL16', 'FKL17A', 'FKL18', 'FKL24A', 'PSTV18', 'fin_g', 'combined_date', 'ref']
yon = quad.drop(columns=columns_to_drop, errors='ignore')

# Set all values in the 'n_preg' column to 3
yon['n_preg'] = 4

aggregation_rules = {
    'subsid': 'max',  
    'age': 'min',     
    'dom': 'min',     
    'age_risk' : 'min',
    'subsid': 'max', 
    'n_preg' : 'min',
    'ref_year' : 'min'
}

# Add rules for columns starting with 'b_', 'a_' or 'c_' to cap their sum at 1
for col in yon.columns:
    if col.startswith('b_') or col.startswith('a_') or col.startswith('c_'):
        aggregation_rules[col] = lambda x: min(x.sum(), 1)

# Perform the groupby and aggregation
fin_yon = yon.groupby('PSTV01').agg(aggregation_rules).reset_index()

fin_yon.to_csv("4th.csv",index=False)

#5th Preg

# Load the visit-level data
quin = pd.read_csv("pregnancy by visit.csv")

# or if you want to continue from the previous dataframe without saving/loading
# quin = df.copy()

# 1st Pregnancy
# Cohort filtering: only patients who had n_preg = 5
valid_pstv01 = quin.loc[quin['n_preg'] == 5, 'PSTV01'].unique()
quin = quin[quin['PSTV01'].isin(valid_pstv01)].reset_index(drop=True)

# Generate pregnancy termination date 'ref' 
ref_map = (
    quin.loc[(quin['n_preg'] == 5) & (quin['fin_g'].notna()), ['PSTV01', 'fin_g']]
      .sort_values(['PSTV01', 'fin_g'])          # prioritaskan tanggal awal
      .drop_duplicates('PSTV01', keep='first')   # pastikan 1 nilai per PSTV01
      .set_index('PSTV01')['fin_g']              # jadi Series siap untuk map()
)
quin['ref'] = quin['PSTV01'].map(ref_map)

#Generate pregnancy start date 'ref_start'
ref_start_map = (
    quin.loc[(quin['n_preg'] == 5) & (quin['ref_start'].notna()), ['PSTV01', 'ref_start']]
         .sort_values(['PSTV01', 'ref_start'])
         .drop_duplicates('PSTV01', keep='first')
         .set_index('PSTV01')['ref_start']
)
quin['ref_start'] = quin['PSTV01'].map(ref_start_map)


# Ensure 'combined_date' and 'ref' are datetime
quin['combined_date'] = pd.to_datetime(quin['combined_date'], errors='coerce')
quin['ref'] = pd.to_datetime(quin['ref'], errors='coerce')
quin['ref_start'] = pd.to_datetime(quin['ref_start'], errors='coerce')


# All condition dictionaries
chronic_conditions = { 
    'dm': ['E10','E11','E12','E13','E14','O24'],
    'malnut': ['E40','E41','E42','E43','E44','E45','E46'],
    'nutri': ['E50','E51','E52','E53','E54','E55','E56','E57','E58','E59','E60','E61','E62','E63','E64'],
    'obese': ['E66'],
    'substance': ['F10','F11','F12','F13','F14','F15','F16','F17','F18','F19'],
    'schizo': ['F20','F21','F22','F23','F24','F25','F28','F29'],
    'neurot': ['F40','F41','F42','F43','F44','F48','F45'],
    'neu_deg': ['G10','G11','G12','G20','G21','G22','G23','G24','G25','G26','G30','G31','G32','G35','G36','G37'],
    'headache': ['G43','G44'],
    'neuropathy': ['G50','G51','G52','G53','G54','G55','G56','G57','G58','G59','G60','G61','G62','G63','G64'],
    'rhd': ['I05','I06','I07','I08','I09'],
    'ht': ['I10','I11','I12','I13','I14','I15','O10','O13','O16'],
    'isch': ['I20','I21','I22','I23','I24','I25'],
    'phd': ['I26','I27','I28'],
    'carditis': ['I30','I32','I33','I38','I39','I40','I41'],
    'cmp': ['I42','I43'],
    'arrythmia': ['I44','I45','I47','I48','I49'],
    'hf': ['I50'],
    'stroke': ['I60','I61','I62','I63','I64','I69'],
    'artery': ['I70','I71','I72','I73','I74','I77','I78','I79'],
    'vein': ['I80','I81','I82','I83','I85','I86','I87','I88','I89'],
    'chronic_res': ['J35','J37','J40','J41','J42','J43','J44','J45'],
    'pul_edema': ['J81'],
    'pleura': ['J90','J91','J92','J93','J94'],
    'oral': ['K00','K01','K02','K03','K04','K05','K06','K07','K08','K09','K10','K11','K12','K13','K14'],
    'gastritis': ['K22','K25','K26','K27','K28','K29','K30'],
    'hernia': ['K40','K41','K42','K43','K44','K45','K46'],
    'intestinal': ['K50','K51','K52','K56','K58','K59','K60','K61','K62','K63'],
    'hemorrh': ['K64'],
    'periton': ['K65'],
    'liver_fail': ['K72'],
    'liver': ['K70','K71','K73','K74','K75','K76'],
    'gallbladder': ['K80','K81','K82','K83'],
    'pancreas': ['K85','K86'],
    'bullous': ['L10','L11','L12','L13','L14'],
    'atopic': ['L20'],
    'dermatitis': ['L21','L23','L25','L26','L27','L28','L30'],
    'urticaria': ['L50'],
    'urolith': ['N20','N21','N22'],
    'endomet': ['N80'],
    'femgen': ['N81','N82','N83','N84','N85','N86','N87','N88','N89','N90'],
    'hypomen': ['N91'],
    'menorrh': ['N92'],
    'dysmen': ['N94']
}
# Infectious conditions
infectious_conditions = {
    'typhoid': ['A01'],
    'cholera': ['A00'],
    'v_age': ['A08'],
    'b_age': ['A00', 'A02', 'A03', 'A04', 'A05'],
    'p_age': ['A06', 'A07'],
    'tb': ['A15', 'A16', 'A17', 'A18', 'A19'],
    'myco': ['A30', 'A31'],
    'lepto': ['A27'],
    'std': ['A51','A52','A53','A54','A55','A56','A57','A58','A59','A63','A64'],
    'torch': ['B58','B06','B25','B00','A60'],
    'v_skin': ['B01','B02','B03','B04','B05','B07','B08','B09'],
    'hepatitis': ['B15','B16','B17','B18','B19'],
    'hiv': ['B20','B21','B22','B23','B24'],
    'sepsis': ['A40','A41'],
    'infla_cns': ['G00','G01','G02','G03','G04','G08','G05','G06','G07','G09'],
    'urti': ['J00','J01','J02','J03','J04','J05','J06','J09','J10','J11'],
    'lrti': ['J12','J13','J14','J15','J16','J17','J18','J20','J21','J22'],
    'uti': ['N30','N34','N39']
}

# Pregnancy conditions
pregnancy_conditions = {
    'abortive': ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07', 'O08'],
    'preecl': ['O11', 'O14'], 'ecl': ['O15'], 'earlyhemo': ['O20'], 'heg': ['O21'],
    'venpreg': ['O22'], 'utipreg': ['O23'], 'malpreg': ['O25'], 'multigest': ['O30'],
    'malpresent': ['O32'], 'disprop': ['O33'], 'abnorpelv': ['O34'], 'fetalprob': ['O35', 'O36'],
    'polyhydra': ['O40'], 'abnamnio': ['O41'], 'prom': ['O42'], 'placental': ['O43'],
    'previa': ['O44'], 'abrupt': ['O45'], 'anh': ['O46'], 'prolong': ['O48'],
    'preterm': ['O60'], 'fail': ['O61'], 'abnforce': ['O62'], 'long': ['O63'],
    'obspelvic': ['O65', 'O66'], 'malpres': ['O64'], 'iph': ['O67'], 'distress': ['O68'],
    'umbilical': ['O69'], 'laceration': ['O70'], 'obstrau': ['O71'], 'pph': ['O72'],
    'retained': ['O73'], 'normal': ['O80'], 'instrum': ['O81'], 'caesar': ['O82'],
    'assisted': ['O83'], 'multiple': ['O84']
}

# Regex conditions
conditions_regex = {
    'arthropathy': r'M(0[0-9]|1[0-9]|2[0-5])',
    'sysconn': r'M3[0-6]',
    'dorsopathy': r'M4[0-9]|M5[0-4]',
    'muscle_dis': r'M6[0-3]',
    'synov_dis': r'M6[5-8]',
    'soft_dis': r'M8[0-9]|M9[0-4]',
    'renal_dis': r'N0[0-9]|N1[0-6]',
    'renal_fail': r'N1[7-9]',
    'breast_dis': r'N6[0-4]',
    'pid': r'N7[0-7]',
    'poison': r'T3[6-9]|T4[0-9]|T50',
    'toxic': r'T5[1-9]|T6[0-5]'
}

# Gabungkan kondisi chronic + infectious
all_conditions = {}
all_conditions.update(chronic_conditions)
all_conditions.update(infectious_conditions)

# --- Pre-compile regex ---
compiled_patterns = {condition: re.compile(regex) for condition, regex in conditions_regex.items()}

# === DATE CONDITIONS ===
before_chronic   = (quin['combined_date'] <= quin['ref'])
before_inf_preg  = (quin['combined_date'] < quin['ref_start'])

after_chronic_inf = (quin['combined_date'] > quin['ref'])
after_preg        = (quin['combined_date'] - quin['ref']).dt.days > 30

during_inf  = (
    (quin['combined_date'] >= quin['ref_start']) &
    (quin['combined_date'] <= quin['ref'])
)
during_preg = (
    (quin['combined_date'] >= quin['ref_start']) &
    (quin['combined_date'] <= (quin['ref'] + pd.Timedelta(days=30)))
)

# === LOOPING ===

# Chronic
for cond, codes in chronic_conditions.items():
    m = (quin['FKP14A'].isin(codes) |
         quin['FKL15A'].isin(codes) |
         quin['FKL17A'].isin(codes) |
         quin['FKL24A'].isin(codes))
    quin[f'b_{cond}'] = (m & before_chronic).astype(int)
    quin[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Infectious
for cond, codes in infectious_conditions.items():
    m = (quin['FKP14A'].isin(codes) |
         quin['FKL15A'].isin(codes) |
         quin['FKL17A'].isin(codes) |
         quin['FKL24A'].isin(codes))
    quin[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    quin[f'c_{cond}'] = (m & during_inf).astype(int)
    quin[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Pregnancy
for cond, codes in pregnancy_conditions.items():
    m = (quin['FKP14A'].isin(codes) |
         quin['FKL15A'].isin(codes) |
         quin['FKL17A'].isin(codes) |
         quin['FKL24A'].isin(codes))
    quin[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    quin[f'c_{cond}'] = (m & during_preg).astype(int)
    quin[f'a_{cond}'] = (m & after_preg).astype(int)

# Regex (chronic rules)
for cond, pat in compiled_patterns.items():
    m = (quin['FKP14A'].str.contains(pat, na=False) |
         quin['FKL15A'].str.contains(pat, na=False) |
         quin['FKL17A'].str.contains(pat, na=False) |
         quin['FKL24A'].str.contains(pat, na=False))
    quin[f'b_{cond}'] = (m & before_chronic).astype(int)
    quin[f'a_{cond}'] = (m & after_chronic_inf).astype(int)


# Extract year from 'ref'
quin['ref_year'] = quin['ref'].dt.year

# Remove columns that are no longer needed
columns_to_drop = ['FKP03', 'FKP04', 'FKP13', 'FKP14A', 'FKL03', 'FKL04', 'FKL09', 'FKL11', 'FKL14', 'FKL15A', 'FKL16', 'FKL17A', 'FKL18', 'FKL24A', 'PSTV18', 'fin_g', 'combined_date', 'ref']
gou = quin.drop(columns=columns_to_drop, errors='ignore')

# Set all values in the 'n_preg' column to 5
gou['n_preg'] = 5

aggregation_rules = {
    'subsid': 'max',  
    'age': 'min',     
    'dom': 'min',     
    'age_risk' : 'min',
    'subsid': 'max', 
    'n_preg' : 'min',
    'ref_year' : 'min'
}

# Add rules for columns starting with 'b_', 'a_' or 'c_' to cap their sum at 1
for col in gou.columns:
    if col.startswith('b_') or col.startswith('a_') or col.startswith('c_'):
        aggregation_rules[col] = lambda x: min(x.sum(), 1)

# Perform the groupby and aggregation
fin_gou = gou.groupby('PSTV01').agg(aggregation_rules).reset_index()

fin_gou.to_csv("5th.csv",index=False)

#6th Pregnancy

# Load the visit-level data
sext = pd.read_csv("pregnancy by visit.csv")

# or if you want to continue from the previous dataframe without saving/loading
# sext = df.copy()


# Cohort filtering: only patients who had n_preg = 6
valid_pstv01 = sext.loc[sext['n_preg'] == 6, 'PSTV01'].unique()
sext = sext[sext['PSTV01'].isin(valid_pstv01)].reset_index(drop=True)

# Generate pregnancy termination date 'ref' 
ref_map = (
    sext.loc[(sext['n_preg'] == 6) & (sext['fin_g'].notna()), ['PSTV01', 'fin_g']]
      .sort_values(['PSTV01', 'fin_g'])          # prioritaskan tanggal awal
      .drop_duplicates('PSTV01', keep='first')   # pastikan 1 nilai per PSTV01
      .set_index('PSTV01')['fin_g']              # jadi Series siap untuk map()
)
sext['ref'] = sext['PSTV01'].map(ref_map)

#Generate pregnancy start date 'ref_start'
ref_start_map = (
    sext.loc[(sext['n_preg'] == 6) & (sext['ref_start'].notna()), ['PSTV01', 'ref_start']]
         .sort_values(['PSTV01', 'ref_start'])
         .drop_duplicates('PSTV01', keep='first')
         .set_index('PSTV01')['ref_start']
)
sext['ref_start'] = sext['PSTV01'].map(ref_start_map)


# Ensure 'combined_date' and 'ref' are datetime
sext['combined_date'] = pd.to_datetime(sext['combined_date'], errors='coerce')
sext['ref'] = pd.to_datetime(sext['ref'], errors='coerce')
sext['ref_start'] = pd.to_datetime(sext['ref_start'], errors='coerce')


# All condition dictionaries
chronic_conditions = { 
    'dm': ['E10','E11','E12','E13','E14','O24'],
    'malnut': ['E40','E41','E42','E43','E44','E45','E46'],
    'nutri': ['E50','E51','E52','E53','E54','E55','E56','E57','E58','E59','E60','E61','E62','E63','E64'],
    'obese': ['E66'],
    'substance': ['F10','F11','F12','F13','F14','F15','F16','F17','F18','F19'],
    'schizo': ['F20','F21','F22','F23','F24','F25','F28','F29'],
    'neurot': ['F40','F41','F42','F43','F44','F48','F45'],
    'neu_deg': ['G10','G11','G12','G20','G21','G22','G23','G24','G25','G26','G30','G31','G32','G35','G36','G37'],
    'headache': ['G43','G44'],
    'neuropathy': ['G50','G51','G52','G53','G54','G55','G56','G57','G58','G59','G60','G61','G62','G63','G64'],
    'rhd': ['I05','I06','I07','I08','I09'],
    'ht': ['I10','I11','I12','I13','I14','I15','O10','O13','O16'],
    'isch': ['I20','I21','I22','I23','I24','I25'],
    'phd': ['I26','I27','I28'],
    'carditis': ['I30','I32','I33','I38','I39','I40','I41'],
    'cmp': ['I42','I43'],
    'arrythmia': ['I44','I45','I47','I48','I49'],
    'hf': ['I50'],
    'stroke': ['I60','I61','I62','I63','I64','I69'],
    'artery': ['I70','I71','I72','I73','I74','I77','I78','I79'],
    'vein': ['I80','I81','I82','I83','I85','I86','I87','I88','I89'],
    'chronic_res': ['J35','J37','J40','J41','J42','J43','J44','J45'],
    'pul_edema': ['J81'],
    'pleura': ['J90','J91','J92','J93','J94'],
    'oral': ['K00','K01','K02','K03','K04','K05','K06','K07','K08','K09','K10','K11','K12','K13','K14'],
    'gastritis': ['K22','K25','K26','K27','K28','K29','K30'],
    'hernia': ['K40','K41','K42','K43','K44','K45','K46'],
    'intestinal': ['K50','K51','K52','K56','K58','K59','K60','K61','K62','K63'],
    'hemorrh': ['K64'],
    'periton': ['K65'],
    'liver_fail': ['K72'],
    'liver': ['K70','K71','K73','K74','K75','K76'],
    'gallbladder': ['K80','K81','K82','K83'],
    'pancreas': ['K85','K86'],
    'bullous': ['L10','L11','L12','L13','L14'],
    'atopic': ['L20'],
    'dermatitis': ['L21','L23','L25','L26','L27','L28','L30'],
    'urticaria': ['L50'],
    'urolith': ['N20','N21','N22'],
    'endomet': ['N80'],
    'femgen': ['N81','N82','N83','N84','N85','N86','N87','N88','N89','N90'],
    'hypomen': ['N91'],
    'menorrh': ['N92'],
    'dysmen': ['N94']
}
# Infectious conditions
infectious_conditions = {
    'typhoid': ['A01'],
    'cholera': ['A00'],
    'v_age': ['A08'],
    'b_age': ['A00', 'A02', 'A03', 'A04', 'A05'],
    'p_age': ['A06', 'A07'],
    'tb': ['A15', 'A16', 'A17', 'A18', 'A19'],
    'myco': ['A30', 'A31'],
    'lepto': ['A27'],
    'std': ['A51','A52','A53','A54','A55','A56','A57','A58','A59','A63','A64'],
    'torch': ['B58','B06','B25','B00','A60'],
    'v_skin': ['B01','B02','B03','B04','B05','B07','B08','B09'],
    'hepatitis': ['B15','B16','B17','B18','B19'],
    'hiv': ['B20','B21','B22','B23','B24'],
    'sepsis': ['A40','A41'],
    'infla_cns': ['G00','G01','G02','G03','G04','G08','G05','G06','G07','G09'],
    'urti': ['J00','J01','J02','J03','J04','J05','J06','J09','J10','J11'],
    'lrti': ['J12','J13','J14','J15','J16','J17','J18','J20','J21','J22'],
    'uti': ['N30','N34','N39']
}

# Pregnancy conditions
pregnancy_conditions = {
    'abortive': ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07', 'O08'],
    'preecl': ['O11', 'O14'], 'ecl': ['O15'], 'earlyhemo': ['O20'], 'heg': ['O21'],
    'venpreg': ['O22'], 'utipreg': ['O23'], 'malpreg': ['O25'], 'multigest': ['O30'],
    'malpresent': ['O32'], 'disprop': ['O33'], 'abnorpelv': ['O34'], 'fetalprob': ['O35', 'O36'],
    'polyhydra': ['O40'], 'abnamnio': ['O41'], 'prom': ['O42'], 'placental': ['O43'],
    'previa': ['O44'], 'abrupt': ['O45'], 'anh': ['O46'], 'prolong': ['O48'],
    'preterm': ['O60'], 'fail': ['O61'], 'abnforce': ['O62'], 'long': ['O63'],
    'obspelvic': ['O65', 'O66'], 'malpres': ['O64'], 'iph': ['O67'], 'distress': ['O68'],
    'umbilical': ['O69'], 'laceration': ['O70'], 'obstrau': ['O71'], 'pph': ['O72'],
    'retained': ['O73'], 'normal': ['O80'], 'instrum': ['O81'], 'caesar': ['O82'],
    'assisted': ['O83'], 'multiple': ['O84']
}

# Regex conditions
conditions_regex = {
    'arthropathy': r'M(0[0-9]|1[0-9]|2[0-5])',
    'sysconn': r'M3[0-6]',
    'dorsopathy': r'M4[0-9]|M5[0-4]',
    'muscle_dis': r'M6[0-3]',
    'synov_dis': r'M6[5-8]',
    'soft_dis': r'M8[0-9]|M9[0-4]',
    'renal_dis': r'N0[0-9]|N1[0-6]',
    'renal_fail': r'N1[7-9]',
    'breast_dis': r'N6[0-4]',
    'pid': r'N7[0-7]',
    'poison': r'T3[6-9]|T4[0-9]|T50',
    'toxic': r'T5[1-9]|T6[0-5]'
}

# Gabungkan kondisi chronic + infectious
all_conditions = {}
all_conditions.update(chronic_conditions)
all_conditions.update(infectious_conditions)

# --- Pre-compile regex ---
compiled_patterns = {condition: re.compile(regex) for condition, regex in conditions_regex.items()}

# === DATE CONDITIONS ===
before_chronic   = (sext['combined_date'] <= sext['ref'])
before_inf_preg  = (sext['combined_date'] < sext['ref_start'])

after_chronic_inf = (sext['combined_date'] > sext['ref'])
after_preg        = (sext['combined_date'] - sext['ref']).dt.days > 30

during_inf  = (
    (sext['combined_date'] >= sext['ref_start']) &
    (sext['combined_date'] <= sext['ref'])
)
during_preg = (
    (sext['combined_date'] >= sext['ref_start']) &
    (sext['combined_date'] <= (sext['ref'] + pd.Timedelta(days=30)))
)

# === LOOPING ===

# Chronic
for cond, codes in chronic_conditions.items():
    m = (sext['FKP14A'].isin(codes) |
         sext['FKL15A'].isin(codes) |
         sext['FKL17A'].isin(codes) |
         sext['FKL24A'].isin(codes))
    sext[f'b_{cond}'] = (m & before_chronic).astype(int)
    sext[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Infectious
for cond, codes in infectious_conditions.items():
    m = (sext['FKP14A'].isin(codes) |
         sext['FKL15A'].isin(codes) |
         sext['FKL17A'].isin(codes) |
         sext['FKL24A'].isin(codes))
    sext[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    sext[f'c_{cond}'] = (m & during_inf).astype(int)
    sext[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Pregnancy
for cond, codes in pregnancy_conditions.items():
    m = (sext['FKP14A'].isin(codes) |
         sext['FKL15A'].isin(codes) |
         sext['FKL17A'].isin(codes) |
         sext['FKL24A'].isin(codes))
    sext[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    sext[f'c_{cond}'] = (m & during_preg).astype(int)
    sext[f'a_{cond}'] = (m & after_preg).astype(int)

# Regex (chronic rules)
for cond, pat in compiled_patterns.items():
    m = (sext['FKP14A'].str.contains(pat, na=False) |
         sext['FKL15A'].str.contains(pat, na=False) |
         sext['FKL17A'].str.contains(pat, na=False) |
         sext['FKL24A'].str.contains(pat, na=False))
    sext[f'b_{cond}'] = (m & before_chronic).astype(int)
    sext[f'a_{cond}'] = (m & after_chronic_inf).astype(int)


# Extract year from 'ref'
sext['ref_year'] = sext['ref'].dt.year

# Remove columns that are no longer needed
columns_to_drop = ['FKP03', 'FKP04', 'FKP13', 'FKP14A', 'FKL03', 'FKL04', 'FKL09', 'FKL11', 'FKL14', 'FKL15A', 'FKL16', 'FKL17A', 'FKL18', 'FKL24A', 'PSTV18', 'fin_g', 'combined_date', 'ref', 'ref_start']
rok = sext.drop(columns=columns_to_drop, errors='ignore')

# Set all values in the 'n_preg' column to 5
rok['n_preg'] = 6

aggregation_rules = {
    'subsid': 'max',  
    'age': 'min',     
    'dom': 'min',     
    'age_risk' : 'min',
    'subsid': 'max', 
    'n_preg' : 'min',
    'ref_year' : 'min'
}

# Add rules for columns starting with 'b_', 'a_' or 'c_' to cap their sum at 1
for col in rok.columns:
    if col.startswith('b_') or col.startswith('a_') or col.startswith('c_'):
        aggregation_rules[col] = lambda x: min(x.sum(), 1)

# Perform the groupby and aggregation
fin_rok = rok.groupby('PSTV01').agg(aggregation_rules).reset_index()

fin_rok.to_csv("6th.csv",index=False)

#7th Pregnancy

# Load the visit-level data
sept = pd.read_csv("pregnancy by visit.csv")

# or if you want to continue from the previous dataframe without saving/loading
# sept = df.copy()


# Cohort filtering: only patients who had n_preg = 7
valid_pstv01 = sept.loc[sept['n_preg'] == 7, 'PSTV01'].unique()
sept = sept[sept['PSTV01'].isin(valid_pstv01)].reset_index(drop=True)

# Generate pregnancy termination date 'ref' 
ref_map = (
    sept.loc[(sept['n_preg'] == 7) & (sept['fin_g'].notna()), ['PSTV01', 'fin_g']]
      .sort_values(['PSTV01', 'fin_g'])          # prioritaskan tanggal awal
      .drop_duplicates('PSTV01', keep='first')   # pastikan 1 nilai per PSTV01
      .set_index('PSTV01')['fin_g']              # jadi Series siap untuk map()
)
sept['ref'] = sept['PSTV01'].map(ref_map)

#Generate pregnancy start date 'ref_start'
ref_start_map = (
    sept.loc[(sept['n_preg'] == 7) & (sept['ref_start'].notna()), ['PSTV01', 'ref_start']]
         .sort_values(['PSTV01', 'ref_start'])
         .drop_duplicates('PSTV01', keep='first')
         .set_index('PSTV01')['ref_start']
)
sept['ref_start'] = sept['PSTV01'].map(ref_start_map)


# Ensure 'combined_date' and 'ref' are datetime
sept['combined_date'] = pd.to_datetime(sept['combined_date'], errors='coerce')
sept['ref'] = pd.to_datetime(sept['ref'], errors='coerce')
sept['ref_start'] = pd.to_datetime(sept['ref_start'], errors='coerce')

# All condition dictionaries
chronic_conditions = { 
    'dm': ['E10','E11','E12','E13','E14','O24'],
    'malnut': ['E40','E41','E42','E43','E44','E45','E46'],
    'nutri': ['E50','E51','E52','E53','E54','E55','E56','E57','E58','E59','E60','E61','E62','E63','E64'],
    'obese': ['E66'],
    'substance': ['F10','F11','F12','F13','F14','F15','F16','F17','F18','F19'],
    'schizo': ['F20','F21','F22','F23','F24','F25','F28','F29'],
    'neurot': ['F40','F41','F42','F43','F44','F48','F45'],
    'neu_deg': ['G10','G11','G12','G20','G21','G22','G23','G24','G25','G26','G30','G31','G32','G35','G36','G37'],
    'headache': ['G43','G44'],
    'neuropathy': ['G50','G51','G52','G53','G54','G55','G56','G57','G58','G59','G60','G61','G62','G63','G64'],
    'rhd': ['I05','I06','I07','I08','I09'],
    'ht': ['I10','I11','I12','I13','I14','I15','O10','O13','O16'],
    'isch': ['I20','I21','I22','I23','I24','I25'],
    'phd': ['I26','I27','I28'],
    'carditis': ['I30','I32','I33','I38','I39','I40','I41'],
    'cmp': ['I42','I43'],
    'arrythmia': ['I44','I45','I47','I48','I49'],
    'hf': ['I50'],
    'stroke': ['I60','I61','I62','I63','I64','I69'],
    'artery': ['I70','I71','I72','I73','I74','I77','I78','I79'],
    'vein': ['I80','I81','I82','I83','I85','I86','I87','I88','I89'],
    'chronic_res': ['J35','J37','J40','J41','J42','J43','J44','J45'],
    'pul_edema': ['J81'],
    'pleura': ['J90','J91','J92','J93','J94'],
    'oral': ['K00','K01','K02','K03','K04','K05','K06','K07','K08','K09','K10','K11','K12','K13','K14'],
    'gastritis': ['K22','K25','K26','K27','K28','K29','K30'],
    'hernia': ['K40','K41','K42','K43','K44','K45','K46'],
    'intestinal': ['K50','K51','K52','K56','K58','K59','K60','K61','K62','K63'],
    'hemorrh': ['K64'],
    'periton': ['K65'],
    'liver_fail': ['K72'],
    'liver': ['K70','K71','K73','K74','K75','K76'],
    'gallbladder': ['K80','K81','K82','K83'],
    'pancreas': ['K85','K86'],
    'bullous': ['L10','L11','L12','L13','L14'],
    'atopic': ['L20'],
    'dermatitis': ['L21','L23','L25','L26','L27','L28','L30'],
    'urticaria': ['L50'],
    'urolith': ['N20','N21','N22'],
    'endomet': ['N80'],
    'femgen': ['N81','N82','N83','N84','N85','N86','N87','N88','N89','N90'],
    'hypomen': ['N91'],
    'menorrh': ['N92'],
    'dysmen': ['N94']
}
# Infectious conditions
infectious_conditions = {
    'typhoid': ['A01'],
    'cholera': ['A00'],
    'v_age': ['A08'],
    'b_age': ['A00', 'A02', 'A03', 'A04', 'A05'],
    'p_age': ['A06', 'A07'],
    'tb': ['A15', 'A16', 'A17', 'A18', 'A19'],
    'myco': ['A30', 'A31'],
    'lepto': ['A27'],
    'std': ['A51','A52','A53','A54','A55','A56','A57','A58','A59','A63','A64'],
    'torch': ['B58','B06','B25','B00','A60'],
    'v_skin': ['B01','B02','B03','B04','B05','B07','B08','B09'],
    'hepatitis': ['B15','B16','B17','B18','B19'],
    'hiv': ['B20','B21','B22','B23','B24'],
    'sepsis': ['A40','A41'],
    'infla_cns': ['G00','G01','G02','G03','G04','G08','G05','G06','G07','G09'],
    'urti': ['J00','J01','J02','J03','J04','J05','J06','J09','J10','J11'],
    'lrti': ['J12','J13','J14','J15','J16','J17','J18','J20','J21','J22'],
    'uti': ['N30','N34','N39']
}

# Pregnancy conditions
pregnancy_conditions = {
    'abortive': ['O00', 'O01', 'O02', 'O03', 'O04', 'O05', 'O06', 'O07', 'O08'],
    'preecl': ['O11', 'O14'], 'ecl': ['O15'], 'earlyhemo': ['O20'], 'heg': ['O21'],
    'venpreg': ['O22'], 'utipreg': ['O23'], 'malpreg': ['O25'], 'multigest': ['O30'],
    'malpresent': ['O32'], 'disprop': ['O33'], 'abnorpelv': ['O34'], 'fetalprob': ['O35', 'O36'],
    'polyhydra': ['O40'], 'abnamnio': ['O41'], 'prom': ['O42'], 'placental': ['O43'],
    'previa': ['O44'], 'abrupt': ['O45'], 'anh': ['O46'], 'prolong': ['O48'],
    'preterm': ['O60'], 'fail': ['O61'], 'abnforce': ['O62'], 'long': ['O63'],
    'obspelvic': ['O65', 'O66'], 'malpres': ['O64'], 'iph': ['O67'], 'distress': ['O68'],
    'umbilical': ['O69'], 'laceration': ['O70'], 'obstrau': ['O71'], 'pph': ['O72'],
    'retained': ['O73'], 'normal': ['O80'], 'instrum': ['O81'], 'caesar': ['O82'],
    'assisted': ['O83'], 'multiple': ['O84']
}

# Regex conditions
conditions_regex = {
    'arthropathy': r'M(0[0-9]|1[0-9]|2[0-5])',
    'sysconn': r'M3[0-6]',
    'dorsopathy': r'M4[0-9]|M5[0-4]',
    'muscle_dis': r'M6[0-3]',
    'synov_dis': r'M6[5-8]',
    'soft_dis': r'M8[0-9]|M9[0-4]',
    'renal_dis': r'N0[0-9]|N1[0-6]',
    'renal_fail': r'N1[7-9]',
    'breast_dis': r'N6[0-4]',
    'pid': r'N7[0-7]',
    'poison': r'T3[6-9]|T4[0-9]|T50',
    'toxic': r'T5[1-9]|T6[0-5]'
}

# Gabungkan kondisi chronic + infectious
all_conditions = {}
all_conditions.update(chronic_conditions)
all_conditions.update(infectious_conditions)

# --- Pre-compile regex ---
compiled_patterns = {condition: re.compile(regex) for condition, regex in conditions_regex.items()}

# === DATE CONDITIONS ===
before_chronic   = (sept['combined_date'] <= sept['ref'])
before_inf_preg  = (sept['combined_date'] < sept['ref_start'])

after_chronic_inf = (sept['combined_date'] > sept['ref'])
after_preg        = (sept['combined_date'] - sept['ref']).dt.days > 30

during_inf  = (
    (sept['combined_date'] >= sept['ref_start']) &
    (sept['combined_date'] <= sept['ref'])
)
during_preg = (
    (sept['combined_date'] >= sept['ref_start']) &
    (sept['combined_date'] <= (sept['ref'] + pd.Timedelta(days=30)))
)

# === LOOPING ===

# Chronic
for cond, codes in chronic_conditions.items():
    m = (sept['FKP14A'].isin(codes) |
         sept['FKL15A'].isin(codes) |
         sept['FKL17A'].isin(codes) |
         sept['FKL24A'].isin(codes))
    sept[f'b_{cond}'] = (m & before_chronic).astype(int)
    sept[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Infectious
for cond, codes in infectious_conditions.items():
    m = (sept['FKP14A'].isin(codes) |
         sept['FKL15A'].isin(codes) |
         sept['FKL17A'].isin(codes) |
         sept['FKL24A'].isin(codes))
    sept[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    sept[f'c_{cond}'] = (m & during_inf).astype(int)
    sept[f'a_{cond}'] = (m & after_chronic_inf).astype(int)

# Pregnancy
for cond, codes in pregnancy_conditions.items():
    m = (sept['FKP14A'].isin(codes) |
         sept['FKL15A'].isin(codes) |
         sept['FKL17A'].isin(codes) |
         sept['FKL24A'].isin(codes))
    sept[f'b_{cond}'] = (m & before_inf_preg).astype(int)
    sept[f'c_{cond}'] = (m & during_preg).astype(int)
    sept[f'a_{cond}'] = (m & after_preg).astype(int)

# Regex (chronic rules)
for cond, pat in compiled_patterns.items():
    m = (sept['FKP14A'].str.contains(pat, na=False) |
         sept['FKL15A'].str.contains(pat, na=False) |
         sept['FKL17A'].str.contains(pat, na=False) |
         sept['FKL24A'].str.contains(pat, na=False))
    sept[f'b_{cond}'] = (m & before_chronic).astype(int)
    sept[f'a_{cond}'] = (m & after_chronic_inf).astype(int)


# Extract year from 'ref'
sept['ref_year'] = sept['ref'].dt.year

# Remove columns that are no longer needed
columns_to_drop = ['FKP03', 'FKP04', 'FKP13', 'FKP14A', 'FKL03', 'FKL04', 'FKL09', 'FKL11', 'FKL14', 'FKL15A', 'FKL16', 'FKL17A', 'FKL18', 'FKL24A', 'PSTV18', 'fin_g', 'combined_date', 'ref', 'ref_start']
nana = sept.drop(columns=columns_to_drop, errors='ignore')

# Set all values in the 'n_preg' column to 5
nana['n_preg'] = 7

aggregation_rules = {
    'subsid': 'max',  
    'age': 'min',     
    'dom': 'min',     
    'age_risk' : 'min',
    'subsid': 'max', 
    'n_preg' : 'min',
    'ref_year' : 'min'
}

# Add rules for columns starting with 'b_', 'a_' or 'c_' to cap their sum at 1
for col in nana.columns:
    if col.startswith('b_') or col.startswith('a_') or col.startswith('c_'):
        aggregation_rules[col] = lambda x: min(x.sum(), 1)

# Perform the groupby and aggregation
fin_nan = nana.groupby('PSTV01').agg(aggregation_rules).reset_index()

fin_nan.to_csv("7th.csv",index=False)

# Merge all tables 
one = pd.read_csv("1st_washed.csv")
two = pd.read_csv("2nd.csv")
thr = pd.read_csv("3rd.csv")
fou = pd.read_csv("4th.csv")
fiv = pd.read_csv("5th.csv")
six = pd.read_csv("6th.csv")
sev = pd.read_csv("7th.csv")

# List of tables
tables = {'one': one, 'two': two, 'thr': thr, 'fou': fou, 'fiv': fiv, 'six': six, 'sev': sev}

# Check the number of columns in each table
columns_count = {name: df.shape[1] for name, df in tables.items()}

# Display the number of columns in each table
for name, count in columns_count.items():
    print(f"Table {name} has {count} columns.")

# Check whether all tables have the same number of columns
all_same = len(set(columns_count.values())) == 1
if all_same:
    print("\nAll tables have the same number of columns.")
else:
    print("\nThe tables have a different number of columns.")


all = pd.concat([one, two, thr, fou, fiv, six, sev], axis=0, ignore_index=True)

all

all.to_csv("final_set.csv",index=False)



