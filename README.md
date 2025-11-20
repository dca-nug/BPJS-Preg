# Pregnancy Cohort Construction – BPJS Kesehatan Contextual Maternal and Child Health Data

## ⚠️ Disclaimer

The Python scripts in this repository are written in **Bahasa Indonesia** to ensure better understanding for local researchers and data analysts. 

---

## Description

This repository contains Python scripts for constructing a pregnancy cohort from the sample data of BPJS Kesehatan (Indonesia’s National Health Insurance). The dataset is part of the *Kontekstual Ibu dan Anak* (Contextual Maternal and Child Health) dataset, which includes health claim records related to maternal and child health services.

These scripts process and structure the data to identify pregnancy episodes and related outcomes.

---

## Data Source

The data used in this project is available **by request** from BPJS Kesehatan at:

👉 [https://data.bpjs-kesehatan.go.id/bpjs-portal/action/landingPage.cbi](https://data.bpjs-kesehatan.go.id/bpjs-portal/action/landingPage.cbi)

> ⚠️ **Note:** Data is only accessible from within Indonesia and requires official request to BPJS Kesehatan.

---

## Use Case

This cohort construction can support:
- Epidemiological research on maternal health, covering conditions before, during, and after pregnancy.
- Policy planning and targeting, using real-world data to understand needs and outcomes in diverse populations.

---

## Requirements

- Python 3.x
- Required libraries include:
  - `pandas`
  - `numpy`
  - `datetime`

---

## How to Use

1. Request and download the dataset from BPJS Kesehatan.
2. Update the file paths in the script to point to your local data.
3. Run the scripts step by step to generate a structured pregnancy cohort.
4. Output will include clean and analysis-ready datasets.

> ⚠️ This repository **does not include the dataset** due to privacy and access restrictions.

---

## Script Overview

This repository includes several processing and analysis scripts that support dataset transformation, cohort construction, quality control, and epidemiologic analysis across multiple data levels (visit, individual, and pregnancy).

**1. `dataset generation.py`**  
Transforms the raw BPJS Kesehatan dataset into structured **visit-level** and **pregnancy-level** datasets.

**2. `dataset generation.ipynb`**  
A stepwise Jupyter notebook implementation of the dataset generation pipeline. This version is recommended for environments with limited memory resources, as it allows incremental execution rather than full-batch processing.

**3. `generate individual-level.py`**  
Derives the **individual-level** dataset by aggregating and restructuring the visit-level dataset.

**4. Quality Control Scripts**  
- `QC_visit individual level.py`: Performs quality control checks on visit-level and individual-level datasets.  
- `QC_pregnancy level.py`: Performs quality control checks on pregnancy-level datasets.

**5. `remove flagged in pregnancy-level.py`**  
Filters or removes pregnancy episodes that are flagged as inconsistent or invalid during the QC process.

**6. Odds Ratio Analysis Scripts**  
- `OR_pregnancy-level.py`: Conducts odds ratio analysis on pregnancy-level datasets.  
- Additional OR scripts are provided for individual-level and visit-level analyses, following the same naming conventions.

---

