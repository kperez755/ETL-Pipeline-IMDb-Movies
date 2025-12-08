# ETL Pipeline – Movies  
A production-ready data engineering project that builds an end-to-end **ETL (Extract, Transform, Load)** and **Machine Learning** pipeline for movie metadata and ratings.

## 🗂 Project Overview  
This repository contains the code to:  
- Extract raw movie data from a single data source (CSV from Kaggle)  
- Transform and clean the data (remove duplicates, fix formats, normalize attributes)  
- Load the processed data into a SQL database (PostgreSQL) for downstream analysis  
- Train a regression model to evaluate feature importance on IMDb ratings  

The goal is to deliver a reliable, repeatable pipeline that produces **clean, analytics‑ready data** and supports machine learning workflows.

---

## 🔍 Data Sources  
The pipeline expects the following raw data file:   
- `movies_metadata.csv` — Kaggle movie metadata  

---

## 🧰 Tools & Technologies  
- **Python**: `pandas`, `numpy`, `sqlalchemy`, `psycopg2`, `xgboost`, `scikit-learn`  
- **PostgreSQL** or compatible SQL database  
- **SQL** for validation and querying  
- **Git/GitHub** for version control  

---

## 🚦 Pipeline Steps  

### 1. Extract  
- Reads raw CSV data into DataFrames  
- Performs initial parsing and formatting  

### 2. Transform  
- Cleans Kaggle metadata (type conversions, removes duplicates, fixes invalid fields)  
- Processes Rejected Data into Two separate CSV files (Dropped_columns and Dropped_rows)
- Merges cleaned datasets into unified movie tables  (movies_cleaned)

### 3. Load  
- Creates database connections  
- Builds database tables (`movies`, `ratings`, etc.)  
- Loads transformed datasets into SQL
- Validates row counts and schema

## ✅ Usage  

### 1. Clone the repository  
```bash
git clone https://github.com/kperez755/ETL-Pipeline-movies.git
cd ETL-Pipeline-movies
```

### 2. Install dependencies  
```bash
pip install -r requirements.txt
```

### 3. Add raw data  
Place your Kaggle files into the `data/raw/` directory.

### 4. Run the ETL pipeline  
```bash
python -m main.py
```

### 5. Verify database output  
Use PgAdmin, DBeaver, or any SQL client to inspect loaded tables.


## 📊 Results  
When the pipeline completes, you will have:  
- A normalized **movies** table with merged metadata  
- A production-ready database for analytics or machine learning workflows  

## 🧠 Future Improvements  
- Orchestrate the pipeline with Airflow or Prefect  
- Add multi-source ingestion (IMDb, TMDB, streaming APIs)  
- Build BI dashboards with Tableau / Power BI / Plotly  
- Deploy the ML model as an API endpoint  

---

## 🙏 Acknowledgements  
This project uses open-source datasets from Kaggle and the broader data community.  
