<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [ETL Pipeline – Movies  ](#etl-pipeline-movies)
   * [🗂 Project Overview  ](#-project-overview)
   * [🔍 Data Sources  ](#-data-sources)
   * [🧰 Tools & Technologies  ](#-tools-technologies)
   * [🚦 Pipeline Steps  ](#-pipeline-steps)
      + [1. Extract  ](#1-extract)
      + [2. Transform  ](#2-transform)
      + [3. Load  ](#3-load)
   * [✅ Usage  ](#-usage)
      + [1. Clone the repository  ](#1-clone-the-repository)
      + [2. Install dependencies  ](#2-install-dependencies)
      + [3. Add raw data  ](#3-add-raw-data)
      + [4. Run the ETL pipeline  ](#4-run-the-etl-pipeline)
      + [5. Verify database output  ](#5-verify-database-output)
   * [📊 Results  ](#-results)
   * [🧠 Future Improvements  ](#-future-improvements)
   * [🙏 Acknowledgements  ](#-acknowledgements)

<!-- TOC end -->

<!-- TOC --><a name="etl-pipeline-movies"></a>
# ETL Pipeline – Movies  
A production-ready data engineering project that builds an end-to-end **ETL (Extract, Transform, Load)** and **Machine Learning** pipeline for movie metadata and ratings.

<!-- TOC --><a name="-project-overview"></a>
## 🗂 Project Overview  
This repository contains the code to:  
- Extract raw movie data from a single data source (CSV from Kaggle)  
- Transform and clean the data (remove duplicates, fix formats, normalize attributes)  
- Load the processed data into a SQL database (PostgreSQL) for downstream analysis  
- Train a regression model to evaluate feature importance on IMDb ratings  

The goal is to deliver a reliable, repeatable pipeline that produces **clean, analytics‑ready data** and supports machine learning workflows.

---

<!-- TOC --><a name="-data-sources"></a>
## 🔍 Data Sources  
The pipeline expects the following raw data file:   
- `movies.csv` — Kaggle movie metadata  

---

<!-- TOC --><a name="-tools-technologies"></a>
## 🧰 Tools & Technologies  
- **Python**: `pandas`, `numpy`, `sqlalchemy`, `psycopg2`, `xgboost`, `scikit-learn`, `joblib`, `unittest`, `logging`
- **PostgreSQL** or compatible SQL database  
- **SQL** for validation and querying  
- **Git/GitHub** for version control  

---

<!-- TOC --><a name="-pipeline-steps"></a>
## 🚦 Pipeline Steps  

<!-- TOC --><a name="1-extract"></a>
### 1. Extract  
- Reads raw CSV data into DataFrames  
- Performs initial parsing and formatting  

<!-- TOC --><a name="2-transform"></a>
### 2. Transform  
- Cleans Kaggle metadata (type conversions, removes duplicates, fixes invalid fields)  
- Processes Rejected Data into Two separate CSV files (Dropped_columns and Dropped_rows)
- Merges cleaned datasets into unified movie tables  (movies_cleaned)

<!-- TOC --><a name="3-load"></a>
### 3. Load  
- Creates database connections  
- Builds database tables (`movies`, `ratings`, etc.)  
- Loads transformed datasets into SQL
- Validates row counts and schema

<!-- TOC --><a name="-usage"></a>
## ✅ Usage  

<!-- TOC --><a name="1-clone-the-repository"></a>
### 1. Clone the repository  
```bash
git clone https://github.com/kperez755/ETL-Pipeline-IMDb-Movies.git
cd ETL-Pipeline-IMDb-Movies
```

<!-- TOC --><a name="2-install-dependencies"></a>
### 2. Install dependencies  
```bash
pip install -r requirements.txt
```

<!-- TOC --><a name="3-add-raw-data"></a>
### 3. Add raw data  
Place your Kaggle files into the `data/raw/` directory.

<!-- TOC --><a name="4-run-the-etl-pipeline"></a>
### 4. Run the ETL pipeline  
```bash
python -m main.py
```

<!-- TOC --><a name="5-verify-database-output"></a>
### 5. Verify database output  
Use PgAdmin, DBeaver, or any SQL client to inspect loaded tables.


<!-- TOC --><a name="-results"></a>
## 📊 Results  
When the pipeline completes, you will have:  
- A normalized **movies** table with merged metadata  
- A production-ready database for analytics or machine learning workflows
- A bar graph visualization ready for visualization and further analysis 

<!-- TOC --><a name="-future-improvements"></a>
## 🧠 Future Improvements  
- Orchestrate the pipeline with Airflow or Prefect  
- Add multi-source ingestion (IMDb, TMDB, streaming APIs)  
- Build BI dashboards with Tableau / Power BI / Plotly  
- Deploy the ML model as an API endpoint  

---

<!-- TOC --><a name="-acknowledgements"></a>
## 🙏 Acknowledgements  
This project uses open-source datasets from Kaggle and the broader data community.  
