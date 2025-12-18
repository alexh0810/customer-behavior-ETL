# Customer Behavior Analytics Pipeline

### Search Trends, Content Engagement & Analytics Marts

An end-to-end analytics engineering project that processes user search behavior and content interaction data, models analytics marts in DuckDB, and exposes insights through SQL, visualizations, and a Streamlit dashboard.

## 1. Problem

Key questions to answer:

- How stable user interests are over time
- Where and how interests shift between categories
- How engaged users are based on actual activity, not assumptions

## 2. Solution Overview

This project implements a complete analytics workflow:

- PySpark ETL for scalable, incremental processing
- LLM-based keyword classification to infer search intent
- DuckDB analytics warehouse for fast, reproducible querying
- SQL analytics views and marts as the single source of truth
- Static Python visualizations for documented outputs
- Streamlit dashboard for interactive exploration

All business logic is defined in SQL marts. Dashboards and charts are read-only consumers.

## 3. Architecture

```
Raw Logs (Search & Content)
        │
        ▼
PySpark ETL (incremental)
        │
        ▼
DuckDB Warehouse
  ├── fact_search_behavior
  ├── fact_content_interactions
  ├── analytics_* views
  └── analytics marts
        │
        ▼
Visualizations & Streamlit Dashboard
```

### 4. Key features

✔ Scalable ETL with PySpark

- User-level aggregations
- Incremental processing by date
- Config-driven via config.yaml

✔ LLM-based Search Classification

- Groq LLM used to classify search keywords into genres
- Robust parsing and LLM mocking in tests

✔ Analytics Warehouse & Marts

- DuckDB used as a lightweight analytics warehouse
- SQL marts define metrics such as:

  - search stability
  - interest transitions
  - engagement segmentation

✔ Correct Engagement Metrics

- ActiveDays computed from real activity
- Engagement tiers derived from observed usage

✔ Multiple Output Layers

- Static charts (PNG)
- Interactive Streamlit dashboard
- SQL marts reusable by BI tools

### 5. Documented Outputs

This project produces documented, reproducible analytics outputs.

#### Static results & explanations

📊 See: `docs/OUTPUT.md`

Includes:

- final charts
- SQL queries used
- interpretation of results

#### Interactive exploration

A Streamlit dashboard provides interactive access to the same analytics marts.

### 6. Project Structure

```bash
customer-behavior-etl/
├── config.yaml
├── src/
│   ├── etl/                 # Spark ETL pipelines
│   ├── llm/                 # LLM classification logic
│   ├── warehouse/           # DuckDB + SQL marts
│   ├── viz/                 # Static visualization scripts
│   └── dashboard/           # Streamlit app
├── docs/                    # OUTPUT.md + charts
├── tests/
├── requirements.txt
├── Dockerfile
└── README.md

```

### 7. Installation

```bash
git clone <repo-url>
cd customer-behavior-etl

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 8. Configuration

All pipeline settings are stored in `config.yaml`:

```yaml
base_path: data/log_search
output_path: output/

max_keywords: 100
batch_size: 30
write_output: false
```

### 9. LLM API Setup

This project uses Groq LLM for keyword classification.

macOS / Linux:

```bash
export GROQ_API_KEY="your_groq_api_key_here"
```

Windows:

```bash
setx GROQ_API_KEY "your_groq_api_key_here"
```

### 10. Running the pipeline

```
# Run ETL
python -m src.etl.ETL_log_search
python -m src.etl.ETL_log_content

# Load data into DuckDB
python -m src.warehouse.load_parquet

# Create analytics views & marts
python -m src.warehouse.run_sql

# Generate static charts
python src/viz/plot_search_stability.py
python src/viz/plot_interest_change_summary.py
python src/viz/plot_contract_engagement.py

```

### 11. Running the dashboard

```bash
PYTHONPATH=. streamlit run src/dashboard/app.py
```

### 12. Testing

```bash
pytest -q
```

### Tech Stack

- PySpark
- DuckDB
- SQL
- Python (matplotlib, Streamlit)
- Groq LLM
