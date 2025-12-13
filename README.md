# Customer Behavior ETL — Search Trends & Genre Classification

This project analyzes user search logs from June and July, extracts each user’s **top search keyword per month**, classifies these keywords into **content genres using an LLM**, and identifies whether the user's search preference **changed** between months.

It is designed to demonstrate practical **data engineering** skills and best practices.

---

# 🚀 Features

### ✔ Scalable ETL with PySpark

Processes large parquet datasets with user-level aggregations.

### ✔ LLM-based keyword classification

Uses Groq LLM to classify search keywords into genres (Action, Horror, K-Drama, etc.).

### ✔ Trend analysis

Detects whether users stayed in the same genre or switched preferences.

### ✔ Professional engineering practices

- Config-driven design (`config.yaml`)
- Pretty logging (timestamped, severity-level)
- Modular folder structure
- Unit tests for utilities + LLM mocking
- Docker-ready
- Fully reproducible local run

---

# 📁 Project Structure

```
customer-behavior-etl/
│
├── config.yaml # Pipeline settings (paths, max keywords, batch size, etc.)
│
├── src/
│ ├── etl/ETL_log_search.py # Main ETL pipeline (Spark)
│ ├── llm/ask_llm.py # LLM batch classifier (Groq)
│ └── utils/parse_helpers.py # Robust JSON extraction utilities
│
├── data/
│ ├── log_search/ # Raw data (not stored in repo)
│ └── sample/ # Small anonymized dataset for demo runs
│
├── scripts/
│ └── make_sample_pandas.py # Creates demo sample data
│
├── tests/
│ ├── test_parse_helpers.py
│ └── test_llm_mock.py
│
├── requirements.txt
├── Dockerfile
├── README.md
└── .gitignore
```

---

# ⚙️ Installation

### 1️⃣ Clone the repository

```bash
git clone <repo-url>
cd customer-behavior-etl
```

2️⃣ Create and activate a virtual environment

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

# 🔧 Configuration

All settings are stored in config.yaml:

```yaml
base_path: 'data/log_search/log_search'
output_path: 'output/final_with_genre'

max_keywords: 100
batch_size: 30
write_output: false
```

Edit this file to:

- Change data input path
- Enable output writing
- Expand number of keywords sent to the LLM
- Change batch size

# 🔐 Groq API Key Setup

This project uses Groq LLM to classify search keywords into content genres.
You must set a Groq API key as an environment variable before running the ETL.

macOS / Linux

```bash
export GROQ_API_KEY="your_groq_api_key_here"
```

Windows

```bash
setx GROQ_API_KEY "your_groq_api_key_here"

```

# ▶️ Running the ETL Pipeline

Once dependencies and config are ready:

```bash
python src/etl/ETL_log_search.py
```

This will:

- Load search logs for June & July
- Compute each user's top keyword in each month
- Rank the most frequent keywords (Top N)
- Send keywords to LLM for classification
- Join predictions back to users
- Compute Trending_Type (Changed / Unchanged)
- Print a sample of results
- Save output (if write_output = true)

# 🧪 Running Tests

```bash
pytest -q
```

Tests include:

- JSON extraction from messy LLM responses
- Classification pipeline with LLM mocking
- Ensuring stability of keyword mapping logic
- These demonstrate engineering discipline beyond basic scripting.

# 🐳 Running with Docker (optional)

Build the container:

```bash
docker build -t customer-etl .
```

Build the container:

```bash
docker run --rm customer-etl
```

📊 Example Output:

```
+--------+-------------------+-------------+-------------------+--------------+-------------+--------------------+
| user_id| most_search_June  |category_june| most_search_July  |category_july |Trending_Type| Previous           |
+--------+-------------------+-------------+-------------------+--------------+-------------+--------------------+
| 12345  | avengers          | Action      | avengers          | Action       | Unchanged   | Unchanged          |
| 67890  | parasite          | K-DRAMA     | outlast           | Horror       | Changed     | K-DRAMA-Horror     |
+--------+-------------------+-------------+-------------------+--------------+-------------+--------------------+

```
