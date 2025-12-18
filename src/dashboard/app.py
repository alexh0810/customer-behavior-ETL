# src/dashboard/app.py

import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

from src.dashboard.queries import (
    SEARCH_STABILITY,
    INTEREST_TRANSITIONS,
    CONTRACT_ENGAGEMENT,
)

# ----------------------------------
# App config
# ----------------------------------
st.set_page_config(
    page_title="Customer Behavior Analytics",
    layout="wide",
)

st.title("📊 Customer Behavior Analytics Dashboard")

# ----------------------------------
# Paths & DB
# ----------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "src" / "warehouse" / "analytics.duckdb"


@st.cache_data
def run_query(sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH))
    df = con.execute(sql).fetchdf()
    con.close()
    return df


# ----------------------------------
# Sidebar navigation
# ----------------------------------
page = st.sidebar.radio(
    "Select analysis",
    [
        "Search Stability",
        "Interest Transitions",
        "Contract Engagement",
    ],
)

# ----------------------------------
# Search Stability
# ----------------------------------
if page == "Search Stability":
    st.subheader("🔍 Search Stability vs Change")

    df = run_query(SEARCH_STABILITY)

    st.bar_chart(df.set_index("trending_type")["percentage"])

    st.markdown(
        """
        **Insight:**  
        Most users tend to keep consistent search interests across periods,
        while a meaningful segment shifts categories, indicating evolving or
        event-driven preferences.
        """
    )

# ----------------------------------
# Interest Transitions
# ----------------------------------
elif page == "Interest Transitions":
    st.subheader("🔁 Top Interest Transitions")

    df = run_query(INTEREST_TRANSITIONS)

    st.bar_chart(df.set_index("transition")["users"])

    st.markdown(
        """
        **Insight:**  
        These transitions show where interest changed most often, helping
        identify emerging trends and cross-category movement.
        """
    )

# ----------------------------------
# Contract Engagement
# ----------------------------------
elif page == "Contract Engagement":
    st.subheader("📈 Contract Engagement Levels")

    df = run_query(CONTRACT_ENGAGEMENT)

    st.bar_chart(df.set_index("engagement_level")["contracts"])

    st.markdown(
        """
        **Insight:**  
        Contracts are segmented into Low, Medium, and High engagement
        based on how frequently content was consumed during the period.
        """
    )
