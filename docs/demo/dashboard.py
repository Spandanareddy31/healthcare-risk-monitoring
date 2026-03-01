import os
import pandas as pd
import streamlit as st

PARQUET_DIR = os.path.join("data", "processed_parquet")

st.set_page_config(page_title="Healthcare Risk Monitor", layout="wide")
st.title("Real-Time Healthcare Risk Monitor")

if not os.path.exists(PARQUET_DIR):
    st.warning("No processed data yet. Start Spark streaming job first.")
    st.stop()

try:
    df = pd.read_parquet(PARQUET_DIR)
except:
    st.warning("Waiting for data to be processed...")
    st.stop()

if df.empty:
    st.warning("No records yet. Wait for streaming data.")
    st.stop()

df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
df = df.sort_values("event_time", ascending=False)

st.subheader("Latest Patient Vitals")
st.dataframe(df.head(50), use_container_width=True)

st.subheader("Risk Distribution")
st.bar_chart(df["risk_label"].value_counts())
