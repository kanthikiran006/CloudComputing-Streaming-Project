import streamlit as st
import pandas as pd
import time

st.set_page_config(page_title="Big Data Dashboard", layout="wide")

st.title("🚀 Real-Time Big Data Intelligence Dashboard")

# -------- AUTO REFRESH --------
st.experimental_rerun if False else None
time.sleep(2)

# -------- LOAD DATA --------
def load_data(path):
    try:
        return pd.read_parquet(path)
    except:
        return pd.DataFrame()

high_risk = load_data("../data/high_risk")
revenue = load_data("../data/revenue")
engagement = load_data("../data/engagement")
sentiment = load_data("../data/sentiment")
trends = load_data("../data/trends")

# -------- SAFE VALUES --------
revenue_val = revenue['total_revenue'].sum() if 'total_revenue' in revenue else 0
engagement_val = engagement['total_likes'].sum() if 'total_likes' in engagement else 0
rating_val = sentiment['avg_rating'].mean() if 'avg_rating' in sentiment else 0

# -------- KPI SECTION --------
st.subheader("📊 Key Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric("🚨 High Risk", len(high_risk))
col2.metric("💰 Revenue", int(revenue_val))
col3.metric("👍 Engagement", int(engagement_val))
col4.metric("⭐ Rating", round(rating_val, 2))

st.divider()

# -------- CHARTS --------
col1, col2 = st.columns(2)

with col1:
    st.subheader("💰 Revenue Trend")
    if 'total_revenue' in revenue:
        st.line_chart(revenue['total_revenue'])

with col2:
    st.subheader("🔥 Engagement Trend")
    if 'total_likes' in engagement:
        st.line_chart(engagement['total_likes'])

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("⭐ Rating Trend")
    if 'avg_rating' in sentiment:
        st.line_chart(sentiment['avg_rating'])

with col2:
    st.subheader("📈 Trend Detection")
    if 'likes_sum' in trends:
        st.bar_chart(trends['likes_sum'])

st.divider()

# -------- FRAUD TABLE --------
st.subheader("🚨 High Risk Transactions")

if not high_risk.empty:
    st.dataframe(high_risk.tail(20))
else:
    st.info("No high-risk data yet...")
