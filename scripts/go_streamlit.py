# go_streamlit.py (cleaned and corrected)

import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import io
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
import os
from urllib.parse import urljoin

# -------------------------------
# 🔐 AWS Setup
# -------------------------------
os.environ["AWS_PROFILE"] = "de_jay_east"
BUCKET = "gp-elt-657082399901-dev"

# -------------------------------
# 📦 Unified Helper: Load Parquet from S3
# -------------------------------
@st.cache_data
def load_parquet_from_s3(bucket, prefix):
    """
    Dynamically load flat or partitioned Parquet files from S3.
    Includes partition columns (like restaurant_id) if present.
    """
    s3 = boto3.client("s3")
    s3_path = f"s3://{bucket}/metrics/{prefix}/"
    fs = s3fs.S3FileSystem()

    # List all objects under the prefix
    result = s3.list_objects_v2(Bucket=bucket, Prefix=f"metrics/{prefix}/")
    if "Contents" not in result:
        raise FileNotFoundError(f"No files found at s3://{bucket}/metrics/{prefix}/")

    # Check if any partition-style keys (e.g. restaurant_id=...) exist
    is_partitioned = any("=" in obj["Key"] for obj in result["Contents"])

    # --- Load partitioned ---
    if is_partitioned:
        all_keys = [obj["Key"] for obj in result["Contents"] if obj["Key"].endswith(".parquet")]
        dfs = []

        for key in all_keys:
            partition_path = f"s3://{bucket}/{key}"
            df = pd.read_parquet(partition_path)
            # Extract partition values from S3 key
            parts = key.replace(f"metrics/{prefix}/", "").split("/")
            for part in parts:
                if "=" in part:
                    col, val = part.split("=")
                    df[col] = val
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    # --- Load flat ---
    for obj in result["Contents"]:
        if obj["Key"].endswith(".parquet"):
            response = s3.get_object(Bucket=bucket, Key=obj["Key"])
            return pd.read_parquet(io.BytesIO(response["Body"].read()))

    raise FileNotFoundError(f"No Parquet files found at s3://{bucket}/metrics/{prefix}/")

# -------------------------------
# 🔹 Sidebar Navigation
# -------------------------------
st.set_page_config("Customer Analytics", layout="wide")
st.sidebar.title("📊 Dashboard Sections")
section = st.sidebar.radio("Select View", [
    "Customer Segmentation", "Churn Risk Indicators", "Sales Trends",
    "Loyalty Program Impact", "Location Performance", "Discount Effectiveness"
])

# ---------------------------
# 🔄 Load All Metrics
# ---------------------------
if st.button("🔄 Clear Cache"):
    st.cache_data.clear()
    st.experimental_rerun()
elif st.button("🔄 Reload Data"):
    st.cache_data.clear()  # If using Streamlit v1.18+


@st.cache_data
def load_data():
    return {
        "revenue": load_parquet_from_s3(BUCKET, "revenue_per_order"),
        "clv": load_parquet_from_s3(BUCKET, "clv_tagged"),
        "rfm": load_parquet_from_s3(BUCKET, "rfm_segmented"), 
        "profile": load_parquet_from_s3(BUCKET, "customer_profile"),
        "daily": load_parquet_from_s3(BUCKET, "sales_trends/daily"),
        "weekly": load_parquet_from_s3(BUCKET, "sales_trends/weekly"),
        "monthly": load_parquet_from_s3(BUCKET, "sales_trends/monthly"),
        "hourly": load_parquet_from_s3(BUCKET, "sales_trends/hourly"),
        "loyalty": load_parquet_from_s3(BUCKET, "loyalty_program_impact"),
        "loyalty_summary": load_parquet_from_s3(BUCKET, "loyalty_program_impact_summary"),
        "locations": load_parquet_from_s3(BUCKET, "top_locations"),
        "discounts": load_parquet_from_s3(BUCKET, "discount_effectiveness")
    }

data = load_data()

# ---------------------------
# 1. Customer Segmentation
# ---------------------------
if section == "Customer Segmentation":
    st.header("Customer Segmentation ( CLV + Loyalty, RFM)")

    # Sidebar restaurant filter
    st.sidebar.header("🔎 Filter Options")
    restaurant_ids = data["rfm"]["restaurant_id"].dropna().unique()
    selected_restaurant = st.sidebar.selectbox("Select Restaurant", restaurant_ids)

    # Filter RFM and CLV
    rfm_df = data["rfm"]
    clv_df = data["clv"]

    rfm_filtered = rfm_df[rfm_df["restaurant_id"] == selected_restaurant]
    clv_filtered = clv_df[clv_df["restaurant_id"] == selected_restaurant]

    
    st.subheader("1. Customer Lifetime Value (CLV): (Top 10)")
    st.dataframe(clv_filtered.sort_values("total_revenue", ascending=False).head(10))

    st.subheader("2. RFM Table: Customer Segmentation & Behavior")
    st.dataframe(rfm_filtered.sort_values("recency"))


    # Merge for RFM x CLV segmentation
    rfm_clv_df = pd.merge(
        rfm_filtered,
        clv_filtered[["customer_id", "clv_tag"]],
        on="customer_id", how="inner"
    )

    st.subheader("📊 RFM by CLV Segment")
    clv_segment = st.selectbox("Filter by CLV Tag", options=["All"] + sorted(rfm_clv_df["clv_tag"].unique()))
    filtered_df = rfm_clv_df if clv_segment == "All" else rfm_clv_df[rfm_clv_df["clv_tag"] == clv_segment]



    fig = px.scatter(
    filtered_df, x="frequency", y="recency",
    color="is_loyalty", size="monetary",
    hover_data=["customer_id", "segment"],
    title="📍 Frequency vs Recency with Loyalty and Spend"
)
    
    st.plotly_chart(fig, use_container_width=True)

    rfm_loyalty_avg = filtered_df.groupby("is_loyalty")[["recency", "frequency", "monetary"]].mean().reset_index()
    rfm_loyalty_avg = rfm_loyalty_avg.melt(id_vars="is_loyalty", var_name="metric", value_name="average")

    fig = px.bar(
        rfm_loyalty_avg,
        x="metric",
        y="average",
        color="is_loyalty",
        barmode="group",
        title="📊 Average RFM Metrics by Loyalty Status"
    )
    st.plotly_chart(fig, use_container_width=True)

    fig = px.histogram(filtered_df, x="recency", color="is_loyalty", barmode="overlay", nbins=30, title="🕒 Recency Distribution by Loyalty")
    st.plotly_chart(fig, use_container_width=True)

    fig = px.histogram(filtered_df, x="monetary", color="is_loyalty", barmode="overlay", nbins=30, title="💰 Monetary Distribution by Loyalty")
    st.plotly_chart(fig, use_container_width=True)


  

## ---------------------------
# 2. Churn Risk Indicators
# ---------------------------

elif section == "Churn Risk Indicators":
    st.header("Churn Risk Indicators Dashboard")

    # --- Load and normalize profile data ---
    profile_df = data.get("profile")
    if profile_df is None:
        st.error("No profile data found.")
        st.stop()
    profile_df.columns = [col.lower().strip() for col in profile_df.columns]

    # --- Load and normalize RFM data ---
    rfm_df = data.get("rfm")
    if rfm_df is None:
        st.error("No RFM data found.")
        st.stop()
    rfm_df.columns = [col.lower().strip() for col in rfm_df.columns]

    required_cols = {"restaurant_id", "customer_id"}
    if not required_cols.issubset(profile_df.columns) or not required_cols.issubset(rfm_df.columns):
        st.error("Required columns ('restaurant_id', 'customer_id') are missing.")
        st.stop()

    # --- Merge datasets ---
    df = pd.merge(rfm_df, profile_df, on=["restaurant_id", "customer_id"], how="inner")

    # --- Convert date ---
    df["last_order_date"] = pd.to_datetime(df["last_order_date"], errors="coerce")

    # --- Sidebar: Restaurant filter ---
    st.sidebar.header("🔎 Filter Options")
    restaurant_ids = df["restaurant_id"].dropna().unique()
    selected_restaurant = st.sidebar.selectbox("Select Restaurant", restaurant_ids)
    df = df[df["restaurant_id"] == selected_restaurant]

    # --- Sidebar: Threshold and filters ---
    threshold = st.sidebar.slider("Inactivity Threshold (days)", min_value=15, max_value=180, value=45, step=5)
    df["alert"] = df["last_order_date"] < (pd.Timestamp.now() - pd.Timedelta(days=threshold))

    st.sidebar.header("🔍 Additional Filters")
    segment_filter = st.sidebar.multiselect("Select Segment(s):", df["segment"].unique(), default=list(df["segment"].unique()))
    activity_filter = st.sidebar.multiselect("Select Churn Tag(s):", df["activity_tag"].unique(), default=list(df["activity_tag"].unique()))
    loyalty_only = st.sidebar.checkbox("🔁 Show Loyalty Members Only", value=False)

    # --- Apply filters ---
    df_filtered = df[
        df["segment"].isin(segment_filter) &
        df["activity_tag"].isin(activity_filter)
    ]
    if loyalty_only:
        df_filtered = df_filtered[df_filtered["is_loyalty"] == True]

    

    # --- Scatter Plot: Order Gap vs Days Since Last Order ---
    fig_gap_vs_recency = px.scatter(
        df_filtered,
        x="avg_gap_days",
        y="days_since_last_order",
        color="segment",
        size="monetary",
        # symbol="activity_tag",
        color_continuous_scale=px.colors.sequential.Viridis,
        size_max=40,
        hover_data=["customer_id", "segment", "is_loyalty"],
        title="📉 Churn Risk: Order Gaps vs Time Since Last Order",
        labels={
            "avg_gap_days": "Avg Gap Between Orders (days)",
            "days_since_last_order": "Days Since Last Order"
        }
    )
    st.plotly_chart(fig_gap_vs_recency, use_container_width=True)

    # --- Area Chart: Revenue Trends by Activity Tag ---
    fig_revenue_trend = px.area(
        df_filtered.sort_values("last_order_date"),
        x="last_order_date",
        y="monetary",
        color="activity_tag",
        title="💸 Revenue Trends by Churn Risk Group"
    )
    st.plotly_chart(fig_revenue_trend, use_container_width=True)

    # --- Aggregated Segment Distribution ---
    segment_dist = (
        df_filtered.groupby(['segment', 'activity_tag'])
        .size()
        .reset_index(name='count')
    )

    # --- Pastel Bar Chart: Segment vs Churn Risk ---
    fig_segment_dist = px.bar(
        segment_dist,
        x="segment",
        y="count",
        color="activity_tag",
        title='🎯 RFM Segment vs Churn Risk Distribution',
        barmode='group',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig_segment_dist.update_traces(marker_line=dict(width=1.5, color='rgba(255, 105, 180, 0.9)'))  # pastel border
    fig_segment_dist.update_layout(
        xaxis_title='RFM Segment',
        yaxis_title='Customer Count',
        title_font=dict(size=22, color='black'),
        plot_bgcolor='rgba(255, 255, 255, 1)',
        paper_bgcolor='rgba(255, 255, 255, 1)',
        legend_title_text='Activity Tag'
    )
    st.plotly_chart(fig_segment_dist, use_container_width=True)

    # --- Table: Detailed Customer Profile ---
    st.subheader("📋 Customer Profile Table")
    st.dataframe(
        df_filtered[[
            "restaurant_id", "customer_id", "segment", "recency", "frequency", "monetary",
            "last_order_date", "avg_gap_days", "avg_pct_change", "days_since_last_order",
            "activity_tag", "is_loyalty"
        ]].sort_values("days_since_last_order", ascending=False)
    )

# ---------------------------
# 3. Sales Trends
# ---------------------------
elif section == "Sales Trends":
    st.header("📈 Sales Trends & Seasonality")

    # --- Load and normalize profile data ---
    daily_df = data.get("daily")
    if daily_df is None:
        st.error("No daily data found.")
        st.stop()
    daily_df.columns = [col.lower().strip() for col in daily_df.columns]

    weekly_df = data.get("weekly")
    if weekly_df is None:
        st.error("No weekly data found.")
        st.stop()
    weekly_df.columns = [col.lower().strip() for col in weekly_df.columns]

    monthly_df = data.get("monthly")
    if monthly_df is None:
        st.error("No monthly data found.")
        st.stop()
    monthly_df.columns = [col.lower().strip() for col in monthly_df.columns]

    # Sidebar selections
    granularity = st.selectbox("View By", ["Daily", "Weekly", "Monthly"])
    df = data.get(granularity.lower())
    df.columns = [col.lower().strip() for col in df.columns]

    if df is None or df.empty:
        st.warning(f"No data available for {granularity}")
        st.stop()

    # Filter UI
    restaurant_ids = df["restaurant_id"].dropna().unique()
    # years = df["year"].dropna().unique() if "year" in df.columns else []
    # categories = df["item_category"].dropna().unique()

    selected_restaurant = st.sidebar.selectbox("Restaurant", restaurant_ids)
    # selected_year = st.sidebar.selectbox("Year", sorted(years, reverse=True)) if len(years) > 0 else None
    # selected_category = st.sidebar.selectbox("Item Category", sorted(categories))

    # Month filter for daily_df
    months = daily_df["month"].dropna().unique() if "month" in daily_df.columns else []
    selected_month = st.sidebar.selectbox("Month", sorted(months)) if len(months) > 0 else None

    # Apply filters to df
    df_filtered = df[df["restaurant_id"] == selected_restaurant]
    # if selected_year and "year" in df_filtered.columns:
    #     df_filtered = df_filtered[df_filtered["year"] == selected_year]
    # if selected_category:
    #     df_filtered = df_filtered[df_filtered["item_category"] == selected_category]

    # Apply filters to daily_df
    filtered_daily_df = daily_df[daily_df["restaurant_id"] == selected_restaurant]
    if selected_month:
        filtered_daily_df = filtered_daily_df[filtered_daily_df["month"] == selected_month]
    # if selected_category:
    #     filtered_daily_df = filtered_daily_df[filtered_daily_df["item_category"] == selected_category]



    # --- Revenue Trend Line Chart ---
    import calendar

    if granularity == "Daily":
        chart_df = filtered_daily_df.copy()
        x_col = "date"
        chart_df["date"] = pd.to_datetime(chart_df["date"])
        chart_df["year"] = chart_df["date"].dt.year
        chart_df["month"] = chart_df["date"].dt.month
        chart_df["month_name"] = chart_df["date"].dt.strftime("%B")
        chart_df["week_of_month"] = chart_df["date"].dt.day // 7 + 1

        # Year filter
        years_available = sorted(chart_df["year"].dropna().unique(), reverse=True)
        selected_year = st.sidebar.selectbox("Year", years_available)
        chart_df = chart_df[chart_df["year"] == selected_year]

        # Month filter
        months_available = sorted(chart_df["month"].dropna().unique())
        month_labels = [calendar.month_name[m] for m in months_available]
        selected_month_name = st.sidebar.selectbox("Month", month_labels)
        selected_month = list(calendar.month_name).index(selected_month_name)
        chart_df = chart_df[chart_df["month"] == selected_month]

        # Week-of-month filter
        weeks_available = sorted(chart_df["week_of_month"].dropna().unique())
        selected_week = st.sidebar.selectbox("Week of Month (1–5)", weeks_available)
        chart_df = chart_df[chart_df["week_of_month"] == selected_week]

        chart_df = chart_df.sort_values("date")

    elif granularity == "Weekly":
        chart_df = df_filtered.copy()
        x_col = "week"
        chart_df["week"] = pd.to_numeric(chart_df["week"], errors="coerce")

        # Year filter
        if "year" in chart_df.columns:
            years_available = sorted(chart_df["year"].dropna().unique(), reverse=True)
            selected_year = st.sidebar.selectbox("Year", years_available)
            chart_df = chart_df[chart_df["year"] == selected_year]

        # Month filter (optional if available)
        if "month" in chart_df.columns:
            chart_df["month_name"] = chart_df["month"].map(lambda m: calendar.month_name[int(m)])
            months_available = sorted(chart_df["month"].dropna().unique())
            month_labels = [calendar.month_name[m] for m in months_available]
            selected_month_name = st.sidebar.selectbox("Month", month_labels)
            selected_month = list(calendar.month_name).index(selected_month_name)
            chart_df = chart_df[chart_df["month"] == selected_month]

    elif granularity == "Monthly":
        chart_df = df_filtered.copy()
        x_col = "month"
        chart_df["month_name"] = chart_df["month"].map(lambda m: calendar.month_name[int(m)])
        month_order = list(calendar.month_name)[1:]  # Skip empty index 0

        chart_df["month_name"] = pd.Categorical(chart_df["month_name"], categories=month_order, ordered=True)
        x_col = "month_name"

        # Year filter
        if "year" in chart_df.columns:
            years_available = sorted(chart_df["year"].dropna().unique(), reverse=True)
            selected_year = st.sidebar.selectbox("Year", years_available)
            chart_df = chart_df[chart_df["year"] == selected_year]

    # --- Plot ---
    fig_trend = px.line(
        chart_df,
        x=x_col,
        y="revenue",
        color="year" if "year" in chart_df.columns else None,
        markers=True,
        title=f"📈 {granularity} Revenue Trends for {selected_restaurant}",
        labels={"revenue": "Revenue", x_col: granularity}
    )
    st.plotly_chart(fig_trend, use_container_width=True)


# ---------------------------
# 4. Loyalty Program Impact
# ---------------------------
elif section == "Loyalty Program Impact":
    st.header("📊 Loyalty Program Impact on Revenue")
    st.dataframe(data["loyalty_summary"])
    fig = px.box(data["loyalty"], x="is_loyalty", y="lifetime_value", color="is_loyalty",
                 title="Lifetime Value Distribution by Loyalty Status")
    st.plotly_chart(fig, use_container_width=True)

    fig = px.bar(
        data["loyalty"], x="is_loyalty", y="repeat_orders",
        title="Repeat Purchase Rate Comparison"
    )
    st.plotly_chart(fig, use_container_width=True)    



# ---------------------------
# 5. Location Performance
# ---------------------------
elif section == "Location Performance":
    st.header("📍 Top Performing Locations")
    df = data["locations"].sort_values("total_revenue", ascending=False)
    st.dataframe(df.head(10))
    fig = px.bar(df, x="restaurant_id", y="total_revenue", title="Revenue by Location")
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------
# 6. Discount Effectiveness
# ---------------------------
elif section == "Discount Effectiveness":
    st.header("📉 Discount vs Full Price Impact")
    df = data["discounts"]
    st.dataframe(df.head(10))
    fig = px.bar(df, x="discount_flag", y="total_revenue", color="discount_flag",
                 title="Revenue Comparison: Discounted vs Full Price")
    st.plotly_chart(fig, use_container_width=True)
