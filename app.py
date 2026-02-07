import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

st.set_page_config(page_title="재고 모니터링 대시보드", layout="wide")

def apply_plotly_theme(fig):
    fig.update_layout(
        template="plotly_white",
        font=dict(size=13),
        title_font=dict(size=16),
        legend_font=dict(size=12),
        xaxis=dict(showgrid=True, gridcolor="#e5e5e5"),
        yaxis=dict(
            showgrid=True,
            gridcolor="#e5e5e5",
            tickformat=",.0f",
        ),
        margin=dict(l=40, r=20, t=60, b=40),
    )
    return fig

@st.cache_data
def load_data():
    sku = pd.read_csv("sku_master.csv")
    demand = pd.read_csv("demand_daily.csv", parse_dates=["date"])
    inv = pd.read_csv("inventory_daily.csv", parse_dates=["date"])
    return sku, demand, inv

sku, demand, inv = load_data()

# DuckDB in-memory (SQL engine)
con = duckdb.connect(database=":memory:")
con.register("sku_master", sku)
con.register("demand_daily", demand)
con.register("inventory_daily", inv)

st.markdown("""
<style>
    h1 { font-size: 2.08rem !important; }
</style>
""", unsafe_allow_html=True)
st.title("📦 재고·수요 모니터링 대시보드")
st.caption("샘플 CSV 데이터 기반으로 SQL(DuckDB)로 KPI를 계산")

# Latest snapshot date
latest_date = con.execute("SELECT MAX(date) FROM inventory_daily").fetchone()[0]

category_map = {
  "ALL": "전체",
  "Motor": "모터",
  "Brake": "브레이크",
  "Steering": "스티어링",
  "Sensor": "센서",
}

warehouse_map = {
  "ALL" : "전체",
  "WH-1": "창고 1",
  "WH-2": "창고 2",
}

plant_map = {
  "ALL": "전체",
  "PLANT-A": "공장 A",
  "PLANT-B": "공장 B",
}

# Sidebar filters
st.sidebar.header("필터")
cat = st.sidebar.selectbox(
    "카테고리",
    options=["ALL"] + sorted(sku["category"].unique()),
    format_func=lambda x: category_map.get(x, x)
)

wh = st.sidebar.selectbox(
    "창고",
    options=["ALL"] + sorted(inv["warehouse"].unique()),
    format_func=lambda x: warehouse_map.get(x, x)
)

sku_pick = st.sidebar.selectbox(
    "SKU",
    options=["ALL"] + sorted(sku["sku"].unique()),
    format_func=lambda x: "전체" if x == "ALL" else x
)

# Build WHERE clauses
where_m = "WHERE 1=1"
if cat != "ALL":
    where_m += f" AND category = '{cat}'"
if sku_pick != "ALL":
    where_m += f" AND sku = '{sku_pick}'"

where_inv = f"WHERE date = '{latest_date}'"
if wh != "ALL":
    where_inv += f" AND warehouse = '{wh}'"
# --- KPIs (Summary) ---
kpi_sql = f"""
WITH base_sku AS (
  SELECT m.sku, m.sku_name, m.category
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
    {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
),
latest_inv AS (
  SELECT sku, SUM(onhand_qty) AS onhand_qty
  FROM inventory_daily
  WHERE date = '{latest_date}'
    {"AND warehouse = '"+wh+"'" if wh!="ALL" else ""}
  GROUP BY sku
),
demand_7d AS (
  SELECT sku, SUM(demand_qty) AS demand_7d
  FROM demand_daily
  WHERE date > '{latest_date}'::DATE - INTERVAL 7 DAY
  GROUP BY sku
),
inv_7d AS (
  SELECT i.date, SUM(i.onhand_qty) AS day_total
  FROM inventory_daily i
  JOIN base_sku b ON i.sku = b.sku
  WHERE i.date >= '{latest_date}'::DATE - INTERVAL 7 DAY AND i.date <= '{latest_date}'
    {"AND i.warehouse = '"+wh+"'" if wh!="ALL" else ""}
  GROUP BY i.date
),
inv_30d AS (
  SELECT i.date, SUM(i.onhand_qty) AS day_total
  FROM inventory_daily i
  JOIN base_sku b ON i.sku = b.sku
  WHERE i.date >= '{latest_date}'::DATE - INTERVAL 30 DAY AND i.date <= '{latest_date}'
    {"AND i.warehouse = '"+wh+"'" if wh!="ALL" else ""}
  GROUP BY i.date
)
SELECT
  COALESCE(SUM(COALESCE(li.onhand_qty,0)), 0) AS total_onhand,
  (SELECT COALESCE(ROUND(AVG(day_total), 0), 0) FROM inv_7d) AS inv_7d_avg,
  COALESCE(SUM(COALESCE(d7.demand_7d,0)), 0) AS total_demand_7d,
  (SELECT COALESCE(ROUND(AVG(day_total), 1), 0) FROM inv_30d) AS avg_onhand_30d,
  SUM(
    CASE
      WHEN COALESCE(d7.demand_7d,0) = 0 THEN 0
      WHEN (COALESCE(li.onhand_qty,0) / NULLIF(d7.demand_7d/7.0, 0)) < 7 THEN 1
      ELSE 0
    END
  ) AS stockout_risk_sku_cnt
FROM base_sku b
LEFT JOIN latest_inv li ON b.sku = li.sku
LEFT JOIN demand_7d d7 ON b.sku = d7.sku
"""
kpi = con.execute(kpi_sql).fetchdf().iloc[0]

# --- Demand trend (Last 60 Days) ---
trend_sql = f"""
WITH base_sku AS (
  SELECT m.sku
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
    {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
)
SELECT d.date, SUM(d.demand_qty) AS demand_qty
FROM demand_daily d
JOIN base_sku b ON d.sku = b.sku
WHERE d.date >= '{latest_date}'::DATE - INTERVAL 60 DAY
GROUP BY d.date
ORDER BY d.date
"""
trend = con.execute(trend_sql).fetchdf()

# --- Inventory trend (Last 60 Days) ---
inv_trend_sql = f"""
WITH base_sku AS (
  SELECT m.sku
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
    {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
)
SELECT i.date, SUM(i.onhand_qty) AS onhand_qty
FROM inventory_daily i
JOIN base_sku b ON i.sku = b.sku
WHERE i.date >= '{latest_date}'::DATE - INTERVAL 60 DAY
  {"AND i.warehouse = '"+wh+"'" if wh!="ALL" else ""}
GROUP BY i.date
ORDER BY i.date
"""
inv_trend = con.execute(inv_trend_sql).fetchdf()

# --- Top SKUs by demand (Last 30 Days) ---
top_sql = f"""
WITH base_sku AS (
  SELECT m.sku
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
)
SELECT d.sku, SUM(d.demand_qty) AS demand_30d
FROM demand_daily d
JOIN base_sku b ON d.sku = b.sku
WHERE d.date > '{latest_date}'::DATE - INTERVAL 30 DAY
GROUP BY d.sku
ORDER BY demand_30d DESC
LIMIT 10
"""
top = con.execute(top_sql).fetchdf()

# --- Top SKUs by inventory (Last 30 Days) ---
top_inv_sql = f"""
WITH base_sku AS (
  SELECT m.sku
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
)
SELECT i.sku, SUM(i.onhand_qty) AS onhand_30d
FROM inventory_daily i
JOIN base_sku b ON i.sku = b.sku
WHERE i.date >= '{latest_date}'::DATE - INTERVAL 30 DAY AND i.date <= '{latest_date}'
  {"AND i.warehouse = '"+wh+"'" if wh!="ALL" else ""}
GROUP BY i.sku
ORDER BY onhand_30d DESC
LIMIT 10
"""
top_inv = con.execute(top_inv_sql).fetchdf()

# --- Stockout Risk Table (Risk Tab) ---
risk_sql = f"""
WITH base_sku AS (
  SELECT m.sku, m.sku_name, m.category
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
    {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
),
latest_inv AS (
  SELECT sku, warehouse, onhand_qty
  FROM inventory_daily
  WHERE date = '{latest_date}'
    {"AND warehouse = '"+wh+"'" if wh!="ALL" else ""}
),
avg_daily_demand AS (
  SELECT sku, AVG(demand_qty) AS avg_daily_demand_14d
  FROM demand_daily
  WHERE date > '{latest_date}'::DATE - INTERVAL 14 DAY
  GROUP BY sku
),
base AS (
  SELECT
    b.sku, b.sku_name, b.category,
    li.warehouse,
    COALESCE(li.onhand_qty, 0) AS onhand_qty,
    COALESCE(ad.avg_daily_demand_14d, 0) AS avg_daily_demand_14d,
    CASE
      WHEN COALESCE(ad.avg_daily_demand_14d, 0) = 0 THEN NULL
      ELSE ROUND(COALESCE(li.onhand_qty,0) / ad.avg_daily_demand_14d, 1)
    END AS coverage_days
  FROM base_sku b
  LEFT JOIN latest_inv li ON b.sku = li.sku
  LEFT JOIN avg_daily_demand ad ON b.sku = ad.sku
)
SELECT *
FROM base
ORDER BY coverage_days ASC NULLS LAST
"""
risk = con.execute(risk_sql).fetchdf()

def assign_risk_level(days):
    if pd.isna(days):
        return "Low"
    if days < 3:
        return "Critical"
    if days < 7:
        return "High"
    if days < 14:
        return "Medium"
    return "Low"

risk["risk_level"] = risk["coverage_days"].apply(assign_risk_level)

# --- Reorder Suggestions Table (Reorder Tab) ---
reorder_sql = f"""
WITH base_sku AS (
  SELECT m.sku, m.sku_name, m.category
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
    {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
),
latest_inv AS (
  SELECT sku, warehouse, onhand_qty
  FROM inventory_daily
  WHERE date = '{latest_date}'
    {"AND warehouse = '"+wh+"'" if wh!="ALL" else ""}
),
avg_daily_demand AS (
  SELECT sku, AVG(demand_qty) AS avg_daily_demand_14d
  FROM demand_daily
  WHERE date > '{latest_date}'::DATE - INTERVAL 14 DAY
  GROUP BY sku
),
base AS (
  SELECT
    b.sku, b.sku_name, b.category,
    li.warehouse,
    COALESCE(li.onhand_qty, 0) AS onhand_qty,
    COALESCE(ad.avg_daily_demand_14d, 0) AS avg_daily_demand_14d,
    ROUND(COALESCE(ad.avg_daily_demand_14d, 0) * 10, 0) AS reorder_point,
    CASE
      WHEN COALESCE(ad.avg_daily_demand_14d, 0) = 0 THEN NULL
      ELSE ROUND(COALESCE(li.onhand_qty,0) / ad.avg_daily_demand_14d, 1)
    END AS coverage_days
  FROM base_sku b
  LEFT JOIN latest_inv li ON b.sku = li.sku
  LEFT JOIN avg_daily_demand ad ON b.sku = ad.sku
),
suggest AS (
  SELECT *,
    GREATEST(reorder_point - onhand_qty, 0) AS recommended_reorder_qty
  FROM base
)
SELECT
  sku, sku_name, category, warehouse,
  onhand_qty, reorder_point,
  avg_daily_demand_14d, coverage_days,
  recommended_reorder_qty
FROM suggest
WHERE (onhand_qty < reorder_point OR (coverage_days IS NOT NULL AND coverage_days < 10))
ORDER BY coverage_days ASC NULLS LAST, recommended_reorder_qty DESC
LIMIT 50
"""
reorder_suggest = con.execute(reorder_sql).fetchdf()

# --- Tabs ---
tab_summary, tab_risk, tab_reorder = st.tabs(["재고 현황 Overview", "재고 리스크 분석", "발주 필요 분석"])

with tab_summary:
    st.subheader("KPI Overview")
    col1, col2, col3, col4, col5 = st.columns(5)

    # NaN / None 안전 처리
    total_onhand = int(pd.to_numeric(kpi["total_onhand"], errors="coerce")) if pd.notna(kpi["total_onhand"]) else 0
    inv_7d_avg = int(pd.to_numeric(kpi["inv_7d_avg"], errors="coerce")) if pd.notna(kpi["inv_7d_avg"]) else 0
    total_demand_7d = int(pd.to_numeric(kpi["total_demand_7d"], errors="coerce")) if pd.notna(kpi["total_demand_7d"]) else 0
    avg_onhand_30d = float(pd.to_numeric(kpi["avg_onhand_30d"], errors="coerce")) if pd.notna(kpi["avg_onhand_30d"]) else 0
    stockout_cnt = int(pd.to_numeric(kpi["stockout_risk_sku_cnt"], errors="coerce")) if pd.notna(kpi["stockout_risk_sku_cnt"]) else 0

    col1.metric("총 재고 수량", f"{total_onhand:,}")
    col2.metric("최근 7일 재고 수량", f"{inv_7d_avg:,}")
    col3.metric("최근 7일 수요량", f"{total_demand_7d:,}")
    col4.metric("최근 30일 평균 재고", f"{avg_onhand_30d:,.1f}")
    col5.metric("품절 위험 SKU수 (7일 이내)", stockout_cnt)


    # Demand trend
    fig_trend = px.line(trend, x="date", y="demand_qty", title="수요 추이 (최근 60일)")
    fig_trend.update_layout(xaxis_title="일자", yaxis_title="수요량")
    fig_trend.update_xaxes(tickformat="%Y-%m-%d")
    fig_trend = apply_plotly_theme(fig_trend)
    st.plotly_chart(fig_trend, use_container_width=True)

    # Inventory trend
    fig_inv_trend = px.line(inv_trend, x="date", y="onhand_qty", title="재고 추이 (최근 60일)")
    fig_inv_trend.update_layout(xaxis_title="일자", yaxis_title="재고 수량")
    fig_inv_trend.update_xaxes(tickformat="%Y-%m-%d")
    fig_inv_trend = apply_plotly_theme(fig_inv_trend)
    st.plotly_chart(fig_inv_trend, use_container_width=True)

    # Top 10 SKUs - Demand
    fig_top = px.bar(top, x="sku", y="demand_30d", title="수요 TOP 10 SKU (최근 30일)")
    fig_top.update_layout(xaxis_title="SKU", yaxis_title="수요량 (최근 30일)")
    fig_top.update_traces(width=0.5)
    fig_top = apply_plotly_theme(fig_top)
    st.plotly_chart(fig_top, use_container_width=True)

    # Top 10 SKUs - Inventory
    fig_top_inv = px.bar(top_inv, x="sku", y="onhand_30d", title="재고 TOP 10 SKU (최근 30일)")
    fig_top_inv.update_layout(xaxis_title="SKU", yaxis_title="재고 수량 (최근 30일 합계)")
    fig_top_inv.update_traces(width=0.5)
    fig_top_inv = apply_plotly_theme(fig_top_inv)
    st.plotly_chart(fig_top_inv, use_container_width=True)

with tab_risk:
    st.subheader("⚠️ 재고 리스크 목록")
    col_left, col_filter = st.columns([2, 1])
    with col_filter:
        risk_period = st.selectbox(
            "재고 소진 기준(일수)",
            options=[7, 14, 21, 30, 60],
            format_func=lambda x: f"{x}일 이내",
            key="risk_period",
        )
        risk_level = st.selectbox(
            "Risk Level",
            options=["전체", "Critical", "High", "Medium", "Low"],
            key="risk_level",
        )
    risk_filtered = risk[
        (risk["coverage_days"].notna()) & (risk["coverage_days"] < risk_period)
    ].copy()
    if risk_level != "전체":
        risk_filtered = risk_filtered[risk_filtered["risk_level"] == risk_level]
    st.caption("커버리지 일수 = 현재 재고 / 최근 14일 평균 일수요 | Risk Level: Critical 0~3일, High 3~7일, Medium 7~14일, Low 14일 이상")
    st.dataframe(risk_filtered, use_container_width=True)

with tab_reorder:
    st.subheader("🔄 발주 필요 목록")
    st.caption("추천 발주 수량 = max(재주문 기준 - 현재 재고, 0)")
    st.dataframe(reorder_suggest, use_container_width=True)
