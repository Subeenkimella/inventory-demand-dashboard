import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

st.set_page_config(page_title="재고·수요 모니터링 대시보드", layout="wide")

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

st.title("📦 재고·수요 모니터링 대시보드")
st.caption("샘플 CSV 데이터 기반으로 SQL(DuckDB)로 KPI를 계산")

# Latest snapshot date
latest_date = con.execute("SELECT MAX(date) FROM inventory_daily").fetchone()[0]

# Sidebar filters
st.sidebar.header("필터")
cat = st.sidebar.selectbox("카테고리", ["ALL"] + sorted(sku["category"].unique()))
wh = st.sidebar.selectbox("창고", ["ALL"] + sorted(inv["warehouse"].unique()))
sku_pick = st.sidebar.selectbox("SKU", ["ALL"] + sorted(sku["sku"].unique()))

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
)
SELECT
  COALESCE(SUM(COALESCE(li.onhand_qty,0)), 0) AS total_onhand,
  COALESCE(SUM(COALESCE(d7.demand_7d,0)), 0) AS total_demand_7d,
  ROUND(COALESCE(AVG(COALESCE(li.onhand_qty,0)), 0), 1) AS avg_onhand,
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

# --- Top SKUs by demand (Last 30 Days) ---
top_sql = f"""
WITH base_sku AS (
  SELECT m.sku
  FROM sku_master m
  WHERE 1=1
    {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
    {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
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
LIMIT 30
"""
risk = con.execute(risk_sql).fetchdf()

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
tab_summary, tab_risk, tab_reorder = st.tabs(["요약", "리스크", "발주 제안"])

with tab_summary:
    st.subheader("핵심 지표")
    col1, col2, col3, col4 = st.columns(4)

    # NaN / None 안전 처리
    total_onhand = int(pd.to_numeric(kpi["total_onhand"], errors="coerce")) if pd.notna(kpi["total_onhand"]) else 0
    total_demand_7d = int(pd.to_numeric(kpi["total_demand_7d"], errors="coerce")) if pd.notna(kpi["total_demand_7d"]) else 0
    avg_onhand = float(pd.to_numeric(kpi["avg_onhand"], errors="coerce")) if pd.notna(kpi["avg_onhand"]) else 0
    stockout_cnt = int(pd.to_numeric(kpi["stockout_risk_sku_cnt"], errors="coerce")) if pd.notna(kpi["stockout_risk_sku_cnt"]) else 0

    col1.metric("총 재고 수량", total_onhand)
    col2.metric("최근 7일 수요", total_demand_7d)
    col3.metric("평균 재고 (필터 기준)", f"{avg_onhand:,.1f}")
    col4.metric("품절 위험 SKUs (7일 이내)", stockout_cnt)


    # Demand trend
    fig_trend = px.line(trend, x="date", y="demand_qty", title="수요 추이 (최근 60일)")
    st.plotly_chart(fig_trend, use_container_width=True)

    # Top 10 SKUs
    fig_top = px.bar(top, x="sku", y="demand_30d", title="수요 TOP 10 SKU (최근 30일)")
    fig_top.update_layout(xaxis_title="SKU", yaxis_title="수요량 (최근 30일)")
    st.plotly_chart(fig_top, use_container_width=True)

with tab_risk:
    st.subheader("⚠️ 재고 리스크 목록")
    st.caption("커버리지 일수 = 현재 재고 / 최근 14일 평균 일수요")
    st.dataframe(risk, use_container_width=True)

with tab_reorder:
    st.subheader("🔄 발주 제안 목록")
    st.caption("추천 발주 수량 = max(재주문 기준 - 현재 재고, 0)")
    st.dataframe(reorder_suggest, use_container_width=True)

with st.expander("SQL 코드 보기"):
    st.code(kpi_sql, language="sql")
    st.code(trend_sql, language="sql")
    st.code(top_sql, language="sql")
    st.code(risk_sql, language="sql")
    st.code(reorder_sql, language="sql")