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
    try:
        inv_txn = pd.read_csv("inventory_txn.csv", parse_dates=["date", "txn_datetime"])
    except FileNotFoundError:
        inv_txn = pd.DataFrame(columns=["txn_datetime", "date", "sku", "warehouse", "txn_type", "qty", "ref_id", "reason_code"])
    return sku, demand, inv, inv_txn

sku, demand, inv, inv_txn = load_data()

# DuckDB in-memory (SQL engine)
con = duckdb.connect(database=":memory:")
con.register("sku_master", sku)
con.register("demand_daily", demand)
con.register("inventory_daily", inv)
if inv_txn is not None and len(inv_txn) > 0:
    con.register("inventory_txn", inv_txn)

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

st.sidebar.header("공통 필터")
range_days = st.sidebar.selectbox(
    "기간 (트렌드)",
    options=[7, 14, 30, 60, 90],
    index=3,
    format_func=lambda x: f"{x}일",
    key="range_days",
)
risk_threshold_days = st.sidebar.selectbox(
    "품절 리스크 기준(일)",
    options=[7, 14, 21, 30, 60],
    index=1,
    format_func=lambda x: f"{x}일 미만",
    key="risk_threshold_days",
)
overstock_threshold_days = st.sidebar.selectbox(
    "과잉재고 기준(일)",
    options=[30, 60, 90, 120],
    index=1,
    format_func=lambda x: f"{x}일 초과",
    key="overstock_threshold_days",
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
# --- Executive Overview KPIs (Tab1) ---
exec_kpi_sql = f"""
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
  WHERE date > '{latest_date}'::DATE - INTERVAL 7 DAY AND date <= '{latest_date}'
  GROUP BY sku
),
sku_dos AS (
  SELECT
    b.sku,
    COALESCE(li.onhand_qty, 0) AS onhand_qty,
    COALESCE(d.demand_7d, 0) AS demand_7d,
    CASE WHEN COALESCE(d.demand_7d, 0) > 0
      THEN ROUND(COALESCE(li.onhand_qty, 0) * 7.0 / NULLIF(d.demand_7d, 0), 1)
      ELSE NULL END AS coverage_days
  FROM base_sku b
  LEFT JOIN latest_inv li ON b.sku = li.sku
  LEFT JOIN demand_7d d ON b.sku = d.sku
)
SELECT
  (SELECT COALESCE(SUM(onhand_qty), 0) FROM sku_dos) AS total_onhand,
  (SELECT COALESCE(SUM(demand_7d), 0) FROM sku_dos) AS total_demand_7d,
  (SELECT MEDIAN(coverage_days) FROM sku_dos WHERE coverage_days IS NOT NULL) AS median_dos,
  (SELECT COUNT(*) FROM sku_dos WHERE coverage_days IS NOT NULL AND coverage_days < {risk_threshold_days}) AS stockout_sku_cnt,
  (SELECT COUNT(*) FROM sku_dos WHERE coverage_days IS NOT NULL AND coverage_days > {overstock_threshold_days}) AS overstock_sku_cnt
"""
exec_kpi = con.execute(exec_kpi_sql).fetchdf().iloc[0]

# --- Demand trend (range_days) ---
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
WHERE d.date >= '{latest_date}'::DATE - INTERVAL {range_days} DAY
GROUP BY d.date
ORDER BY d.date
"""
trend = con.execute(trend_sql).fetchdf()

# --- Inventory trend (range_days) ---
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
WHERE i.date >= '{latest_date}'::DATE - INTERVAL {range_days} DAY
  {"AND i.warehouse = '"+wh+"'" if wh!="ALL" else ""}
GROUP BY i.date
ORDER BY i.date
"""
inv_trend = con.execute(inv_trend_sql).fetchdf()

# --- Category inventory share (latest_date, inventory_daily + sku_master) ---
cat_inv_sql = f"""
SELECT m.category, COALESCE(SUM(i.onhand_qty), 0) AS onhand_qty
FROM sku_master m
LEFT JOIN inventory_daily i ON i.sku = m.sku AND i.date = '{latest_date}'
  {"AND i.warehouse = '"+wh+"'" if wh!="ALL" else ""}
WHERE 1=1
  {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
  {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
  {"AND EXISTS (SELECT 1 FROM inventory_daily i2 WHERE i2.sku = m.sku AND i2.warehouse = '"+wh+"')" if wh!="ALL" else ""}
GROUP BY m.category
ORDER BY onhand_qty DESC
"""
cat_inv = con.execute(cat_inv_sql).fetchdf()

# --- Category demand share (last 30 days, demand_daily + sku_master) ---
cat_demand_sql = f"""
SELECT m.category, COALESCE(SUM(d.demand_qty), 0) AS demand_qty
FROM sku_master m
LEFT JOIN demand_daily d ON d.sku = m.sku
  AND d.date > '{latest_date}'::DATE - INTERVAL 30 DAY AND d.date <= '{latest_date}'
WHERE 1=1
  {"AND m.category = '"+cat+"'" if cat!="ALL" else ""}
  {"AND m.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
  {"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '"+wh+"')" if wh!="ALL" else ""}
GROUP BY m.category
ORDER BY demand_qty DESC
"""
cat_demand = con.execute(cat_demand_sql).fetchdf()

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


# --- IN/OUT trend (inventory_txn, last 60 days, filter by cat/wh/sku_pick) ---
txn_in_trend = None
txn_out_trend = None

if inv_txn is not None and len(inv_txn) > 0:
    txn_trend_sql = f"""
    WITH filtered AS (
      SELECT
        CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE) AS dt,
        CAST(t.qty AS DOUBLE) AS qty
      FROM inventory_txn t
      WHERE CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE)
            BETWEEN '{latest_date}'::DATE - INTERVAL 60 DAY AND '{latest_date}'::DATE
        {"AND t.warehouse = '"+wh+"'" if wh!="ALL" else ""}
        {"AND t.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
        {"AND EXISTS (SELECT 1 FROM sku_master m WHERE m.sku = t.sku AND m.category = '"+cat+"')" if cat!="ALL" else ""}
    )
    SELECT
      dt AS date,
      SUM(CASE WHEN qty > 0 THEN qty ELSE 0 END) AS in_qty,
      SUM(CASE WHEN qty < 0 THEN ABS(qty) ELSE 0 END) AS out_qty
    FROM filtered
    GROUP BY dt
    ORDER BY dt
    """

    txn_trend = con.execute(txn_trend_sql).fetchdf()

    # qty가 0만 있는 경우 사용자에게 명확히 안내
    if txn_trend.empty or ((txn_trend["in_qty"].fillna(0).sum() == 0) and (txn_trend["out_qty"].fillna(0).sum() == 0)):
        txn_in_trend = None
        txn_out_trend = None
    else:
        txn_in_trend = txn_trend[["date", "in_qty"]].rename(columns={"in_qty": "qty"})
        txn_out_trend = txn_trend[["date", "out_qty"]].rename(columns={"out_qty": "qty"})

# --- 품절 리스크 분석 Table (Risk Tab) ---
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
  WHERE date > '{latest_date}'::DATE - INTERVAL 14 DAY AND date <= '{latest_date}'
  GROUP BY sku
),
demand_7d_cte AS (
  SELECT sku, SUM(demand_qty) AS demand_7d
  FROM demand_daily
  WHERE date > '{latest_date}'::DATE - INTERVAL 7 DAY AND date <= '{latest_date}'
  GROUP BY sku
),
base AS (
  SELECT
    b.sku, b.sku_name, b.category,
    li.warehouse,
    COALESCE(li.onhand_qty, 0) AS onhand_qty,
    COALESCE(ad.avg_daily_demand_14d, 0) AS avg_daily_demand_14d,
    COALESCE(d7.demand_7d, 0) AS demand_7d,
    CASE
      WHEN COALESCE(ad.avg_daily_demand_14d, 0) = 0 THEN NULL
      ELSE ROUND(COALESCE(li.onhand_qty,0) / ad.avg_daily_demand_14d, 1)
    END AS coverage_days
  FROM base_sku b
  LEFT JOIN latest_inv li ON b.sku = li.sku
  LEFT JOIN avg_daily_demand ad ON b.sku = ad.sku
  LEFT JOIN demand_7d_cte d7 ON b.sku = d7.sku
)
SELECT
  sku, sku_name, category, warehouse,
  onhand_qty, avg_daily_demand_14d, demand_7d, coverage_days,
  CASE
    WHEN coverage_days IS NOT NULL THEN date_add('{latest_date}'::DATE, CAST(CEIL(coverage_days) AS INTEGER))
    ELSE NULL
  END AS estimated_stockout_date
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

# --- 재고 건전성 분석 Tab: health 테이블 (demand_30d, coverage_days, warehouse 포함) ---
health_sql = f"""
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
demand_30d_cte AS (
  SELECT sku, SUM(demand_qty) AS demand_30d
  FROM demand_daily
  WHERE date > '{latest_date}'::DATE - INTERVAL 30 DAY AND date <= '{latest_date}'
  GROUP BY sku
),
avg_daily_demand AS (
  SELECT sku, AVG(demand_qty) AS avg_daily_demand_14d
  FROM demand_daily
  WHERE date > '{latest_date}'::DATE - INTERVAL 14 DAY AND date <= '{latest_date}'
  GROUP BY sku
),
base AS (
  SELECT
    b.sku, b.sku_name, b.category,
    li.warehouse,
    COALESCE(li.onhand_qty, 0) AS onhand_qty,
    COALESCE(d30.demand_30d, 0) AS demand_30d,
    COALESCE(ad.avg_daily_demand_14d, 0) AS avg_daily_demand_14d,
    CASE
      WHEN COALESCE(ad.avg_daily_demand_14d, 0) = 0 THEN NULL
      ELSE ROUND(COALESCE(li.onhand_qty, 0) / ad.avg_daily_demand_14d, 1)
    END AS coverage_days
  FROM base_sku b
  LEFT JOIN latest_inv li ON b.sku = li.sku
  LEFT JOIN demand_30d_cte d30 ON b.sku = d30.sku
  LEFT JOIN avg_daily_demand ad ON b.sku = ad.sku
)
SELECT sku, sku_name, category, warehouse, onhand_qty, demand_30d, avg_daily_demand_14d, coverage_days
FROM base
ORDER BY coverage_days ASC NULLS LAST
"""
health = con.execute(health_sql).fetchdf()
health["risk_level"] = health["coverage_days"].apply(assign_risk_level)

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
tab_exec, tab_health, tab_stockout, tab_actions, tab_movements = st.tabs([
    "Overview",
    "재고 건전성 분석",
    "품절 리스크 분석",
    "발주·조치 제안",
    "재고 In/Out 분석",
])

with tab_exec:
    st.subheader("Overview")
    col1, col2, col3, col4, col5 = st.columns(5)

    total_onhand = int(pd.to_numeric(exec_kpi["total_onhand"], errors="coerce")) if pd.notna(exec_kpi["total_onhand"]) else 0
    total_demand_7d = int(pd.to_numeric(exec_kpi["total_demand_7d"], errors="coerce")) if pd.notna(exec_kpi["total_demand_7d"]) else 0
    median_dos_val = exec_kpi["median_dos"]
    median_dos_str = f"{median_dos_val:,.1f}" if pd.notna(median_dos_val) and (median_dos_val == median_dos_val) else "—"
    stockout_sku_cnt = int(pd.to_numeric(exec_kpi["stockout_sku_cnt"], errors="coerce")) if pd.notna(exec_kpi["stockout_sku_cnt"]) else 0
    overstock_sku_cnt = int(pd.to_numeric(exec_kpi["overstock_sku_cnt"], errors="coerce")) if pd.notna(exec_kpi["overstock_sku_cnt"]) else 0

    col1.metric("현재 총 재고 (개)", f"{total_onhand:,}")
    col2.metric("최근 7일 수요 (개)", f"{total_demand_7d:,}")
    col3.metric("DOS 중앙값 (일)", median_dos_str)
    col4.metric("품절 리스크 SKU 수", f"{stockout_sku_cnt:,}")
    col5.metric("과잉재고 SKU 수", f"{overstock_sku_cnt:,}")

    col_trend_demand, col_trend_inv = st.columns(2)
    with col_trend_demand:
        fig_trend = px.line(trend, x="date", y="demand_qty", title=f"수요 추이 (최근 {range_days}일)")
        fig_trend.update_layout(xaxis_title="일자", yaxis_title="수요량")
        fig_trend.update_xaxes(tickformat="%Y-%m-%d")
        fig_trend.update_yaxes(tickformat=",.0f")
        fig_trend = apply_plotly_theme(fig_trend)
        st.plotly_chart(fig_trend, use_container_width=True)
    with col_trend_inv:
        fig_inv_trend = px.line(inv_trend, x="date", y="onhand_qty", title=f"재고 추이 (최근 {range_days}일)")
        fig_inv_trend.update_layout(xaxis_title="일자", yaxis_title="재고 수량")
        fig_inv_trend.update_xaxes(tickformat="%Y-%m-%d")
        fig_inv_trend.update_yaxes(tickformat=",.0f")
        fig_inv_trend = apply_plotly_theme(fig_inv_trend)
        st.plotly_chart(fig_inv_trend, use_container_width=True)

    st.subheader("분해 뷰")
    col_cat_inv, col_cat_demand = st.columns(2)
    with col_cat_inv:
        if not cat_inv.empty and cat_inv["onhand_qty"].sum() > 0:
            fig_cat_inv = px.pie(cat_inv, values="onhand_qty", names="category", title="카테고리별 재고 비중 (latest 기준)")
            fig_cat_inv.update_traces(textinfo="percent+label")
            fig_cat_inv = apply_plotly_theme(fig_cat_inv)
            st.plotly_chart(fig_cat_inv, use_container_width=True)
        else:
            st.caption("카테고리별 재고 비중: 데이터 없음")
    with col_cat_demand:
        if not cat_demand.empty and cat_demand["demand_qty"].sum() > 0:
            fig_cat_demand = px.pie(cat_demand, values="demand_qty", names="category", title="카테고리별 수요 비중 (최근 30일)")
            fig_cat_demand.update_traces(textinfo="percent+label")
            fig_cat_demand = apply_plotly_theme(fig_cat_demand)
            st.plotly_chart(fig_cat_demand, use_container_width=True)
        else:
            st.caption("카테고리별 수요 비중: 데이터 없음")

with tab_health:
    st.subheader("재고 건전성 분석")
    st.caption("재고 부족/적정/과잉 구조 파악")

    # A. 무수요 SKU 수 카드 + DOS 분포 히스토그램
    no_demand_cnt = int(health["coverage_days"].isna().sum())
    health_with_dos = health[health["coverage_days"].notna()].copy()

    row_cards, row_hist = st.columns([1, 3])
    with row_cards:
        st.metric("무수요 SKU 수", f"{no_demand_cnt:,}")
    with row_hist:
        if not health_with_dos.empty:
            fig_hist = px.histogram(
                health_with_dos,
                x="coverage_days",
                nbins=min(40, max(10, len(health_with_dos) // 3)),
                title="커버리지(DOS) 분포",
                labels={"coverage_days": "Coverage Days (DOS)"},
            )
            fig_hist.update_layout(xaxis_title="Coverage Days (DOS)", yaxis_title="SKU 수")
            fig_hist.update_yaxes(tickformat=",.0f")
            fig_hist = apply_plotly_theme(fig_hist)
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.caption("DOS 데이터 없음 (전체 무수요 또는 필터 결과 없음)")

    # B. 2x2 매트릭스(산점도): X=demand_30d, Y=coverage_days
    scatter_df = health_with_dos.copy()
    scatter_df["dos"] = scatter_df["coverage_days"]

    if not scatter_df.empty:
        med_demand_30d = float(scatter_df["demand_30d"].median())
        y_threshold = overstock_threshold_days

        fig_scatter = px.scatter(
            scatter_df,
            x="demand_30d",
            y="coverage_days",
            hover_data={
                "sku": True,
                "sku_name": True,
                "category": True,
                "onhand_qty": ",.0f",
                "demand_30d": ",.0f",
                "dos": ",.1f",
            },
            title="수요 vs 커버리지 (2x2 매트릭스)",
        )
        fig_scatter.update_layout(
            xaxis_title="최근 30일 수요 합 (SKU)",
            yaxis_title="Coverage Days (DOS)",
        )
        fig_scatter.update_yaxes(tickformat=",.0f")
        fig_scatter.update_xaxes(tickformat=",.0f")
        fig_scatter.add_hline(y=y_threshold, line_dash="dash", line_color="gray")
        fig_scatter.add_vline(x=med_demand_30d, line_dash="dash", line_color="gray")
        fig_scatter = apply_plotly_theme(fig_scatter)
        st.plotly_chart(fig_scatter, use_container_width=True)
        st.caption(
            f"4분면: X 기준 = demand_30d 중앙값({med_demand_30d:,.0f}), "
            f"Y 기준 = 과잉재고 기준({y_threshold}일). "
            "좌하=저수요·저커버리지, 좌상=저수요·고커버리지(과잉), 우하=고수요·저커버리지(부족), 우상=고수요·고커버리지."
        )
    else:
        st.caption("산점도: DOS가 있는 SKU가 없어 표시하지 않습니다.")

    # C. 드릴다운 테이블
    st.subheader("드릴다운 테이블")
    display_health = health[
        ["sku", "sku_name", "category", "warehouse", "onhand_qty", "demand_30d", "avg_daily_demand_14d", "coverage_days", "risk_level"]
    ].copy()
    display_health = display_health.sort_values("coverage_days", ascending=True, na_position="last")
    st.dataframe(display_health, use_container_width=True)

with tab_stockout:
    st.subheader("품절 리스크 분석")
    st.caption("DOS(재고 소진 예상일수) = 현재 재고 / 최근 14일 평균 일수요 | Risk Level: Critical 0~3일, High 3~7일, Medium 7~14일, Low 14일 이상")

    risk_period_options = [7, 14, 21, 30, 60]
    risk_period_default_idx = risk_period_options.index(risk_threshold_days) if risk_threshold_days in risk_period_options else 1
    risk_period_days = st.selectbox(
        "재고 소진 기준(일수)",
        options=risk_period_options,
        index=risk_period_default_idx,
        format_func=lambda x: f"{x}일 미만",
        key="risk_period_days",
    )
    risk_level_filter = st.selectbox(
        "Risk Level",
        options=["전체", "Critical", "High", "Medium", "Low"],
        key="risk_level_filter",
    )

    risk_filtered = risk[
        (risk["coverage_days"].notna()) & (risk["coverage_days"] < risk_period_days)
    ].copy()
    if risk_level_filter != "전체":
        risk_filtered = risk_filtered[risk_filtered["risk_level"] == risk_level_filter]

    # 상단 KPI: Critical/High/Medium/Low SKU 수, 리스크 재고 수량 합, 예상 소진일 Top10 평균
    cnt_critical = int((risk_filtered["risk_level"] == "Critical").sum())
    cnt_high = int((risk_filtered["risk_level"] == "High").sum())
    cnt_medium = int((risk_filtered["risk_level"] == "Medium").sum())
    cnt_low = int((risk_filtered["risk_level"] == "Low").sum())
    risk_onhand_sum = int(risk_filtered["onhand_qty"].sum()) if not risk_filtered.empty else 0
    top10_coverage = risk_filtered.nsmallest(10, "coverage_days")["coverage_days"]
    avg_top10 = float(top10_coverage.mean()) if len(top10_coverage) > 0 else None

    col_c, col_h, col_m, col_l, col_sum, col_avg = st.columns(6)
    col_c.metric("Critical SKU 수", f"{cnt_critical:,}")
    col_h.metric("High SKU 수", f"{cnt_high:,}")
    col_m.metric("Medium SKU 수", f"{cnt_medium:,}")
    col_l.metric("Low SKU 수", f"{cnt_low:,}")
    col_sum.metric("리스크 재고 수량 합", f"{risk_onhand_sum:,}")
    col_avg.metric("예상 소진일 Top10 평균(일)", f"{avg_top10:,.1f}" if avg_top10 is not None else "—")

    # 리스크 테이블: coverage_days NOT NULL AND coverage_days < risk_period_days, risk_level 적용
    st.subheader("리스크 테이블")
    display_risk = risk_filtered[
        ["sku", "sku_name", "category", "warehouse", "onhand_qty", "avg_daily_demand_14d", "coverage_days", "estimated_stockout_date", "demand_7d", "risk_level"]
    ].copy()
    display_risk = display_risk.sort_values("coverage_days", ascending=True)
    st.dataframe(display_risk, use_container_width=True)
    st.caption("DOS(재고 소진 예상일수) 기준 리스크 구간만 표시. estimated_stockout_date = 기준일 + CEIL(DOS)일.")

with tab_actions:
    st.subheader("발주·조치 제안")
    st.caption("정책에 따른 추천 발주 수량 (target_stock = 일평균수요 × (리드타임 + 목표커버 + 안전재고), recommended_order_qty = max(target_stock - 현재재고, 0), MOQ 적용)")

    # 1) 정책 설정 패널
    st.subheader("정책 설정")
    col_lt, col_tc, col_ss, col_moq = st.columns(4)
    with col_lt:
        lead_time_days = st.number_input("lead_time_days (리드타임, 일)", min_value=0, value=7, step=1, key="lead_time_days")
    with col_tc:
        target_cover_days = st.number_input("target_cover_days (목표 커버 일수)", min_value=0, value=14, step=1, key="target_cover_days")
    with col_ss:
        safety_stock_days = st.number_input("safety_stock_days (안전재고 일수)", min_value=0, value=3, step=1, key="safety_stock_days")
    with col_moq:
        moq = st.number_input("moq (최소 발주 수량, 0=미적용)", min_value=0, value=0, step=1, key="moq")

    # 2) 추천 발주 계산 (risk 기준: warehouse 필터 이미 반영됨)
    actions_base = risk[["sku", "sku_name", "category", "warehouse", "onhand_qty", "avg_daily_demand_14d", "coverage_days"]].copy()
    onhand = pd.to_numeric(actions_base["onhand_qty"], errors="coerce").fillna(0)
    avg_d = pd.to_numeric(actions_base["avg_daily_demand_14d"], errors="coerce").fillna(0)
    total_days = lead_time_days + target_cover_days + safety_stock_days
    target_stock = (avg_d * total_days).round(0).astype(int)
    recommended_order_qty = (target_stock - onhand).clip(lower=0).astype(int)
    if moq > 0:
        recommended_order_qty = recommended_order_qty.where(recommended_order_qty <= 0, recommended_order_qty.clip(lower=moq)).astype(int)
    actions_base["target_stock"] = target_stock
    actions_base["recommended_order_qty"] = recommended_order_qty

    # 3) recommended_order_qty > 0 만 표시, 정렬: coverage_days ASC, recommended_order_qty DESC
    actions_display = actions_base[actions_base["recommended_order_qty"] > 0].copy()
    actions_display = actions_display.sort_values(
        ["coverage_days", "recommended_order_qty"],
        ascending=[True, False],
        na_position="last",
    )

    # 4) 테이블 열 (단위/콤마 포맷은 st.dataframe이 숫자 컬럼 자동 포맷, 또는 column_config 사용)
    st.subheader("추천 발주 테이블")
    display_cols = ["sku", "sku_name", "category", "warehouse", "onhand_qty", "avg_daily_demand_14d", "coverage_days", "target_stock", "recommended_order_qty"]
    out = actions_display[display_cols].copy()
    out["onhand_qty"] = out["onhand_qty"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
    out["avg_daily_demand_14d"] = out["avg_daily_demand_14d"].apply(lambda x: f"{float(x):,.1f}" if pd.notna(x) else "0")
    out["coverage_days"] = out["coverage_days"].apply(lambda x: f"{float(x):,.1f}" if pd.notna(x) else "—")
    out["target_stock"] = out["target_stock"].apply(lambda x: f"{int(x):,}")
    out["recommended_order_qty"] = out["recommended_order_qty"].apply(lambda x: f"{int(x):,}")
    st.dataframe(out, use_container_width=True)
    st.caption("recommended_order_qty > 0 인 SKU만 표시. 정렬: coverage_days ASC, recommended_order_qty DESC.")

with tab_movements:
    st.subheader("재고 입·출고 이력")
    st.caption("inventory_txn 기반 입출고 추이 및 트랜잭션 목록")

    if inv_txn is None or len(inv_txn) == 0:
        st.info("inventory_txn 데이터가 없거나 비어 있습니다. CSV를 추가하면 입출고 차트와 트랜잭션 테이블이 표시됩니다.")
    else:
        # 1) 입출고 집계 (dt/qty 강제 캐스팅, range_days)
        txn_trend_sql = f"""
        WITH filtered AS (
          SELECT
            CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE) AS dt,
            TRY_CAST(t.qty AS DOUBLE) AS qty
          FROM inventory_txn t
          WHERE CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE)
                BETWEEN '{latest_date}'::DATE - INTERVAL {range_days} DAY AND '{latest_date}'::DATE
            {"AND t.warehouse = '"+wh+"'" if wh!="ALL" else ""}
            {"AND t.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
            {"AND EXISTS (SELECT 1 FROM sku_master m WHERE m.sku = t.sku AND m.category = '"+cat+"')" if cat!="ALL" else ""}
        )
        SELECT
          dt AS date,
          SUM(CASE WHEN COALESCE(qty, 0) > 0 THEN qty ELSE 0 END) AS in_qty,
          SUM(CASE WHEN COALESCE(qty, 0) < 0 THEN ABS(qty) ELSE 0 END) AS out_qty
        FROM filtered
        GROUP BY dt
        ORDER BY dt
        """
        txn_trend = con.execute(txn_trend_sql).fetchdf()
        sum_in = txn_trend["in_qty"].fillna(0).sum() if not txn_trend.empty else 0
        sum_out = txn_trend["out_qty"].fillna(0).sum() if not txn_trend.empty else 0
        has_data = (sum_in != 0 or sum_out != 0) and not txn_trend.empty

        if not has_data:
            st.warning("필터 조건 내 입출고 합계(in_qty/out_qty)가 0이거나 데이터가 없습니다. 기간·창고·SKU·카테고리 필터를 확인하거나, qty가 0이 아닌 트랜잭션이 있는지 확인하세요.")
        else:
            col_in, col_out = st.columns(2)
            with col_in:
                fig_in = px.bar(txn_trend, x="date", y="in_qty", title=f"입고(IN) — 최근 {range_days}일")
                fig_in.update_layout(xaxis_title="일자", yaxis_title="입고 수량")
                fig_in.update_xaxes(tickformat="%Y-%m-%d")
                fig_in.update_yaxes(tickformat=",.0f")
                fig_in = apply_plotly_theme(fig_in)
                st.plotly_chart(fig_in, use_container_width=True)
            with col_out:
                fig_out = px.bar(txn_trend, x="date", y="out_qty", title=f"출고(OUT) — 최근 {range_days}일")
                fig_out.update_layout(xaxis_title="일자", yaxis_title="출고 수량")
                fig_out.update_xaxes(tickformat="%Y-%m-%d")
                fig_out.update_yaxes(tickformat=",.0f")
                fig_out = apply_plotly_theme(fig_out)
                st.plotly_chart(fig_out, use_container_width=True)

        # 3) 트랜잭션 테이블 (txn_datetime DESC, limit 200)
        txn_list_sql = f"""
        SELECT
          t.txn_datetime,
          CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE) AS dt,
          t.sku, t.warehouse, t.txn_type,
          TRY_CAST(t.qty AS DOUBLE) AS qty,
          t.ref_id, t.reason_code
        FROM inventory_txn t
        WHERE 1=1
          {"AND t.warehouse = '"+wh+"'" if wh!="ALL" else ""}
          {"AND t.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
          {"AND EXISTS (SELECT 1 FROM sku_master m WHERE m.sku = t.sku AND m.category = '"+cat+"')" if cat!="ALL" else ""}
        ORDER BY t.txn_datetime DESC
        LIMIT 200
        """
        txn_list = con.execute(txn_list_sql).fetchdf()
        st.subheader("트랜잭션 목록 (최신 200건)")
        if txn_list.empty:
            st.caption("필터 조건에 맞는 트랜잭션이 없습니다.")
        else:
            st.dataframe(txn_list, use_container_width=True)
        st.caption("dt = COALESCE(date, txn_datetime 날짜). qty: 숫자형. 정렬: txn_datetime DESC, 최대 200건.")
