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
tab_exec, tab_health, tab_stockout, tab_actions, tab_movements = st.tabs([
    "Executive Overview",
    "Inventory Health",
    "Stockout Risk",
    "Actions",
    "Movements (Optional)",
])

with tab_exec:
    st.subheader("Executive Overview")
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
    st.subheader("Inventory Health")
    st.info("구성 예정")

with tab_stockout:
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

with tab_actions:
    st.subheader("🔄 발주 필요 목록")
    st.caption("추천 발주 수량 = max(재주문 기준 - 현재 재고, 0)")
    st.dataframe(reorder_suggest, use_container_width=True)

with tab_movements:
    st.subheader("Movements (Optional)")
    st.info("입출고 추이 등 구성 예정")
