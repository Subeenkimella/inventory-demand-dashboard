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


def get_base_sku_where(cat, wh, sku_pick):
    """공통 필터(cat, wh, sku_pick)에 해당하는 SQL AND 절 문자열. base_sku CTE에서 WHERE 1=1 뒤에 붙인다."""
    parts = []
    if cat != "ALL":
        parts.append(f"AND m.category = '{cat}'")
    if sku_pick != "ALL":
        parts.append(f"AND m.sku = '{sku_pick}'")
    if wh != "ALL":
        parts.append(f"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '{wh}')")
    return "\n    ".join(parts) if parts else ""


def compute_dos(onhand_qty, avg_daily_demand):
    """재고 커버 일수(DOS). avg_daily_demand가 0이면 None 반환."""
    if avg_daily_demand is None or pd.isna(avg_daily_demand) or float(avg_daily_demand) == 0:
        return None
    o = float(onhand_qty or 0)
    return round(o / float(avg_daily_demand), 1)

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

# 공통 필터 UI — 왼쪽 사이드바 (key로 session_state에 저장, 수동 초기화 없이 위젯만 사용)
st.sidebar.header("공통 필터")
cat_opts = ["ALL"] + sorted(sku["category"].unique())
wh_opts = ["ALL"] + sorted(inv["warehouse"].unique())
sku_opts = ["ALL"] + sorted(sku["sku"].unique())

st.sidebar.selectbox(
    "카테고리",
    options=cat_opts,
    index=cat_opts.index(st.session_state.get("cat", "ALL")) if st.session_state.get("cat", "ALL") in cat_opts else 0,
    format_func=lambda x: category_map.get(x, x),
    key="cat",
)
st.sidebar.selectbox(
    "창고",
    options=wh_opts,
    index=wh_opts.index(st.session_state.get("wh", "ALL")) if st.session_state.get("wh", "ALL") in wh_opts else 0,
    format_func=lambda x: warehouse_map.get(x, x),
    key="wh",
)
st.sidebar.selectbox(
    "SKU",
    options=sku_opts,
    index=sku_opts.index(st.session_state.get("sku_pick", "ALL")) if st.session_state.get("sku_pick", "ALL") in sku_opts else 0,
    format_func=lambda x: "전체" if x == "ALL" else x,
    key="sku_pick",
)

cat = st.session_state.get("cat", "ALL")
wh = st.session_state.get("wh", "ALL")
sku_pick = st.session_state.get("sku_pick", "ALL")
base_where = get_base_sku_where(cat, wh, sku_pick)

def _inv_wh_where(wh):
    return f"AND warehouse = '{wh}'" if wh != "ALL" else ""

def _inv_wh_join(wh):
    return f"AND i.warehouse = '{wh}'" if wh != "ALL" else ""

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
    st.caption("한눈에 보는 재고·수요 요약 — 어떤 구간이 부족/과잉인가?")

    # 탭 내부 필터: 추이 조회기간(trend_days), DOS 산정 기준(dos_basis_days)
    ov_trend_opts = [30, 60, 90, 180, "ALL"]
    ov_dos_opts = [7, 14, 30]
    col_ov1, col_ov2 = st.columns(2)
    with col_ov1:
        trend_days_val = st.selectbox(
            "추이 조회 기간(일)",
            options=ov_trend_opts,
            index=ov_trend_opts.index(st.session_state.get("ov_trend_days", 60)) if st.session_state.get("ov_trend_days", 60) in ov_trend_opts else 1,
            format_func=lambda x: "전체(365일)" if x == "ALL" else f"{x}일",
            key="ov_trend_days",
        )
    with col_ov2:
        dos_basis_days = st.selectbox(
            "DOS 산정 기준(최근 N일 평균 일수요)",
            options=ov_dos_opts,
            index=ov_dos_opts.index(st.session_state.get("ov_dos_basis_days", 14)) if st.session_state.get("ov_dos_basis_days", 14) in ov_dos_opts else 1,
            format_func=lambda x: f"{x}일",
            key="ov_dos_basis_days",
        )
    trend_days = 365 if trend_days_val == "ALL" else trend_days_val

    # KPI: dos_basis_days 기준 median_dos, stockout(<14일), overstock(>60일)
    exec_kpi_sql = f"""
    WITH base_sku AS (
      SELECT m.sku, m.sku_name, m.category
      FROM sku_master m
      WHERE 1=1
      {base_where}
    ),
    latest_inv AS (
      SELECT sku, SUM(onhand_qty) AS onhand_qty
      FROM inventory_daily
      WHERE date = '{latest_date}'
      {_inv_wh_where(wh)}
      GROUP BY sku
    ),
    demand_Nd AS (
      SELECT sku, SUM(demand_qty) AS demand_Nd
      FROM demand_daily
      WHERE date > '{latest_date}'::DATE - INTERVAL {dos_basis_days} DAY AND date <= '{latest_date}'
      GROUP BY sku
    ),
    sku_dos AS (
      SELECT
        b.sku,
        COALESCE(li.onhand_qty, 0) AS onhand_qty,
        COALESCE(d.demand_Nd, 0) AS demand_Nd,
        CASE WHEN COALESCE(d.demand_Nd, 0) > 0
          THEN ROUND(COALESCE(li.onhand_qty, 0) * {dos_basis_days} * 1.0 / NULLIF(d.demand_Nd, 0), 1)
          ELSE NULL END AS coverage_days
      FROM base_sku b
      LEFT JOIN latest_inv li ON b.sku = li.sku
      LEFT JOIN demand_Nd d ON b.sku = d.sku
    )
    SELECT
      (SELECT COALESCE(SUM(onhand_qty), 0) FROM sku_dos) AS total_onhand,
      (SELECT COALESCE(SUM(demand_Nd), 0) FROM sku_dos) AS total_demand_Nd,
      (SELECT MEDIAN(coverage_days) FROM sku_dos WHERE coverage_days IS NOT NULL) AS median_dos,
      (SELECT COUNT(*) FROM sku_dos WHERE coverage_days IS NOT NULL AND coverage_days < 14) AS stockout_sku_cnt,
      (SELECT COUNT(*) FROM sku_dos WHERE coverage_days IS NOT NULL AND coverage_days > 60) AS overstock_sku_cnt
    """
    exec_kpi = con.execute(exec_kpi_sql).fetchdf().iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)
    total_onhand = int(pd.to_numeric(exec_kpi["total_onhand"], errors="coerce")) if pd.notna(exec_kpi["total_onhand"]) else 0
    total_demand_Nd = int(pd.to_numeric(exec_kpi["total_demand_Nd"], errors="coerce")) if pd.notna(exec_kpi["total_demand_Nd"]) else 0
    median_dos_val = exec_kpi["median_dos"]
    median_dos_str = f"{median_dos_val:,.1f}" if pd.notna(median_dos_val) and (median_dos_val == median_dos_val) else "—"
    stockout_sku_cnt = int(pd.to_numeric(exec_kpi["stockout_sku_cnt"], errors="coerce")) if pd.notna(exec_kpi["stockout_sku_cnt"]) else 0
    overstock_sku_cnt = int(pd.to_numeric(exec_kpi["overstock_sku_cnt"], errors="coerce")) if pd.notna(exec_kpi["overstock_sku_cnt"]) else 0
    col1.metric("현재 총 재고 (개)", f"{total_onhand:,}")
    col2.metric(f"최근 {dos_basis_days}일 수요 (개)", f"{total_demand_Nd:,}")
    col3.metric("재고 커버 일수(DOS) 중앙값", median_dos_str)
    col4.metric("품절 리스크 SKU 수 (DOS<14일)", f"{stockout_sku_cnt:,}")
    col5.metric("과잉재고 SKU 수 (DOS>60일)", f"{overstock_sku_cnt:,}")

    st.caption(f"※ DOS는 최근 {dos_basis_days}일 평균 일수요 기준. 부족/과잉은 14일·60일 기준.")

    # 추이: trend_days로 재조회
    trend_sql = f"""
    WITH base_sku AS (
      SELECT m.sku FROM sku_master m WHERE 1=1
      {base_where}
    )
    SELECT d.date, SUM(d.demand_qty) AS demand_qty
    FROM demand_daily d
    JOIN base_sku b ON d.sku = b.sku
    WHERE d.date >= '{latest_date}'::DATE - INTERVAL {trend_days} DAY
    GROUP BY d.date
    ORDER BY d.date
    """
    trend = con.execute(trend_sql).fetchdf()
    inv_trend_sql = f"""
    WITH base_sku AS (
      SELECT m.sku FROM sku_master m WHERE 1=1
      {base_where}
    )
    SELECT i.date, SUM(i.onhand_qty) AS onhand_qty
    FROM inventory_daily i
    JOIN base_sku b ON i.sku = b.sku
    WHERE i.date >= '{latest_date}'::DATE - INTERVAL {trend_days} DAY
    {_inv_wh_where(wh)}
    GROUP BY i.date
    ORDER BY i.date
    """
    inv_trend = con.execute(inv_trend_sql).fetchdf()

    col_trend_demand, col_trend_inv = st.columns(2)
    with col_trend_demand:
        fig_trend = px.line(trend, x="date", y="demand_qty", title=f"수요 추이 (최근 {trend_days}일)" if trend_days != 365 else "수요 추이 (전체)")
        fig_trend.update_layout(xaxis_title="일자", yaxis_title="수요량")
        fig_trend.update_xaxes(tickformat="%Y-%m-%d")
        fig_trend.update_yaxes(tickformat=",.0f")
        fig_trend = apply_plotly_theme(fig_trend)
        st.plotly_chart(fig_trend, use_container_width=True)
    with col_trend_inv:
        fig_inv_trend = px.line(inv_trend, x="date", y="onhand_qty", title=f"재고 추이 (최근 {trend_days}일)" if trend_days != 365 else "재고 추이 (전체)")
        fig_inv_trend.update_layout(xaxis_title="일자", yaxis_title="재고 수량")
        fig_inv_trend.update_xaxes(tickformat="%Y-%m-%d")
        fig_inv_trend.update_yaxes(tickformat=",.0f")
        fig_inv_trend = apply_plotly_theme(fig_inv_trend)
        st.plotly_chart(fig_inv_trend, use_container_width=True)

    # 카테고리별 재고 비중 (탭 내 실행)
    cat_inv_sql = f"""
    SELECT m.category, COALESCE(SUM(i.onhand_qty), 0) AS onhand_qty
    FROM sku_master m
    LEFT JOIN inventory_daily i ON i.sku = m.sku AND i.date = '{latest_date}' {_inv_wh_join(wh)}
    WHERE 1=1
    {base_where}
    GROUP BY m.category
    ORDER BY onhand_qty DESC
    """
    cat_inv = con.execute(cat_inv_sql).fetchdf()

    if not cat_inv.empty and cat_inv["onhand_qty"].sum() > 0:
        cat_inv_display = cat_inv.copy()
        cat_inv_display["category_ko"] = cat_inv_display["category"].map(lambda x: category_map.get(x, x))
        fig_cat_inv = px.bar(
            cat_inv_display,
            x="onhand_qty",
            y="category_ko",
            orientation="h",
            title="카테고리별 재고 비중 (기준일 기준)",
            labels={"onhand_qty": "재고 수량", "category_ko": "카테고리"},
        )
        fig_cat_inv.update_layout(
            yaxis={"categoryorder": "total ascending"},
            bargap=0.6,
        )
        fig_cat_inv.update_xaxes(tickformat=",.0f")
        fig_cat_inv.update_traces(
            marker_color=["#b5dde8", "#8fc9dc", "#6bb5d0", "#52a0c4", "#4a90b0", "#3d7d98"],
            marker_line_color="rgba(255,255,255,0.9)",
            marker_line_width=0.5,
        )
        fig_cat_inv = apply_plotly_theme(fig_cat_inv)
        st.plotly_chart(fig_cat_inv, use_container_width=True)
    else:
        st.caption("카테고리별 재고 비중: 데이터 없음")

    # 카테고리별 수요 비중: cat=ALL, sku_pick=ALL 일 때만 표시
    if cat == "ALL" and sku_pick == "ALL":
        cat_demand_sql = f"""
        SELECT m.category, COALESCE(SUM(d.demand_qty), 0) AS demand_qty
        FROM sku_master m
        LEFT JOIN demand_daily d ON d.sku = m.sku
          AND d.date > '{latest_date}'::DATE - INTERVAL 30 DAY AND d.date <= '{latest_date}'
        WHERE 1=1
        {base_where}
        GROUP BY m.category
        ORDER BY demand_qty DESC
        """
        cat_demand = con.execute(cat_demand_sql).fetchdf()
        if not cat_demand.empty and cat_demand["demand_qty"].sum() > 0:
            cat_demand_display = cat_demand.copy()
            cat_demand_display["category_ko"] = cat_demand_display["category"].map(lambda x: category_map.get(x, x))
            fig_cat_demand = px.bar(
                cat_demand_display,
                x="demand_qty",
                y="category_ko",
                orientation="h",
                title="카테고리별 수요 비중 (최근 30일)",
                labels={"demand_qty": "수요량", "category_ko": "카테고리"},
            )
            fig_cat_demand.update_layout(yaxis={"categoryorder": "total ascending"}, bargap=0.6)
            fig_cat_demand.update_xaxes(tickformat=",.0f")
            fig_cat_demand = apply_plotly_theme(fig_cat_demand)
            st.plotly_chart(fig_cat_demand, use_container_width=True)
    else:
        st.caption("카테고리별 수요 비중: 전체 카테고리·전체 SKU 선택 시에만 표시됩니다.")

with tab_health:
    st.subheader("재고 건전성 분석")
    st.caption("부족/적정/과잉이 얼마나 있고, 어떤 SKU가 문제인가?")

    # 탭 내부 필터 3개: dos_basis_days, shortage_days, over_days
    col_dos_basis, col_risk, col_over = st.columns(3)
    with col_dos_basis:
        health_dos_basis_days = st.selectbox(
            "DOS 산정 기준(최근 N일 평균 일수요)",
            options=[7, 14, 30],
            index=[7, 14, 30].index(st.session_state.get("health_dos_basis_days", 14)) if st.session_state.get("health_dos_basis_days", 14) in [7, 14, 30] else 1,
            format_func=lambda x: f"{x}일",
            key="health_dos_basis_days",
        )
    with col_risk:
        shortage_days = st.selectbox(
            "부족 기준선(일)",
            options=[7, 14, 21],
            index=[7, 14, 21].index(st.session_state.get("health_shortage_days", 14)) if st.session_state.get("health_shortage_days", 14) in [7, 14, 21] else 1,
            format_func=lambda x: f"{x}일 미만",
            key="health_shortage_days",
        )
    with col_over:
        over_days = st.selectbox(
            "과잉 기준선(일)",
            options=[30, 60, 90, 120],
            index=[30, 60, 90, 120].index(st.session_state.get("health_over_days", 60)) if st.session_state.get("health_over_days", 60) in [30, 60, 90, 120] else 1,
            format_func=lambda x: f"{x}일 초과",
            key="health_over_days",
        )

    # Health SQL (탭 내부 실행)
    health_sql_tab = f"""
    WITH base_sku AS (
      SELECT m.sku, m.sku_name, m.category
      FROM sku_master m
      WHERE 1=1
      {base_where}
    ),
    latest_inv AS (
      SELECT sku, warehouse, onhand_qty
      FROM inventory_daily
      WHERE date = '{latest_date}'
      {_inv_wh_where(wh)}
    ),
    demand_30d_cte AS (
      SELECT sku, SUM(demand_qty) AS demand_30d
      FROM demand_daily
      WHERE date > '{latest_date}'::DATE - INTERVAL 30 DAY AND date <= '{latest_date}'
      GROUP BY sku
    ),
    avg_daily_demand AS (
      SELECT sku, AVG(demand_qty) AS avg_daily_demand_Nd
      FROM demand_daily
      WHERE date > '{latest_date}'::DATE - INTERVAL {health_dos_basis_days} DAY AND date <= '{latest_date}'
      GROUP BY sku
    ),
    base AS (
      SELECT
        b.sku, b.sku_name, b.category,
        li.warehouse,
        COALESCE(li.onhand_qty, 0) AS onhand_qty,
        COALESCE(d30.demand_30d, 0) AS demand_30d,
        COALESCE(ad.avg_daily_demand_Nd, 0) AS avg_daily_demand_Nd,
        CASE
          WHEN COALESCE(ad.avg_daily_demand_Nd, 0) = 0 THEN NULL
          ELSE ROUND(COALESCE(li.onhand_qty, 0) / ad.avg_daily_demand_Nd, 1)
        END AS coverage_days
      FROM base_sku b
      LEFT JOIN latest_inv li ON b.sku = li.sku
      LEFT JOIN demand_30d_cte d30 ON b.sku = d30.sku
      LEFT JOIN avg_daily_demand ad ON b.sku = ad.sku
    )
    SELECT sku, sku_name, category, warehouse, onhand_qty, demand_30d, avg_daily_demand_Nd, coverage_days
    FROM base
    ORDER BY coverage_days ASC NULLS LAST
    """
    health = con.execute(health_sql_tab).fetchdf()

    def assign_bucket(row):
        cd = row["coverage_days"]
        if pd.isna(cd):
            return "수요0"
        if cd < shortage_days:
            return "부족"
        if cd > over_days:
            return "과잉"
        return "적정"

    health["bucket"] = health.apply(assign_bucket, axis=1)
    health_with_dos = health[health["coverage_days"].notna()].copy()

    # 구간별 SKU 수 카드 4개 + DOS 분포 히스토그램(기준선 + 부족/과잉 비율 문장)
    cnt_short = int((health["bucket"] == "부족").sum())
    cnt_ok = int((health["bucket"] == "적정").sum())
    cnt_over = int((health["bucket"] == "과잉").sum())
    cnt_nodemand = int((health["bucket"] == "수요0").sum())
    total_sku = len(health)
    pct_short = (cnt_short / total_sku * 100) if total_sku else 0
    pct_over = (cnt_over / total_sku * 100) if total_sku else 0

    row_c1, row_c2, row_hist = st.columns([1, 1, 2])
    with row_c1:
        st.metric("부족 SKU 수", f"{cnt_short:,}")
        st.metric("적정 SKU 수", f"{cnt_ok:,}")
    with row_c2:
        st.metric("과잉 SKU 수", f"{cnt_over:,}")
        st.metric(f"최근 {health_dos_basis_days}일 수요 0 SKU 수", f"{cnt_nodemand:,}")
    with row_hist:
        if not health_with_dos.empty:
            fig_hist = px.histogram(
                health_with_dos,
                x="coverage_days",
                nbins=min(40, max(10, len(health_with_dos) // 3)),
                title="재고 커버 일수(DOS) 분포 (기준선: 부족/과잉)",
                labels={"coverage_days": "재고 커버 일수(DOS)"},
            )
            fig_hist.update_layout(xaxis_title="재고 커버 일수(DOS)", yaxis_title="SKU 수")
            fig_hist.update_yaxes(tickformat=",.0f")
            fig_hist.add_vline(x=shortage_days, line_dash="dash", line_color="crimson")
            fig_hist.add_vline(x=over_days, line_dash="dash", line_color="steelblue")
            fig_hist = apply_plotly_theme(fig_hist)
            st.plotly_chart(fig_hist, use_container_width=True)
            st.caption(f"부족 비율(DOS<{shortage_days}일): {pct_short:.1f}% | 과잉 비율(DOS>{over_days}일): {pct_over:.1f}%")
        else:
            st.caption("DOS 데이터 없음 (전체 수요0 또는 필터 결과 없음)")

    # 우선순위 매트릭스(고수요×부족): X=demand_30d(80% 분위), Y=DOS(shortage_days 기준선), 수요0 제외
    if not health_with_dos.empty:
        health_demand_cut = 0.8
        x_cut = float(health_with_dos["demand_30d"].quantile(health_demand_cut))
        y_cut = shortage_days

        fig_scatter = px.scatter(
            health_with_dos,
            x="demand_30d",
            y="coverage_days",
            size="demand_30d",
            color="bucket",
            color_discrete_map={"부족": "#e74c3c", "적정": "#2ecc71", "과잉": "#3498db"},
            hover_data={
                "sku": True,
                "sku_name": True,
                "category": True,
                "onhand_qty": ",.0f",
                "demand_30d": ",.0f",
                "coverage_days": ",.1f",
                "bucket": True,
            },
            title="우선순위 매트릭스(고수요×부족)",
        )
        fig_scatter.update_layout(
            xaxis_title="최근 30일 수요(개)",
            yaxis_title="재고 커버 일수(DOS)",
        )
        fig_scatter.update_yaxes(tickformat=",.0f")
        fig_scatter.update_xaxes(tickformat=",.0f")
        fig_scatter.add_hline(y=y_cut, line_dash="dash", line_color="gray")
        fig_scatter.add_vline(x=x_cut, line_dash="dash", line_color="gray")
        fig_scatter = apply_plotly_theme(fig_scatter)
        st.plotly_chart(fig_scatter, use_container_width=True)
        st.caption(
            "우하(고수요·저DOS): 최우선 발주/대체 | "
            "좌하(저수요·저DOS): 단종/주문주기 검토 | "
            "우상(고수요·고DOS): 적정 버퍼 | "
            "좌상(저수요·고DOS): 과잉/재고조정"
        )
    else:
        st.caption("산점도: DOS가 있는 SKU가 없어 표시하지 않습니다.")

    # 드릴다운 테이블: bucket 멀티셀렉트(기본 부족·과잉), 정렬 bucket(부족→과잉→적정→수요0) → DOS 오름차순
    st.subheader("드릴다운 테이블")
    bucket_order = {"부족": 0, "과잉": 1, "적정": 2, "수요0": 3}
    health["_bucket_order"] = health["bucket"].map(bucket_order)

    bucket_options = ["부족", "과잉", "적정", "수요0"]
    selected_buckets = st.multiselect(
        "구간(bucket)",
        options=bucket_options,
        default=["부족", "과잉"],
        key="health_bucket_filter",
    )
    if not selected_buckets:
        selected_buckets = bucket_options
    display_health = health[health["bucket"].isin(selected_buckets)].copy()
    display_health = display_health.sort_values(["_bucket_order", "coverage_days"], ascending=[True, True], na_position="last")
    display_health = display_health[
        ["sku", "sku_name", "category", "warehouse", "onhand_qty", "demand_30d", "avg_daily_demand_Nd", "coverage_days", "bucket"]
    ].drop(columns=["_bucket_order"], errors="ignore")
    st.dataframe(display_health, use_container_width=True)

with tab_stockout:
    st.subheader("품절 리스크 분석")
    st.caption("N일 내 품절 예상 SKU 리스트·일정·우선순위 — DOS = 최근 14일 평균 일수요 기준.")

    # 탭 내부 필터: stockout_within_days, risk_level_filter. DOS 기준 14일 고정.
    stockout_within_opts = [7, 14, 21, 30, 60]
    stockout_within_days = st.selectbox(
        "N일 내 품절 기준(일)",
        options=stockout_within_opts,
        index=stockout_within_opts.index(st.session_state.get("risk_stockout_within_days", 14)) if st.session_state.get("risk_stockout_within_days", 14) in stockout_within_opts else 1,
        format_func=lambda x: f"{x}일 미만",
        key="risk_stockout_within_days",
    )
    risk_level_filter = st.selectbox(
        "리스크 등급",
        options=["전체", "Critical", "High", "Medium", "Low"],
        key="risk_level_filter",
    )

    # 품절 리스크 SQL (탭 내 실행, DOS 기준 14일)
    risk_sql = f"""
    WITH base_sku AS (
      SELECT m.sku, m.sku_name, m.category
      FROM sku_master m
      WHERE 1=1
      {base_where}
    ),
    latest_inv AS (
      SELECT sku, warehouse, onhand_qty
      FROM inventory_daily
      WHERE date = '{latest_date}'
      {_inv_wh_where(wh)}
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
        CASE WHEN COALESCE(ad.avg_daily_demand_14d, 0) = 0 THEN NULL
          ELSE ROUND(COALESCE(li.onhand_qty, 0) / ad.avg_daily_demand_14d, 1) END AS coverage_days
      FROM base_sku b
      LEFT JOIN latest_inv li ON b.sku = li.sku
      LEFT JOIN avg_daily_demand ad ON b.sku = ad.sku
      LEFT JOIN demand_7d_cte d7 ON b.sku = d7.sku
    )
    SELECT
      sku, sku_name, category, warehouse,
      onhand_qty, avg_daily_demand_14d, demand_7d, coverage_days,
      CASE WHEN coverage_days IS NOT NULL THEN date_add('{latest_date}'::DATE, CAST(CEIL(coverage_days) AS INTEGER)) ELSE NULL END AS estimated_stockout_date
    FROM base
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
    risk["priority_score"] = risk.apply(
        lambda r: (r["demand_7d"] or 0) / max((r["coverage_days"] or 1), 1), axis=1
    )

    risk_filtered = risk[
        (risk["coverage_days"].notna()) & (risk["coverage_days"] < stockout_within_days)
    ].copy()
    if risk_level_filter != "전체":
        risk_filtered = risk_filtered[risk_filtered["risk_level"] == risk_level_filter]

    # KPI 3개: 리스크 SKU 수, 가장 빠른 예상 품절일, 리스크 수요(최근 7일 수요 합)
    risk_sku_cnt = len(risk_filtered)
    earliest_stockout = risk_filtered["estimated_stockout_date"].min() if not risk_filtered.empty and risk_filtered["estimated_stockout_date"].notna().any() else None
    risk_demand_7d = int(risk_filtered["demand_7d"].sum()) if not risk_filtered.empty else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("리스크 SKU 수", f"{risk_sku_cnt:,}")
    col2.metric("가장 빠른 예상 품절일", str(earliest_stockout) if earliest_stockout is not None and pd.notna(earliest_stockout) else "—")
    col3.metric("리스크 수요(최근 7일 수요 합)", f"{risk_demand_7d:,}")

    # 테이블: sku, sku_name, warehouse, DOS, 예상품절일, onhand, avg_daily_demand, demand_7d, risk_level, priority_score. 정렬: 예상품절일 오름차순, priority_score 내림차순
    st.subheader("품절 리스크 테이블")
    display_cols = [
        "sku", "sku_name", "warehouse",
        "coverage_days", "estimated_stockout_date",
        "onhand_qty", "avg_daily_demand_14d", "demand_7d",
        "risk_level", "priority_score",
    ]
    display_risk = risk_filtered[display_cols].copy()
    display_risk = display_risk.rename(columns={"coverage_days": "DOS", "estimated_stockout_date": "예상품절일", "avg_daily_demand_14d": "avg_daily_demand"})
    display_risk = display_risk.sort_values(["예상품절일", "priority_score"], ascending=[True, False], na_position="last")
    st.dataframe(display_risk, use_container_width=True)
    st.caption("priority_score = demand_7d / max(DOS, 1). 예상품절일 = 기준일 + CEIL(DOS)일.")

with tab_actions:
    st.subheader("발주·조치 제안")
    st.caption("정책 파라미터 기반 추천 발주(why/reason, 예상 품절일 포함). target_stock = 일평균수요×(리드타임+목표커버+안전재고).")

    # 정책 설정
    st.subheader("정책 설정")
    col_lt, col_tc, col_ss, col_moq = st.columns(4)
    with col_lt:
        lead_time_days = st.number_input("리드타임(일)", min_value=0, value=7, step=1, key="lead_time_days")
    with col_tc:
        target_cover_days = st.number_input("목표 커버 일수", min_value=0, value=14, step=1, key="target_cover_days")
    with col_ss:
        safety_stock_days = st.number_input("안전재고 일수", min_value=0, value=3, step=1, key="safety_stock_days")
    with col_moq:
        moq = st.number_input("최소 발주 수량(MOQ, 0=미적용)", min_value=0, value=0, step=1, key="moq")

    # 발주 base SQL (탭 내 실행, DOS 14일, 예상품절일 포함)
    actions_sql = f"""
    WITH base_sku AS (
      SELECT m.sku, m.sku_name, m.category
      FROM sku_master m
      WHERE 1=1
      {base_where}
    ),
    latest_inv AS (
      SELECT sku, warehouse, onhand_qty
      FROM inventory_daily
      WHERE date = '{latest_date}'
      {_inv_wh_where(wh)}
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
        COALESCE(ad.avg_daily_demand_14d, 0) AS avg_daily_demand_14d,
        ROUND(COALESCE(ad.avg_daily_demand_14d, 0) * 10, 0) AS reorder_point,
        CASE WHEN COALESCE(ad.avg_daily_demand_14d, 0) = 0 THEN NULL
          ELSE ROUND(COALESCE(li.onhand_qty, 0) / ad.avg_daily_demand_14d, 1) END AS coverage_days
      FROM base_sku b
      LEFT JOIN latest_inv li ON b.sku = li.sku
      LEFT JOIN avg_daily_demand ad ON b.sku = ad.sku
    )
    SELECT
      sku, sku_name, category, warehouse,
      onhand_qty, reorder_point, avg_daily_demand_14d, coverage_days,
      CASE WHEN coverage_days IS NOT NULL THEN date_add('{latest_date}'::DATE, CAST(CEIL(coverage_days) AS INTEGER)) ELSE NULL END AS estimated_stockout_date
    FROM base
    """
    actions_df = con.execute(actions_sql).fetchdf()
    onhand = pd.to_numeric(actions_df["onhand_qty"], errors="coerce").fillna(0)
    avg_d = pd.to_numeric(actions_df["avg_daily_demand_14d"], errors="coerce").fillna(0)
    total_days = lead_time_days + target_cover_days + safety_stock_days
    target_stock = (avg_d * total_days).round(0).astype(int)
    recommended_order_qty = (target_stock - onhand).clip(lower=0).astype(int)
    if moq > 0:
        recommended_order_qty = recommended_order_qty.where(recommended_order_qty <= 0, recommended_order_qty.clip(lower=moq)).astype(int)
    actions_base = actions_df.copy()
    actions_base["target_stock"] = target_stock
    actions_base["recommended_order_qty"] = recommended_order_qty

    # reason MECE: (1) 즉시위험(DOS<shortage), (2) 정책보충(ROP 미달), (3) 기타. shortage = target_cover_days
    def assign_reason(row):
        if pd.notna(row["coverage_days"]) and row["coverage_days"] < target_cover_days:
            return "즉시위험(DOS<목표커버)"
        if row["onhand_qty"] < row["target_stock"]:
            return "정책보충(ROP 미달)"
        return "기타"

    actions_base["reason"] = actions_base.apply(assign_reason, axis=1)
    actions_display = actions_base[actions_base["recommended_order_qty"] > 0].copy()
    actions_display = actions_display.sort_values(
        ["estimated_stockout_date", "recommended_order_qty"],
        ascending=[True, False],
        na_position="last",
    )

    # reason 멀티셀렉트 (기본 1,2)
    reason_options = ["즉시위험(DOS<목표커버)", "정책보충(ROP 미달)", "기타"]
    selected_reasons = st.multiselect(
        "추천 사유(reason)",
        options=reason_options,
        default=["즉시위험(DOS<목표커버)", "정책보충(ROP 미달)"],
        key="actions_reason_filter",
    )
    if not selected_reasons:
        selected_reasons = reason_options
    actions_filtered = actions_display[actions_display["reason"].isin(selected_reasons)].copy()

    # 테이블: 예상품절일 포함, 정렬 예상품절일 오름차순, 추천수량 내림차순
    st.subheader("추천 발주 테이블")
    display_cols = ["sku", "sku_name", "category", "warehouse", "reason", "estimated_stockout_date", "onhand_qty", "avg_daily_demand_14d", "coverage_days", "target_stock", "recommended_order_qty"]
    out = actions_filtered[[c for c in display_cols if c in actions_filtered.columns]].copy()
    out = out.rename(columns={"estimated_stockout_date": "예상품절일", "coverage_days": "DOS"})
    out["onhand_qty"] = out["onhand_qty"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
    out["avg_daily_demand_14d"] = out["avg_daily_demand_14d"].apply(lambda x: f"{float(x):,.1f}" if pd.notna(x) else "0")
    out["DOS"] = out["DOS"].apply(lambda x: f"{float(x):,.1f}" if pd.notna(x) else "—")
    out["target_stock"] = out["target_stock"].apply(lambda x: f"{int(x):,}")
    out["recommended_order_qty"] = out["recommended_order_qty"].apply(lambda x: f"{int(x):,}")
    st.dataframe(out, use_container_width=True)
    st.caption("recommended_order_qty > 0 만 표시. 정렬: 예상품절일 오름차순, 추천수량 내림차순.")

with tab_movements:
    st.subheader("재고 입·출고 이력")
    st.caption("inventory_txn 기반 입출고 추이 및 트랜잭션 목록")

    if inv_txn is None or len(inv_txn) == 0:
        st.info("inventory_txn 데이터가 없거나 비어 있습니다. CSV를 추가하면 입출고 차트와 트랜잭션 테이블이 표시됩니다.")
    else:
        # 기간 필터: 탭 내부에서 선택 (mv_range_days, 기본 60)
        mov_range_days = st.selectbox(
            "분석 기간(일)",
            options=[7, 14, 30, 60, 90],
            index=[7, 14, 30, 60, 90].index(st.session_state.get("mov_range_days", 60)) if st.session_state.get("mov_range_days", 60) in [7, 14, 30, 60, 90] else 3,
            format_func=lambda x: f"{x}일",
            key="mov_range_days",
        )

        # 입출고 집계 (dt/qty 강제 캐스팅, mov_range_days)
        txn_trend_sql = f"""
        WITH filtered AS (
          SELECT
            CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE) AS dt,
            TRY_CAST(t.qty AS DOUBLE) AS qty
          FROM inventory_txn t
          WHERE CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE)
                BETWEEN '{latest_date}'::DATE - INTERVAL {mov_range_days} DAY AND '{latest_date}'::DATE
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
        if not txn_trend.empty:
            txn_trend["net_qty"] = txn_trend["in_qty"].fillna(0) - txn_trend["out_qty"].fillna(0)
        sum_in = txn_trend["in_qty"].fillna(0).sum() if not txn_trend.empty else 0
        sum_out = txn_trend["out_qty"].fillna(0).sum() if not txn_trend.empty else 0
        sum_net = sum_in - sum_out

        # 필터 반영 후 트랜잭션 row 수
        txn_count_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM inventory_txn t
        WHERE CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE)
              BETWEEN '{latest_date}'::DATE - INTERVAL {mov_range_days} DAY AND '{latest_date}'::DATE
          {"AND t.warehouse = '"+wh+"'" if wh!="ALL" else ""}
          {"AND t.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
          {"AND EXISTS (SELECT 1 FROM sku_master m WHERE m.sku = t.sku AND m.category = '"+cat+"')" if cat!="ALL" else ""}
        """
        txn_row_count = int(con.execute(txn_count_sql).fetchone()[0])

        # 진단 카드 4개: 트랜잭션 row 수, 입고 합, 출고 합, 순변화(Net) 합
        st.caption("진단 (필터 반영)")
        col_diag1, col_diag2, col_diag3, col_diag4 = st.columns(4)
        col_diag1.metric("트랜잭션 row 수", f"{txn_row_count:,}")
        col_diag2.metric("입고(in) 합", f"{sum_in:,.0f}")
        col_diag3.metric("출고(out) 합", f"{sum_out:,.0f}")
        col_diag4.metric("순변화(Net) 합", f"{sum_net:,.0f}")

        # 차트 3개: 입고 bar, 출고 bar, 순변화(net) line
        has_rows = not txn_trend.empty
        if not has_rows:
            st.warning("필터 조건 내 집계된 일자(row)가 없습니다. 기간·창고·SKU·카테고리 필터를 완화하거나, 해당 기간에 트랜잭션이 있는지 확인하세요.")
        else:
            col_in, col_out = st.columns(2)
            with col_in:
                fig_in = px.bar(txn_trend, x="date", y="in_qty", title=f"입고(IN) — 최근 {mov_range_days}일")
                fig_in.update_layout(xaxis_title="일자", yaxis_title="입고 수량")
                fig_in.update_xaxes(tickformat="%Y-%m-%d")
                fig_in.update_yaxes(tickformat=",.0f")
                fig_in = apply_plotly_theme(fig_in)
                st.plotly_chart(fig_in, use_container_width=True)
            with col_out:
                fig_out = px.bar(txn_trend, x="date", y="out_qty", title=f"출고(OUT) — 최근 {mov_range_days}일")
                fig_out.update_layout(xaxis_title="일자", yaxis_title="출고 수량")
                fig_out.update_xaxes(tickformat="%Y-%m-%d")
                fig_out.update_yaxes(tickformat=",.0f")
                fig_out = apply_plotly_theme(fig_out)
                st.plotly_chart(fig_out, use_container_width=True)
            fig_net = px.line(txn_trend, x="date", y="net_qty", title=f"순변화(Net = 입고−출고) — 최근 {mov_range_days}일")
            fig_net.update_layout(xaxis_title="일자", yaxis_title="순변화 수량")
            fig_net.update_xaxes(tickformat="%Y-%m-%d")
            fig_net.update_yaxes(tickformat=",.0f")
            fig_net.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_net = apply_plotly_theme(fig_net)
            st.plotly_chart(fig_net, use_container_width=True)

        # 테이블 2개 뷰: 최신 200건 / qty 절대값 Top 50 (큰 거래 원인)
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
        txn_top50_sql = f"""
        SELECT
          t.txn_datetime,
          CAST(COALESCE(t.date, CAST(t.txn_datetime AS DATE)) AS DATE) AS dt,
          t.sku, t.warehouse, t.txn_type,
          TRY_CAST(t.qty AS DOUBLE) AS qty,
          ABS(TRY_CAST(t.qty AS DOUBLE)) AS abs_qty,
          t.ref_id, t.reason_code
        FROM inventory_txn t
        WHERE 1=1
          {"AND t.warehouse = '"+wh+"'" if wh!="ALL" else ""}
          {"AND t.sku = '"+sku_pick+"'" if sku_pick!="ALL" else ""}
          {"AND EXISTS (SELECT 1 FROM sku_master m WHERE m.sku = t.sku AND m.category = '"+cat+"')" if cat!="ALL" else ""}
        ORDER BY abs_qty DESC
        LIMIT 50
        """
        txn_top50 = con.execute(txn_top50_sql).fetchdf()
        if not txn_top50.empty and "abs_qty" in txn_top50.columns:
            txn_top50 = txn_top50.drop(columns=["abs_qty"], errors="ignore")

        view_txn = st.radio("테이블 뷰", ["최신 200건", "qty 절대값 Top 50 (큰 거래 원인)"], horizontal=True, key="mov_view")
        if view_txn == "최신 200건":
            st.subheader("트랜잭션 목록 (최신 200건)")
            if txn_list.empty:
                st.caption("필터 조건에 맞는 트랜잭션이 없습니다.")
            else:
                st.dataframe(txn_list, use_container_width=True)
        else:
            st.subheader("qty 절대값 Top 50 (큰 거래 원인)")
            if txn_top50.empty:
                st.caption("필터 조건에 맞는 트랜잭션이 없습니다.")
            else:
                st.dataframe(txn_top50, use_container_width=True)
        st.caption("dt = COALESCE(date, txn_datetime 날짜). qty: 숫자형.")
