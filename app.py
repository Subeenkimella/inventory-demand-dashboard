"""
[리팩터링 요약]
- 스토리라인 기준 전면 재구성: 1) 지금 재고 상태는 안전한가? 2) 어떤 SKU가 문제인가, 왜? 3) 언제 문제가 발생하는가? 4) 무엇을 조치해야 하는가?
- 탭 4개만 사용: Overview(요약), 재고 위험 원인 분석(Cause), 품절 발생 시점 분석(Time), 권장 발주·재고 조정(Action).
- 사이드바: 공통 필터만(카테고리/창고/SKU/기준일). 예측 설정·입출고 추적 탭 제거.
- 용어: 약어는 최초 1회 풀네임 병기. 금지어(과잉/무수요/핫이슈/오늘 조치/Top5 등) 제거, 현업 표현(재고 과다 SKU, 우선 점검 대상 등) 사용.
- Overview: KPI 4개만, 각 KPI 하단 해석 문장 1줄, 하단 "[지금 가장 먼저 봐야 할 이유]" 문장형 요약 3줄.
- 각 탭: 테이블 상단 "이 테이블을 왜 봐야 하는지" 설명 문장 필수. 한 화면 핵심 문장 최대 3개.
"""
import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

st.set_page_config(page_title="재고·수요 운영 대시보드", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
  [data-testid="stSidebar"] { font-size: 0.8125rem; }
  h1 { font-size: 1.85rem !important; font-weight: 600; margin-bottom: 0.25rem !important; }
  h2 { font-size: 1.25rem !important; font-weight: 600; margin-top: 1.25rem !important; }
  [data-testid="stMetricValue"] { font-size: 1.5rem !important; font-weight: 600; }
  [data-testid="stMetricLabel"] { font-size: 0.9rem !important; color: #555; }
  .stCaptionContainer { font-size: 0.85rem !important; color: #666; }
</style>
""", unsafe_allow_html=True)


def apply_plotly_theme(fig):
    fig.update_layout(
        template="plotly_white",
        font=dict(size=13),
        margin=dict(l=40, r=20, t=60, b=40),
        xaxis=dict(showgrid=True, gridcolor="#e5e5e5"),
        yaxis=dict(showgrid=True, gridcolor="#e5e5e5", tickformat=",.0f"),
    )
    return fig


def add_ref_hline(fig, y, label, line_dash="dash", line_color="gray"):
    fig.add_hline(y=y, line_dash=line_dash, line_color=line_color)
    fig.add_annotation(x=1, y=y, xref="paper", yref="y", text=label, showarrow=False, xanchor="right", yanchor="bottom")
    return fig


def add_ref_vline(fig, x, label, line_dash="dash", line_color="gray"):
    try:
        x_safe = float(pd.to_numeric(x, errors="coerce")) if pd.notna(x) else None
    except Exception:
        x_safe = x
    if x_safe is not None:
        fig.add_vline(x=x_safe, line_dash=line_dash, line_color=line_color)
        fig.add_annotation(x=x_safe, y=1, xref="x", yref="paper", text=label, showarrow=False, yanchor="bottom", xanchor="left")
    return fig


def fmt_qty(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{int(v):,}"


def fmt_days(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):.1f}"


def fmt_date(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return str(pd.to_datetime(v).date()) if hasattr(pd.to_datetime(v), "date") else str(v)


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
con = duckdb.connect(database=":memory:")
con.register("sku_master", sku)
con.register("demand_daily", demand)
con.register("inventory_daily", inv)


def get_base_sku_where(cat, wh, sku_pick):
    parts = []
    if cat != "ALL":
        parts.append(f"AND m.category = '{cat}'")
    if sku_pick != "ALL":
        parts.append(f"AND m.sku = '{sku_pick}'")
    if wh != "ALL":
        parts.append(f"AND EXISTS (SELECT 1 FROM inventory_daily i WHERE i.sku = m.sku AND i.warehouse = '{wh}')")
    return "\n    ".join(parts) if parts else ""


def _inv_wh_where(wh):
    return f"AND warehouse = '{wh}'" if wh != "ALL" else ""


# 정책 상수 (품절 위험/재고 과다/리드타임 기준)
SHORTAGE_DAYS = 14   # 재고 커버 일수(DOS) 이하면 품절 위험
OVER_DAYS = 60       # DOS 초과면 재고 과다 검토
LEAD_TIME_DAYS = 7   # 리드타임; 예상 품절일이 이보다 빠르면 긴급
DOS_BASIS_DAYS = 14  # DOS 산정 시 최근 N일 평균 일수요

# --- 사이드바: 공통 필터만 (카테고리 / 창고 / SKU / 기준일) ---
st.sidebar.header("조회 조건")
all_dates = con.execute("SELECT DISTINCT date FROM inventory_daily ORDER BY date DESC").fetchdf()
date_opts = all_dates["date"].astype(str).tolist() if not all_dates.empty else []
default_date = date_opts[0] if date_opts else None
if not date_opts:
    st.sidebar.caption("기준일 선택을 위해 재고 일별 데이터가 필요합니다.")

cat_opts = ["ALL"] + sorted(sku["category"].unique().tolist())
wh_opts = ["ALL"] + sorted(inv["warehouse"].unique().tolist())
sku_opts = ["ALL"] + sorted(sku["sku"].unique().tolist())
category_map = {"ALL": "전체", "Motor": "모터", "Brake": "브레이크", "Steering": "스티어링", "Sensor": "센서"}
warehouse_map = {"ALL": "전체", "WH-1": "창고 1", "WH-2": "창고 2"}

cat = st.sidebar.selectbox(
    "카테고리",
    options=cat_opts,
    index=0,
    format_func=lambda x: category_map.get(x, x),
    key="cat",
)
wh = st.sidebar.selectbox(
    "창고",
    options=wh_opts,
    index=0,
    format_func=lambda x: warehouse_map.get(x, x),
    key="wh",
)
sku_pick = st.sidebar.selectbox(
    "SKU",
    options=sku_opts,
    index=0,
    format_func=lambda x: "전체" if x == "ALL" else x,
    key="sku_pick",
)
if date_opts:
    date_idx = date_opts.index(st.session_state.get("base_date", default_date)) if st.session_state.get("base_date", default_date) in date_opts else 0
    base_date = st.sidebar.selectbox(
        "기준일",
        options=date_opts,
        index=date_idx,
        key="base_date",
    )
else:
    base_date = None

if base_date is None:
    st.warning("재고 일별 데이터가 없습니다. inventory_daily.csv를 확인하세요.")
    st.stop()

base_where = get_base_sku_where(cat, wh, sku_pick)

# --- 공통 KPI/원인/시점/조치용 데이터 (한 번 계산) ---
kpi_sql = f"""
WITH base_sku AS (SELECT m.sku FROM sku_master m WHERE 1=1 {base_where}),
latest_inv AS (
  SELECT sku, SUM(onhand_qty) AS onhand_qty
  FROM inventory_daily
  WHERE date = '{base_date}' {_inv_wh_where(wh)}
  GROUP BY sku
),
demand_14 AS (
  SELECT sku, SUM(demand_qty) AS demand_14
  FROM demand_daily
  WHERE date > '{base_date}'::DATE - INTERVAL {DOS_BASIS_DAYS} DAY AND date <= '{base_date}'
  GROUP BY sku
),
demand_7 AS (
  SELECT COALESCE(SUM(d.demand_qty), 0) AS v FROM demand_daily d
  JOIN base_sku b ON d.sku = b.sku
  WHERE d.date > '{base_date}'::DATE - INTERVAL 7 DAY AND d.date <= '{base_date}'
),
sku_dos AS (
  SELECT
    b.sku,
    COALESCE(li.onhand_qty, 0) AS onhand_qty,
    COALESCE(d.demand_14, 0) AS demand_14,
    CASE WHEN COALESCE(d.demand_14, 0) > 0
      THEN ROUND(COALESCE(li.onhand_qty, 0) * {DOS_BASIS_DAYS} * 1.0 / NULLIF(d.demand_14, 0), 1)
      ELSE NULL END AS coverage_days
  FROM base_sku b
  LEFT JOIN latest_inv li ON b.sku = li.sku
  LEFT JOIN demand_14 d ON b.sku = d.sku
)
SELECT
  (SELECT COALESCE(SUM(onhand_qty), 0) FROM sku_dos) AS total_onhand,
  (SELECT COALESCE(v, 0) FROM demand_7) AS demand_cur_7,
  (SELECT MEDIAN(coverage_days) FROM sku_dos WHERE coverage_days IS NOT NULL) AS median_dos,
  (SELECT COUNT(*) FROM sku_dos WHERE coverage_days IS NOT NULL AND coverage_days < {SHORTAGE_DAYS}) AS stockout_sku_cnt
"""
kpi_row = con.execute(kpi_sql).fetchdf().iloc[0]
total_onhand = int(pd.to_numeric(kpi_row["total_onhand"], errors="coerce")) if pd.notna(kpi_row["total_onhand"]) else 0
demand_cur_7 = int(pd.to_numeric(kpi_row["demand_cur_7"], errors="coerce")) if pd.notna(kpi_row["demand_cur_7"]) else 0
median_dos_val = kpi_row["median_dos"]
stockout_sku_cnt = int(pd.to_numeric(kpi_row["stockout_sku_cnt"], errors="coerce")) if pd.notna(kpi_row["stockout_sku_cnt"]) else 0

# 원인/시점/조치용 상세 (30일 수요 + DOS + 예상 품절일)
cause_sql = f"""
WITH base_sku AS (
  SELECT m.sku, m.sku_name, m.category
  FROM sku_master m WHERE 1=1 {base_where}
),
latest_inv AS (
  SELECT sku, warehouse, onhand_qty
  FROM inventory_daily
  WHERE date = '{base_date}' {_inv_wh_where(wh)}
),
demand_30 AS (
  SELECT sku, SUM(demand_qty) AS demand_30d
  FROM demand_daily
  WHERE date > '{base_date}'::DATE - INTERVAL 30 DAY AND date <= '{base_date}'
  GROUP BY sku
),
demand_14 AS (
  SELECT sku, SUM(demand_qty) AS demand_14
  FROM demand_daily
  WHERE date > '{base_date}'::DATE - INTERVAL {DOS_BASIS_DAYS} DAY AND date <= '{base_date}'
  GROUP BY sku
)
SELECT
  b.sku, b.sku_name, b.category, li.warehouse,
  COALESCE(li.onhand_qty, 0) AS onhand_qty,
  COALESCE(d30.demand_30d, 0) AS demand_30d,
  CASE WHEN COALESCE(d14.demand_14, 0) > 0
    THEN ROUND(COALESCE(li.onhand_qty, 0) * {DOS_BASIS_DAYS} * 1.0 / NULLIF(d14.demand_14, 0), 1)
    ELSE NULL END AS coverage_days,
  CASE WHEN COALESCE(d14.demand_14, 0) > 0
    THEN date_add('{base_date}'::DATE, CAST(CEIL(COALESCE(li.onhand_qty, 0) * {DOS_BASIS_DAYS} * 1.0 / NULLIF(d14.demand_14, 0)) AS INTEGER))
    ELSE NULL END AS estimated_stockout_date
FROM base_sku b
LEFT JOIN latest_inv li ON b.sku = li.sku
LEFT JOIN demand_30 d30 ON b.sku = d30.sku
LEFT JOIN demand_14 d14 ON b.sku = d14.sku
"""
cause_df = con.execute(cause_sql).fetchdf()

# --- 본문: 타이틀 + 탭 4개 ---
st.title("재고·수요 운영 대시보드")
st.caption("기준일 기준으로 재고 상태 → 원인 → 시점 → 조치 순서로 판단하고 행동하세요.")

tab_overview, tab_cause, tab_time, tab_action = st.tabs([
    "Overview (요약)",
    "재고 위험 원인 분석",
    "품절 발생 시점 분석",
    "권장 발주·재고 조정",
])

# ========== 1) Overview (요약) — 스토리라인 1번: 지금 재고 상태는 안전한가? ==========
with tab_overview:
    st.subheader("지금 재고 상태는 안전한가?")
    st.caption("아래 4개 지표와 해석으로 현재 상태를 판단하세요. 상세 원인·시점·조치는 다음 탭에서 확인하세요.")

    median_dos_str = f"{median_dos_val:,.1f}일" if pd.notna(median_dos_val) and median_dos_val == median_dos_val else "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("전체 재고 수량", fmt_qty(total_onhand))
    c1.caption("현재 기준일 기준 보유 재고 총량입니다.")
    c2.metric("최근 7일 수요 합계", fmt_qty(demand_cur_7))
    c2.caption("최근 7일 실적 수요 합계로, 수요 강도를 보여줍니다.")
    c3.metric("재고 커버 일수(Days of Supply, DOS) 중앙값", median_dos_str)
    if pd.notna(median_dos_val) and median_dos_val == median_dos_val:
        _cmp = "정책 기준(" + str(SHORTAGE_DAYS) + "일) 대비 여유 있음" if median_dos_val >= SHORTAGE_DAYS else "정책 기준(" + str(SHORTAGE_DAYS) + "일) 미만으로 주의 필요"
        c3.caption(f"재고 커버 일수 중앙값은 {median_dos_val:.1f}일로, {_cmp}.")
    else:
        c3.caption("DOS = 재고 ÷ 일평균 수요. 수요가 없는 SKU는 제외됩니다.")
    c4.metric("품절 위험 SKU 수", fmt_qty(stockout_sku_cnt))
    c4.caption(f"{SHORTAGE_DAYS}일 이내 품절 위험 SKU가 {stockout_sku_cnt}건 존재." if stockout_sku_cnt > 0 else f"{SHORTAGE_DAYS}일 이내 품절 위험 SKU는 없습니다.")

    st.divider()
    st.markdown("**지금 가장 먼저 봐야 할 이유**")
    # 다른 탭 결과를 요약한 문장 3줄 (Overview에서 새 계산 없이 기존 집계만 사용)
    reason_lines = []
    if not cause_df.empty:
        has_dos_under_7 = (cause_df["coverage_days"].notna()) & (cause_df["coverage_days"] < 7)
        if has_dos_under_7.any():
            reason_lines.append("재고 커버 일수 7일 미만 SKU가 존재합니다.")
        latest_dt = pd.to_datetime(base_date)
        lead_cut = latest_dt + pd.Timedelta(days=LEAD_TIME_DAYS)
        est_series = pd.to_datetime(cause_df["estimated_stockout_date"], errors="coerce")
        before_lead = est_series.notna() & (est_series < lead_cut)
        if before_lead.any():
            reason_lines.append("예상 품절일이 리드타임보다 빠른 SKU가 발생했습니다.")
        high_demand = cause_df["demand_30d"] >= cause_df["demand_30d"].quantile(0.75)
        low_dos = cause_df["coverage_days"].notna() & (cause_df["coverage_days"] < SHORTAGE_DAYS)
        if (high_demand & low_dos).any():
            reason_lines.append("최근 수요 증가 대비 재고 보충이 부족한 SKU가 존재합니다.")
    while len(reason_lines) < 3:
        reason_lines.append("—")
    for i, line in enumerate(reason_lines[:3], 1):
        st.markdown(f"{i}. {line}")

# ========== 2) 재고 위험 원인 분석 (Cause) — 스토리라인 2번: 어떤 SKU가 문제인가, 왜? ==========
with tab_cause:
    st.subheader("어떤 SKU가 문제인가, 왜 문제인가?")
    st.caption("수요와 재고 커버 일수(DOS) 매트릭스로 원인을 파악하세요. 수요가 많고 DOS가 짧은 영역이 즉시 발주 검토 대상입니다.")

    health = cause_df.copy()
    health_with_dos = health[health["coverage_days"].notna()].copy()

    if not health_with_dos.empty:
        demand_p75 = float(health_with_dos["demand_30d"].quantile(0.75))
        fig = px.scatter(
            health_with_dos,
            x="demand_30d",
            y="coverage_days",
            size="demand_30d",
            hover_data=["sku", "sku_name", "onhand_qty", "demand_30d", "coverage_days"],
            title="수요 × 재고 커버 일수(DOS) 매트릭스",
        )
        fig.update_layout(xaxis_title="최근 30일 수요(개)", yaxis_title="재고 커버 일수(DOS)")
        add_ref_hline(fig, SHORTAGE_DAYS, f"품절 위험 기준({SHORTAGE_DAYS}일)", line_color="crimson")
        add_ref_hline(fig, OVER_DAYS, f"재고 과다 검토 기준({OVER_DAYS}일)", line_color="steelblue")
        add_ref_vline(fig, demand_p75, "수요 상위 25%", line_color="gray")
        fig = apply_plotly_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "우하: 수요가 많고 재고 커버 일수가 짧아 품절 가능성이 높은 영역(즉시 발주 검토). "
            "좌하: 수요는 적으나 DOS 짧음(주문주기·리드타임 검토). "
            "우상: 수요 많고 DOS 충분(적정). "
            "좌상: 수요 적고 DOS 김(재고 조정 검토 대상)."
        )

    st.markdown("**재고 커버 일수가 정책 기준보다 짧고, 수요 영향도가 높은 SKU**")
    st.caption("이 테이블은 품절 위험 SKU 중 수요가 많은 순으로, 발주 우선순위를 정할 때 봐야 합니다.")
    short_high = health[(health["coverage_days"].notna()) & (health["coverage_days"] < SHORTAGE_DAYS) & (health["demand_30d"] > 0)].copy()
    if not short_high.empty:
        demand_p75_val = short_high["demand_30d"].quantile(0.75)
        short_high = short_high[short_high["demand_30d"] >= demand_p75_val].sort_values("coverage_days", ascending=True)
    if not short_high.empty:
        disp = short_high[["sku", "sku_name", "warehouse", "onhand_qty", "demand_30d", "coverage_days"]].copy()
        disp["onhand_qty"] = disp["onhand_qty"].apply(fmt_qty)
        disp["demand_30d"] = disp["demand_30d"].apply(fmt_qty)
        disp["coverage_days"] = disp["coverage_days"].apply(lambda x: fmt_days(x) + "일" if pd.notna(x) else "—")
        disp = disp.rename(columns={"sku": "SKU", "sku_name": "품목명", "warehouse": "창고", "onhand_qty": "현재고(개)", "demand_30d": "최근 30일 수요(개)", "coverage_days": "재고 커버 일수(DOS)"})
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else:
        st.caption("해당 조건을 만족하는 SKU가 없습니다.")

# ========== 3) 품절 발생 시점 분석 (Time) — 스토리라인 3번: 언제 문제가 터지는가? ==========
with tab_time:
    st.subheader("언제 문제가 발생하는가?")
    st.caption("예상 품절일과 리드타임 대비 여유로 긴급·주의·안정을 구분하세요.")

    time_df = cause_df.copy()
    time_df["estimated_stockout_date"] = pd.to_datetime(time_df["estimated_stockout_date"], errors="coerce")
    latest_dt = pd.to_datetime(base_date)
    lead_cut = latest_dt + pd.Timedelta(days=LEAD_TIME_DAYS)
    within_14 = latest_dt + pd.Timedelta(days=14)

    def status_mark(row):
        est = row.get("estimated_stockout_date")
        cov = row.get("coverage_days")
        if pd.isna(est) and (pd.isna(cov) or cov <= 0):
            return "🟢", "안정"
        if pd.isna(est):
            return "🟢", "안정"
        if est < lead_cut:
            return "🔴", "긴급"
        if est < within_14:
            return "🟠", "주의"
        return "🟢", "안정"

    time_df["_mark"] = time_df.apply(lambda r: status_mark(r)[0], axis=1)
    time_df["상태"] = time_df.apply(lambda r: status_mark(r)[1], axis=1)

    st.markdown("**예상 품절일·재고 커버 일수(DOS)·리드타임 대비 여부**")
    st.caption("이 테이블은 언제 품절이 발생할지 날짜와 상태로 확인할 때 봐야 합니다. 🔴 긴급: 예상 품절일 < 리드타임. 🟠 주의: 14일 이내 소진. 🟢 안정.")

    show_time = time_df[time_df["coverage_days"].notna()].copy()
    show_time = show_time.sort_values("estimated_stockout_date", ascending=True, na_position="last")
    if not show_time.empty:
        disp_t = show_time[["sku", "sku_name", "warehouse", "estimated_stockout_date", "coverage_days", "_mark", "상태"]].copy()
        disp_t["estimated_stockout_date"] = disp_t["estimated_stockout_date"].apply(fmt_date)
        disp_t["coverage_days"] = disp_t["coverage_days"].apply(lambda x: fmt_days(x) + "일" if pd.notna(x) else "—")
        disp_t["예상 품절일"] = disp_t["estimated_stockout_date"]
        disp_t["재고 커버 일수(DOS)"] = disp_t["coverage_days"]
        disp_t["상태 마크"] = disp_t["_mark"]
        disp_t = disp_t[["sku", "sku_name", "warehouse", "예상 품절일", "재고 커버 일수(DOS)", "상태 마크", "상태"]]
        disp_t = disp_t.rename(columns={"sku": "SKU", "sku_name": "품목명", "warehouse": "창고"})
        st.dataframe(disp_t, use_container_width=True, hide_index=True)
    else:
        st.caption("DOS가 산출된 SKU가 없습니다.")

# ========== 4) 권장 발주·재고 조정 (Action) — 스토리라인 4번: 그래서 무엇을 조치해야 하는가? ==========
with tab_action:
    st.subheader("그래서 무엇을 조치해야 하는가?")
    st.caption("사유·조치하지 않을 경우 리스크·권장 조치를 한 테이블에서 확인하세요.")

    st.markdown("**즉시 발주 또는 재고 조정 검토가 필요한 SKU**")
    st.caption("이 테이블은 지금 무엇을 해야 하는지(발주/유지/재고 감축) 결정할 때 봐야 합니다.")

    action_list = []
    for _, row in cause_df.iterrows():
        cov = row.get("coverage_days")
        onhand = int(row.get("onhand_qty", 0) or 0)
        d30 = float(row.get("demand_30d", 0) or 0)
        est = row.get("estimated_stockout_date")

        if pd.notna(cov) and cov < SHORTAGE_DAYS and d30 > 0:
            사유 = f"재고 커버 일수가 정책 기준({SHORTAGE_DAYS}일)보다 짧음(현재 {fmt_days(cov)}일)."
            리스크 = "발주 지연 시 품절로 이어질 수 있음."
            권장_조치 = "발주"
        elif pd.notna(cov) and cov > OVER_DAYS and d30 <= (cause_df["demand_30d"].quantile(0.25) if len(cause_df) else 0):
            사유 = f"재고 커버 일수가 {OVER_DAYS}일을 초과하고 최근 수요가 낮음."
            리스크 = "재고 유지 비용·폐기 리스크 증가."
            권장_조치 = "재고 감축"
        elif d30 == 0 and onhand > 0:
            사유 = "최근 30일 수요가 없는 SKU로 재고만 보유."
            리스크 = "재고 부패·폐기 가능성."
            권장_조치 = "재고 조정 검토"
        elif pd.notna(cov) and cov >= SHORTAGE_DAYS and cov <= OVER_DAYS:
            continue  # 유지 대상은 테이블에서 제외(즉시 조치 대상만 표시)
        else:
            continue
        action_list.append({
            "SKU": row["sku"],
            "품목명": row.get("sku_name", ""),
            "창고": row.get("warehouse", "—"),
            "왜 이 SKU를 조치해야 하는가 (사유)": 사유,
            "조치하지 않을 경우 발생하는 리스크": 리스크,
            "권장 조치": 권장_조치,
        })

    action_df = pd.DataFrame(action_list)
    if not action_df.empty:
        st.dataframe(action_df, use_container_width=True, hide_index=True)
    else:
        st.caption("즉시 발주 또는 재고 조정이 필요한 SKU가 없습니다.")

# --- 삭제 요소 요약 및 탭 역할 (주석) ---
# [삭제한 요소]
# - 예측 설정(모델/기간/학습 구간), MAPE·신뢰도, 예측 기준/실적 전환
# - 입출고 추적 탭, 재고 적정성 탭(히스토그램·구간별 카드·Top N 리스트)
# - 용어: 과잉/무수요/핫이슈/오늘 조치/Top5/Top10/핫 SKU/주요 SKU
# - KPI: 과잉 SKU 수, MAPE&신뢰도
# - 개요 내 품절 위험 Top5·과잉/무수요 Top5·오늘 조치 Top5 테이블
# [각 탭 역할 1줄]
# - Overview: 1번 질문(지금 재고 상태는 안전한가?)에 답하는 KPI 4개 + 해석 + 지금 봐야 할 이유 3줄
# - 재고 위험 원인 분석: 2번 질문(어떤 SKU가 문제인가, 왜?)에 답하는 수요×DOS 매트릭스 + 행동 중심 리스트
# - 품절 발생 시점 분석: 3번 질문(언제 문제가 터지는가?)에 답하는 예상 품절일·상태 마크(긴급/주의/안정)
# - 권장 발주·재고 조정: 4번 질문(무엇을 조치해야 하는가?)에 답하는 사유·리스크·권장 조치 테이블
