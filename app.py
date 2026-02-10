# test commit - 

import os
import re
import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import math

st.set_page_config(page_title="재고·수요 운영 대시보드", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
  [data-testid="stSidebar"] { font-size: 0.8125rem; }
  h1 { font-size: 1.85rem !important; font-weight: 600; margin-bottom: 0.25rem !important; }
  h2 { font-size: 1.25rem !important; font-weight: 600; margin-top: 1.25rem !important; }
  [data-testid="stMetricValue"] { font-size: 1.5rem !important; font-weight: 600; }
  [data-testid="stMetricLabel"] { font-size: 0.9rem !important; color: #555; }
  .stCaptionContainer { font-size: 0.85rem !important; color: #666; }
  .header-info-box {
    padding: 0.6rem 0.9rem;
    border-radius: 8px;
    font-size: 0.8rem;
    line-height: 1.4;
    margin-bottom: 0.5rem;
    border: 1px solid #e2e8f0;
    background: #f8fafc;
  }
  .header-info-box .label { font-weight: 600; color: #64748b; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.03em; margin-bottom: 0.2rem; }
  .header-info-box .value { color: #0f172a; }
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


def _data_file_mtime():
    """CSV 수정 시 캐시가 무효화되도록 파일 mtime을 캐시 키로 사용."""
    t1 = os.path.getmtime("inventory_daily.csv") if os.path.exists("inventory_daily.csv") else 0
    t2 = os.path.getmtime("demand_daily.csv") if os.path.exists("demand_daily.csv") else 0
    return (t1, t2)


@st.cache_data
def load_data(_cache_key):
    sku = pd.read_csv("sku_master.csv")
    demand = pd.read_csv("demand_daily.csv", parse_dates=["date"])
    inv = pd.read_csv("inventory_daily.csv", parse_dates=["date"])
    try:
        inv_txn = pd.read_csv("inventory_txn.csv", parse_dates=["date", "txn_datetime"])
    except FileNotFoundError:
        inv_txn = pd.DataFrame(columns=["txn_datetime", "date", "sku", "warehouse", "txn_type", "qty", "ref_id", "reason_code"])
    return sku, demand, inv, inv_txn


def compute_forecast(demand_df, sku_df, cat, wh, sku_pick, base_date_str, horizon_days=60, lookback_days=180, window_days=14):
    """
    간단한 수요 예측: Moving Average 기반.
    - 최근 lookback_days 구간에서 SKU별 일별 수요 사용
    - 각 SKU별 최근 window_days 평균 수요를 horizon_days 기간 동안 고정 예측
    - 창고 필터(wh)는 예측 대상 SKU만 제한하는 용도로만 사용 (수요는 전체 합계 기준)
    """
    if demand_df is None or demand_df.empty:
        return pd.DataFrame(columns=["date", "sku", "forecast_qty"])
    latest = pd.to_datetime(base_date_str)
    df = demand_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    # 필터: 기준일 이전 lookback_days 구간
    start = latest - pd.Timedelta(days=lookback_days)
    df = df[(df["date"] > start) & (df["date"] <= latest)]
    # 카테고리·SKU 필터
    sku_filtered = sku_df.copy()
    if cat != "ALL":
        sku_filtered = sku_filtered[sku_filtered["category"] == cat]
    if sku_pick != "ALL":
        sku_filtered = sku_filtered[sku_filtered["sku"] == sku_pick]
    sku_list = sku_filtered["sku"].unique().tolist()
    if not sku_list:
        return pd.DataFrame(columns=["date", "sku", "forecast_qty"])
    df = df[df["sku"].isin(sku_list)]
    if df.empty:
        return pd.DataFrame(columns=["date", "sku", "forecast_qty"])
    rows = []
    for sku_code, g in df.groupby("sku"):
        g = g.sort_values("date")
        hist_window = g[g["date"] > latest - pd.Timedelta(days=window_days)]
        if hist_window.empty:
            continue
        avg_val = max(0.0, hist_window["demand_qty"].mean())
        for i in range(1, horizon_days + 1):
            fd = latest + pd.Timedelta(days=i)
            rows.append({"date": fd, "sku": sku_code, "forecast_qty": avg_val})
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["date", "sku", "forecast_qty"])


def compute_forecast_metrics(forecast_daily_df, latest_inv_df, horizon_days, base_date_str):
    """
    forecast_daily(date, sku, forecast_qty)와 latest_inv(sku, onhand_qty)로
    forecast_avg_daily, forecast_dos, stockout_date_forecast, forecast_demand_next7 계산.
    """
    if forecast_daily_df is None or forecast_daily_df.empty:
        return pd.DataFrame()
    latest = pd.to_datetime(base_date_str)
    f = forecast_daily_df.copy()
    f["date"] = pd.to_datetime(f["date"])
    inv = latest_inv_df.copy()
    if inv.empty:
        return pd.DataFrame()
    if "warehouse" in inv.columns:
        inv = inv.groupby("sku")["onhand_qty"].sum().reset_index()
    agg = f.groupby("sku").agg(forecast_total=("forecast_qty", "sum")).reset_index()
    agg["forecast_avg_daily"] = (agg["forecast_total"] / float(horizon_days)).round(2)
    f7 = f[f["date"] <= latest + pd.Timedelta(days=7)]
    next7 = f7.groupby("sku")["forecast_qty"].sum().reset_index().rename(columns={"forecast_qty": "forecast_demand_next7"})
    agg = agg.merge(next7, on="sku", how="left").fillna({"forecast_demand_next7": 0})
    agg = agg.merge(inv, on="sku", how="left")
    agg["onhand_qty"] = agg["onhand_qty"].fillna(0)
    def _dos(row):
        if row["forecast_avg_daily"] and row["forecast_avg_daily"] > 0:
            return round(row["onhand_qty"] / row["forecast_avg_daily"], 1)
        return None
    agg["forecast_dos"] = agg.apply(_dos, axis=1)
    stockout_rows = []
    for sku_code, g in f.groupby("sku"):
        g = g.sort_values("date").copy()
        onhand = float(agg.loc[agg["sku"] == sku_code, "onhand_qty"].iloc[0]) if (agg["sku"] == sku_code).any() else 0.0
        g["cum"] = g["forecast_qty"].cumsum()
        over = g[g["cum"] > onhand]
        d = over["date"].iloc[0] if not over.empty else pd.NaT
        stockout_rows.append({"sku": sku_code, "stockout_date_forecast": d})
    stockout_df = pd.DataFrame(stockout_rows)
    agg = agg.merge(stockout_df, on="sku", how="left")
    return agg


def compute_mape_backtest(demand_df, base_date_str, backtest_days=14, window_days=14):
    """
    Naive backtest: 마지막 backtest_days 동안, t일의 예측을 그 이전 window_days 평균으로 추정.
    Mean Absolute Percentage Error (평균 절대 백분율 오차, MAPE)를 반환.
    """
    if demand_df is None or demand_df.empty:
        return None, 0
    latest = pd.to_datetime(base_date_str)
    df = demand_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    start = latest - pd.Timedelta(days=backtest_days)
    actuals = df[(df["date"] > start) & (df["date"] <= latest)]
    if actuals.empty:
        return None, 0
    errors = []
    for (sku_code, dt), g in actuals.groupby(["sku", "date"]):
        actual = g["demand_qty"].sum()
        if actual <= 0:
            continue
        hist = df[(df["sku"] == sku_code) & (df["date"] < dt) & (df["date"] >= dt - pd.Timedelta(days=window_days))]
        if hist.empty:
            continue
        pred = max(0.0, hist["demand_qty"].mean())
        ape = abs(actual - pred) / actual if actual else 0
        errors.append(ape)
    if not errors:
        return None, 0
    mape_pct = sum(errors) / len(errors) * 100.0
    return mape_pct, len(errors)


sku, demand, inv, inv_txn = load_data(_data_file_mtime())
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


# --- 사이드바: 조회 조건만 (정책/예측은 관리자 탭에서) ---
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
base_date_ts = pd.to_datetime(base_date)

# --- 정책·예측 설정: 관리자 탭에서 설정한 값 사용 (session_state, 없으면 기본값) ---
lead_time_days = int(st.session_state.get("admin_lead_time_days", 7))
shortage_days = int(st.session_state.get("admin_shortage_days", 14))
over_days = int(st.session_state.get("admin_over_days", 60))
dos_basis_days = int(st.session_state.get("admin_dos_basis_days", 14))
if over_days <= shortage_days:
    over_days = shortage_days + 1
    st.session_state["admin_over_days"] = over_days
SHORTAGE_DAYS = shortage_days
OVER_DAYS = over_days
LEAD_TIME_DAYS = lead_time_days
DOS_BASIS_DAYS = dos_basis_days

MODEL_NAME = st.session_state.get("admin_forecast_model", "MovingAvg(14)")
FORECAST_HORIZON_DAYS = int(st.session_state.get("admin_forecast_horizon", 60))
FORECAST_LOOKBACK_DAYS = int(st.session_state.get("admin_forecast_lookback", 180))
# MovingAvg(N)에서 N 추출, 없으면 14
if "MovingAvg" in MODEL_NAME:
    m = re.search(r"\((\d+)\)", MODEL_NAME)
    forecast_window_days = int(m.group(1)) if m else 14
else:
    forecast_window_days = 14

# --- 예측 계산 (옵션 B: 내부 예측 유지, 실패 시 자동 폴백 A) ---
forecast_daily = compute_forecast(
    demand_df=demand,
    sku_df=sku,
    cat=cat,
    wh=wh,
    sku_pick=sku_pick,
    base_date_str=base_date,
    horizon_days=FORECAST_HORIZON_DAYS,
    lookback_days=FORECAST_LOOKBACK_DAYS,
    window_days=forecast_window_days,
)
latest_inv_df = con.execute(
    f"""
    SELECT sku, SUM(onhand_qty) AS onhand_qty
    FROM inventory_daily
    WHERE date = '{base_date}' {_inv_wh_where(wh)}
    GROUP BY sku
    """
).fetchdf()
forecast_metrics_df = compute_forecast_metrics(forecast_daily, latest_inv_df, FORECAST_HORIZON_DAYS, base_date) if not latest_inv_df.empty else pd.DataFrame()
use_forecast = not forecast_metrics_df.empty
mape_pct, mape_n = compute_mape_backtest(demand, base_date) if use_forecast else (None, 0)
if not use_forecast:
    forecast_daily = pd.DataFrame()
    forecast_metrics_df = pd.DataFrame()


def forecast_confidence_label(mape, n):
    if mape is None or n < 10:
        return "정보 부족"
    if mape <= 20:
        return "높음"
    if mape <= 40:
        return "보통"
    return "낮음"


forecast_confidence = forecast_confidence_label(mape_pct, mape_n) if use_forecast else "—"

# --- 공통 KPI/원인/시점/조치용 데이터 (실적 기반 DOS) ---
kpi_sql = f"""
WITH base_sku AS (SELECT m.sku, m.category FROM sku_master m WHERE 1=1 {base_where}),
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
  SELECT COALESCE(SUM(d.demand_qty), 0) AS v
  FROM demand_daily d
  JOIN base_sku b ON d.sku = b.sku
  WHERE d.date > '{base_date}'::DATE - INTERVAL 7 DAY AND d.date <= '{base_date}'
),
sku_dos AS (
  SELECT
    b.sku,
    b.category,
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

detail_sql = f"""
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
),
demand_7d AS (
  SELECT sku, SUM(demand_qty) AS demand_7d
  FROM demand_daily
  WHERE date > '{base_date}'::DATE - INTERVAL 7 DAY AND date <= '{base_date}'
  GROUP BY sku
)
SELECT
  b.sku, b.sku_name, b.category, li.warehouse,
  COALESCE(li.onhand_qty, 0) AS onhand_qty,
  COALESCE(d30.demand_30d, 0) AS demand_30d,
  COALESCE(d7.demand_7d, 0) AS demand_7d,
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
LEFT JOIN demand_7d d7 ON b.sku = d7.sku
"""
base_df = con.execute(detail_sql).fetchdf()

# --- (A) base_df 생성 직후: 예측 merge 및 dos_used/est_date_used/demand7_used 생성 ---
if base_df.empty:
    # 빈 경우에도 아래 컬럼들이 존재하도록 미리 생성
    base_df["dos_used"] = pd.Series(dtype="float")
    base_df["est_date_used"] = pd.Series(dtype="datetime64[ns]")
    base_df["demand7_used"] = pd.Series(dtype="float")
else:
    if use_forecast and not forecast_metrics_df.empty:
        fm = forecast_metrics_df[["sku", "forecast_dos", "stockout_date_forecast", "forecast_demand_next7"]].drop_duplicates("sku")
        base_df = base_df.merge(fm, on="sku", how="left")
        base_df["dos_used"] = base_df.apply(
            lambda r: r["forecast_dos"] if pd.notna(r.get("forecast_dos")) else r["coverage_days"],
            axis=1,
        )
        base_df["est_date_used"] = base_df.apply(
            lambda r: r["stockout_date_forecast"] if pd.notna(r.get("stockout_date_forecast")) else r["estimated_stockout_date"],
            axis=1,
        )
        base_df["demand7_used"] = base_df.apply(
            lambda r: r["forecast_demand_next7"] if pd.notna(r.get("forecast_demand_next7")) else r["demand_7d"],
            axis=1,
        )
    else:
        base_df["dos_used"] = base_df["coverage_days"]
        base_df["est_date_used"] = base_df["estimated_stockout_date"]
        base_df["demand7_used"] = base_df["demand_7d"]


# --- (C) 상태 컬럼(상태/_mark) 한 번만 생성 ---
def classify_status(est_date, dos):
    # 1) DOS가 있으면 DOS를 최우선으로 상태 결정 (운영 관점에서 가장 안정적)
    if pd.notna(dos):
        if dos < LEAD_TIME_DAYS:
            return "🔴", "긴급"
        if dos < SHORTAGE_DAYS:
            return "🟠", "주의"
        return "🟢", "안정"

    # 2) DOS가 없으면(수요 0 등) 날짜로 보조 판단
    est = pd.to_datetime(est_date, errors="coerce")
    if pd.isna(est):
        # 수요가 없어 DOS도/품절일도 산출 불가 → 품절 관점은 안정,
        # 대신 Action에서 '수요 없음 + 재고 보유'로 잡아야 함
        return "🟢", "안정"

    if est < base_date_ts + pd.Timedelta(days=LEAD_TIME_DAYS):
        return "🔴", "긴급"
    if est < base_date_ts + pd.Timedelta(days=SHORTAGE_DAYS):
        return "🟠", "주의"
    return "🟢", "안정"

if base_df.empty:
    base_df["_mark"] = pd.Series(dtype="object")
    base_df["상태"] = pd.Series(dtype="object")
else:
    marks, labels = zip(*[
        classify_status(r.get("est_date_used"), r.get("dos_used"))
        for _, r in base_df.iterrows()
    ])
    base_df["_mark"] = list(marks)
    base_df["상태"] = list(labels)

base_df["priority_score"] = base_df.apply(
    lambda r: (r.get("demand7_used") or 0) / max((r.get("dos_used") or 1), 1),
    axis=1,
)


# --- 상단 헤더: 왼쪽 타이틀 + 오른쪽 상단 정책/예측 박스 2개 ---
col_title, col_boxes = st.columns([2, 1])
with col_title:
    st.title("재고·수요 운영 대시보드")
with col_boxes:
    policy_text = (
        f"🔴 긴급: DOS < LT({LEAD_TIME_DAYS}일) | "
        f"🟠 주의: LT({LEAD_TIME_DAYS}일) ≤ DOS < {SHORTAGE_DAYS}일 | "
        f"🟢 안정: {SHORTAGE_DAYS}일 ≤ DOS | "
        f"🔵 과다: DOS > {OVER_DAYS}일"
    )
    policy_html = f'<div class="header-info-box"><div class="label">🔧 정책 기준</div><div class="value">{policy_text}</div></div>'
    st.markdown(policy_html, unsafe_allow_html=True)
    if use_forecast:
        forecast_text = f"{MODEL_NAME} · 학습 {FORECAST_LOOKBACK_DAYS}일 · 예측 {FORECAST_HORIZON_DAYS}일 · 신뢰도 {forecast_confidence}"
        forecast_html = f'<div class="header-info-box"><div class="label">📈 예측 사용</div><div class="value">{forecast_text}</div></div>'
    else:
        forecast_text = "실적 기반 — Days of Supply (재고 커버 일수, DOS)만 사용"
        forecast_html = f'<div class="header-info-box"><div class="label">📈 예측</div><div class="value">{forecast_text}</div></div>'
    st.markdown(forecast_html, unsafe_allow_html=True)

tab_overview, tab_cause, tab_time, tab_action, tab_admin = st.tabs([
    "Overview",
    "재고 위험 SKU 분석",
    "품절 발생 SKU 분석",
    "권장 발주·재고 분석",
    "관리자 페이지(Optional)",
])

# ========== 1) Overview (요약) — 1) 지금 재고 상태는 안전한가? ==========
with tab_overview:
    # 탭 상단 상태 배지 + 핵심 한 문장
    worst_state = "안정"
    worst_state, worst_mark = "안정", "🟢"
    if not base_df.empty:
        if (base_df["상태"] == "긴급").any():
            worst_state, worst_mark = "긴급", "🔴"
        elif (base_df["상태"] == "주의").any():
            worst_state, worst_mark = "주의", "🟠"

    risk_cnt = int((base_df["dos_used"].notna() & (base_df["dos_used"] < SHORTAGE_DAYS)).sum()) if not base_df.empty else 0
    st.markdown(f"{worst_mark} 현재 재고 상태: {worst_state} · 품절 위험 SKU {risk_cnt}건")


    median_dos_str = f"{median_dos_val:,.1f}일" if pd.notna(median_dos_val) and median_dos_val == median_dos_val else "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("전체 재고 수량", fmt_qty(total_onhand))
    c1.caption("현재 기준 재고 수량")
    c2.metric("최근 7일 수요 합계", fmt_qty(demand_cur_7))
    c3.metric("Days of Supply(재고커버일수, DOS) 중앙값", median_dos_str)
    if pd.notna(median_dos_val) and median_dos_val == median_dos_val:
        _cmp = "정책 기준(" + str(SHORTAGE_DAYS) + "일) 대비 여유 있음" if median_dos_val >= SHORTAGE_DAYS else "정책 기준(" + str(SHORTAGE_DAYS) + "일) 미만으로 주의 필요"
        c3.caption(f"정책 기준 대비 {_cmp}.")
    else:
        c3.caption("DOS는 현재 기준 재고 수량 ÷ 일평균 수요로 산출")
    c4.metric("품절 위험 SKU 수", fmt_qty(stockout_sku_cnt))
    c4.caption(f"정책 기준 {SHORTAGE_DAYS}일 이내 소진 예상 SKU 수")

    st.divider()
    col_pie, col_bar = st.columns(2)
    with col_pie:
        st.markdown("**재고 상태 분포**")
        if not base_df.empty:
            status_counts = base_df["상태"].value_counts().rename_axis("상태").reset_index(name="count")
            color_map = {"긴급": "#e11d48", "주의": "#f97316", "안정": "#22c55e"}
            fig_pie = px.pie(status_counts, names="상태", values="count", color="상태", color_discrete_map=color_map, hole=0.4)
            fig_pie.update_layout(showlegend=True)
            fig_pie = apply_plotly_theme(fig_pie)
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.caption("표시할 상태 데이터가 없습니다.")
    with col_bar:
        if cat == "ALL" and not base_df.empty:
            st.markdown("**카테고리별 품절 위험 SKU 수**")
            risk_df = base_df[base_df["상태"].isin(["긴급", "주의"])].copy()
            if not risk_df.empty:
                bar_df = risk_df.groupby("category")["sku"].nunique().reset_index(name="risk_sku_cnt")
                fig_bar = px.bar(bar_df, x="category", y="risk_sku_cnt", labels={"category": "카테고리", "risk_sku_cnt": "품절 위험 SKU 수"})
                fig_bar = apply_plotly_theme(fig_bar)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.caption("품절 위험 SKU가 없습니다.")
        else:
            st.markdown("**카테고리별 품절 위험 SKU 수**")
            st.caption("카테고리가 전체일 때만 표시됩니다.")

    st.divider()
    st.markdown("**지금 가장 먼저 봐야 할 이유**")
    col_a, col_b, col_c = st.columns(3)
    if not base_df.empty:
        urgent_mask = base_df["상태"] == "긴급"
        warn_mask = base_df["상태"] == "주의"
        high_demand = base_df["demand_30d"] >= base_df["demand_30d"].quantile(0.75)
        low_dos = base_df["dos_used"].notna() & (base_df["dos_used"] < SHORTAGE_DAYS)
        high_demand_low_dos = (high_demand & low_dos)
        n_urgent = int(urgent_mask.sum())
        n_warn = int(warn_mask.sum())
        n_hdld = int(high_demand_low_dos.sum())
    else:
        n_urgent = n_warn = n_hdld = 0
    col_a.markdown(f"🔴 LT 이전 품절 {n_urgent}건")
    col_b.markdown(f"🟠 14일 이내 소진 {n_warn}건")
    col_c.markdown(f"⚠ 수요 급증 대비 재고 부족 {n_hdld}건")

# ========== 2) 재고 위험 원인 분석 (Cause) — 2) 어떤 SKU가 문제인가, 왜? ==========
with tab_cause:
    worst_state = "안정"
    worst_mark = "🟢"
    if not base_df.empty:
        if (base_df["상태"] == "긴급").any():
            worst_state, worst_mark = "긴급", "🔴"
        elif (base_df["상태"] == "주의").any():
            worst_state, worst_mark = "주의", "🟠"
    st.markdown(f"{worst_mark} 문제 SKU와 원인을 확인하세요.")

    health = base_df.copy()
    health_with_dos = health[health["dos_used"].notna()].copy()

    col_cards, col_chart = st.columns([1, 2])
    with col_cards:
        if not health_with_dos.empty:
            demand_p75 = float(health_with_dos["demand_30d"].quantile(0.75))
            demand_p25 = float(health_with_dos["demand_30d"].quantile(0.25))
            cond_high_short = (health_with_dos["demand_30d"] >= demand_p75) & (health_with_dos["dos_used"] < SHORTAGE_DAYS)
            cond_low_long = (health_with_dos["demand_30d"] <= demand_p25) & (health_with_dos["dos_used"] > OVER_DAYS)
            cond_zero_with_stock = (health_with_dos["demand_30d"] == 0) & (health_with_dos["onhand_qty"] > 0)
            st.metric("수요 높음 + DOS 짧음", f"{int(cond_high_short.sum()):,}건")
            st.metric("수요 낮음 + DOS 김", f"{int(cond_low_long.sum()):,}건")
            st.metric("최근 수요 0 + 재고 보유", f"{int(cond_zero_with_stock.sum()):,}건")
        else:
            st.caption("원인 분석을 위한 데이터가 부족합니다.")
    with col_chart:
        if not health_with_dos.empty:
            demand_p75 = float(health_with_dos["demand_30d"].quantile(0.75))
            fig = px.scatter(
                health_with_dos,
                x="demand_30d",
                y="dos_used",
                size="demand_30d",
                color="상태",
                color_discrete_map={"긴급": "#e11d48", "주의": "#f97316", "안정": "#22c55e"},
                hover_data=["sku", "sku_name", "onhand_qty", "demand_30d", "dos_used"],
                title="수요 × 재고 커버 일수(DOS) 매트릭스",
            )
            fig.update_layout(xaxis_title="최근 30일 수요(개)", yaxis_title="재고 커버 일수(DOS)")
            add_ref_hline(fig, SHORTAGE_DAYS, f"품절 위험 기준({SHORTAGE_DAYS}일)", line_color="crimson")
            add_ref_hline(fig, OVER_DAYS, f"재고 과다 검토 기준({OVER_DAYS}일)", line_color="steelblue")
            add_ref_vline(fig, demand_p75, "수요 상위 25%", line_color="gray")
            fig = apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("표시할 데이터가 없습니다.")

    st.markdown("**[SKU 분석] 재고 커버 일수가 정책 기준보다 짧고, 수요 영향도가 높아 우선 점검 필요**")
    short_high = health_with_dos[(health_with_dos["dos_used"] < SHORTAGE_DAYS) & (health_with_dos["demand_30d"] > 0)].copy()
    if not short_high.empty:
        demand_p75_val = short_high["demand_30d"].quantile(0.75)
        short_high = short_high[short_high["demand_30d"] >= demand_p75_val].sort_values("dos_used", ascending=True)
        disp = short_high[["sku", "sku_name", "warehouse", "onhand_qty", "demand_30d", "dos_used", "_mark", "상태"]].copy()
        disp["onhand_qty"] = disp["onhand_qty"].apply(fmt_qty)
        disp["demand_30d"] = disp["demand_30d"].apply(fmt_qty)
        disp["dos_used"] = disp["dos_used"].apply(lambda x: fmt_days(x) + "일" if pd.notna(x) else "—")
        disp = disp.rename(columns={
            "sku": "SKU",
            "sku_name": "품목명",
            "warehouse": "창고",
            "onhand_qty": "현재고(개)",
            "demand_30d": "최근 30일 수요(개)",
            "dos_used": "재고 커버 일수(DOS)",
            "_mark": "상태 마크",
        })
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else:
        st.caption("해당 조건을 만족하는 SKU가 없습니다.")

# ========== 3) 품절 발생 시점 분석 (Time) — 3) 언제 문제가 발생하는가? ==========
with tab_time:
    worst_state = "안정"
    worst_mark = "🟢"
    if not base_df.empty:
        if (base_df["상태"] == "긴급").any():
            worst_state, worst_mark = "긴급", "🔴"
        elif (base_df["상태"] == "주의").any():
            worst_state, worst_mark = "주의", "🟠"
    st.markdown(f"{worst_mark} 언제 품절이 발생하는지 타임라인으로 확인하세요.")

    time_df = base_df.copy()
    time_df["est_date_used"] = pd.to_datetime(time_df["est_date_used"], errors="coerce")

    st.markdown("**[SKU 분석] 예상 품절 타임라인**" + (" (예측)" if use_forecast else " (실적 기반)"))
    if not time_df.empty and time_df["est_date_used"].notna().any():
        tl = time_df[time_df["est_date_used"].notna()].copy()
        tl["date"] = tl["est_date_used"]
        tl["count"] = 1
        fig_t = px.scatter(
            tl,
            x="date",
            y="sku",
            color="상태",
            color_discrete_map={"긴급": "#e11d48", "주의": "#f97316", "안정": "#22c55e"},
            hover_data=["sku", "sku_name", "warehouse", "dos_used"]
        )
        fig_t.update_layout(xaxis_title="예상 품절일", yaxis_title="SKU")
        fig_t = apply_plotly_theme(fig_t)
        st.plotly_chart(fig_t, use_container_width=True)
    else:
        st.caption("예상 품절일 정보가 없습니다.")

    st.markdown("**[SKU 분석] 예상 품절일·DOS·리드타임 대비 상태 확인**" + (" (예측)" if use_forecast else " (실적 기반)"))
    show_time = time_df[time_df["dos_used"].notna()].copy()
    show_time = show_time.sort_values(["상태", "est_date_used"], ascending=[True, True])
    if not show_time.empty:
        disp_t = show_time[["sku", "sku_name", "warehouse", "est_date_used", "dos_used", "_mark", "상태"]].copy()
        disp_t["예상 품절일"] = disp_t["est_date_used"].apply(fmt_date)
        disp_t["재고 커버 일수(DOS)"] = disp_t["dos_used"].apply(lambda x: fmt_days(x) + "일" if pd.notna(x) else "—")
        disp_t = disp_t.rename(columns={
            "sku": "SKU",
            "sku_name": "품목명",
            "warehouse": "창고",
            "_mark": "상태 마크",
        })
        # 긴급이 위로 오도록 상태 순서 정렬
        state_order = {"긴급": 0, "주의": 1, "안정": 2}
        disp_t["_order"] = disp_t["상태"].map(state_order)
        disp_t = disp_t.sort_values(["_order", "예상 품절일"])
        disp_t = disp_t.drop(columns=["_order"])
        st.dataframe(disp_t, use_container_width=True, hide_index=True)
    else:
        st.caption("DOS가 산출된 SKU가 없습니다.")

# ========== 4) 권장 발주·재고 조정 (Action) — 4) 무엇을 조치해야 하는가? ==========
with tab_action:
    worst_state = "안정"
    worst_mark = "🟢"
    if not base_df.empty:
        if (base_df["상태"] == "긴급").any():
            worst_state, worst_mark = "긴급", "🔴"
        elif (base_df["상태"] == "주의").any():
            worst_state, worst_mark = "주의", "🟠"
    st.markdown(f"{worst_mark} 지금 발주·재고 조정이 필요한 SKU를 우선순위로 정렬했습니다.")

    st.markdown("**즉시 발주 또는 재고 조정 검토가 필요한 SKU**" + (" (예측 기반)" if use_forecast else " (실적 기반)"))
    st.caption("이 테이블은 왜 조치해야 하는지, 조치하지 않을 경우 리스크, 권장 조치를 한 번에 보여줍니다.")

    action_list = []
    if not base_df.empty:
        demand_p25 = float(base_df["demand_30d"].quantile(0.25)) if not base_df["demand_30d"].empty else 0
        for _, row in base_df.iterrows():
            cov = row.get("dos_used")
            onhand = int(row.get("onhand_qty", 0) or 0)
            d30 = float(row.get("demand_30d", 0) or 0)
            state_mark = row.get("_mark", "🟢")
            state_label = row.get("상태", "안정")

            reason = risk = action = None
            if pd.notna(cov) and cov < SHORTAGE_DAYS and d30 > 0:
                reason = f"재고 커버 일수가 정책 기준({SHORTAGE_DAYS}일)보다 짧음(현재 {fmt_days(cov)}일)."
                risk = "발주 지연 시 품절로 이어질 수 있음."
                action = "발주"
            elif pd.notna(cov) and cov > OVER_DAYS and d30 <= demand_p25:
                reason = f"재고 커버 일수가 {OVER_DAYS}일을 초과하고 최근 수요가 낮음."
                risk = "재고 유지 비용·폐기 리스크 증가."
                action = "재고 감축"
            elif d30 == 0 and onhand > 0:
                reason = "최근 30일 수요가 없는 SKU로 재고만 보유."
                risk = "재고 부패·폐기 가능성."
                action = "재고 조정 검토"
            else:
                continue
            action_list.append({
                "상태 마크": state_mark,
                "SKU": row["sku"],
                "품목명": row.get("sku_name", ""),
                "창고": row.get("warehouse", "—"),
                "왜 조치해야 하는가(사유)": reason,
                "조치하지 않을 경우 리스크": risk,
                "권장 조치": action,
                "우선순위 점수": row.get("priority_score", 0.0),
            })

    action_df = pd.DataFrame(action_list)
    if not action_df.empty:
        action_df = action_df.sort_values("우선순위 점수", ascending=False)
        st.dataframe(action_df, use_container_width=True, hide_index=True)
    else:
        st.caption("즉시 발주 또는 재고 조정이 필요한 SKU가 없습니다.")

# ========== 5) 관리자 — 정책 설정 + 예측 모델 설정 ==========
with tab_admin:
    st.subheader("정책 설정")
    st.caption("리드타임·품절 위험·재고 과다 기준과 DOS 산정 기간을 설정합니다. 변경 후 다른 탭에서 즉시 반영됩니다.")
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        st.number_input("리드타임 LT (일)", min_value=1, value=st.session_state.get("admin_lead_time_days", 7), key="admin_lead_time_days", step=1)
    with p2:
        st.number_input("품절 위험 기준 DOS (일)", min_value=1, value=st.session_state.get("admin_shortage_days", 14), key="admin_shortage_days", step=1)
    with p3:
        st.number_input("재고 과다 기준 DOS (일)", min_value=1, value=st.session_state.get("admin_over_days", 60), key="admin_over_days", step=1)
    with p4:
        st.number_input("DOS 산정 기간 (최근 N일)", min_value=1, value=st.session_state.get("admin_dos_basis_days", 14), key="admin_dos_basis_days", step=1)
    if st.session_state.get("admin_over_days", 60) <= st.session_state.get("admin_shortage_days", 14):
        st.warning("재고 과다 기준이 품절 위험 기준 이하입니다. 저장 시 자동 보정(과다 = 품절위험+1)됩니다.")
    st.divider()
    st.subheader("예측 모델 설정")
    st.caption("수요 예측에 사용할 모델·학습일·예측일을 설정합니다. 변경 후 다른 탭에서 즉시 반영됩니다.")
    model_opts = ["MovingAvg(7)", "MovingAvg(14)", "MovingAvg(30)", "SeasonalNaive(7)"]
    idx = model_opts.index(st.session_state.get("admin_forecast_model", "MovingAvg(14)")) if st.session_state.get("admin_forecast_model", "MovingAvg(14)") in model_opts else 1
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        st.selectbox(
            "예측 모델",
            options=model_opts,
            index=idx,
            key="admin_forecast_model",
            help="MovingAvg(N): 최근 N일 수요 평균. SeasonalNaive(7): 최근 7일 패턴 반복.",
        )
    with f2:
        st.number_input("학습 구간 (일)", min_value=30, value=st.session_state.get("admin_forecast_lookback", 180), key="admin_forecast_lookback", step=1)
    with f3:
        st.number_input("예측 기간 (일)", min_value=7, value=st.session_state.get("admin_forecast_horizon", 60), key="admin_forecast_horizon", step=1)
    with f4:
        st.caption("**현재 적용**  \n모델: " + st.session_state.get("admin_forecast_model", "MovingAvg(14)") + "  \n학습 " + str(st.session_state.get("admin_forecast_lookback", 180)) + "일 · 예측 " + str(st.session_state.get("admin_forecast_horizon", 60)) + "일")
