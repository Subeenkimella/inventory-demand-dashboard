import math
import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

st.set_page_config(page_title="재고·수요 모니터링 대시보드", layout="wide", initial_sidebar_state="expanded")

# [문구 스타일 가이드]
# 1. 톤: 현업(구매/자재/SCM)이 5초 안에 상태·리스크·조치를 파악할 수 있도록 짧고 단정하게(한 문장 25~60자).
# 2. 용어: 영어 혼용 금지. MAPE·SKU·DOS 등 업계 약어는 괄호로 1회만 풀어서 병기 후 이후 약어만 사용.
# 3. 지표: DOS 첫 등장 시 "DOS(재고 커버리지 일수) = 재고 ÷ 일평균 수요" 풀이. 기준선(부족/과잉)은 캡션에 명시.
# 4. 구분: 실적 기준 / 예측 기준으로 표기. 경고는 "원인→영향→확인할 것" 순 1~2문장.
# 5. 단위: 수량은 콤마 정수, 비율은 % 소수 1자리, 일수는 소수 1자리, 일자는 YYYY-MM-DD.

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


# Plotly add_vline/add_hline(..., annotation_text=...) can raise TypeError when x/y is a
# pandas Timestamp (Plotly internally tries sum([Timestamp]) for annotation position).
# Use separate add_vline/add_hline (no annotation_text) plus add_annotation for labels.

def to_plotly_x(value):
    """Convert pandas Timestamp/date/string to a value safe for Plotly (datetime or string). Numerics passed through."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    try:
        return pd.to_datetime(value).to_pydatetime()
    except Exception:
        return str(value)


def add_ref_vline(fig, x, label="기준일", line_dash="dot", line_color="gray"):
    """Draw a vertical reference line and a separate text annotation (avoids annotation_text Timestamp bug)."""
    x0 = to_plotly_x(x)
    fig.add_vline(x=x0, line_dash=line_dash, line_color=line_color)
    fig.add_annotation(
        x=x0, y=1, xref="x", yref="paper",
        text=label, showarrow=False,
        yanchor="bottom", xanchor="left",
    )
    return fig


def add_ref_hline(fig, y, label, line_dash="dot", line_color="gray"):
    """Draw a horizontal reference line and a separate text annotation (avoids annotation_text bug)."""
    fig.add_hline(y=y, line_dash=line_dash, line_color=line_color)
    fig.add_annotation(
        x=1, y=y, xref="paper", yref="y",
        text=label, showarrow=False,
        xanchor="right", yanchor="bottom",
    )
    return fig


def add_ref_vrect(fig, x0, x1, label="", fill_color="rgba(255,165,0,0.1)", line_color="orange"):
    """Shade a period and add a separate text annotation (no annotation_text in add_vrect)."""
    x0_safe = to_plotly_x(x0)
    x1_safe = to_plotly_x(x1)
    fig.add_vrect(x0=x0_safe, x1=x1_safe, fillcolor=fill_color, line_width=0)
    if label:
        fig.add_annotation(
            x=x1_safe, y=1, xref="x", yref="paper",
            text=label, showarrow=False,
            yanchor="bottom", xanchor="left",
        )
    return fig


def fmt_qty(v):
    """Format quantity as comma-separated integer."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{int(v):,}"


def fmt_pct(v):
    """Format rate as % with 1 decimal."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):.1f}%"


def fmt_days(v):
    """Format days with 1 decimal."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):.1f}"


def fmt_date(v):
    """Format date as YYYY-MM-DD."""
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


def _get_filtered_skus_pandas(sku_df, inv_df, cat, wh, sku_pick):
    """공통 필터(cat, wh, sku_pick)에 해당하는 SKU 목록. pandas로 계산(캐시용)."""
    m = sku_df.copy()
    if cat != "ALL":
        m = m[m["category"] == cat]
    if sku_pick != "ALL":
        m = m[m["sku"] == sku_pick]
    if wh != "ALL":
        wh_skus = set(inv_df[inv_df["warehouse"] == wh]["sku"].unique())
        m = m[m["sku"].isin(wh_skus)]
    return m["sku"].unique().tolist()


@st.cache_data
def compute_forecast(_demand_df, _sku_df, _inv_df, cat, wh, sku_pick, latest_date_str, horizon_days, model_type, lookback_days):
    """
    수요 예측: forecast_daily(date, sku, forecast_qty) 생성.
    - MovingAvg(N): 최근 N일 평균을 horizon_days 동안 동일 값으로 예측.
    - SeasonalNaive(7): 최근 7일 패턴을 horizon_days에 반복.
    lookback_days 범위 내 데이터만 사용. 공통 필터(cat/wh/sku_pick) 적용 SKU만 예측.
    """
    latest_date = pd.to_datetime(latest_date_str)
    demand = _demand_df[_demand_df["date"].notna()].copy()
    demand["date"] = pd.to_datetime(demand["date"])
    lookback_start = latest_date - pd.Timedelta(days=lookback_days)
    demand = demand[(demand["date"] >= lookback_start) & (demand["date"] <= latest_date)]

    sku_list = _get_filtered_skus_pandas(_sku_df, _inv_df, cat, wh, sku_pick)
    if not sku_list:
        return pd.DataFrame(columns=["date", "sku", "forecast_qty"])

    # model_type 파싱: "MovingAvg(7)" -> ("MovingAvg", 7), "SeasonalNaive(7)" -> ("SeasonalNaive", 7)
    if "MovingAvg" in model_type:
        n = int(model_type.replace("MovingAvg(", "").replace(")", ""))
        method, param = "MovingAvg", n
    elif "SeasonalNaive" in model_type:
        n = int(model_type.replace("SeasonalNaive(", "").replace(")", ""))
        method, param = "SeasonalNaive", n
    else:
        method, param = "MovingAvg", 14

    rows = []
    for sku in sku_list:
        d = demand[demand["sku"] == sku].sort_values("date")
        if d.empty:
            continue
        if method == "MovingAvg":
            last_n = d.tail(param)
            if last_n.empty:
                continue
            avg_val = last_n["demand_qty"].mean()
            for i in range(1, horizon_days + 1):
                fd = latest_date + pd.Timedelta(days=i)
                rows.append({"date": fd, "sku": sku, "forecast_qty": max(0, avg_val)})
        else:  # SeasonalNaive(7)
            last_n = d.tail(param)
            if len(last_n) < param:
                avg_val = last_n["demand_qty"].mean()
                for i in range(1, horizon_days + 1):
                    fd = latest_date + pd.Timedelta(days=i)
                    rows.append({"date": fd, "sku": sku, "forecast_qty": max(0, avg_val)})
            else:
                pattern = last_n["demand_qty"].values
                for i in range(1, horizon_days + 1):
                    fd = latest_date + pd.Timedelta(days=i)
                    qty = pattern[(i - 1) % len(pattern)]
                    rows.append({"date": fd, "sku": sku, "forecast_qty": max(0, float(qty))})

    if not rows:
        return pd.DataFrame(columns=["date", "sku", "forecast_qty"])
    return pd.DataFrame(rows)


def compute_forecast_metrics(forecast_daily_df, latest_inv_df, horizon_days, latest_date):
    """
    forecast_daily(date, sku, forecast_qty)와 latest_inv(sku, onhand_qty)로
    forecast_avg_daily, forecast_dos, stockout_date_forecast, forecast_demand_next7 계산.
    """
    if forecast_daily_df.empty:
        return pd.DataFrame()
    latest_date = pd.to_datetime(latest_date)
    f = forecast_daily_df.copy()
    f["date"] = pd.to_datetime(f["date"])

    inv = latest_inv_df.copy()
    if "warehouse" in inv.columns:
        inv = inv.groupby("sku")["onhand_qty"].sum().reset_index()
    else:
        inv = inv[["sku", "onhand_qty"]].drop_duplicates()

    agg = f.groupby("sku").agg(forecast_total=("forecast_qty", "sum")).reset_index()
    agg["forecast_avg_daily"] = (agg["forecast_total"] / horizon_days).round(2)
    f7 = f[f["date"] <= latest_date + pd.Timedelta(days=7)]
    next7 = f7.groupby("sku")["forecast_qty"].sum().reset_index()
    next7 = next7.rename(columns={"forecast_qty": "forecast_demand_next7"})
    agg = agg.merge(next7, on="sku", how="left").fillna(0)
    agg = agg.merge(inv, on="sku", how="left")
    agg["onhand_qty"] = agg["onhand_qty"].fillna(0)
    agg["forecast_dos"] = agg.apply(
        lambda r: round(r["onhand_qty"] / r["forecast_avg_daily"], 1) if r["forecast_avg_daily"] and r["forecast_avg_daily"] > 0 else None,
        axis=1,
    )

    # stockout_date_forecast: 날짜 오름차순 cumsum, onhand_qty 초과하는 첫 date
    stockout_list = []
    for sku in agg["sku"].unique():
        df_sku = f[f["sku"] == sku].sort_values("date")
        if df_sku.empty:
            stockout_list.append({"sku": sku, "stockout_date_forecast": None})
            continue
        onhand = float(agg.loc[agg["sku"] == sku, "onhand_qty"].iloc[0])
        df_sku = df_sku.copy()
        df_sku["cum"] = df_sku["forecast_qty"].cumsum()
        over = df_sku[df_sku["cum"] > onhand]
        d = over["date"].iloc[0] if len(over) > 0 else None
        stockout_list.append({"sku": sku, "stockout_date_forecast": d})
    stockout_df = pd.DataFrame(stockout_list)
    agg = agg.merge(stockout_df, on="sku", how="left")
    return agg


def compute_mape_backtest(demand_df, lookback_days, model_type, latest_date_str, backtest_days=14):
    """
    Naive backtest: for last `backtest_days` days, predict day t with mean of previous N days (same N as model).
    Returns aggregate MAPE % and count of (sku, date) points used.
    """
    if demand_df is None or demand_df.empty:
        return None, 0
    latest = pd.to_datetime(latest_date_str)
    if "MovingAvg" in model_type:
        n = int(model_type.replace("MovingAvg(", "").replace(")", ""))
    elif "SeasonalNaive" in model_type:
        n = int(model_type.replace("SeasonalNaive(", "").replace(")", ""))
    else:
        n = 14
    demand = demand_df.copy()
    demand["date"] = pd.to_datetime(demand["date"])
    start = latest - pd.Timedelta(days=backtest_days)
    # We need actuals in [start, latest] and history before that for prediction
    actuals = demand[(demand["date"] >= start) & (demand["date"] <= latest)]
    if actuals.empty:
        return None, 0
    errors = []
    for (sku, date), g in actuals.groupby(["sku", "date"]):
        actual = g["demand_qty"].sum()
        if actual <= 0:
            continue
        hist = demand[(demand["sku"] == sku) & (demand["date"] < date) & (demand["date"] >= date - pd.Timedelta(days=n))]
        pred = hist["demand_qty"].mean() if len(hist) > 0 else 0
        pred = max(0, pred)
        ape = abs(actual - pred) / actual if actual else 0
        errors.append(ape)
    if not errors:
        return None, 0
    mape_pct = sum(errors) / len(errors) * 100
    return mape_pct, len(errors)

st.markdown("""
<style>
    /* 실무형 대시보드 가독성 */
    h1 { font-size: 1.85rem !important; font-weight: 600; margin-bottom: 0.25rem !important; }
    h2 { font-size: 1.25rem !important; font-weight: 600; margin-top: 1.25rem !important; }
    h3 { font-size: 1.05rem !important; font-weight: 600; color: #333; margin-top: 1rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.5rem !important; font-weight: 600; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; color: #555; }
    .stCaptionContainer { font-size: 0.85rem !important; color: #666; }
    hr { margin: 1rem 0 !important; }
</style>
""", unsafe_allow_html=True)
st.title("📦 재고·수요 모니터링 대시보드")
st.caption("기준일 기준 재고·수요·예측을 한 화면에서 확인하고, 상태·리스크·조치를 바로 파악할 수 있습니다.")

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

# --- 사이드바: 조회 조건 + 예측 설정 ---
st.sidebar.header("조회 조건")
st.sidebar.caption("카테고리·창고·SKU로 분석 대상을 선택하세요.")
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

st.sidebar.divider()
st.sidebar.header("예측 설정")
st.sidebar.caption("수요 예측 모델·기간. Overview·리스크·발주 탭에 반영됩니다.")
horizon_opts = [7, 14, 30, 60]
model_opts = ["MovingAvg(7)", "MovingAvg(14)", "MovingAvg(30)", "SeasonalNaive(7)"]
lookback_opts = [90, 180, 365]
horizon_days = st.sidebar.selectbox(
    "예측 기간(일)",
    options=horizon_opts,
    index=horizon_opts.index(st.session_state.get("forecast_horizon_days", 14)) if st.session_state.get("forecast_horizon_days", 14) in horizon_opts else 1,
    format_func=lambda x: f"{x}일",
    key="forecast_horizon_days",
)
model_type = st.sidebar.selectbox(
    "예측 모델",
    options=model_opts,
    index=model_opts.index(st.session_state.get("forecast_model_type", "MovingAvg(14)")) if st.session_state.get("forecast_model_type", "MovingAvg(14)") in model_opts else 1,
    key="forecast_model_type",
)
lookback_days = st.sidebar.selectbox(
    "학습 구간(일)",
    options=lookback_opts,
    index=lookback_opts.index(st.session_state.get("forecast_lookback_days", 180)) if st.session_state.get("forecast_lookback_days", 180) in lookback_opts else 1,
    format_func=lambda x: f"{x}일",
    key="forecast_lookback_days",
)
st.sidebar.divider()
show_only_exceptions = st.sidebar.toggle("예외만 보기", value=True, key="show_only_exceptions", help="ON이면 부족·과잉·품절 위험 행만 표시")
sku_search_term = st.sidebar.text_input("SKU 검색(명·코드)", value=st.session_state.get("sku_search_term", ""), key="sku_search_term", placeholder="테이블에서 SKU로 필터")
cat = st.session_state.get("cat", "ALL")
wh = st.session_state.get("wh", "ALL")
sku_pick = st.session_state.get("sku_pick", "ALL")
base_where = get_base_sku_where(cat, wh, sku_pick)

def _inv_wh_where(wh):
    return f"AND warehouse = '{wh}'" if wh != "ALL" else ""

def _inv_wh_join(wh):
    return f"AND i.warehouse = '{wh}'" if wh != "ALL" else ""


def _base_sku_cte(base_where, with_name=True):
    """Return CTE SQL for base SKUs: WITH base_sku AS (SELECT m.sku ... WHERE 1=1 {base_where}). Optionally include 'WITH base_sku AS'."""
    sql = f"(SELECT m.sku FROM sku_master m WHERE 1=1 {base_where})"
    return f"WITH base_sku AS {sql}" if with_name else sql


# 수요 예측 결과 (캐시: cat/wh/sku_pick/horizon/model/lookback/latest_date 변경 시 재계산)
forecast_daily = compute_forecast(demand, sku, inv, cat, wh, sku_pick, str(latest_date), horizon_days, model_type, lookback_days)
latest_inv_df = con.execute(f"""
  SELECT sku, warehouse, onhand_qty
  FROM inventory_daily
  WHERE date = '{latest_date}'
  {_inv_wh_where(wh)}
""").fetchdf()
forecast_metrics_df = pd.DataFrame()
if not forecast_daily.empty and not latest_inv_df.empty:
    forecast_metrics_df = compute_forecast_metrics(forecast_daily, latest_inv_df, horizon_days, latest_date)

# --- Ops Header ---
st.markdown("---")
st.markdown(f"**기준일** `{fmt_date(latest_date)}`")
filter_parts = []
if cat != "ALL":
    filter_parts.append(f"카테고리: {category_map.get(cat, cat)}")
if wh != "ALL":
    filter_parts.append(f"창고: {warehouse_map.get(wh, wh)}")
if sku_pick != "ALL":
    filter_parts.append(f"SKU: {sku_pick}")
st.caption(" · ".join(filter_parts) if filter_parts else "필터 없음 (전체 카테고리·창고·SKU)")
st.caption(f"예측: {model_type} · 학습 {lookback_days}일 · 예측 기간 {horizon_days}일")
st.markdown("---")

# Lightweight counts for "Recommended next step" (fixed dos_basis=14)
_summary_sql = f"""
WITH base_sku AS (SELECT m.sku FROM sku_master m WHERE 1=1 {base_where}),
latest_inv AS (SELECT sku, SUM(onhand_qty) AS onhand_qty FROM inventory_daily WHERE date = '{latest_date}' {_inv_wh_where(wh)} GROUP BY sku),
demand_14 AS (SELECT sku, SUM(demand_qty) AS demand_14 FROM demand_daily WHERE date > '{latest_date}'::DATE - INTERVAL 14 DAY AND date <= '{latest_date}' GROUP BY sku),
sku_dos AS (
  SELECT b.sku,
    CASE WHEN COALESCE(d.demand_14, 0) > 0 THEN ROUND(COALESCE(li.onhand_qty, 0) * 14.0 / NULLIF(d.demand_14, 0), 1) ELSE NULL END AS coverage_days
  FROM base_sku b
  LEFT JOIN latest_inv li ON b.sku = li.sku
  LEFT JOIN demand_14 d ON b.sku = d.sku
)
SELECT
  (SELECT COUNT(*) FROM sku_dos WHERE coverage_days IS NOT NULL AND coverage_days < 14) AS stockout_cnt,
  (SELECT COUNT(*) FROM sku_dos WHERE coverage_days IS NOT NULL AND coverage_days > 60) AS overstock_cnt
"""
try:
    _summary = con.execute(_summary_sql).fetchdf().iloc[0]
    summary_stockout_cnt = int(pd.to_numeric(_summary["stockout_cnt"], errors="coerce")) if pd.notna(_summary["stockout_cnt"]) else 0
    summary_overstock_cnt = int(pd.to_numeric(_summary["overstock_cnt"], errors="coerce")) if pd.notna(_summary["overstock_cnt"]) else 0
except Exception:
    summary_stockout_cnt = 0
    summary_overstock_cnt = 0

# --- Tabs ---
tab_exec, tab_health, tab_stockout, tab_actions, tab_movements = st.tabs([
    "Overview",
    "재고 적정성",
    "품절 위험",
    "발주·조치",
    "입출고 추적",
])

with tab_exec:
    st.subheader("Overview")
    st.caption("기준일 KPI·우선 점검 이슈·추이·카테고리 비중을 한눈에 보고, 바로 할 일부터 진행하세요.")
    if summary_stockout_cnt > 0:
        st.info("**바로 할 일:** **품절 위험** 탭에서 Critical SKU부터 확인하세요.")
    elif summary_overstock_cnt > 0:
        st.info("**바로 할 일:** **재고 적정성** 탭에서 과잉 구간을 확인한 뒤 **발주·조치** 탭에서 조치하세요.")
    else:
        st.info("**바로 할 일:** **재고 적정성**·**품절 위험** 탭에서 예외가 없는지 확인하세요.")

    st.markdown(f"**적용 예측:** {model_type} · 학습 {lookback_days}일 · 예측 기간 {horizon_days}일")

    st.markdown("#### 1. 현황 요약 (기준일 기준)")
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
    WITH base_sku AS (SELECT m.sku, m.sku_name, m.category FROM sku_master m WHERE 1=1 {base_where}),
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
    # Deltas vs 7 days ago (onhand) and vs previous 7 days (demand)
    delta_sql = f"""
    WITH base_sku AS (SELECT m.sku FROM sku_master m WHERE 1=1 {base_where}),
    inv_now AS (SELECT SUM(onhand_qty) AS v FROM inventory_daily i JOIN base_sku b ON i.sku = b.sku WHERE i.date = '{latest_date}' {_inv_wh_where(wh)}),
    inv_7d AS (SELECT SUM(onhand_qty) AS v FROM inventory_daily i JOIN base_sku b ON i.sku = b.sku WHERE i.date = '{latest_date}'::DATE - INTERVAL 7 DAY {_inv_wh_where(wh)}),
    demand_cur_7 AS (SELECT COALESCE(SUM(d.demand_qty), 0) AS v FROM demand_daily d JOIN base_sku b ON d.sku = b.sku WHERE d.date > '{latest_date}'::DATE - INTERVAL 7 DAY AND d.date <= '{latest_date}'::DATE),
    demand_prev_7 AS (SELECT COALESCE(SUM(d.demand_qty), 0) AS v FROM demand_daily d JOIN base_sku b ON d.sku = b.sku WHERE d.date > '{latest_date}'::DATE - INTERVAL 14 DAY AND d.date <= '{latest_date}'::DATE - INTERVAL 7 DAY)
    SELECT (SELECT COALESCE(v, 0) FROM inv_now) AS onhand_now, (SELECT COALESCE(v, 0) FROM inv_7d) AS onhand_7d_ago,
           (SELECT COALESCE(v, 0) FROM demand_cur_7) AS demand_cur_7, (SELECT COALESCE(v, 0) FROM demand_prev_7) AS demand_prev_7
    """
    try:
        delta_row = con.execute(delta_sql).fetchdf().iloc[0]
        onhand_now = int(pd.to_numeric(delta_row["onhand_now"], errors="coerce")) if pd.notna(delta_row["onhand_now"]) else 0
        onhand_7d = int(pd.to_numeric(delta_row["onhand_7d_ago"], errors="coerce")) if pd.notna(delta_row["onhand_7d_ago"]) else 0
        demand_cur_7 = int(pd.to_numeric(delta_row["demand_cur_7"], errors="coerce")) if pd.notna(delta_row["demand_cur_7"]) else 0
        demand_prev_7 = int(pd.to_numeric(delta_row["demand_prev_7"], errors="coerce")) if pd.notna(delta_row["demand_prev_7"]) else 0
        delta_onhand = (onhand_now - onhand_7d) if (onhand_now or onhand_7d) else None
        delta_demand = (demand_cur_7 - demand_prev_7) if (demand_cur_7 is not None and demand_prev_7 is not None) else None
    except Exception:
        delta_onhand = None
        delta_demand = None

    col1, col2, col3, col4, col5 = st.columns(5)
    total_onhand = int(pd.to_numeric(exec_kpi["total_onhand"], errors="coerce")) if pd.notna(exec_kpi["total_onhand"]) else 0
    total_demand_Nd = int(pd.to_numeric(exec_kpi["total_demand_Nd"], errors="coerce")) if pd.notna(exec_kpi["total_demand_Nd"]) else 0
    median_dos_val = exec_kpi["median_dos"]
    median_dos_str = f"{median_dos_val:,.1f}" if pd.notna(median_dos_val) and (median_dos_val == median_dos_val) else "—"
    stockout_sku_cnt = int(pd.to_numeric(exec_kpi["stockout_sku_cnt"], errors="coerce")) if pd.notna(exec_kpi["stockout_sku_cnt"]) else 0
    overstock_sku_cnt = int(pd.to_numeric(exec_kpi["overstock_sku_cnt"], errors="coerce")) if pd.notna(exec_kpi["overstock_sku_cnt"]) else 0
    col1.metric("현재 재고(총 수량)", fmt_qty(total_onhand), delta=delta_onhand if delta_onhand is not None else None)
    col2.metric(f"최근 {dos_basis_days}일 수요 합계", fmt_qty(total_demand_Nd), delta=delta_demand if delta_demand is not None else None)
    col3.metric("커버리지(DOS) 중앙값(일)", median_dos_str)
    col4.metric("품절 위험 SKU 수 (DOS 14일 미만)", fmt_qty(stockout_sku_cnt))
    col5.metric("과잉 재고 SKU 수 (DOS 60일 초과)", fmt_qty(overstock_sku_cnt))

    st.caption(f"DOS(재고 커버리지 일수) = 재고 ÷ 일평균 수요. 기준: 최근 {dos_basis_days}일. 품절 위험 14일 미만, 과잉 60일 초과.")

    st.markdown("#### 우선 점검 이슈 (상위 5건)")
    hot_sql = f"""
    WITH base_sku AS (SELECT m.sku, m.sku_name FROM sku_master m WHERE 1=1 {base_where}),
    latest_inv AS (
      SELECT sku, warehouse, onhand_qty
      FROM inventory_daily
      WHERE date = '{latest_date}' {_inv_wh_where(wh)}
    ),
    demand_14 AS (SELECT sku, SUM(demand_qty) AS demand_14d FROM demand_daily
      WHERE date > '{latest_date}'::DATE - INTERVAL 14 DAY AND date <= '{latest_date}' GROUP BY sku),
    demand_30 AS (SELECT sku, SUM(demand_qty) AS demand_30d FROM demand_daily
      WHERE date > '{latest_date}'::DATE - INTERVAL 30 DAY AND date <= '{latest_date}' GROUP BY sku)
    SELECT b.sku, b.sku_name, li.warehouse,
      COALESCE(li.onhand_qty, 0) AS onhand_qty,
      COALESCE(d14.demand_14d, 0) AS demand_14d,
      COALESCE(d30.demand_30d, 0) AS demand_30d,
      CASE WHEN COALESCE(d14.demand_14d, 0) > 0
        THEN ROUND(COALESCE(li.onhand_qty, 0) * 14.0 / NULLIF(d14.demand_14d, 0), 1) ELSE NULL END AS coverage_days
    FROM base_sku b
    LEFT JOIN latest_inv li ON b.sku = li.sku
    LEFT JOIN demand_14 d14 ON b.sku = d14.sku
    LEFT JOIN demand_30 d30 ON b.sku = d30.sku
    """
    hot_df = con.execute(hot_sql).fetchdf()
    if not hot_df.empty and not forecast_metrics_df.empty and "stockout_date_forecast" in forecast_metrics_df.columns:
        hot_df = hot_df.merge(
            forecast_metrics_df[["sku", "stockout_date_forecast"]].drop_duplicates("sku"),
            on="sku", how="left"
        )
    else:
        hot_df["stockout_date_forecast"] = pd.NaT
    lead_days_hot = st.session_state.get("lead_time_days", 7)
    shortage_days_hot = 14
    over_days_hot = 60
    issues = []
    for _, row in hot_df.iterrows():
        cov = row.get("coverage_days")
        onhand = int(row.get("onhand_qty", 0) or 0)
        d14 = float(row.get("demand_14d", 0) or 0)
        d30 = float(row.get("demand_30d", 0) or 0)
        stockout_d = row.get("stockout_date_forecast")
        issue_type = None
        severity = "Medium"
        rec_qty = None
        if cov is not None and cov < shortage_days_hot and (d14 > 0 or d30 > 0):
            issue_type = "품절 임박"
            severity = "Critical" if (cov is not None and cov < 7) else ("High" if (cov is not None and cov < 14) else "Medium")
            if d14 > 0:
                target = max(0, int(math.ceil(d14 / 14 * (lead_days_hot + shortage_days_hot))))
                rec_qty = max(0, target - onhand)
        elif d30 > 0 and cov is not None and cov < 21:
            p75_d30 = hot_df["demand_30d"].quantile(0.75) if len(hot_df) else 0
            if d30 >= p75_d30:
                issue_type = "고수요·저커버리지"
                severity = "High" if (cov is not None and cov < 14) else "Medium"
                if d14 > 0:
                    target = max(0, int(math.ceil(d14 / 14 * (lead_days_hot + shortage_days_hot))))
                    rec_qty = max(0, target - onhand)
        if issue_type is None and cov is not None and cov > over_days_hot:
            p25_d30 = hot_df["demand_30d"].quantile(0.25) if len(hot_df) else 0
            if d30 <= p25_d30 and d30 == d30:
                issue_type = "과잉·저회전"
                severity = "Medium"
        if issue_type is None and d30 == 0 and onhand > 0:
            issue_type = "무수요 재고"
            severity = "Medium"
        if issue_type is not None:
            issues.append({
                "SKU": row["sku"],
                "창고": row.get("warehouse") or "—",
                "이슈 유형": issue_type,
                "심각도": severity,
                "예상 품절일": fmt_date(stockout_d) if pd.notna(stockout_d) else "—",
                "권장 발주수량": rec_qty if rec_qty is not None else "—",
            })
    hot_issues_df = pd.DataFrame(issues)
    if not hot_issues_df.empty:
        sev_order = {"Critical": 0, "High": 1, "Medium": 2}
        hot_issues_df["_sev"] = hot_issues_df["심각도"].map(sev_order)
        hot_issues_df = hot_issues_df.sort_values(["_sev", "이슈 유형"]).drop(columns=["_sev"]).head(5)
        st.dataframe(hot_issues_df, use_container_width=True, hide_index=True)
    else:
        st.caption("현재 필터에서 우선 점검 이슈가 없습니다.")

    st.divider()
    st.markdown("#### 2. 미래 전망 (예측 기준)")
    if not forecast_daily.empty:
        mape_pct, mape_n = compute_mape_backtest(demand, lookback_days, model_type, str(latest_date), 14)
        if mape_pct is not None:
            if mape_pct < 20:
                confidence_hint = "높음"
            elif mape_pct < 40:
                confidence_hint = "보통"
            else:
                confidence_hint = "낮음"
        else:
            confidence_hint = "—"
        with st.expander("모델 설명"):
            st.write("**MovingAvg(N):** 최근 N일 수요 평균으로 일별 예측. 단순·안정적.")
            st.write("**SeasonalNaive(N):** 최근 N일 패턴을 일별로 반복. 주간 계절성에 적합.")
            st.write(f"**학습 구간:** 최근 {lookback_days}일. **마지막 사용일:** {fmt_date(latest_date)}.")
        forecast_total = int(forecast_daily["forecast_qty"].sum())
        latest_dt = pd.to_datetime(latest_date)
        f7_cut = latest_dt + pd.Timedelta(days=7)
        lead_days = st.session_state.get("lead_time_days", 7)
        lead_cut = latest_dt + pd.Timedelta(days=lead_days)
        f_daily = forecast_daily.copy()
        f_daily["date"] = pd.to_datetime(f_daily["date"])
        forecast_next7 = int(f_daily[f_daily["date"] <= f7_cut]["forecast_qty"].sum())
        lead_time_total = int(f_daily[f_daily["date"] <= lead_cut]["forecast_qty"].sum())
        horizon_cut = latest_dt + pd.Timedelta(days=horizon_days)
        risk_in_horizon = 0
        if not forecast_metrics_df.empty and "stockout_date_forecast" in forecast_metrics_df.columns:
            fm = forecast_metrics_df[forecast_metrics_df["stockout_date_forecast"].notna()].copy()
            fm["stockout_date_forecast"] = pd.to_datetime(fm["stockout_date_forecast"])
            risk_in_horizon = int((fm["stockout_date_forecast"] <= horizon_cut).sum())
        col_f1, col_f2, col_f3 = st.columns(3)
        col_f1.metric(f"향후 {horizon_days}일 예상 수요 합계", fmt_qty(forecast_total))
        col_f2.metric(
            f"향후 {lead_days}일 예상 수요" + (" (리드타임 구간)" if lead_days != 7 else ""),
            fmt_qty(lead_time_total if lead_days != 7 else forecast_next7),
        )
        col_f3.metric(f"예측 기준 품절 위험 SKU 수 (향후 {horizon_days}일 이내)", fmt_qty(risk_in_horizon))
        if mape_pct is not None:
            st.caption(f"**MAPE(평균절대백분율오차)** 최근 14일 백테스트: {mape_pct:.1f}% (n={mape_n}). 참고용이며, **예측 신뢰도:** {confidence_hint} (20% 미만=높음, 20–40%=보통, 40% 초과=낮음).")
    else:
        st.caption("예측 데이터가 없습니다. 사이드바에서 예측 설정·조회 조건을 확인하세요.")

    st.divider()
    st.markdown("#### 3. 수요·재고 추이")
    show_forecast_overlay = st.toggle("예측선 표시", value=True, key="ov_show_forecast_overlay")
    trend_sql = f"""
    {_base_sku_cte(base_where)}
    SELECT d.date, SUM(d.demand_qty) AS demand_qty
    FROM demand_daily d
    JOIN base_sku b ON d.sku = b.sku
    WHERE d.date >= '{latest_date}'::DATE - INTERVAL {trend_days} DAY
    GROUP BY d.date
    ORDER BY d.date
    """
    trend = con.execute(trend_sql).fetchdf()
    inv_trend_sql = f"""
    {_base_sku_cte(base_where)}
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
        fig_trend.update_traces(name="실적 수요")
        if show_forecast_overlay and not forecast_daily.empty:
            forecast_agg = forecast_daily.groupby("date")["forecast_qty"].sum().reset_index()
            fig_trend.add_scatter(
                x=forecast_agg["date"],
                y=forecast_agg["forecast_qty"],
                name="예측 수요",
                line=dict(dash="dash", color="orange"),
                mode="lines",
            )
            latest_dt_trend = pd.to_datetime(latest_date)
            horizon_end = latest_dt_trend + pd.Timedelta(days=horizon_days)
            add_ref_vrect(fig_trend, latest_dt_trend, horizon_end, label="Forecast horizon", fill_color="rgba(255,165,0,0.15)", line_color="orange")
        add_ref_vline(fig_trend, latest_date, "기준일", line_dash="dot", line_color="gray")
        fig_trend.update_layout(xaxis_title="일자", yaxis_title="수요량", legend_title=None)
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

    st.divider()
    st.markdown("#### 4. 카테고리별 비중")
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
        total_inv = cat_inv["onhand_qty"].sum()
        cat_inv = cat_inv.assign(pct=(cat_inv["onhand_qty"] / total_inv * 100))
        top5_inv = cat_inv.head(5)
        others_inv = cat_inv.iloc[5:]
        if len(others_inv) > 0:
            others_row = pd.DataFrame([{"category": "Others", "onhand_qty": others_inv["onhand_qty"].sum(), "pct": others_inv["pct"].sum()}])
            cat_inv_display = pd.concat([top5_inv, others_row], ignore_index=True)
        else:
            cat_inv_display = top5_inv.copy()
        cat_inv_display["category_ko"] = cat_inv_display["category"].map(lambda x: category_map.get(x, x))
        cat_inv_display["label"] = cat_inv_display.apply(lambda r: f"{r['category_ko']} ({fmt_pct(r['pct'])})", axis=1)
        fig_cat_inv = px.bar(
            cat_inv_display,
            x="onhand_qty",
            y="label",
            orientation="h",
            title="카테고리별 재고 비중 (기준일) — 수량·%",
            labels={"onhand_qty": "재고 수량", "label": "카테고리"},
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
            total_demand = cat_demand["demand_qty"].sum()
            cat_demand = cat_demand.assign(pct=(cat_demand["demand_qty"] / total_demand * 100))
            top5_demand = cat_demand.head(5)
            others_demand = cat_demand.iloc[5:]
            if len(others_demand) > 0:
                others_row_d = pd.DataFrame([{"category": "Others", "demand_qty": others_demand["demand_qty"].sum(), "pct": others_demand["pct"].sum()}])
                cat_demand_display = pd.concat([top5_demand, others_row_d], ignore_index=True)
            else:
                cat_demand_display = top5_demand.copy()
            cat_demand_display["category_ko"] = cat_demand_display["category"].map(lambda x: category_map.get(x, x))
            cat_demand_display["label"] = cat_demand_display.apply(lambda r: f"{r['category_ko']} ({fmt_pct(r['pct'])})", axis=1)
            fig_cat_demand = px.bar(
                cat_demand_display,
                x="demand_qty",
                y="label",
                orientation="h",
                title="카테고리별 수요 비중 (최근 30일) — 수량·%",
                labels={"demand_qty": "수요량", "label": "카테고리"},
            )
            fig_cat_demand.update_layout(yaxis={"categoryorder": "total ascending"}, bargap=0.6)
            fig_cat_demand.update_xaxes(tickformat=",.0f")
            fig_cat_demand = apply_plotly_theme(fig_cat_demand)
            st.plotly_chart(fig_cat_demand, use_container_width=True)
    else:
        st.caption("카테고리별 수요 비중: 전체 카테고리·전체 SKU 선택 시에만 표시됩니다.")

    st.divider()
    st.markdown("#### 5. 우선 조치 상위 10건")
    if not forecast_metrics_df.empty and not forecast_daily.empty:
        lead_time_days_ov = st.session_state.get("lead_time_days", 7)
        target_cover_days_ov = st.session_state.get("target_cover_days", 14)
        safety_stock_days_ov = st.session_state.get("safety_stock_days", 3)
        moq_ov = st.session_state.get("moq", 0)
        actions_sql_ov = f"""
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
        base AS (
          SELECT b.sku, b.sku_name, b.category, li.warehouse, COALESCE(li.onhand_qty, 0) AS onhand_qty
          FROM base_sku b
          LEFT JOIN latest_inv li ON b.sku = li.sku
        )
        SELECT sku, sku_name, category, warehouse, onhand_qty FROM base
        """
        actions_base_ov = con.execute(actions_sql_ov).fetchdf()
        latest_dt_ov = pd.to_datetime(latest_date)
        lead_cut_ov = latest_dt_ov + pd.Timedelta(days=lead_time_days_ov)
        f_daily_ov = forecast_daily.copy()
        f_daily_ov["date"] = pd.to_datetime(f_daily_ov["date"])
        f_lead_ov = f_daily_ov[f_daily_ov["date"] <= lead_cut_ov].groupby("sku")["forecast_qty"].sum()
        f_metrics_ov = forecast_metrics_df[["sku", "forecast_avg_daily", "forecast_dos", "stockout_date_forecast"]].drop_duplicates("sku")
        actions_base_ov = actions_base_ov.merge(f_metrics_ov, on="sku", how="inner")
        actions_base_ov["lead_time_forecast"] = actions_base_ov["sku"].map(lambda s: f_lead_ov.get(s, 0) if s in f_lead_ov.index else 0)
        fa_ov = actions_base_ov["forecast_avg_daily"].fillna(0)
        lt_f_ov = actions_base_ov["lead_time_forecast"].fillna(0)
        target_stock_ov = (lt_f_ov + fa_ov * target_cover_days_ov + fa_ov * safety_stock_days_ov).round(0).astype(int)
        onhand_ov = pd.to_numeric(actions_base_ov["onhand_qty"], errors="coerce").fillna(0)
        rec_ov = (target_stock_ov - onhand_ov).clip(lower=0).astype(int)
        if moq_ov > 0:
            rec_ov = rec_ov.where(rec_ov <= 0, rec_ov.clip(lower=moq_ov)).astype(int)
        actions_base_ov["target_stock"] = target_stock_ov
        actions_base_ov["recommended_order_qty"] = rec_ov
        actions_base_ov["estimated_stockout_date"] = actions_base_ov["stockout_date_forecast"]
        actions_base_ov["coverage_days"] = actions_base_ov["forecast_dos"]
        def reason_ov(row):
            if pd.notna(row["coverage_days"]) and row["coverage_days"] < target_cover_days_ov:
                return "예측 품절 임박"
            if row["onhand_qty"] < row["target_stock"]:
                return "리드타임 수요 대비 부족"
            return "정책 보충"
        actions_base_ov["reason"] = actions_base_ov.apply(reason_ov, axis=1)
        top10 = actions_base_ov[actions_base_ov["recommended_order_qty"] > 0].copy()
        top10 = top10.sort_values(["estimated_stockout_date", "recommended_order_qty"], ascending=[True, False], na_position="last").head(10)
        display_top10 = top10[["sku", "sku_name", "warehouse", "estimated_stockout_date", "coverage_days", "recommended_order_qty", "reason"]].copy()
        display_top10 = display_top10.rename(columns={"estimated_stockout_date": "예상 품절일(예측)", "coverage_days": "DOS(예측)"})
        if display_top10.empty:
            st.caption("권장 발주 대상이 없습니다.")
        else:
            st.dataframe(display_top10, use_container_width=True)
            st.caption("전체 목록·정책 변경은 **발주·조치** 탭에서 확인하세요.")
    else:
        st.caption("예측 결과가 있을 때만 표시됩니다. 예측 설정 적용 후 새로고침하세요.")

with tab_health:
    st.subheader("재고 적정성")
    st.caption("부족·적정·과잉 구간별 SKU 수와 DOS 분포. 품절 위험·발주·조치 탭으로 이어서 조치하세요.")
    if summary_stockout_cnt > 0:
        st.info("**바로 할 일:** 커버리지 부족 SKU가 있습니다. **품절 위험**·**발주·조치** 탭에서 발주 검토하세요.")
    st.markdown("**기준 설정**")
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

    st.markdown("**구간별 SKU 수**")
    row_c1, row_c2, row_hist = st.columns([1, 1, 2])
    with row_c1:
        st.metric("부족", f"{cnt_short:,}건")
        st.metric("적정", f"{cnt_ok:,}건")
    with row_c2:
        st.metric("과잉", f"{cnt_over:,}건")
        st.metric(f"수요 없음(최근 {health_dos_basis_days}일)", f"{cnt_nodemand:,}건")
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
            add_ref_vline(fig_hist, shortage_days, f"부족 기준선({shortage_days}일)", line_dash="dash", line_color="crimson")
            add_ref_vline(fig_hist, over_days, f"과잉 기준선({over_days}일)", line_dash="dash", line_color="steelblue")
            fig_hist = apply_plotly_theme(fig_hist)
            st.plotly_chart(fig_hist, use_container_width=True)
            st.caption(f"부족 비율 {pct_short:.1f}% (기준선 {shortage_days}일 미만) · 과잉 비율 {pct_over:.1f}% (기준선 {over_days}일 초과)")
        else:
            st.caption(f"DOS 데이터 없음. 최근 {health_dos_basis_days}일 수요 0이거나 필터 결과 없음.")

    # 우선순위 매트릭스(고수요×부족): X=demand_30d(80% 분위), Y=DOS(shortage_days 기준선), 수요 없음 구간 제외
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
        add_ref_hline(fig_scatter, y_cut, f"부족 기준({shortage_days}일)", line_dash="dash", line_color="gray")
        add_ref_vline(fig_scatter, x_cut, "80% 분위", line_dash="dash", line_color="gray")
        fig_scatter = apply_plotly_theme(fig_scatter)
        st.plotly_chart(fig_scatter, use_container_width=True)
        st.caption(
            "**우하** 고수요·저DOS → 최우선 발주 | **좌하** 저수요·저DOS → 주문주기 검토 | "
            "**우상** 고수요·고DOS → 적정 | **좌상** 저수요·고DOS → 과잉·재고 조정."
        )
    else:
        st.caption("DOS가 있는 SKU가 없어 산점도를 표시하지 않습니다.")

    st.divider()
    st.markdown("**구간별 상세 목록**")
    bucket_order = {"부족": 0, "과잉": 1, "적정": 2, "수요0": 3}
    health["_bucket_order"] = health["bucket"].map(bucket_order)
    _nodemand_label = f"최근 {health_dos_basis_days}일 수요 없음(0)"
    bucket_options = ["부족", "과잉", "적정", "수요0"]
    default_buckets = ["부족", "과잉"] if show_only_exceptions else bucket_options
    selected_buckets = st.multiselect(
        "구간",
        options=bucket_options,
        default=default_buckets,
        format_func=lambda x: _nodemand_label if x == "수요0" else x,
        key="health_bucket_filter",
    )
    if not selected_buckets:
        selected_buckets = bucket_options
    display_health = health[health["bucket"].isin(selected_buckets)].copy()
    if (sku_search_term or "").strip():
        term = (sku_search_term or "").strip().lower()
        display_health = display_health[
            display_health["sku"].astype(str).str.lower().str.contains(term, na=False)
            | display_health["sku_name"].astype(str).str.lower().str.contains(term, na=False)
        ]
    display_health = display_health.sort_values(["_bucket_order", "coverage_days"], ascending=[True, True], na_position="last")
    display_health = display_health[
        ["sku", "sku_name", "category", "warehouse", "onhand_qty", "demand_30d", "avg_daily_demand_Nd", "coverage_days", "bucket"]
    ].drop(columns=["_bucket_order"], errors="ignore")
    display_health["bucket"] = display_health["bucket"].replace("수요0", _nodemand_label)
    # Formatted copy for display (qty comma, days 1 decimal)
    display_health_fmt = display_health.copy()
    display_health_fmt["onhand_qty"] = display_health_fmt["onhand_qty"].apply(lambda v: fmt_qty(v))
    display_health_fmt["demand_30d"] = display_health_fmt["demand_30d"].apply(lambda v: fmt_qty(v))
    display_health_fmt["avg_daily_demand_Nd"] = display_health_fmt["avg_daily_demand_Nd"].apply(lambda v: fmt_qty(v))
    display_health_fmt["coverage_days"] = display_health_fmt["coverage_days"].apply(lambda v: fmt_days(v) if pd.notna(v) else "—")
    st.dataframe(display_health_fmt, use_container_width=True, hide_index=True)
    st.download_button("재고 적정성 목록 내려받기 (CSV)", data=display_health.to_csv(index=False).encode("utf-8-sig"), file_name="health_list.csv", mime="text/csv", key="dl_health")

with tab_stockout:
    st.subheader("품절 위험")
    st.caption("N일 이내 품절 예상 SKU·예상 품절일·위험등급. Critical부터 확인한 뒤 **발주·조치** 탭에서 권장 발주수량 확인하세요.")
    if summary_stockout_cnt > 0:
        st.info("**바로 할 일:** 아래 목록에서 Critical SKU를 확인한 뒤 **발주·조치** 탭에서 권장 발주수량을 확인하세요.")
    st.markdown("**조회 조건**")
    risk_basis_opts = ["실적 기준(14일 평균)", "예측 기준(horizon)"]
    risk_basis = st.selectbox(
        "품절 산정 기준",
        options=risk_basis_opts,
        index=risk_basis_opts.index(st.session_state.get("risk_basis", "실적 기준(14일 평균)")) if st.session_state.get("risk_basis", "실적 기준(14일 평균)") in risk_basis_opts else 0,
        key="risk_basis",
    )
    stockout_within_opts = [7, 14, 21, 30, 60]
    stockout_within_days = st.selectbox(
        "품절 위험 기준(N일 미만)",
        options=stockout_within_opts,
        index=stockout_within_opts.index(st.session_state.get("risk_stockout_within_days", 14)) if st.session_state.get("risk_stockout_within_days", 14) in stockout_within_opts else 1,
        format_func=lambda x: f"{x}일 미만",
        key="risk_stockout_within_days",
    )
    risk_level_filter = st.selectbox(
        "위험등급",
        options=["전체", "Critical", "High", "Medium", "Low"],
        key="risk_level_filter",
    )

    # 품절 리스크 SQL (탭 내 실행, 과거 14일 기준)
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

    use_forecast_risk = risk_basis == "예측 기준(horizon)" and not forecast_metrics_df.empty
    if use_forecast_risk:
        sku_info = risk[["sku", "sku_name", "category", "warehouse", "onhand_qty"]].drop_duplicates("sku")
        risk_f = forecast_metrics_df.merge(sku_info, on="sku", how="inner", suffixes=("", "_y"))
        risk_f = risk_f[[c for c in risk_f.columns if not c.endswith("_y")]]
        risk_f = risk_f.rename(columns={
            "forecast_dos": "coverage_days",
            "stockout_date_forecast": "estimated_stockout_date",
            "forecast_avg_daily": "avg_daily_demand_14d",
            "forecast_demand_next7": "demand_7d",
        })
        risk_f["risk_level"] = risk_f["coverage_days"].apply(assign_risk_level)
        risk_f["priority_score"] = risk_f.apply(
            lambda r: (r["demand_7d"] or 0) / max((r["coverage_days"] or 1), 1), axis=1
        )
        risk = risk_f

    risk_filtered = risk[
        (risk["coverage_days"].notna()) & (risk["coverage_days"] < stockout_within_days)
    ].copy()
    if risk_level_filter != "전체":
        risk_filtered = risk_filtered[risk_filtered["risk_level"] == risk_level_filter]

    # KPI 3개: 리스크 SKU 수, 가장 빠른 예상 품절일, 리스크 수요(최근 7일/예측 7일 합)
    risk_sku_cnt = len(risk_filtered)
    earliest_stockout = risk_filtered["estimated_stockout_date"].min() if not risk_filtered.empty and risk_filtered["estimated_stockout_date"].notna().any() else None
    demand_col = "demand_7d"
    risk_demand_7d = int(risk_filtered[demand_col].sum()) if not risk_filtered.empty and demand_col in risk_filtered.columns else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("품절 위험 SKU 수", fmt_qty(risk_sku_cnt))
    col2.metric("가장 빠른 예상 품절일" + (" (예측 기준)" if use_forecast_risk else ""), fmt_date(earliest_stockout) if earliest_stockout is not None and pd.notna(earliest_stockout) else "—")
    col3.metric("위험 구간 수요(7일)" + (" 예측" if use_forecast_risk else " 실적"), fmt_qty(risk_demand_7d))

    st.markdown("**품절 위험 SKU 목록**" + (" (예측 기준)" if use_forecast_risk else ""))
    risk_for_table = risk_filtered if show_only_exceptions else risk
    if risk_level_filter != "전체" and show_only_exceptions:
        risk_for_table = risk_for_table[risk_for_table["risk_level"] == risk_level_filter]
    display_cols = [
        "sku", "sku_name", "warehouse",
        "coverage_days", "estimated_stockout_date",
        "onhand_qty", "avg_daily_demand_14d", "demand_7d",
        "risk_level", "priority_score",
    ]
    display_cols = [c for c in display_cols if c in risk_for_table.columns]
    display_risk = risk_for_table[display_cols].copy()
    if (sku_search_term or "").strip():
        term = (sku_search_term or "").strip().lower()
        display_risk = display_risk[
            display_risk["sku"].astype(str).str.lower().str.contains(term, na=False)
            | display_risk["sku_name"].astype(str).str.lower().str.contains(term, na=False)
        ]
    display_risk = display_risk.rename(columns={
        "coverage_days": "커버리지(DOS)",
        "estimated_stockout_date": "예상 품절일",
        "avg_daily_demand_14d": "avg_daily_demand",
    })
    sort_options = ["예상 품절일 빠른 순", "우선순위 점수 높은 순", "커버리지(DOS) 짧은 순", "SKU"]
    risk_sort = st.selectbox("정렬 기준", options=sort_options, index=0, key="risk_sort_by")
    if risk_sort == "예상 품절일 빠른 순":
        sort_col = "예상 품절일"
        if sort_col in display_risk.columns:
            display_risk = display_risk.sort_values([sort_col, "priority_score"], ascending=[True, False], na_position="last")
    elif risk_sort == "우선순위 점수 높은 순" and "priority_score" in display_risk.columns:
        display_risk = display_risk.sort_values("priority_score", ascending=False, na_position="last")
    elif risk_sort == "커버리지(DOS) 짧은 순" and "커버리지(DOS)" in display_risk.columns:
        display_risk = display_risk.sort_values("커버리지(DOS)", ascending=True, na_position="last")
    elif risk_sort == "SKU":
        display_risk = display_risk.sort_values("sku", ascending=True)
    date_col = "예상 품절일"
    display_risk_fmt = display_risk.copy()
    for qty_col in ["onhand_qty", "avg_daily_demand", "demand_7d"]:
        if qty_col in display_risk_fmt.columns:
            display_risk_fmt[qty_col] = display_risk_fmt[qty_col].apply(lambda v: fmt_qty(v))
    if "커버리지(DOS)" in display_risk_fmt.columns:
        display_risk_fmt["커버리지(DOS)"] = display_risk_fmt["커버리지(DOS)"].apply(lambda v: fmt_days(v) if pd.notna(v) else "—")
    if date_col in display_risk_fmt.columns:
        display_risk_fmt[date_col] = display_risk_fmt[date_col].apply(lambda v: fmt_date(v) if pd.notna(v) else "—")
    st.dataframe(display_risk_fmt, use_container_width=True, hide_index=True)
    st.download_button("품절 위험 목록 내려받기 (CSV)", data=display_risk.to_csv(index=False).encode("utf-8-sig"), file_name="risk_list.csv", mime="text/csv", key="dl_risk")
    st.caption("우선순위 점수 = 7일 수요 ÷ 커버리지(DOS). " + ("예상 품절일 = 누적 예측 수요로 재고 소진되는 첫 날(예측 기준)." if use_forecast_risk else "예상 품절일 = 기준일 + DOS(올림)일(실적 기준)."))

with tab_actions:
    st.subheader("발주·조치")
    st.caption("정책(리드타임·목표 커버리지·안전재고·최소발주수량)에 따른 권장 발주수량. 예상 품절일 빠른 순으로 조치하세요.")
    if summary_stockout_cnt > 0:
        st.info("**바로 할 일:** 정렬 기준을 예상 품절일로 두고 Critical SKU부터 발주하세요.")
    st.markdown("**발주 기준**")
    actions_basis_opts = ["실적 기준(14일 평균)", "예측 기준(horizon)"]
    actions_basis = st.selectbox(
        "발주 산정 기준",
        options=actions_basis_opts,
        index=actions_basis_opts.index(st.session_state.get("actions_basis", "실적 기준(14일 평균)")) if st.session_state.get("actions_basis", "실적 기준(14일 평균)") in actions_basis_opts else 0,
        key="actions_basis",
    )
    use_forecast_actions = actions_basis == "예측 기준(horizon)" and not forecast_metrics_df.empty

    st.divider()
    st.markdown("**정책 파라미터**")
    col_lt, col_tc, col_ss, col_moq = st.columns(4)
    with col_lt:
        lead_time_days = st.number_input("리드타임(일)", min_value=0, value=7, step=1, key="lead_time_days")
    with col_tc:
        target_cover_days = st.number_input("목표 커버리지(일)", min_value=0, value=14, step=1, key="target_cover_days")
    with col_ss:
        safety_stock_days = st.number_input("안전재고(일)", min_value=0, value=3, step=1, key="safety_stock_days")
    with col_moq:
        moq = st.number_input("최소발주수량(MOQ, 0=미적용)", min_value=0, value=0, step=1, key="moq")

    # 발주 base SQL (탭 내 실행, 과거 14일 기준)
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

    # 예측 기반일 때: lead_time_forecast + (forecast_avg_daily * target_cover_days) + safety_stock_forecast
    if use_forecast_actions:
        latest_dt = pd.to_datetime(latest_date)
        lead_cut = latest_dt + pd.Timedelta(days=lead_time_days)
        f_daily = forecast_daily.copy()
        f_daily["date"] = pd.to_datetime(f_daily["date"])
        f_lead = f_daily[f_daily["date"] <= lead_cut].groupby("sku")["forecast_qty"].sum().reindex(actions_base["sku"]).fillna(0)
        f_metrics = forecast_metrics_df[["sku", "forecast_avg_daily", "forecast_dos", "stockout_date_forecast"]].drop_duplicates("sku")
        actions_base = actions_base.merge(f_metrics, on="sku", how="left", suffixes=("", "_f"))
        actions_base["lead_time_forecast"] = actions_base["sku"].map(lambda s: f_lead.get(s, 0) if s in f_lead.index else 0)
        fa = actions_base["forecast_avg_daily"].fillna(0)
        lt_f = actions_base["lead_time_forecast"].fillna(0)
        target_stock = (lt_f + fa * target_cover_days + fa * safety_stock_days).round(0).astype(int)
        onhand = pd.to_numeric(actions_base["onhand_qty"], errors="coerce").fillna(0)
        recommended_order_qty = (target_stock - onhand).clip(lower=0).astype(int)
        if moq > 0:
            recommended_order_qty = recommended_order_qty.where(recommended_order_qty <= 0, recommended_order_qty.clip(lower=moq)).astype(int)
        actions_base["target_stock"] = target_stock
        actions_base["recommended_order_qty"] = recommended_order_qty
        actions_base["coverage_days"] = actions_base["forecast_dos"]
        actions_base["estimated_stockout_date"] = actions_base["stockout_date_forecast"]
        actions_base["avg_daily_demand_14d"] = actions_base["forecast_avg_daily"]
        actions_base = actions_base.drop(columns=["forecast_avg_daily", "forecast_dos", "stockout_date_forecast", "lead_time_forecast"], errors="ignore")

    # reason MECE: 예측 시 "예측 품절 임박" / "리드타임 수요 대비 부족" / "정책 보충". 과거 시 기존 유지.
    def assign_reason_past(row):
        if pd.notna(row["coverage_days"]) and row["coverage_days"] < target_cover_days:
            return "즉시위험(DOS<목표커버)"
        if row["onhand_qty"] < row["target_stock"]:
            return "정책보충(ROP 미달)"
        return "기타"

    def assign_reason_forecast(row):
        if pd.notna(row["coverage_days"]) and row["coverage_days"] < target_cover_days:
            return "예측 품절 임박"
        if row["onhand_qty"] < row["target_stock"]:
            return "리드타임 수요 대비 부족"
        return "정책 보충"

    actions_base["reason"] = actions_base.apply(
        assign_reason_forecast if use_forecast_actions else assign_reason_past, axis=1
    )
    actions_display = actions_base[actions_base["recommended_order_qty"] > 0].copy()
    actions_display = actions_display.sort_values(
        ["estimated_stockout_date", "recommended_order_qty"],
        ascending=[True, False],
        na_position="last",
    )

    # reason 멀티셀렉트; when show_only_exceptions OFF, default to all reasons
    reason_options = (
        ["예측 품절 임박", "리드타임 수요 대비 부족", "정책 보충"]
        if use_forecast_actions
        else ["즉시위험(DOS<목표커버)", "정책보충(ROP 미달)", "기타"]
    )
    default_reasons = reason_options[:2] if show_only_exceptions else reason_options
    selected_reasons = st.multiselect(
        "추천 사유",
        options=reason_options,
        default=default_reasons,
        key="actions_reason_filter",
    )
    if not selected_reasons:
        selected_reasons = reason_options
    actions_filtered = actions_display[actions_display["reason"].isin(selected_reasons)].copy()
    if not show_only_exceptions:
        actions_for_table = actions_base[actions_base["reason"].isin(selected_reasons)].copy()
    else:
        actions_for_table = actions_filtered.copy()
    if (sku_search_term or "").strip():
        term = (sku_search_term or "").strip().lower()
        actions_for_table = actions_for_table[
            actions_for_table["sku"].astype(str).str.lower().str.contains(term, na=False)
            | actions_for_table["sku_name"].astype(str).str.lower().str.contains(term, na=False)
        ]

    st.divider()
    st.markdown("**권장 발주 목록**" + (" (예측 기준)" if use_forecast_actions else ""))
    sort_options_act = ["예상 품절일 빠른 순", "권장 발주수량 많은 순", "커버리지(DOS) 짧은 순", "SKU"]
    act_sort = st.selectbox("정렬 기준", options=sort_options_act, index=0, key="actions_sort_by")
    date_col_act = "estimated_stockout_date"
    if act_sort == "예상 품절일 빠른 순":
        actions_for_table = actions_for_table.sort_values([date_col_act, "recommended_order_qty"], ascending=[True, False], na_position="last")
    elif act_sort == "권장 발주수량 많은 순":
        actions_for_table = actions_for_table.sort_values("recommended_order_qty", ascending=False, na_position="last")
    elif act_sort == "커버리지(DOS) 짧은 순":
        actions_for_table = actions_for_table.sort_values("coverage_days", ascending=True, na_position="last")
    elif act_sort == "SKU":
        actions_for_table = actions_for_table.sort_values("sku", ascending=True)
    display_cols = ["sku", "sku_name", "category", "warehouse", "reason", "estimated_stockout_date", "onhand_qty", "avg_daily_demand_14d", "coverage_days", "target_stock", "recommended_order_qty"]
    display_cols = [c for c in display_cols if c in actions_for_table.columns]
    out = actions_for_table[display_cols].copy()
    out = out.rename(columns={
        "estimated_stockout_date": "예상 품절일",
        "coverage_days": "커버리지(DOS)",
    })
    out["onhand_qty"] = out["onhand_qty"].apply(lambda x: fmt_qty(x))
    out["avg_daily_demand_14d"] = out["avg_daily_demand_14d"].apply(lambda x: fmt_days(x) if pd.notna(x) else "—")
    out["커버리지(DOS)"] = out["커버리지(DOS)"].apply(lambda x: fmt_days(x) if pd.notna(x) else "—")
    out["target_stock"] = out["target_stock"].apply(lambda x: fmt_qty(x))
    out["recommended_order_qty"] = out["recommended_order_qty"].apply(lambda x: fmt_qty(x))
    date_col_out = "예상 품절일"
    if date_col_out in out.columns:
        out[date_col_out] = out[date_col_out].apply(lambda v: fmt_date(v) if pd.notna(v) else "—")
    st.dataframe(out, use_container_width=True, hide_index=True)
    st.download_button("발주·조치 목록 내려받기 (CSV)", data=actions_for_table.to_csv(index=False).encode("utf-8-sig"), file_name="actions_list.csv", mime="text/csv", key="dl_actions")
    st.caption("권장 발주수량이 0보다 큰 SKU만 표시(예외만 보기 ON 시). 정렬: 예상 품절일 빠른 순 → 권장 발주수량 많은 순.")

with tab_movements:
    st.subheader("입출고 추적")
    st.caption("기간 내 입고·출고·순증감과 거래 상세. 재고 변동 원인 파악·대량 거래 확인에 활용하세요.")
    if inv_txn is None or len(inv_txn) == 0:
        st.info("입출고 거래(inventory_txn) 데이터가 없습니다. CSV를 적재하면 입출고 차트와 거래 목록이 표시됩니다.")
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

        st.markdown("**집계 요약**")
        col_diag1, col_diag2, col_diag3, col_diag4 = st.columns(4)
        col_diag1.metric("건수", f"{txn_row_count:,}건")
        col_diag2.metric("입고 합계", f"{sum_in:,.0f}")
        col_diag3.metric("출고 합계", f"{sum_out:,.0f}")
        col_diag4.metric("순증감(입고−출고)", f"{sum_net:,.0f}")

        # 차트 3개: 입고 bar, 출고 bar, 순변화(net) line
        has_rows = not txn_trend.empty
        if not has_rows:
            st.warning("필터 조건에서 집계된 일자가 없습니다. 기간·창고·SKU·카테고리 필터를 완화하거나, 해당 기간에 거래가 있는지 확인하세요.")
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
            add_ref_hline(fig_net, 0, "0", line_dash="dash", line_color="gray")
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

        view_txn = st.radio("목록 보기", ["최신 200건", "수량 큰 순 상위 50건(주요 거래)"], horizontal=True, key="mov_view")
        if view_txn == "최신 200건":
            st.markdown("**거래 목록 (최신 200건)**")
            if txn_list.empty:
                st.caption("필터 조건에 맞는 거래가 없습니다.")
            else:
                st.dataframe(txn_list, use_container_width=True)
        else:
            st.markdown("**수량 큰 순 상위 50건 (주요 거래)**")
            if txn_top50.empty:
                st.caption("필터 조건에 맞는 거래가 없습니다.")
            else:
                st.dataframe(txn_top50, use_container_width=True)
        st.caption("일자 = 거래일 기준. 수량: 입고(+) / 출고(−).")
