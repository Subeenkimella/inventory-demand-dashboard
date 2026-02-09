"""
Generate inventory_txn.csv from inventory_daily and demand_daily.
Covers last 60 days of inventory_daily. IN aligns with inventory increase, OUT with demand.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

np.random.seed(43)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

inv = pd.read_csv("inventory_daily.csv", parse_dates=["date"])
demand = pd.read_csv("demand_daily.csv", parse_dates=["date"])
sku_master = pd.read_csv("sku_master.csv")

date_max = inv["date"].max()
date_min_60 = date_max - pd.Timedelta(days=59)
inv_range = inv[(inv["date"] >= date_min_60) & (inv["date"] <= date_max)]
demand_range = demand[(demand["date"] >= date_min_60) & (demand["date"] <= date_max)]

skus = sku_master["sku"].tolist()
warehouses = ["WH-1", "WH-2"]
txn_types_all = ["IN", "OUT", "TRANSFER_IN", "TRANSFER_OUT", "ADJUST", "RETURN", "SCRAP"]
reason_codes = ["RCV", "SALE", "XFER", "ADJ", "RTV", "SCRAP", "CYCLE"]

rows = []
ref_id = 10000

# Day-over-day inventory delta per (date, sku, warehouse) for IN/OUT
inv_sorted = inv_range.sort_values(["sku", "warehouse", "date"])
inv_sorted["prev_qty"] = inv_sorted.groupby(["sku", "warehouse"])["onhand_qty"].shift(1)
inv_sorted["delta"] = inv_sorted["onhand_qty"] - inv_sorted["prev_qty"].fillna(inv_sorted["onhand_qty"])

demand_by_date_sku = demand_range.groupby(["date", "sku"])["demand_qty"].sum().reset_index()
demand_by_date_sku.columns = ["date", "sku", "demand_qty"]

for _, r in inv_sorted.iterrows():
    if pd.isna(r["prev_qty"]):
        continue
    d, sku, wh, qty, prev, delta = r["date"], r["sku"], r["warehouse"], r["onhand_qty"], r["prev_qty"], r["delta"]
    n_txns = np.random.randint(1, 4)
    if delta > 0:
        # IN: split positive delta into IN txns
        remain = int(delta)
        for _ in range(n_txns):
            if remain <= 0:
                break
            amt = min(remain, np.random.randint(5, max(6, remain // 2 + 1)))
            amt = max(1, amt)
            remain -= amt
            t = d + pd.Timedelta(hours=np.random.randint(8, 18), minutes=np.random.randint(0, 60))
            rows.append({
                "txn_datetime": t.strftime("%Y-%m-%d %H:%M:%S"),
                "date": d.strftime("%Y-%m-%d"),
                "sku": sku,
                "warehouse": wh,
                "txn_type": "IN",
                "qty": amt,
                "ref_id": f"REF-{ref_id}",
                "reason_code": "RCV",
            })
            ref_id += 1
    elif delta < 0:
        # OUT: split negative delta into OUT txns (qty stored as negative per spec)
        remain = int(-delta)
        for _ in range(n_txns):
            if remain <= 0:
                break
            amt = min(remain, np.random.randint(5, max(6, remain // 2 + 1)))
            amt = max(1, amt)
            remain -= amt
            t = d + pd.Timedelta(hours=np.random.randint(8, 18), minutes=np.random.randint(0, 60))
            rows.append({
                "txn_datetime": t.strftime("%Y-%m-%d %H:%M:%S"),
                "date": d.strftime("%Y-%m-%d"),
                "sku": sku,
                "warehouse": wh,
                "txn_type": "OUT",
                "qty": -amt,
                "ref_id": f"REF-{ref_id}",
                "reason_code": "SALE",
            })
            ref_id += 1

# Add extra OUT txns aligned with demand (same date/sku, spread across warehouses)
for _, r in demand_by_date_sku.iterrows():
    if r["demand_qty"] <= 0:
        continue
    d, sku, dmd = r["date"], r["sku"], int(r["demand_qty"])
    if sku not in skus:
        continue
    whs = inv_range[(inv_range["date"] == d) & (inv_range["sku"] == sku)]["warehouse"].tolist()
    if not whs:
        whs = list(np.random.choice(warehouses, size=1))
    out_per_wh = max(1, dmd // len(whs))
    for wh in whs[:2]:
        amt = min(out_per_wh + np.random.randint(0, 5), dmd)
        amt = max(1, amt)
        t = d + pd.Timedelta(hours=np.random.randint(9, 17), minutes=np.random.randint(0, 60))
        rows.append({
            "txn_datetime": t.strftime("%Y-%m-%d %H:%M:%S"),
            "date": d.strftime("%Y-%m-%d"),
            "sku": sku,
            "warehouse": wh,
            "txn_type": "OUT",
            "qty": -amt,
            "ref_id": f"REF-{ref_id}",
            "reason_code": "SALE",
        })
        ref_id += 1
        if len(rows) >= 800:
            break
    if len(rows) >= 800:
        break

# Add some ADJUST / RETURN / SCRAP for variety (minor volume)
for _ in range(80):
    d = date_min_60 + pd.Timedelta(days=np.random.randint(0, 60))
    sku = np.random.choice(skus)
    wh = np.random.choice(warehouses)
    txn_type = np.random.choice(["ADJUST", "RETURN", "SCRAP"])
    qty = np.random.randint(1, 15)
    if txn_type == "ADJUST":
        qty = qty if np.random.rand() > 0.5 else -qty
    elif txn_type == "RETURN":
        qty = qty  # positive
    else:
        qty = -qty
    t = d + pd.Timedelta(hours=np.random.randint(8, 18), minutes=np.random.randint(0, 60))
    rows.append({
        "txn_datetime": t.strftime("%Y-%m-%d %H:%M:%S"),
        "date": d.strftime("%Y-%m-%d"),
        "sku": sku,
        "warehouse": wh,
        "txn_type": txn_type,
        "qty": qty,
        "ref_id": f"REF-{ref_id}",
        "reason_code": reason_codes[txn_types_all.index(txn_type)] if txn_type in txn_types_all else "ADJ",
    })
    ref_id += 1

df = pd.DataFrame(rows)
df = df.sort_values(["date", "txn_datetime"]).reset_index(drop=True)
df.to_csv("inventory_txn.csv", index=False)
print(f"Generated inventory_txn.csv: {len(df)} rows, date range {df['date'].min()} ~ {df['date'].max()}")
