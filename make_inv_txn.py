"""Generate inventory_txn.csv using only stdlib. Run: python3 make_inv_txn.py"""
import csv
from datetime import datetime, timedelta
from collections import defaultdict

# Read inventory_daily to get date range and deltas
inv_rows = []
with open("inventory_daily.csv", newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        row["onhand_qty"] = int(row["onhand_qty"])
        inv_rows.append(row)

# Sort by sku, warehouse, date
inv_rows.sort(key=lambda x: (x["sku"], x["warehouse"], x["date"]))

# Build prev qty per (sku, warehouse)
prev = {}
deltas = []  # (date, sku, warehouse, delta)
for row in inv_rows:
    key = (row["sku"], row["warehouse"])
    qty = row["onhand_qty"]
    if key in prev:
        delta = qty - prev[key]
        if delta != 0:
            deltas.append((row["date"], row["sku"], row["warehouse"], delta))
    prev[key] = qty

# Date range: last 60 days of inventory_daily
dates = sorted(set(r["date"] for r in inv_rows))
date_max = dates[-1]
dt_max = datetime.strptime(date_max, "%Y-%m-%d").date()
dt_min_60 = dt_max - timedelta(days=59)
date_min_60 = dt_min_60.strftime("%Y-%m-%d")
dates_60 = [d for d in dates if date_min_60 <= d <= date_max]

# Read demand_daily for OUT alignment
demand_by_ds = defaultdict(int)
with open("demand_daily.csv", newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        if date_min_60 <= row["date"] <= date_max:
            demand_by_ds[(row["date"], row["sku"])] += int(row["demand_qty"])

# Read skus
skus = []
with open("sku_master.csv", newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    skus = [row["sku"] for row in r]
warehouses = ["WH-1", "WH-2"]

out_rows = []
ref_counter = [10000]

def emit(txn_date, sku, wh, txn_type, qty, reason):
    ref_counter[0] += 1
    rid = ref_counter[0]
    h, m = 8 + (rid % 10), rid % 60
    txn_dt = f"{txn_date} {h:02d}:{m:02d}:00"
    out_rows.append({
        "txn_datetime": txn_dt,
        "date": txn_date,
        "sku": sku,
        "warehouse": wh,
        "txn_type": txn_type,
        "qty": str(qty),
        "ref_id": f"REF-{rid}",
        "reason_code": reason,
    })

# Emit IN/OUT from deltas (only in 60-day range)
for txn_date, sku, wh, delta in deltas:
    if txn_date < date_min_60 or txn_date > date_max:
        continue
    if delta > 0:
        emit(txn_date, sku, wh, "IN", delta, "RCV")
    else:
        emit(txn_date, sku, wh, "OUT", delta, "SALE")

# Add OUT txns from demand (same date/sku)
for (d, sku), qty in demand_by_ds.items():
    if qty <= 0:
        continue
    for wh in warehouses:
        emit(d, sku, wh, "OUT", -min(qty, 30), "SALE")
    if len(out_rows) >= 600:
        break
if len(out_rows) >= 600:
    out_rows = out_rows[:600]

# Add a few ADJUST/RETURN/SCRAP
import random
random.seed(42)
for _ in range(60):
    d = dates_60[random.randint(0, len(dates_60) - 1)]
    sku = random.choice(skus)
    wh = random.choice(warehouses)
    t = random.choice([("ADJUST", 1), ("ADJUST", -1), ("RETURN", 1), ("SCRAP", -1)])
    emit(d, sku, wh, t[0], t[1] * (5 + random.randint(0, 10)), t[0][:3])

out_rows.sort(key=lambda x: (x["date"], x["txn_datetime"]))

with open("inventory_txn.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["txn_datetime", "date", "sku", "warehouse", "txn_type", "qty", "ref_id", "reason_code"])
    w.writeheader()
    w.writerows(out_rows)

print(f"Wrote inventory_txn.csv: {len(out_rows)} rows")
