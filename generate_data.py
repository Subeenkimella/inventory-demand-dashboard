import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)
start = datetime.today().date() - timedelta(days=89)
dates = pd.date_range(start=start, periods=90, freq="D")

skus = [f"SKU-{i:03d}" for i in range(1, 31)]
plants = ["PLANT-A", "PLANT-B"]
whs = ["WH-1", "WH-2"]
cats = ["Motor", "Brake", "Steering", "Sensor"]

demand_rows = []
for d in dates:
    for sku in skus:
        base = np.random.randint(5, 40)
        season = 1 + 0.2*np.sin((d.dayofyear % 30)/30 * 2*np.pi)
        demand = max(0, int(base * season + np.random.normal(0, 3)))
        demand_rows.append([d.date(), sku, np.random.choice(plants), np.random.choice(cats), demand])

demand = pd.DataFrame(demand_rows, columns=["date", "sku", "plant", "category", "demand_qty"])

inv_rows = []
last_stock = {sku: np.random.randint(200, 800) for sku in skus}
for d in dates:
    for sku in skus:
        stock = last_stock[sku]
        stock = max(0, stock - np.random.randint(0, 35) + np.random.randint(0, 45))
        last_stock[sku] = stock
        inv_rows.append([d.date(), sku, np.random.choice(whs), stock])

inventory = pd.DataFrame(inv_rows, columns=["date", "sku", "warehouse", "onhand_qty"])

master = pd.DataFrame({
    "sku": skus,
    "sku_name": [f"Part {i:03d}" for i in range(1, 31)],
    "category": [np.random.choice(cats) for _ in skus],
    "uom": ["EA"] * len(skus),
    "reorder_point": np.random.randint(80, 200, size=len(skus)),
})

master.to_csv("sku_master.csv", index=False)
demand.to_csv("demand_daily.csv", index=False)
inventory.to_csv("inventory_daily.csv", index=False)

print("Generated: sku_master.csv, demand_daily.csv, inventory_daily.csv")
