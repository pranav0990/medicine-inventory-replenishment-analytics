"""
Medicine Inventory and Replenishment Analytics — deterministic data generator.

This script creates clean baseline data plus separate controlled quality-test data.
It does NOT perform data cleaning, deduplication, quarantine, Delta MERGE, or
business calculations; those belong in the Databricks Silver/Gold notebooks.

Run:
    pip install pandas
    python inventory_data_generator_corrected.py

Output:
    generated_data/
"""

from pathlib import Path
import random
import pandas as pd
from datetime import datetime, timedelta

# ------------------------------ Configuration ------------------------------
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

NUM_PRODUCTS = 50
NUM_SUPPLIERS = 10
NUM_WAREHOUSES = 5
NUM_MOVEMENTS = 10_000
NUM_DEMAND_DAYS = 14
DEMAND_ROWS_PER_DAY = 200
BASE_DATE = datetime(2026, 9, 30)  # fixed date for reproducibility
OUTPUT_DIR = Path("generated_data")

# ------------------------------ Reference data -----------------------------
suppliers = pd.DataFrame([
    {
        "supplier_id": f"S{i:03d}",
        "supplier_name": f"Pharma Supplier {i:02d}",
        "supplier_status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]),
    }
    for i in range(1, NUM_SUPPLIERS + 1)
])

warehouses = pd.DataFrame([
    {"warehouse_id": f"W{i:03d}", "warehouse_name": f"Warehouse {i:02d}"}
    for i in range(1, NUM_WAREHOUSES + 1)
])

categories = [
    "Analgesic", "Antibiotic", "Antihistamine", "Antiviral", "Cardiovascular"
]
product_names = [
    "Paracetamol", "Amoxicillin", "Cetirizine", "Azithromycin", "Ibuprofen",
    "Metformin", "Amlodipine", "Omeprazole", "Ciprofloxacin", "Pantoprazole"
]

# Create supplier-product relationships first so every product can include a
# valid preferred supplier_id, as required by the product reference schema.
supplier_product_rows = []
product_rows = []
supplier_ids = suppliers["supplier_id"].tolist()

for i in range(1, NUM_PRODUCTS + 1):
    product_id = f"P{i:03d}"
    selected_suppliers = random.sample(supplier_ids, random.randint(1, 3))
    preferred_supplier = random.choice(selected_suppliers)

    for supplier_id in selected_suppliers:
        supplier_product_rows.append({
            "supplier_id": supplier_id,
            "product_id": product_id,
            "lead_time_days": random.randint(3, 14),
            "unit_cost": round(random.uniform(5, 100), 2),
            "minimum_order_quantity": random.choice([10, 20, 50, 100]),
            "is_preferred": supplier_id == preferred_supplier,
        })

    product_rows.append({
        "product_id": product_id,
        "product_name": (
            f"{random.choice(product_names)} {random.choice([250, 500, 650])}mg"
        ),
        "category": random.choice(categories),
        "supplier_id": preferred_supplier,
    })

products = pd.DataFrame(product_rows)
supplier_product = pd.DataFrame(supplier_product_rows)
supplier_lead_times = supplier_product[
    ["supplier_id", "product_id", "lead_time_days"]
].copy()

# ------------------------------ Opening inventory --------------------------
opening_rows = []
batch_counter = 1

for row in products.to_dict("records"):
    for warehouse_id in random.sample(
        warehouses["warehouse_id"].tolist(), random.randint(1, 2)
    ):
        opening_rows.append({
            "warehouse_id": warehouse_id,
            "product_id": row["product_id"],
            "batch_id": f"B{batch_counter:04d}",
            "opening_quantity": random.randint(50, 500),
            "expiry_date": (
                BASE_DATE + timedelta(days=random.randint(30, 365))
            ).date().isoformat(),
        })
        batch_counter += 1

opening_inventory = pd.DataFrame(opening_rows)
inventory_records = opening_inventory.to_dict("records")
inventory_by_key = {
    (r["warehouse_id"], r["product_id"], r["batch_id"]): int(r["opening_quantity"])
    for r in inventory_records
}

# ------------------------------ Clean baseline movements -------------------
# This baseline is intentionally valid. Invalid examples are isolated in
# quality_test_batch.csv so they do not pollute the normal source data.
stock_movements_rows = []
running_stock = inventory_by_key.copy()
movement_keys = list(inventory_by_key.keys())

for i in range(1, NUM_MOVEMENTS + 1):
    key = random.choice(movement_keys)
    available = running_stock[key]

    if available < 30:
        movement_type = "RECEIPT"
    else:
        movement_type = random.choices(["ISSUE", "RECEIPT"], weights=[70, 30])[0]

    if movement_type == "ISSUE":
        quantity = random.randint(1, min(30, available))
        running_stock[key] -= quantity
    else:
        quantity = random.randint(10, 50)
        running_stock[key] += quantity

    # Events are intentionally generated in chronological order. The business
    # logic must still sort/process by event_time rather than file row order.
    event_time = BASE_DATE - timedelta(days=random.randint(0, 30),
                                       hours=random.randint(0, 23),
                                       minutes=random.randint(0, 59))
    stock_movements_rows.append({
        "movement_id": f"M{i:06d}",
        "warehouse_id": key[0],
        "product_id": key[1],
        "batch_id": key[2],
        "movement_type": movement_type,
        "quantity": quantity,
        "event_time": event_time.isoformat(),
    })

stock_movements = pd.DataFrame(stock_movements_rows)

# ------------------------------ Daily demand -------------------------------
demand_rows = []
inventory_by_product_warehouse = [
    (r["warehouse_id"], r["product_id"]) for r in inventory_records
]

for day in range(NUM_DEMAND_DAYS):
    demand_date = (BASE_DATE - timedelta(days=day)).date().isoformat()
    for _ in range(DEMAND_ROWS_PER_DAY):
        warehouse_id, product_id = random.choice(inventory_by_product_warehouse)
        demand_rows.append({
            "date": demand_date,
            "warehouse_id": warehouse_id,
            "product_id": product_id,
            "units_requested": random.randint(1, 50),
        })

daily_demand = pd.DataFrame(demand_rows)

# ------------------------------ Incremental batches ------------------------
# Valid records with unique IDs. Replaying the same file should not double
# count movements if the Silver layer implements idempotent movement_id logic.
incremental_batches = []
for batch_number in range(1, 4):
    batch_size = random.randint(100, 500)
    rows = []
    for i in range(1, batch_size + 1):
        key = random.choice(movement_keys)
        movement_type = random.choice(["ISSUE", "RECEIPT"])
        quantity = random.randint(1, 50)
        event_time = BASE_DATE + timedelta(
            days=batch_number,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        rows.append({
            "movement_id": f"INC{batch_number}_{i:04d}",
            "warehouse_id": key[0],
            "product_id": key[1],
            "batch_id": key[2],
            "movement_type": movement_type,
            "quantity": quantity,
            "event_time": event_time.isoformat(),
        })
    incremental_batches.append(pd.DataFrame(rows))

# ------------------------------ Quality test batch -------------------------
# Deliberately invalid scenarios for Silver validation/quarantine testing.
# The test_case and expected_* columns are test metadata, not production
# movement columns. Process these tests separately from baseline movements.
quality_test_batch = pd.DataFrame([
    {
        "test_case": "missing_movement_id", "movement_id": None,
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 5,
        "expected_result": "REJECTED", "expected_reason": "MISSING_MOVEMENT_ID",
    },
    {
        "test_case": "unknown_product", "movement_id": "Q001",
        "warehouse_id": "W001", "product_id": "P999", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 5,
        "expected_result": "REJECTED", "expected_reason": "UNKNOWN_PRODUCT",
    },
    {
        "test_case": "unknown_warehouse", "movement_id": "Q002",
        "warehouse_id": "W999", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 5,
        "expected_result": "REJECTED", "expected_reason": "UNKNOWN_WAREHOUSE",
    },
    {
        "test_case": "invalid_movement_type", "movement_id": "Q003",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "TRANSFER", "quantity": 5,
        "expected_result": "REJECTED", "expected_reason": "INVALID_MOVEMENT_TYPE",
    },
    {
        "test_case": "zero_quantity", "movement_id": "Q004",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 0,
        "expected_result": "REJECTED", "expected_reason": "NON_POSITIVE_QUANTITY",
    },
    {
        "test_case": "negative_quantity", "movement_id": "Q005",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": -5,
        "expected_result": "REJECTED", "expected_reason": "NON_POSITIVE_QUANTITY",
    },
    {
        "test_case": "exact_duplicate_first", "movement_id": "Q006",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "RECEIPT", "quantity": 10,
        "expected_result": "ACCEPTED", "expected_reason": "",
    },
    {
        "test_case": "exact_duplicate_replay", "movement_id": "Q006",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "RECEIPT", "quantity": 10,
        "expected_result": "DEDUPLICATED", "expected_reason": "DUPLICATE_MOVEMENT_ID",
    },
    {
        "test_case": "conflicting_duplicate_first", "movement_id": "Q007",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 10,
        "expected_result": "QUARANTINED", "expected_reason": "CONFLICTING_DUPLICATE_MOVEMENT_ID",
    },
    {
        "test_case": "conflicting_duplicate_second", "movement_id": "Q007",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 15,
        "expected_result": "QUARANTINED", "expected_reason": "CONFLICTING_DUPLICATE_MOVEMENT_ID",
    },
    {
        "test_case": "negative_stock_flag", "movement_id": "Q008",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 999999,
        "expected_result": "FLAG_NEGATIVE_STOCK", "expected_reason": "NEGATIVE_RESULTING_STOCK",
    },
])

# ------------------------------ Acceptance fixtures ------------------------
# These describe sequential processing expectations. Silver tests must apply
# them in order against a test opening balance of 100 units.
acceptance_fixture = pd.DataFrame([
    {
        "sequence": 1, "test_case": "normal_stock_update",
        "action": "UPSERT", "movement_id": "TEST001",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 20,
        "expected_result": "ACCEPTED", "expected_stock": 80,
        "expected_reason": "",
    },
    {
        "sequence": 2, "test_case": "replay_same_movement",
        "action": "UPSERT", "movement_id": "TEST001",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 20,
        "expected_result": "DEDUPLICATED", "expected_stock": 80,
        "expected_reason": "DUPLICATE_MOVEMENT_ID",
    },
    {
        "sequence": 3, "test_case": "correct_movement_quantity",
        "action": "CORRECT", "movement_id": "TEST001",
        "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 15,
        "expected_result": "CORRECTED", "expected_stock": 85,
        "expected_reason": "CORRECTED_FROM_20_TO_15",
    },
])

# Seven complete dates with explicit zero demand; separate from stock movements.
zero_demand_fixture = pd.DataFrame([
    {
        "date": (BASE_DATE.date() - timedelta(days=day)).isoformat(),
        "warehouse_id": "W001",
        "product_id": "P002",
        "units_requested": 0,
    }
    for day in range(6, -1, -1)
])

# ------------------------------ Inject controlled quality issues ------------
# Keep the generator's in-memory reference data clean while writing CSV files
# that intentionally contain a small, documented set of bad records. This lets
# Bronze ingest the raw records and Silver demonstrate validation/quarantine.
# The original CSV filenames are preserved.

# Master/reference data quality cases.
suppliers_out = pd.concat([suppliers, pd.DataFrame([
    {"supplier_id": None, "supplier_name": "Missing ID Supplier", "supplier_status": "ACTIVE"},
    {"supplier_id": "S_BAD_UNKNOWN", "supplier_name": None, "supplier_status": "ACTIVE"},
])], ignore_index=True)

warehouses_out = pd.concat([warehouses, pd.DataFrame([
    {"warehouse_id": None, "warehouse_name": "Missing ID Warehouse"},
    {"warehouse_id": "W_BAD_DUP", "warehouse_name": "Duplicate Test Warehouse A"},
    {"warehouse_id": "W_BAD_DUP", "warehouse_name": "Duplicate Test Warehouse B"},
])], ignore_index=True)

products_out = pd.concat([products, pd.DataFrame([
    {"product_id": None, "product_name": "Missing ID Product", "category": "Analgesic", "supplier_id": supplier_ids[0]},
    {"product_id": "P_BAD_SUPPLIER", "product_name": "Unknown Supplier Product", "category": "Antibiotic", "supplier_id": "S999"},
    {"product_id": "P_BAD_DUP", "product_name": "Duplicate Product A", "category": "Analgesic", "supplier_id": supplier_ids[0]},
    {"product_id": "P_BAD_DUP", "product_name": "Duplicate Product B", "category": "Antiviral", "supplier_id": supplier_ids[0]},
    {"product_id": "P_BAD_CATEGORY", "product_name": "Invalid Category Product", "category": "UnknownCategory", "supplier_id": supplier_ids[0]},
])], ignore_index=True)

supplier_product_out = pd.concat([supplier_product, pd.DataFrame([
    {"supplier_id": "S999", "product_id": "P001", "lead_time_days": 7, "unit_cost": 12.5, "minimum_order_quantity": 10, "is_preferred": False},
    {"supplier_id": supplier_ids[0], "product_id": "P999", "lead_time_days": 7, "unit_cost": 12.5, "minimum_order_quantity": 10, "is_preferred": False},
    {"supplier_id": supplier_ids[0], "product_id": "P001", "lead_time_days": -2, "unit_cost": 12.5, "minimum_order_quantity": 10, "is_preferred": False},
])], ignore_index=True)

opening_inventory_out = pd.concat([opening_inventory, pd.DataFrame([
    {"warehouse_id": "W999", "product_id": "P001", "batch_id": "B_BAD_WH", "opening_quantity": 25, "expiry_date": BASE_DATE.date().isoformat()},
    {"warehouse_id": "W001", "product_id": "P999", "batch_id": "B_BAD_PRODUCT", "opening_quantity": 25, "expiry_date": BASE_DATE.date().isoformat()},
    {"warehouse_id": "W001", "product_id": "P001", "batch_id": None, "opening_quantity": 25, "expiry_date": "not-a-date"},
    {"warehouse_id": "W001", "product_id": "P001", "batch_id": "B_BAD_QTY", "opening_quantity": -10, "expiry_date": (BASE_DATE + timedelta(days=90)).date().isoformat()},
])], ignore_index=True)

# Transaction data quality cases. These are mixed into stock_movements.csv so
# the ordinary Bronze -> Silver path will encounter them.
movement_quality_rows = [
    {"movement_id": None, "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 5, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_UNKNOWN_PRODUCT", "warehouse_id": "W001", "product_id": "P999", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 5, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_UNKNOWN_WAREHOUSE", "warehouse_id": "W999", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 5, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_TYPE", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "TRANSFER", "quantity": 5, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_ZERO_QTY", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 0, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_NEG_QTY", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": -5, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_EXACT_DUP", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "RECEIPT", "quantity": 10, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_EXACT_DUP", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "RECEIPT", "quantity": 10, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_CONFLICT_DUP", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 10, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_CONFLICT_DUP", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 15, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_NEG_STOCK", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 999999, "event_time": BASE_DATE.isoformat()},
    {"movement_id": "BAD_EVENT_TIME", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "RECEIPT", "quantity": 5, "event_time": "not-a-timestamp"},
]
stock_movements_out = pd.concat([stock_movements, pd.DataFrame(movement_quality_rows)], ignore_index=True)

# Invalid demand rows are deliberately appended; zero-demand fixture remains a
# separate valid business test (zero demand is not itself a data-quality error).
daily_demand_out = pd.concat([daily_demand, pd.DataFrame([
    {"date": "not-a-date", "warehouse_id": "W001", "product_id": "P001", "units_requested": 10},
    {"date": BASE_DATE.date().isoformat(), "warehouse_id": "W999", "product_id": "P001", "units_requested": 10},
    {"date": BASE_DATE.date().isoformat(), "warehouse_id": "W001", "product_id": "P999", "units_requested": 10},
    {"date": BASE_DATE.date().isoformat(), "warehouse_id": "W001", "product_id": "P001", "units_requested": -3},
])], ignore_index=True)

# Incremental files also contain a small, controlled mix of valid and invalid
# rows. Replaying a batch should be idempotent in Silver, not in this generator.
for idx, batch in enumerate(incremental_batches, start=1):
    if idx == 1:
        injected = pd.DataFrame([
            {"movement_id": f"INC_BAD_{idx}_MISSING", "warehouse_id": "W001", "product_id": "P001", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 0, "event_time": BASE_DATE.isoformat()},
            {"movement_id": f"INC_BAD_{idx}_PRODUCT", "warehouse_id": "W001", "product_id": "P999", "batch_id": "B0001", "movement_type": "ISSUE", "quantity": 5, "event_time": BASE_DATE.isoformat()},
        ])
        incremental_batches[idx - 1] = pd.concat([batch, injected], ignore_index=True)

# ------------------------------ Save files ----------------------------------
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Keep the original filenames from the earlier generator (including the
# singular supplier_product.csv and incremental_batch_1.csv naming pattern).
datasets = {
    "suppliers.csv": suppliers_out,
    "warehouses.csv": warehouses_out,
    "products.csv": products_out,
    "supplier_product.csv": supplier_product_out,
    "opening_inventory.csv": opening_inventory_out,
    "stock_movements.csv": stock_movements_out,
    "daily_demand.csv": daily_demand_out,
    "supplier_lead_times.csv": supplier_lead_times,
    "acceptance_fixture.csv": acceptance_fixture,
    "zero_demand_fixture.csv": zero_demand_fixture,
}
for filename, dataframe in datasets.items():
    dataframe.to_csv(OUTPUT_DIR / filename, index=False)

for batch_number, dataframe in enumerate(incremental_batches, start=1):
    dataframe.to_csv(OUTPUT_DIR / f"incremental_batch_{batch_number}.csv", index=False)

print("Data generation completed successfully.")
print(f"Output directory: {OUTPUT_DIR.resolve()}")
for filename, dataframe in datasets.items():
    print(f"{filename}: {len(dataframe):,} rows (includes controlled invalid cases where applicable)")
for batch_number, dataframe in enumerate(incremental_batches, start=1):
    print(f"incremental_batch_{batch_number}.csv: {len(dataframe):,} rows")
print("\nSilver notebook must validate, quarantine/reject, deduplicate, and log these cases.")
