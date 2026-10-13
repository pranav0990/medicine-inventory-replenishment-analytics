"""Medicine Inventory and Replenishment Analytics — final deterministic generator.

Creates valid baseline data and controlled data-quality defects in the SAME
source CSVs that the Bronze -> Silver pipeline will process.

The generator does not clean or quarantine records; Databricks Silver does that.

Run:
    pip install pandas
    python inventory_data_generator_corrected.py

Output directory: generated_data/
"""
from pathlib import Path
import os
import random

import pandas as pd

from datetime import datetime, timedelta

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

# ---------------- Configuration ----------------
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

NUM_PRODUCTS = 50
NUM_SUPPLIERS = 10
NUM_WAREHOUSES = 5
NUM_MOVEMENTS = 10_000
NUM_DEMAND_DAYS = 14
DEMAND_ROWS_PER_DAY = 200

BASE_DATE = datetime(2026, 9, 30, 23, 59, 0)
OUTPUT_DIR = Path("generated_data")

MOVEMENT_COLUMNS = [
    "movement_id", "warehouse_id", "product_id", "batch_id",
    "movement_type", "quantity", "event_time"
]
DEMAND_COLUMNS = [
    "date", "warehouse_id", "product_id", "units_requested"
]

# ---------------- Clean reference data in memory ----------------
suppliers = pd.DataFrame([
    {
        "supplier_id": f"S{i:03d}",
        "supplier_name": f"Pharma Supplier {i:02d}",
        "supplier_status": random.choice(
            ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]
        )
    }
    for i in range(1, NUM_SUPPLIERS + 1)
])

warehouses = pd.DataFrame([
    {
        "warehouse_id": f"W{i:03d}",
        "warehouse_name": f"Warehouse {i:02d}"
    }
    for i in range(1, NUM_WAREHOUSES + 1)
])

categories = [
    "Analgesic", "Antibiotic", "Antihistamine",
    "Antiviral", "Cardiovascular"
]

product_names = [
    "Paracetamol", "Amoxicillin", "Cetirizine", "Azithromycin",
    "Ibuprofen", "Metformin", "Amlodipine", "Omeprazole",
    "Ciprofloxacin", "Pantoprazole"
]

supplier_ids = suppliers["supplier_id"].tolist()

product_rows = []
supplier_product_rows = []

for i in range(1, NUM_PRODUCTS + 1):
    product_id = f"P{i:03d}"
    selected_suppliers = random.sample(
        supplier_ids, random.randint(1, 3)
    )
    preferred_supplier = random.choice(selected_suppliers)

    for supplier_id in selected_suppliers:
        supplier_product_rows.append({
            "supplier_id": supplier_id,
            "product_id": product_id,
            "lead_time_days": random.randint(3, 14),
            "unit_cost": round(random.uniform(5, 100), 2),
            "minimum_order_quantity": random.choice([10, 20, 50, 100]),
            "is_preferred": supplier_id == preferred_supplier
        })

    product_rows.append({
        "product_id": product_id,
        "product_name": (
            f"{random.choice(product_names)} "
            f"{random.choice([250, 500, 650])}mg"
        ),
        "category": random.choice(categories),
        "supplier_id": preferred_supplier
    })

products = pd.DataFrame(product_rows)
supplier_product = pd.DataFrame(supplier_product_rows)

supplier_lead_times = supplier_product[
    ["supplier_id", "product_id", "lead_time_days"]
].copy()

# ---------------- Opening inventory ----------------
opening_rows = []
batch_counter = 1

for product in products.to_dict("records"):
    selected_warehouses = random.sample(
        warehouses["warehouse_id"].tolist(),
        random.randint(1, 2)
    )

    for warehouse_id in selected_warehouses:
        opening_rows.append({
            "warehouse_id": warehouse_id,
            "product_id": product["product_id"],
            "batch_id": f"B{batch_counter:04d}",
            "opening_quantity": random.randint(50, 500),
            "expiry_date": (
                BASE_DATE + timedelta(days=random.randint(30, 365))
            ).date().isoformat()
        })
        batch_counter += 1

opening_inventory = pd.DataFrame(opening_rows)
inventory_records = opening_inventory.to_dict("records")

inventory_by_key = {
    (row["warehouse_id"], row["product_id"], row["batch_id"]):
        int(row["opening_quantity"])
    for row in inventory_records
}

movement_keys = list(inventory_by_key.keys())

# ---------------- Baseline stock movements ----------------
# Valid movements are generated in the same order as their timestamps.
# Stock is simulated in that same order to avoid the earlier timestamp mismatch.

stock_movements_rows = []
running_stock = inventory_by_key.copy()

start_time = BASE_DATE - timedelta(days=30)
end_time = BASE_DATE
span_seconds = int((end_time - start_time).total_seconds())

for i in range(1, NUM_MOVEMENTS + 1):
    key = random.choice(movement_keys)
    available = running_stock[key]

    if available < 30:
        movement_type = "RECEIPT"
    else:
        movement_type = random.choices(
            ["ISSUE", "RECEIPT"], weights=[70, 30]
        )[0]

    if movement_type == "ISSUE":
        quantity = random.randint(1, min(30, available))
        running_stock[key] -= quantity
    else:
        quantity = random.randint(10, 50)
        running_stock[key] += quantity

    seconds_offset = int(
        (i - 1) * span_seconds / max(NUM_MOVEMENTS - 1, 1)
    )
    event_time = start_time + timedelta(seconds=seconds_offset)

    stock_movements_rows.append({
        "movement_id": f"M{i:06d}",
        "warehouse_id": key[0],
        "product_id": key[1],
        "batch_id": key[2],
        "movement_type": movement_type,
        "quantity": quantity,
        "event_time": event_time.isoformat()
    })

stock_movements = pd.DataFrame(
    stock_movements_rows, columns=MOVEMENT_COLUMNS
)

# ---------------- Daily demand ----------------
demand_rows = []

product_warehouse_pairs = [
    (row["warehouse_id"], row["product_id"])
    for row in inventory_records
]

for day in range(NUM_DEMAND_DAYS):
    demand_date = (
        BASE_DATE.date() - timedelta(days=day)
    ).isoformat()

    for _ in range(DEMAND_ROWS_PER_DAY):
        warehouse_id, product_id = random.choice(
            product_warehouse_pairs
        )

        demand_rows.append({
            "date": demand_date,
            "warehouse_id": warehouse_id,
            "product_id": product_id,
            "units_requested": random.randint(1, 50)
        })

daily_demand = pd.DataFrame(
    demand_rows, columns=DEMAND_COLUMNS
)

# ---------------- Incremental batches ----------------
# Valid incremental rows use unique IDs and valid references.
# Deliberate invalid records are injected into each batch below.

incremental_batches = []
incremental_stock = running_stock.copy()

for batch_number in range(1, 4):
    batch_size = random.randint(100, 500)
    rows = []

    for i in range(1, batch_size + 1):
        key = random.choice(movement_keys)
        available = incremental_stock[key]

        if available < 30:
            movement_type = "RECEIPT"
        else:
            movement_type = random.choice(["ISSUE", "RECEIPT"])

        if movement_type == "ISSUE":
            quantity = random.randint(1, min(30, available))
            incremental_stock[key] -= quantity
        else:
            quantity = random.randint(10, 50)
            incremental_stock[key] += quantity

        event_time = end_time + timedelta(
            days=batch_number, seconds=i
        )

        rows.append({
            "movement_id": f"INC{batch_number}_{i:04d}",
            "warehouse_id": key[0],
            "product_id": key[1],
            "batch_id": key[2],
            "movement_type": movement_type,
            "quantity": quantity,
            "event_time": event_time.isoformat()
        })

    incremental_batches.append(
        pd.DataFrame(rows, columns=MOVEMENT_COLUMNS)
    )

# ---------------- Invalid supplier records ----------------
suppliers_out = pd.concat([
    suppliers,
    pd.DataFrame([
        {
            "supplier_id": None,
            "supplier_name": "Missing ID Supplier",
            "supplier_status": "ACTIVE"
        },
        {
            "supplier_id": "S_BAD_NAME",
            "supplier_name": None,
            "supplier_status": "ACTIVE"
        }
    ])
], ignore_index=True)

# ---------------- Invalid warehouse records ----------------
warehouses_out = pd.concat([
    warehouses,
    pd.DataFrame([
        {
            "warehouse_id": None,
            "warehouse_name": "Missing ID Warehouse"
        },
        {
            "warehouse_id": "W_BAD_DUP",
            "warehouse_name": "Duplicate Warehouse A"
        },
        {
            "warehouse_id": "W_BAD_DUP",
            "warehouse_name": "Duplicate Warehouse B"
        },
        {
            "warehouse_id": "W_BAD_NAME",
            "warehouse_name": None
        }
    ])
], ignore_index=True)

# ---------------- Invalid product records ----------------
products_out = pd.concat([
    products,
    pd.DataFrame([
        {
            "product_id": None,
            "product_name": "Missing ID Product",
            "category": "Analgesic",
            "supplier_id": supplier_ids[0]
        },
        {
            "product_id": "P_BAD_NAME",
            "product_name": None,
            "category": "Analgesic",
            "supplier_id": supplier_ids[0]
        },
        {
            "product_id": "P_BAD_SUPPLIER",
            "product_name": "Unknown Supplier Product",
            "category": "Antibiotic",
            "supplier_id": "S999"
        },
        {
            "product_id": "P_BAD_DUP",
            "product_name": "Duplicate Product A",
            "category": "Analgesic",
            "supplier_id": supplier_ids[0]
        },
        {
            "product_id": "P_BAD_DUP",
            "product_name": "Duplicate Product B",
            "category": "Antiviral",
            "supplier_id": supplier_ids[0]
        },
        {
            "product_id": "P_BAD_CATEGORY",
            "product_name": "Invalid Category Product",
            "category": "UnknownCategory",
            "supplier_id": supplier_ids[0]
        }
    ])
], ignore_index=True)

# ---------------- Invalid supplier-product relationships ----------------
first_relationship = supplier_product.iloc[0]

supplier_product_out = pd.concat([
    supplier_product,
    pd.DataFrame([
        {
            "supplier_id": "S999",
            "product_id": "P001",
            "lead_time_days": 7,
            "unit_cost": 12.5,
            "minimum_order_quantity": 10,
            "is_preferred": False
        },
        {
            "supplier_id": supplier_ids[0],
            "product_id": "P999",
            "lead_time_days": 7,
            "unit_cost": 12.5,
            "minimum_order_quantity": 10,
            "is_preferred": False
        },
        {
            "supplier_id": supplier_ids[0],
            "product_id": "P001",
            "lead_time_days": -2,
            "unit_cost": 12.5,
            "minimum_order_quantity": 10,
            "is_preferred": False
        },
        {
            "supplier_id": first_relationship["supplier_id"],
            "product_id": first_relationship["product_id"],
            "lead_time_days": int(first_relationship["lead_time_days"]),
            "unit_cost": float(first_relationship["unit_cost"]),
            "minimum_order_quantity": int(
                first_relationship["minimum_order_quantity"]
            ),
            "is_preferred": bool(first_relationship["is_preferred"])
        }
    ])
], ignore_index=True)

# ---------------- Invalid opening inventory records ----------------
opening_inventory_out = pd.concat([
    opening_inventory,
    pd.DataFrame([
        {
            "warehouse_id": "W999",
            "product_id": "P001",
            "batch_id": "B_BAD_WH",
            "opening_quantity": 25,
            "expiry_date": BASE_DATE.date().isoformat()
        },
        {
            "warehouse_id": "W001",
            "product_id": "P999",
            "batch_id": "B_BAD_PRODUCT",
            "opening_quantity": 25,
            "expiry_date": BASE_DATE.date().isoformat()
        },
        {
            "warehouse_id": "W001",
            "product_id": "P001",
            "batch_id": None,
            "opening_quantity": 25,
            "expiry_date": "not-a-date"
        },
        {
            "warehouse_id": "W001",
            "product_id": "P001",
            "batch_id": "B_BAD_QTY",
            "opening_quantity": -10,
            "expiry_date": (
                BASE_DATE + timedelta(days=90)
            ).date().isoformat()
        },
        {
            "warehouse_id": "W001",
            "product_id": "P001",
            "batch_id": "B_BAD_DUP",
            "opening_quantity": 20,
            "expiry_date": (
                BASE_DATE + timedelta(days=90)
            ).date().isoformat()
        },
        {
            "warehouse_id": "W001",
            "product_id": "P001",
            "batch_id": "B_BAD_DUP",
            "opening_quantity": 25,
            "expiry_date": (
                BASE_DATE + timedelta(days=100)
            ).date().isoformat()
        }
    ])
], ignore_index=True)

# ---------------- Invalid stock movements ----------------
movement_quality_rows = [
    {
        "movement_id": None, "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 5,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_UNKNOWN_PRODUCT", "warehouse_id": "W001",
        "product_id": "P999", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 5,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_UNKNOWN_WAREHOUSE", "warehouse_id": "W999",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 5,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_TYPE", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "TRANSFER", "quantity": 5,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_ZERO_QTY", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 0,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_NEG_QTY", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": -5,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_EXACT_DUP", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "RECEIPT", "quantity": 10,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_EXACT_DUP", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "RECEIPT", "quantity": 10,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_CONFLICT_DUP", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 10,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_CONFLICT_DUP", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 15,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_NEG_STOCK", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "ISSUE", "quantity": 999999,
        "event_time": end_time.isoformat()
    },
    {
        "movement_id": "BAD_EVENT_TIME", "warehouse_id": "W001",
        "product_id": "P001", "batch_id": "B0001",
        "movement_type": "RECEIPT", "quantity": 5,
        "event_time": "not-a-timestamp"
    }
]

stock_movements_out = pd.concat([
    stock_movements,
    pd.DataFrame(movement_quality_rows, columns=MOVEMENT_COLUMNS)
], ignore_index=True)

# ---------------- Invalid demand records ----------------
daily_demand_out = pd.concat([
    daily_demand,
    pd.DataFrame([
        {
            "date": "not-a-date", "warehouse_id": "W001",
            "product_id": "P001", "units_requested": 10
        },
        {
            "date": BASE_DATE.date().isoformat(), "warehouse_id": "W999",
            "product_id": "P001", "units_requested": 10
        },
        {
            "date": BASE_DATE.date().isoformat(), "warehouse_id": "W001",
            "product_id": "P999", "units_requested": 10
        },
        {
            "date": BASE_DATE.date().isoformat(), "warehouse_id": "W001",
            "product_id": "P001", "units_requested": -3
        },
        {
            "date": BASE_DATE.date().isoformat(), "warehouse_id": "W001",
            "product_id": "P001", "units_requested": None
        }
    ])
], ignore_index=True)

# ---------------- Inject invalid records into all incremental batches ----------------
for batch_number, batch in enumerate(incremental_batches, start=1):
    if batch_number == 1:
        bad_rows = [
            {
                "movement_id": "INC_BAD_1_ZERO", "warehouse_id": "W001",
                "product_id": "P001", "batch_id": "B0001",
                "movement_type": "ISSUE", "quantity": 0,
                "event_time": end_time.isoformat()
            },
            {
                "movement_id": "INC_BAD_1_PRODUCT", "warehouse_id": "W001",
                "product_id": "P999", "batch_id": "B0001",
                "movement_type": "ISSUE", "quantity": 5,
                "event_time": end_time.isoformat()
            }
        ]
    elif batch_number == 2:
        bad_rows = [
            {
                "movement_id": "INC_BAD_2_TYPE", "warehouse_id": "W001",
                "product_id": "P001", "batch_id": "B0001",
                "movement_type": "TRANSFER", "quantity": 5,
                "event_time": (
                    end_time + timedelta(days=2)
                ).isoformat()
            },
            {
                "movement_id": "INC_BAD_2_TIME", "warehouse_id": "W001",
                "product_id": "P001", "batch_id": "B0001",
                "movement_type": "RECEIPT", "quantity": 5,
                "event_time": "not-a-timestamp"
            }
        ]
    else:
        bad_rows = [
            {
                "movement_id": "INC_BAD_3_NEG", "warehouse_id": "W001",
                "product_id": "P001", "batch_id": "B0001",
                "movement_type": "ISSUE", "quantity": -7,
                "event_time": (
                    end_time + timedelta(days=3)
                ).isoformat()
            },
            {
                "movement_id": "INC_BAD_3_CONFLICT", "warehouse_id": "W001",
                "product_id": "P001", "batch_id": "B0001",
                "movement_type": "ISSUE", "quantity": 7,
                "event_time": (
                    end_time + timedelta(days=3)
                ).isoformat()
            },
            {
                "movement_id": "INC_BAD_3_CONFLICT", "warehouse_id": "W001",
                "product_id": "P001", "batch_id": "B0001",
                "movement_type": "ISSUE", "quantity": 9,
                "event_time": (
                    end_time + timedelta(days=3)
                ).isoformat()
            }
        ]

    incremental_batches[batch_number - 1] = pd.concat([
        batch,
        pd.DataFrame(bad_rows, columns=MOVEMENT_COLUMNS)
    ], ignore_index=True)

# ---------------- Acceptance fixtures ----------------
# Test sequentially against an isolated opening balance of 100 units.
acceptance_fixture = pd.DataFrame([
    {
        "sequence": 1, "test_case": "normal_stock_update",
        "action": "UPSERT", "movement_id": "TEST001",
        "warehouse_id": "W001", "product_id": "P001",
        "batch_id": "B0001", "movement_type": "ISSUE",
        "quantity": 20, "expected_result": "ACCEPTED",
        "expected_stock": 80, "expected_reason": ""
    },
    {
        "sequence": 2, "test_case": "replay_same_movement",
        "action": "UPSERT", "movement_id": "TEST001",
        "warehouse_id": "W001", "product_id": "P001",
        "batch_id": "B0001", "movement_type": "ISSUE",
        "quantity": 20, "expected_result": "DEDUPLICATED",
        "expected_stock": 80,
        "expected_reason": "DUPLICATE_MOVEMENT_ID"
    },
    {
        "sequence": 3, "test_case": "correct_movement_quantity",
        "action": "CORRECT", "movement_id": "TEST001",
        "warehouse_id": "W001", "product_id": "P001",
        "batch_id": "B0001", "movement_type": "ISSUE",
        "quantity": 15, "expected_result": "CORRECTED",
        "expected_stock": 85,
        "expected_reason": "CORRECTED_FROM_20_TO_15"
    }
])

# Seven complete dates with intentional zero demand.
zero_demand_fixture = pd.DataFrame([
    {
        "date": (
            BASE_DATE.date() - timedelta(days=day)
        ).isoformat(),
        "warehouse_id": "W001",
        "product_id": "P002",
        "units_requested": 0
    }
    for day in range(6, -1, -1)
])

# ---------------- Write established CSV filenames ----------------
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
    "zero_demand_fixture.csv": zero_demand_fixture
}

for filename, dataframe in datasets.items():
    dataframe.to_csv(OUTPUT_DIR / filename, index=False)

for batch_number, dataframe in enumerate(incremental_batches, start=1):
    dataframe.to_csv(
        OUTPUT_DIR / f"incremental_batch_{batch_number}.csv",
        index=False
    )

# ---------------- Sanity checks ----------------
assert len(products) == NUM_PRODUCTS
assert len(suppliers) == NUM_SUPPLIERS
assert len(warehouses) == NUM_WAREHOUSES
assert len(stock_movements) == NUM_MOVEMENTS
assert stock_movements["event_time"].is_monotonic_increasing
assert stock_movements["movement_id"].is_unique
assert (stock_movements["quantity"] > 0).all()
assert stock_movements["movement_type"].isin(["ISSUE", "RECEIPT"]).all()
assert len(zero_demand_fixture) == 7
assert (zero_demand_fixture["units_requested"] == 0).all()

print("Generation complete.")
print("Controlled invalid records are intentionally included.")
print(f"Output folder: {OUTPUT_DIR.resolve()}")

for filename, dataframe in datasets.items():
    print(f"{filename}: {len(dataframe):,} rows")

for batch_number, dataframe in enumerate(incremental_batches, start=1):
    print(f"incremental_batch_{batch_number}.csv: {len(dataframe):,} rows")

print(
    "Baseline movement timestamps are ordered; malformed injected timestamps "
    "are intentional test cases."
)
print(
    "Silver must validate, quarantine/reject, deduplicate, and log these cases."
)
# ==================================================
# ADLS AUTOMATIC UPLOAD
# ==================================================
#
# Required GitHub Codespaces secrets:
#
# AZURE_CLIENT_ID
# AZURE_TENANT_ID
# AZURE_CLIENT_SECRET
#
# Required Azure role:
#
# Storage Blob Data Contributor
#
# Storage account:
#
# stteam1medinv
#
# Container:
#
# 1capstone
#
# Final structure:
#
# 1capstone/
# └── inventory_files/
#     ├── landing/
#     ├── incremental/
#     ├── checkpoint/
#     └── quarantine/
#
# The data generator writes only to:
#
# landing/
# incremental/
#
# Databricks will later use:
#
# checkpoint/
# quarantine/
# ==================================================


# ---------------- ADLS configuration ----------------

STORAGE_ACCOUNT_NAME = "stteam1medinv"
CONTAINER_NAME = "1capstone"

ADLS_PROJECT_ROOT = "inventory_files"

LANDING_DIRECTORY = (
    f"{ADLS_PROJECT_ROOT}/landing"
)

INCREMENTAL_DIRECTORY = (
    f"{ADLS_PROJECT_ROOT}/incremental"
)

CHECKPOINT_DIRECTORY = (
    f"{ADLS_PROJECT_ROOT}/checkpoint"
)

QUARANTINE_DIRECTORY = (
    f"{ADLS_PROJECT_ROOT}/quarantine"
)


# ---------------- Read Codespaces secrets ----------------

azure_client_id = os.getenv(
    "AZURE_CLIENT_ID"
)

azure_tenant_id = os.getenv(
    "AZURE_TENANT_ID"
)

azure_client_secret = os.getenv(
    "AZURE_CLIENT_SECRET"
)


# ---------------- Validate secrets ----------------

missing_secrets = []

if not azure_client_id:
    missing_secrets.append(
        "AZURE_CLIENT_ID"
    )

if not azure_tenant_id:
    missing_secrets.append(
        "AZURE_TENANT_ID"
    )

if not azure_client_secret:
    missing_secrets.append(
        "AZURE_CLIENT_SECRET"
    )

if missing_secrets:
    raise RuntimeError(
        "The following GitHub Codespaces secrets "
        "are unavailable: "
        + ", ".join(missing_secrets)
        + ". Stop and restart the Codespace after "
        "adding or updating Codespaces secrets."
    )

print("\nAzure credentials are available.")
print(
    "Client secret value will not be displayed."
)


# ---------------- Create Azure credential ----------------

credential = ClientSecretCredential(
    tenant_id=azure_tenant_id,
    client_id=azure_client_id,
    client_secret=azure_client_secret
)


# ---------------- Connect to ADLS ----------------

account_url = (
    f"https://{STORAGE_ACCOUNT_NAME}"
    ".dfs.core.windows.net"
)

service_client = DataLakeServiceClient(
    account_url=account_url,
    credential=credential
)

file_system_client = (
    service_client.get_file_system_client(
        file_system=CONTAINER_NAME
    )
)


# ---------------- Test container access ----------------

try:
    file_system_client.get_file_system_properties()

    print(
        f"Connected successfully to container: "
        f"{CONTAINER_NAME}"
    )

except Exception as error:
    raise RuntimeError(
        "Authentication succeeded, but access to "
        f"the ADLS container '{CONTAINER_NAME}' "
        "could not be verified. Check that the "
        "medicine-inventory-data-generator app has "
        "Storage Blob Data Contributor on storage "
        f"account '{STORAGE_ACCOUNT_NAME}'."
    ) from error


# ---------------- Create required directories ----------------

required_directories = [
    LANDING_DIRECTORY,
    INCREMENTAL_DIRECTORY,
    CHECKPOINT_DIRECTORY,
    QUARANTINE_DIRECTORY
]

for directory_path in required_directories:

    directory_client = (
        file_system_client.get_directory_client(
            directory=directory_path
        )
    )

    try:
        directory_client.create_directory()

        print(
            f"Created ADLS directory: "
            f"{directory_path}"
        )

    except ResourceExistsError:

        print(
            f"ADLS directory already exists: "
            f"{directory_path}"
        )


# ---------------- Define upload function ----------------

def upload_file_to_adls(
    local_file_path,
    destination_directory
):
    """
    Upload one locally generated CSV file to ADLS.

    Existing files with the same destination name
    are overwritten.
    """

    local_path = Path(
        local_file_path
    )

    if not local_path.exists():
        raise FileNotFoundError(
            f"Generated file was not found: "
            f"{local_path}"
        )

    if not local_path.is_file():
        raise ValueError(
            f"Expected a file but found: "
            f"{local_path}"
        )

    directory_client = (
        file_system_client.get_directory_client(
            directory=destination_directory
        )
    )

    file_client = (
        directory_client.get_file_client(
            file=local_path.name
        )
    )

    with local_path.open("rb") as source_file:

        file_client.upload_data(
            source_file,
            overwrite=True
        )

    destination_path = (
        f"{destination_directory}/"
        f"{local_path.name}"
    )

    print(
        f"Uploaded: {local_path}"
        f" -> {destination_path}"
    )


# ---------------- Landing file mapping ----------------
#
# These are baseline/reference files.
# They will be uploaded to:
#
# inventory_files/landing/
# ----------------

landing_files = [
    "suppliers.csv",
    "warehouses.csv",
    "products.csv",
    "supplier_product.csv",
    "opening_inventory.csv",
    "stock_movements.csv",
    "daily_demand.csv",
    "supplier_lead_times.csv",
    "acceptance_fixture.csv",
    "zero_demand_fixture.csv"
]


# ---------------- Incremental file mapping ----------------
#
# These files will be uploaded to:
#
# inventory_files/incremental/
# ----------------

incremental_files = [
    "incremental_batch_1.csv",
    "incremental_batch_2.csv",
    "incremental_batch_3.csv"
]


# ---------------- Upload landing datasets ----------------

print("\n==========================================")
print("Uploading landing files")
print("==========================================")

for filename in landing_files:

    local_file_path = (
        OUTPUT_DIR / filename
    )

    upload_file_to_adls(
        local_file_path=local_file_path,
        destination_directory=LANDING_DIRECTORY
    )


# ---------------- Upload incremental datasets ----------------

print("\n==========================================")
print("Uploading incremental files")
print("==========================================")

for filename in incremental_files:
    local_file_path = OUTPUT_DIR / filename

    upload_file_to_adls(
        local_file_path=local_file_path,
        destination_directory=INCREMENTAL_DIRECTORY
    )



