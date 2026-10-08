import pandas as pd
import random
from datetime import datetime, timedelta


# ==================================================
# Configuration
# ==================================================

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

NUM_PRODUCTS = 50
NUM_SUPPLIERS = 10
NUM_WAREHOUSES = 5
NUM_MOVEMENTS = 10000
NUM_DEMAND_DAYS = 14

# Fixed date for reproducibility
# Do NOT use datetime.now()
BASE_DATE = datetime(2026, 9, 30)


# ==================================================
# Generate Suppliers
# ==================================================

suppliers = []

for i in range(1, NUM_SUPPLIERS + 1):

    suppliers.append({
        "supplier_id": f"S{i:03d}",
        "supplier_name": f"Pharma Supplier {i:02d}",
        "supplier_status": random.choice(
            ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]
        )
    })

suppliers = pd.DataFrame(suppliers)


# ==================================================
# Generate Warehouses
# ==================================================

warehouses = []

for i in range(1, NUM_WAREHOUSES + 1):

    warehouses.append({
        "warehouse_id": f"W{i:03d}",
        "warehouse_name": f"Warehouse {i:02d}"
    })

warehouses = pd.DataFrame(warehouses)


# ==================================================
# Generate Products
# ==================================================

categories = [
    "Analgesic",
    "Antibiotic",
    "Antihistamine",
    "Antiviral",
    "Cardiovascular"
]

product_names = [
    "Paracetamol",
    "Amoxicillin",
    "Cetirizine",
    "Azithromycin",
    "Ibuprofen",
    "Metformin",
    "Amlodipine",
    "Omeprazole",
    "Ciprofloxacin",
    "Pantoprazole"
]

products = []

for i in range(1, NUM_PRODUCTS + 1):

    product_id = f"P{i:03d}"

    base_name = random.choice(product_names)

    product_name = (
        f"{base_name} "
        f"{random.choice([250, 500, 650])}mg"
    )

    category = random.choice(categories)

    products.append({
        "product_id": product_id,
        "product_name": product_name,
        "category": category
    })

products = pd.DataFrame(products)


# ==================================================
# Generate Supplier-Product Relationships
# ==================================================
# Each product can have 1-3 suppliers

supplier_product = []

for _, product in products.iterrows():

    number_of_suppliers = random.randint(1, 3)

    selected_suppliers = random.sample(
        list(suppliers["supplier_id"]),
        number_of_suppliers
    )

    # Randomly choose one preferred supplier
    preferred_supplier = random.choice(selected_suppliers)

    for supplier_id in selected_suppliers:

        supplier_product.append({
            "supplier_id": supplier_id,
            "product_id": product["product_id"],
            "lead_time_days": random.randint(3, 14),
            "unit_cost": round(
                random.uniform(5, 100),
                2
            ),
            "minimum_order_quantity": random.choice(
                [10, 20, 50, 100]
            ),
            "is_preferred": (
                supplier_id == preferred_supplier
            )
        })

supplier_product = pd.DataFrame(supplier_product)


# ==================================================
# Display Reference Data
# ==================================================

print("\n================ SUPPLIERS ================")
print(suppliers.head())

print("\nNumber of suppliers:", len(suppliers))

print("\n================ WAREHOUSES ================")
print(warehouses)

print("\nNumber of warehouses:", len(warehouses))

print("\n================ PRODUCTS ================")
print(products.head())

print("\nNumber of products:", len(products))

print("\n=========== SUPPLIER-PRODUCT ===============")
print(supplier_product.head(10))

print(
    "\nNumber of supplier-product relationships:",
    len(supplier_product)
)


# ==================================================
# Generate Opening Inventory
# ==================================================

opening_inventory = []

batch_counter = 1

for _, product in products.iterrows():

    # Each product is stored in 1-2 warehouses
    selected_warehouses = random.sample(
        list(warehouses["warehouse_id"]),
        random.randint(1, 2)
    )

    for warehouse_id in selected_warehouses:

        batch_id = f"B{batch_counter:04d}"
        batch_counter += 1

        opening_quantity = random.randint(50, 500)

        # Expiry date between 30 and 365 days
        expiry_date = BASE_DATE + timedelta(
            days=random.randint(30, 365)
        )

        opening_inventory.append({
            "warehouse_id": warehouse_id,
            "product_id": product["product_id"],
            "batch_id": batch_id,
            "opening_quantity": opening_quantity,
            "expiry_date": expiry_date.date()
        })


opening_inventory = pd.DataFrame(opening_inventory)


# ==================================================
# Display Opening Inventory
# ==================================================

print("\n================ OPENING INVENTORY ================")
print(opening_inventory.head(10))

print(
    "\nNumber of opening inventory records:",
    len(opening_inventory)
)


# ==================================================
# Generate Stock Movements
# ==================================================

stock_movements = []

movement_types = ["ISSUE", "RECEIPT"]

# Track approximate stock for realistic movement generation
running_stock = {}

for _, row in opening_inventory.iterrows():

    key = (
        row["warehouse_id"],
        row["product_id"],
        row["batch_id"]
    )

    running_stock[key] = row["opening_quantity"]


for i in range(1, NUM_MOVEMENTS + 1):

    # Select an existing inventory record
    inventory_row = opening_inventory.sample(
        n=1,
        random_state=RANDOM_SEED + i
    ).iloc[0]

    warehouse_id = inventory_row["warehouse_id"]
    product_id = inventory_row["product_id"]
    batch_id = inventory_row["batch_id"]

    key = (
        warehouse_id,
        product_id,
        batch_id
    )

    current_stock = running_stock[key]

    # If stock is low, prefer receipt
    if current_stock < 30:
        movement_type = "RECEIPT"

    else:
        movement_type = random.choices(
            ["ISSUE", "RECEIPT"],
            weights=[70, 30]
        )[0]

    if movement_type == "ISSUE":

        # Do not normally issue more than available stock
        quantity = random.randint(
            1,
            min(30, current_stock)
        )

        running_stock[key] -= quantity

    else:

        quantity = random.randint(10, 50)

        running_stock[key] += quantity

    # Generate event time within previous 30 days
    event_time = BASE_DATE - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    movement_id = f"M{i:06d}"

    stock_movements.append({
        "movement_id": movement_id,
        "warehouse_id": warehouse_id,
        "product_id": product_id,
        "batch_id": batch_id,
        "movement_type": movement_type,
        "quantity": quantity,
        "event_time": event_time
    })


stock_movements = pd.DataFrame(stock_movements)


# ==================================================
# Display Stock Movements
# ==================================================

print("\n================ STOCK MOVEMENTS ================")
print(stock_movements.head(10))

print(
    "\nNumber of stock movements:",
    len(stock_movements)
)

print(
    "\nMovement type distribution:"
)

print(
    stock_movements["movement_type"].value_counts()
)


# ==================================================
# Generate Daily Demand
# ==================================================

daily_demand = []

for day in range(NUM_DEMAND_DAYS):

    demand_date = BASE_DATE - timedelta(
        days=day
    )

    # 200 demand records per day
    for record_number in range(200):

        inventory_row = opening_inventory.sample(
            n=1,
            random_state=(
                RANDOM_SEED
                + day * 1000
                + record_number
            )
        ).iloc[0]

        daily_demand.append({
            "date": demand_date.date(),
            "warehouse_id": inventory_row["warehouse_id"],
            "product_id": inventory_row["product_id"],
            "units_requested": random.randint(1, 50)
        })


daily_demand = pd.DataFrame(daily_demand)


# ==================================================
# Display Daily Demand
# ==================================================

print("\n================ DAILY DEMAND ================")
print(daily_demand.head(10))

print(
    "\nNumber of daily demand records:",
    len(daily_demand)
)


# ==================================================
# Supplier Lead Times
# ==================================================

# Lead times are already generated as part
# of the supplier_product relationship.

supplier_lead_times = supplier_product[
    [
        "supplier_id",
        "product_id",
        "lead_time_days"
    ]
].copy()


# ==================================================
# Display Supplier Lead Times
# ==================================================

print(
    "\n================ SUPPLIER LEAD TIMES ================"
)

print(
    supplier_lead_times.head(10)
)

print(
    "\nNumber of supplier-product lead-time records:",
    len(supplier_lead_times)
)



# ==================================================
# Generate Incremental Stock Movement Batches
# ==================================================

incremental_batches = []

for batch_number in range(1, 4):

    # Each batch contains 100-500 records
    batch_size = random.randint(100, 500)

    batch_records = []

    for i in range(batch_size):

        # Select an existing inventory record
        inventory_row = opening_inventory.sample(
            n=1,
            random_state=(
                RANDOM_SEED
                + batch_number * 10000
                + i
            )
        ).iloc[0]

        warehouse_id = inventory_row["warehouse_id"]
        product_id = inventory_row["product_id"]
        batch_id = inventory_row["batch_id"]

        movement_type = random.choice(
            ["ISSUE", "RECEIPT"]
        )

        quantity = random.randint(1, 50)

        event_time = BASE_DATE + timedelta(
            days=batch_number,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        movement_id = (
            f"INC{batch_number}_{i + 1:04d}"
        )

        batch_records.append({
            "movement_id": movement_id,
            "warehouse_id": warehouse_id,
            "product_id": product_id,
            "batch_id": batch_id,
            "movement_type": movement_type,
            "quantity": quantity,
            "event_time": event_time
        })

    incremental_batch = pd.DataFrame(
        batch_records
    )

    incremental_batches.append(
        incremental_batch
    )

    print(
        f"\nIncremental Batch {batch_number}: "
        f"{len(incremental_batch)} records"
    )


# ==================================================
# Generate Acceptance Test Fixture
# ==================================================
# Small deterministic dataset used to test
# Databricks validation and business rules.
#
# These records are NOT part of the main
# production-like dataset.

acceptance_fixture = pd.DataFrame([

    # ------------------------------------------------
    # TEST 1: Normal stock update
    # ------------------------------------------------
    {
        "test_case": "normal_stock_update",
        "movement_id": "TEST001",
        "warehouse_id": "W001",
        "product_id": "P001",
        "batch_id": "B0001",
        "movement_type": "ISSUE",
        "quantity": 20,
        "expected_result": "ACCEPTED",
        "expected_stock": 80,
        "expected_reason": ""
    },

    # ------------------------------------------------
    # TEST 2: Replay / duplicate movement
    # ------------------------------------------------
    {
        "test_case": "replay_same_movement",
        "movement_id": "TEST001",
        "warehouse_id": "W001",
        "product_id": "P001",
        "batch_id": "B0001",
        "movement_type": "ISSUE",
        "quantity": 20,
        "expected_result": "DEDUPLICATED",
        "expected_stock": 80,
        "expected_reason": "DUPLICATE_MOVEMENT_ID"
    },

    # ------------------------------------------------
    # TEST 3: Unknown product
    # ------------------------------------------------
    {
        "test_case": "unknown_product",
        "movement_id": "TEST002",
        "warehouse_id": "W001",
        "product_id": "P999",
        "batch_id": "B0001",
        "movement_type": "ISSUE",
        "quantity": 20,
        "expected_result": "REJECTED",
        "expected_stock": None,
        "expected_reason": "UNKNOWN_PRODUCT"
    },

    # ------------------------------------------------
    # TEST 4: Negative quantity
    # ------------------------------------------------
    {
        "test_case": "negative_quantity",
        "movement_id": "TEST003",
        "warehouse_id": "W001",
        "product_id": "P001",
        "batch_id": "B0001",
        "movement_type": "ISSUE",
        "quantity": -20,
        "expected_result": "REJECTED",
        "expected_stock": None,
        "expected_reason": "NON_POSITIVE_QUANTITY"
    },

    # ------------------------------------------------
    # TEST 5: Correction
    # ------------------------------------------------
    {
        "test_case": "correction",
        "movement_id": "TEST004",
        "warehouse_id": "W001",
        "product_id": "P001",
        "batch_id": "B0001",
        "movement_type": "ISSUE",
        "quantity": 15,
        "expected_result": "ACCEPTED",
        "expected_stock": 85,
        "expected_reason": "CORRECTED_FROM_20_TO_15"
    },

    # ------------------------------------------------
    # TEST 6: Zero demand
    # ------------------------------------------------
    {
        "test_case": "zero_demand",
        "movement_id": None,
        "warehouse_id": "W001",
        "product_id": "P002",
        "batch_id": "B0002",
        "movement_type": None,
        "quantity": 0,
        "expected_result": "NO_DEMAND",
        "expected_stock": None,
        "expected_reason": "ZERO_DEMAND_FOR_7_DAYS"
    }
])


# ==================================================
# Display Acceptance Fixture
# ==================================================

print(
    "\n================ ACCEPTANCE TEST FIXTURE ================"
)

print(acceptance_fixture)

print(
    "\nNumber of acceptance fixture records:",
    len(acceptance_fixture)
)
