import pandas as pd
import random

# --------------------------------------------------
# Configuration
# --------------------------------------------------

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Number of records
NUM_PRODUCTS = 50
NUM_SUPPLIERS = 10
NUM_WAREHOUSES = 5


# --------------------------------------------------
# Generate Suppliers
# --------------------------------------------------

suppliers = [
    f"S{i:03d}"
    for i in range(1, NUM_SUPPLIERS + 1)
]


# --------------------------------------------------
# Generate Warehouses
# --------------------------------------------------

warehouses = [
    f"W{i:03d}"
    for i in range(1, NUM_WAREHOUSES + 1)
]


# --------------------------------------------------
# Generate Products
# --------------------------------------------------

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

    product_name = f"{base_name} {random.choice([250, 500, 650])}mg"

    category = random.choice(categories)

    supplier_id = random.choice(suppliers)

    products.append({
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "supplier_id": supplier_id
    })


products = pd.DataFrame(products)


# --------------------------------------------------
# Display generated data
# --------------------------------------------------

print("Suppliers:")
print(suppliers)

print("\nWarehouses:")
print(warehouses)

print("\nProducts:")
print(products.head())

print("\nNumber of products:", len(products))



from datetime import datetime, timedelta

# --------------------------------------------------
# Generate Opening Inventory
# --------------------------------------------------

opening_inventory = []

batch_counter = 1

for _, product in products.iterrows():

    # Select 1-2 warehouses for each product
    selected_warehouses = random.sample(
        warehouses,
        random.randint(1, 2)
    )

    for warehouse_id in selected_warehouses:

        batch_id = f"B{batch_counter:04d}"
        batch_counter += 1

        opening_quantity = random.randint(50, 500)

        # Generate expiry date between 30 and 365 days
        expiry_date = datetime.now() + timedelta(
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


# --------------------------------------------------
# Display Opening Inventory
# --------------------------------------------------

print("\nOpening Inventory:")
print(opening_inventory.head(10))

print(
    "\nNumber of opening inventory records:",
    len(opening_inventory)
)

# --------------------------------------------------
# Generate Stock Movements
# --------------------------------------------------

NUM_MOVEMENTS = 10000

stock_movements = []

movement_types = ["ISSUE", "RECEIPT"]

for i in range(1, NUM_MOVEMENTS + 1):

    # Select an existing inventory record
    inventory_row = opening_inventory.sample(
        n=1,
        random_state=RANDOM_SEED + i
    ).iloc[0]

    movement_id = f"M{i:06d}"

    warehouse_id = inventory_row["warehouse_id"]
    product_id = inventory_row["product_id"]
    batch_id = inventory_row["batch_id"]

    movement_type = random.choice(movement_types)

    quantity = random.randint(1, 50)

    # Generate event date within the recent period
    event_time = datetime.now() - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

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


# --------------------------------------------------
# Display Stock Movements
# --------------------------------------------------

print("\nStock Movements:")
print(stock_movements.head(10))

print(
    "\nNumber of stock movements:",
    len(stock_movements)
)

print("\nMovement type distribution:")
print(stock_movements["movement_type"].value_counts())


# --------------------------------------------------
# Generate Daily Demand
# --------------------------------------------------

daily_demand = []

NUM_DEMAND_DAYS = 14

base_date = datetime(2026, 9, 30)

for day in range(NUM_DEMAND_DAYS):

    demand_date = base_date - timedelta(days=day)

    # Generate demand for different warehouse-product combinations
    for _ in range(200):

        inventory_row = opening_inventory.sample(
            n=1,
            random_state=RANDOM_SEED + day * 1000 + _
        ).iloc[0]

        daily_demand.append({
            "date": demand_date.date(),
            "warehouse_id": inventory_row["warehouse_id"],
            "product_id": inventory_row["product_id"],
            "units_requested": random.randint(1, 50)
        })

daily_demand = pd.DataFrame(daily_demand)

print("\nDaily Demand:")
print(daily_demand.head(10))

print("\nNumber of daily demand records:", len(daily_demand))

# --------------------------------------------------
# Generate Supplier Lead Times
# --------------------------------------------------

supplier_lead_times = []

for _, product in products.iterrows():

    supplier_lead_times.append({
        "supplier_id": product["supplier_id"],
        "product_id": product["product_id"],
        "lead_time_days": random.randint(3, 14)
    })

supplier_lead_times = pd.DataFrame(supplier_lead_times)

print("\nSupplier Lead Times:")
print(supplier_lead_times.head(10))

print("\nNumber of supplier lead-time records:",
      len(supplier_lead_times))
