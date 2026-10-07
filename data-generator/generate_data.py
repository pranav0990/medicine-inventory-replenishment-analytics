import pandas as pd

# Create sample medicine products
products = pd.DataFrame({
    "product_id": ["P001", "P002", "P003", "P004", "P005"],
    "product_name": [
        "Paracetamol 500mg",
        "Amoxicillin 250mg",
        "Cetirizine 10mg",
        "Azithromycin 500mg",
        "Ibuprofen 400mg"
    ],
    "category": [
        "Analgesic",
        "Antibiotic",
        "Antihistamine",
        "Antibiotic",
        "Analgesic"
    ],
    "supplier_id": [
        "S001",
        "S002",
        "S001",
        "S003",
        "S002"
    ]
})

print(products)

# Create opening inventory
opening_inventory = pd.DataFrame({
    "warehouse_id": ["W001", "W001", "W002", "W002", "W003"],
    "product_id": ["P001", "P002", "P001", "P003", "P004"],
    "batch_id": ["B001", "B002", "B003", "B004", "B005"],
    "opening_quantity": [100, 200, 150, 120, 80],
    "expiry_date": [
        "2027-01-15",
        "2026-12-20",
        "2027-03-10",
        "2026-11-30",
        "2027-02-28"
    ]
})

print("\nOpening Inventory:")
print(opening_inventory)

# Create stock movements
stock_movements = pd.DataFrame({
    "movement_id": ["M001", "M002", "M003", "M004", "M005"],
    "warehouse_id": ["W001", "W001", "W002", "W002", "W003"],
    "product_id": ["P001", "P001", "P002", "P003", "P004"],
    "batch_id": ["B001", "B001", "B002", "B004", "B005"],
    "movement_type": ["ISSUE", "RECEIPT", "ISSUE", "ISSUE", "RECEIPT"],
    "quantity": [20, 50, 30, 25, 40],
    "event_time": [
        "2026-09-20 10:00:00",
        "2026-09-21 14:00:00",
        "2026-09-21 16:00:00",
        "2026-09-22 11:00:00",
        "2026-09-23 09:00:00"
    ]
})

print("\nStock Movements:")
print(stock_movements)

# Create daily demand
daily_demand = pd.DataFrame({
    "date": [
        "2026-09-20",
        "2026-09-21",
        "2026-09-22",
        "2026-09-23",
        "2026-09-24",
        "2026-09-25",
        "2026-09-26"
    ],
    "warehouse_id": [
        "W001",
        "W001",
        "W001",
        "W001",
        "W001",
        "W001",
        "W001"
    ],
    "product_id": [
        "P001",
        "P001",
        "P001",
        "P001",
        "P001",
        "P001",
        "P001"
    ],
    "units_requested": [20, 25, 30, 0, 25, 20, 30]
})

print("\nDaily Demand:")
print(daily_demand)

# Create supplier lead times
supplier_lead_times = pd.DataFrame({
    "supplier_id": ["S001", "S002", "S003", "S001", "S002"],
    "product_id": ["P001", "P002", "P004", "P003", "P005"],
    "lead_time_days": [7, 10, 5, 6, 8]
})

print("\nSupplier Lead Times:")
print(supplier_lead_times)
