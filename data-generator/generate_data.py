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
