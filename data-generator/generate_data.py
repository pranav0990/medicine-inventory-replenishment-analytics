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
