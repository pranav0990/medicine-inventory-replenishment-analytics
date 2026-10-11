cat > README.md <<'EOF'
# Medicine Inventory & Replenishment Analytics

## Project Overview

This project is an end-to-end data engineering and analytics solution for medicine inventory management and replenishment.

The solution ingests synthetic medicine inventory data, processes it through Bronze, Silver, and Gold layers in Databricks, and produces analytics for inventory monitoring and supplier replenishment decisions.

## Architecture

- **Data Generation:** Python synthetic data generator
- **Storage:** Azure Data Lake Storage Gen2
- **Processing:** Azure Databricks
- **Data Lakehouse:** Delta Lake
- **Governance:** Unity Catalog
- **Orchestration:** Apache Airflow on Astronomer Astro
- **Monitoring:** New Relic
- **Analytics:** Power BI
- **AI Assistant:** Databricks Genie

## Databricks Data Layers

### Bronze Layer

Raw source data loaded from Azure Data Lake Storage Gen2 into Delta tables.

**8 Bronze tables:**

1. bronze_products
2. bronze_suppliers
3. bronze_warehouses
4. bronze_opening_inventory
5. bronze_stock_movements
6. bronze_daily_demand
7. bronze_supplier_lead_times
8. bronze_supplier_products

### Silver Layer

Cleaned and validated data prepared for analytics.

**8 Silver tables:**

1. silver_products
2. silver_suppliers
3. silver_warehouses
4. silver_opening_inventory
5. silver_stock_movements
6. silver_daily_demand
7. silver_supplier_lead_times
8. silver_supplier_products

### Gold Layer

Business-ready datasets for inventory and replenishment analytics.

**3 Gold tables:**

1. gold_inventory_position
2. gold_replenishment
3. gold_inventory_summary

## Key Analytics

The Gold layer provides:

- Current inventory position
- Net inventory movement
- Average daily demand
- Reorder point
- Reorder status
- Suggested order quantity
- Supplier and lead-time information
- Warehouse inventory summaries
- Inventory and replenishment KPIs

## Repository Structure

```text
medicine-inventory-replenishment-analytics/
├── data-generator/
│   └── generate_data.py
├── README.md
└── .gitignore