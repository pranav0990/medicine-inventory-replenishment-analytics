# Databricks notebook source
catalog = "medicine_inventory"
schema = "inventory"

volume_path = (
    "/Volumes/"
    "medicine_inventory/"
    "inventory/"
    "inventory_files"
)

landing_path = f"{volume_path}/landing"
incremental_path = f"{volume_path}/incremental"
checkpoint_path = f"{volume_path}/checkpoint"
quarantine_path = f"{volume_path}/quarantine"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("Catalog:", catalog)
print("Schema:", schema)
print("Volume:", volume_path)

# COMMAND ----------

display(
    dbutils.fs.ls(volume_path)
)

# COMMAND ----------

display(
    dbutils.fs.ls(landing_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS medicine_inventory.inventory.bronze_products;

# COMMAND ----------

catalog = "medicine_inventory"
schema = "inventory"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

df_products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{landing_path}/products.csv")
)

display(df_products)

# COMMAND ----------

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{schema}.bronze_products")

# COMMAND ----------


spark.sql(f"SELECT * FROM {catalog}.{schema}.bronze_products").show(10)

# COMMAND ----------

df_suppliers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{landing_path}/suppliers.csv")
)

display(df_suppliers)

(
    df_suppliers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.bronze_suppliers"
    )
)

# COMMAND ----------

df_warehouses = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{landing_path}/warehouses.csv")
)

display(df_warehouses)

(
    df_warehouses.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.bronze_warehouses"
    )
)

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{landing_path}/opening_inventory.csv")
)

df.printSchema()

# COMMAND ----------

files_to_load = [
    "products.csv",
    "suppliers.csv",
    "warehouses.csv",
    "opening_inventory.csv",
    "stock_movements.csv",
    "daily_demand.csv",
    "supplier_lead_times.csv",
    "supplier_product.csv"
]

for file_name in files_to_load:

    table_name = (
        "bronze_"
        + file_name
        .replace(".csv", "")
    )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(
            f"{landing_path}/{file_name}"
        )
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            f"{catalog}.{schema}.{table_name}"
        )
    )

    print(
        f"Created: {table_name}"
        f" | Rows: {df.count()}"
    )

# COMMAND ----------

spark.sql("""
SHOW TABLES IN medicine_inventory.inventory
""").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL medicine_inventory.inventory.gold_kpi;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE medicine_inventory.inventory.bronze_stock_movements;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM medicine_inventory.inventory.bronze_stock_movements
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT
# MAGIC ON TABLE medicine_inventory.inventory.bronze_stock_movements
# MAGIC TO `sakit@rutikap268gmail.onmicrosoft.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE medicine_inventory.inventory.bronze_stock_movements;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT MODIFY
# MAGIC ON TABLE medicine_inventory.inventory.bronze_stock_movements
# MAGIC TO `sakit@rutikap268gmail.onmicrosoft.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG medicine_inventory
# MAGIC TO `sakit@rutikap268gmail.onmicrosoft.com`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA medicine_inventory.inventory
# MAGIC TO `sakit@rutikap268gmail.onmicrosoft.com`;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON SCHEMA medicine_inventory.inventory
# MAGIC TO `sakit@rutikap268gmail.onmicrosoft.com`;

# COMMAND ----------

