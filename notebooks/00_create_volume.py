# Databricks notebook source
catalog = "medicine_inventory"
schema = "inventory"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("Catalog:", catalog)
print("Schema:", schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW STORAGE CREDENTIALS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS medicine_inventory_location
# MAGIC URL 'abfss://1capstone@stteam1medinv.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL medicine_inventory_credential)
# MAGIC COMMENT 'External location for medicine inventory ADLS container';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG medicine_inventory
# MAGIC TO `aasawari@rutikap268gmail.onmicrosoft.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA
# MAGIC ON SCHEMA inventory
# MAGIC TO `aasawari@rutikap268gmail.onmicrosoft.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS
# MAGIC medicine_inventory.inventory.inventory_files
# MAGIC LOCATION 'abfss://1capstone@stteam1medinv.dfs.core.windows.net/inventory_files'
# MAGIC COMMENT 'External volume for medicine inventory pipeline files';

# COMMAND ----------

