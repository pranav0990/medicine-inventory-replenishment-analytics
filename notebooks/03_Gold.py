# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "medicine_inventory"
schema = "inventory"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("Catalog:", catalog)
print("Schema:", schema)

# COMMAND ----------

required_silver_tables = [
    "silver_products",
    "silver_suppliers",
    "silver_warehouses",
    "silver_opening_inventory",
    "silver_stock_movements",
    "silver_daily_demand",
    "silver_supplier_lead_times",
    "silver_supplier_products"
]

missing_tables = []

for table_name in required_silver_tables:

    full_table_name = (
        f"{catalog}.{schema}.{table_name}"
    )

    if spark.catalog.tableExists(full_table_name):

        row_count = spark.table(
            full_table_name
        ).count()

        print(
            f"Found: {table_name}"
            f" | Rows: {row_count}"
        )

    else:
        missing_tables.append(table_name)


if missing_tables:

    raise RuntimeError(
        "Missing Silver tables: "
        + ", ".join(missing_tables)
    )

print("\nAll required Silver tables are available.")

# COMMAND ----------

df_movement_summary = spark.sql(
    f"""
    SELECT
        movement_type,
        COUNT(*) AS movement_count,
        SUM(quantity) AS total_quantity
    FROM {catalog}.{schema}.silver_stock_movements
    GROUP BY movement_type
    ORDER BY movement_type
    """
)

display(df_movement_summary)

# COMMAND ----------

df_opening_summary = spark.sql(
    f"""
    SELECT
        warehouse_id,
        product_id,
        SUM(opening_quantity) AS opening_inventory,
        COUNT(DISTINCT batch_id) AS batch_count,
        MIN(expiry_date) AS earliest_expiry_date,
        MAX(expiry_date) AS latest_expiry_date
    FROM {catalog}.{schema}.silver_opening_inventory
    GROUP BY
        warehouse_id,
        product_id
    """
)

display(df_opening_summary)

# COMMAND ----------

df_inventory_movement = spark.sql(
    f"""
    SELECT
        warehouse_id,
        product_id,

        SUM(
            CASE
                WHEN movement_type = 'RECEIPT'
                    THEN quantity

                WHEN movement_type = 'ISSUE'
                    THEN -quantity

                ELSE 0
            END
        ) AS net_movement,

        SUM(
            CASE
                WHEN movement_type = 'RECEIPT'
                    THEN quantity
                ELSE 0
            END
        ) AS total_receipt_quantity,

        SUM(
            CASE
                WHEN movement_type = 'ISSUE'
                    THEN quantity
                ELSE 0
            END
        ) AS total_issue_quantity,

        COUNT(*) AS movement_count,

      MAX(event_time) AS latest_movement_time

    FROM {catalog}.{schema}.silver_stock_movements

    GROUP BY
        warehouse_id,
        product_id
    """
)

display(df_inventory_movement)

# COMMAND ----------

df_inventory_position = (
    df_opening_summary.alias("opening")
    .join(
        df_inventory_movement.alias("movement"),
        on=["warehouse_id", "product_id"],
        how="left"
    )
    .select(
        F.col("warehouse_id"),
        F.col("product_id"),
        F.col("opening.opening_inventory").alias(
            "opening_inventory"
        ),
        F.col("opening.batch_count").alias(
            "batch_count"
        ),
        F.col("opening.earliest_expiry_date").alias(
            "earliest_expiry_date"
        ),
        F.col("opening.latest_expiry_date").alias(
            "latest_expiry_date"
        ),
        F.coalesce(
            F.col("movement.net_movement"),
            F.lit(0)
        ).alias("net_movement"),
        F.coalesce(
            F.col("movement.total_receipt_quantity"),
            F.lit(0)
        ).alias("total_receipt_quantity"),
        F.coalesce(
            F.col("movement.total_issue_quantity"),
            F.lit(0)
        ).alias("total_issue_quantity"),
        F.coalesce(
            F.col("movement.movement_count"),
            F.lit(0)
        ).alias("movement_count"),
        F.col("movement.latest_movement_time").alias(
            "latest_movement_time"
        )
    )
    .withColumn(
        "current_inventory",
        F.col("opening_inventory")
        + F.col("net_movement")
    )
)

display(df_inventory_position)

# COMMAND ----------

negative_inventory_df = (
    df_inventory_position
    .filter(
        F.col("current_inventory") < 0
    )
)

negative_inventory_count = (
    negative_inventory_df.count()
)

print(
    "Negative inventory positions:",
    negative_inventory_count
)

display(negative_inventory_df)

# COMMAND ----------

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    {catalog}.{schema}.gold_inventory_position
    """
)

(
    df_inventory_position.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.gold_inventory_position"
    )
)

print(
    "Created:",
    f"{catalog}.{schema}.gold_inventory_position"
)

# COMMAND ----------

display(
    spark.table(
        f"{catalog}.{schema}.gold_inventory_position"
    )
)

# COMMAND ----------

df_daily_demand_totals = (
    spark.table(
        f"{catalog}.{schema}.silver_daily_demand"
    )
    .groupBy(
        "date",
        "warehouse_id",
        "product_id"
    )
    .agg(
        F.sum("units_requested").alias(
            "daily_units_requested"
        )
    )
)

display(df_daily_demand_totals)

# COMMAND ----------

df_avg_demand = (
    df_daily_demand_totals
    .groupBy(
        "warehouse_id",
        "product_id"
    )
    .agg(
        F.round(
            F.avg("daily_units_requested"),
            2
        ).alias("avg_daily_demand"),
        F.max("daily_units_requested").alias(
            "peak_daily_demand"
        ),
        F.sum("daily_units_requested").alias(
            "total_demand"
        ),
        F.countDistinct("date").alias(
            "demand_days"
        ),
        F.max("date").alias(
            "latest_demand_date"
        )
    )
)

display(df_avg_demand)

# COMMAND ----------

supplier_priority_window = (
    Window
    .partitionBy("product_id")
    .orderBy(
        F.col("is_preferred").desc(),
        F.col("unit_cost").asc(),
        F.col("lead_time_days").asc(),
        F.col("supplier_id").asc()
    )
)

df_preferred_supplier = (
    spark.table(
        f"{catalog}.{schema}.silver_supplier_products"
    )
    .withColumn(
        "supplier_rank",
        F.row_number().over(
            supplier_priority_window
        )
    )
    .filter(
        F.col("supplier_rank") == 1
    )
    .select(
        "product_id",
        "supplier_id",
        "lead_time_days",
        "unit_cost",
        "minimum_order_quantity",
        "is_preferred"
    )
)

display(df_preferred_supplier)

# COMMAND ----------

df_products = (
    spark.table(
        f"{catalog}.{schema}.silver_products"
    )
    .select(
        "product_id",
        "product_name",
        "category"
    )
)

df_warehouses = (
    spark.table(
        f"{catalog}.{schema}.silver_warehouses"
    )
    .select(
        "warehouse_id",
        "warehouse_name"
    )
)

df_suppliers = (
    spark.table(
        f"{catalog}.{schema}.silver_suppliers"
    )
    .select(
        "supplier_id",
        "supplier_name",
        "supplier_status"
    )
)


# COMMAND ----------

df_replenishment_base = (
    df_inventory_position.alias("inventory")
    .join(
        df_avg_demand.alias("demand"),
        on=["warehouse_id", "product_id"],
        how="left"
    )
    .join(
        df_preferred_supplier.alias("supplier_product"),
        on="product_id",
        how="left"
    )
    .join(
        df_products.alias("product"),
        on="product_id",
        how="left"
    )
    .join(
        df_warehouses.alias("warehouse"),
        on="warehouse_id",
        how="left"
    )
    .join(
        df_suppliers.alias("supplier"),
        on="supplier_id",
        how="left"
    )
)

# COMMAND ----------

df_replenishment = (
    df_replenishment_base
    .withColumn(
        "avg_daily_demand",
        F.coalesce(
            F.col("avg_daily_demand"),
            F.lit(0.0)
        )
    )
    .withColumn(
        "peak_daily_demand",
        F.coalesce(
            F.col("peak_daily_demand"),
            F.lit(0)
        )
    )
    .withColumn(
        "total_demand",
        F.coalesce(
            F.col("total_demand"),
            F.lit(0)
        )
    )
    .withColumn(
        "demand_days",
        F.coalesce(
            F.col("demand_days"),
            F.lit(0)
        )
    )
    .withColumn(
        "reorder_point",
        F.when(
            F.col("lead_time_days").isNotNull(),
            F.ceil(
                F.col("avg_daily_demand")
                * F.col("lead_time_days")
            )
        )
    )
    .withColumn(
        "days_of_supply",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.round(
                F.col("current_inventory")
                / F.col("avg_daily_demand"),
                2
            )
        )
    )
    .withColumn(
        "reorder_status",
        F.when(
            F.col("supplier_id").isNull(),
            F.lit("SUPPLIER_NOT_AVAILABLE")
        )
        .when(
            F.col("avg_daily_demand") == 0,
            F.lit("NO_DEMAND")
        )
        .when(
            F.col("current_inventory")
            <= F.col("reorder_point"),
            F.lit("REORDER")
        )
        .otherwise(
            F.lit("NO_REORDER")
        )
    )
    .withColumn(
        "suggested_order_quantity",
        F.when(
            F.col("reorder_status") == "REORDER",
            F.greatest(
                F.ceil(
                    F.col("reorder_point")
                    - F.col("current_inventory")
                ),
                F.col("minimum_order_quantity")
            )
        )
        .otherwise(
            F.lit(0)
        )
    )
    .select(
        "warehouse_id",
        "warehouse_name",
        "product_id",
        "product_name",
        "category",
        "supplier_id",
        "supplier_name",
        "supplier_status",
        "opening_inventory",
        "total_receipt_quantity",
        "total_issue_quantity",
        "net_movement",
        "current_inventory",
        "batch_count",
        "earliest_expiry_date",
        "latest_expiry_date",
        "latest_movement_time",
        "avg_daily_demand",
        "peak_daily_demand",
        "total_demand",
        "demand_days",
        "latest_demand_date",
        "lead_time_days",
        "unit_cost",
        "minimum_order_quantity",
        "reorder_point",
        "days_of_supply",
        "reorder_status",
        F.col(
            "suggested_order_quantity"
        ).cast("long").alias(
            "suggested_order_quantity"
        )
    )
)

display(df_replenishment)

# COMMAND ----------

df_replenishment_summary = (
    df_replenishment
    .groupBy("reorder_status")
    .agg(
        F.count("*").alias(
            "position_count"
        ),
        F.sum(
            "suggested_order_quantity"
        ).alias(
            "total_suggested_quantity"
        )
    )
    .orderBy("reorder_status")
)

display(df_replenishment_summary)

# COMMAND ----------

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    {catalog}.{schema}.gold_replenishment
    """
)

(
    df_replenishment.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.gold_replenishment"
    )
)

print(
    "Created:",
    f"{catalog}.{schema}.gold_replenishment"
)

# COMMAND ----------

df_inventory_summary = (
    df_replenishment
    .groupBy(
        "warehouse_id",
        "warehouse_name"
    )
    .agg(
        F.countDistinct("product_id").alias(
            "product_count"
        ),
        F.sum("opening_inventory").alias(
            "total_opening_inventory"
        ),
        F.sum("total_receipt_quantity").alias(
            "total_receipt_quantity"
        ),
        F.sum("total_issue_quantity").alias(
            "total_issue_quantity"
        ),
        F.sum("current_inventory").alias(
            "total_current_inventory"
        ),
        F.round(
            F.avg("current_inventory"),
            2
        ).alias(
            "avg_current_inventory"
        ),
        F.sum(
            F.when(
                F.col("reorder_status") == "REORDER",
                1
            ).otherwise(0)
        ).alias(
            "reorder_product_count"
        ),
        F.sum(
            F.when(
                F.col("reorder_status") == "NO_DEMAND",
                1
            ).otherwise(0)
        ).alias(
            "zero_demand_product_count"
        ),
        F.sum(
            "suggested_order_quantity"
        ).alias(
            "total_suggested_order_quantity"
        )
    )
    .orderBy("warehouse_id")
)

display(df_inventory_summary)

# COMMAND ----------

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    {catalog}.{schema}.gold_inventory_summary
    """
)

(
    df_inventory_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.gold_inventory_summary"
    )
)

print(
    "Created:",
    f"{catalog}.{schema}.gold_inventory_summary"
)

# COMMAND ----------

df_kpi = (
    df_replenishment
    .agg(
        F.count("*").alias(
            "total_inventory_positions"
        ),
        F.countDistinct("warehouse_id").alias(
            "total_warehouses"
        ),
        F.countDistinct("product_id").alias(
            "total_products"
        ),
        F.sum(
            F.when(
                F.col("reorder_status") == "REORDER",
                1
            ).otherwise(0)
        ).alias(
            "reorder_positions"
        ),
        F.sum(
            F.when(
                F.col("reorder_status") == "NO_REORDER",
                1
            ).otherwise(0)
        ).alias(
            "no_reorder_positions"
        ),
        F.sum(
            F.when(
                F.col("reorder_status") == "NO_DEMAND",
                1
            ).otherwise(0)
        ).alias(
            "zero_demand_positions"
        ),
        F.sum(
            F.when(
                F.col("reorder_status")
                == "SUPPLIER_NOT_AVAILABLE",
                1
            ).otherwise(0)
        ).alias(
            "supplier_not_available_positions"
        ),
        F.sum(
            "suggested_order_quantity"
        ).alias(
            "total_suggested_order_quantity"
        ),
        F.sum(
            "current_inventory"
        ).alias(
            "total_current_inventory"
        ),
        F.sum(
            "opening_inventory"
        ).alias(
            "total_opening_inventory"
        ),
        F.sum(
            "total_receipt_quantity"
        ).alias(
            "total_receipt_quantity"
        ),
        F.sum(
            "total_issue_quantity"
        ).alias(
            "total_issue_quantity"
        ),
        F.round(
            F.avg("current_inventory"),
            2
        ).alias(
            "avg_inventory_per_position"
        )
    )
    .withColumn(
        "reorder_percentage",
        F.when(
            F.col("total_inventory_positions") > 0,
            F.round(
                (
                    F.col("reorder_positions")
                    / F.col("total_inventory_positions")
                ) * 100,
                2
            )
        )
        .otherwise(
            F.lit(0.0)
        )
    )
    .withColumn(
        "generated_at",
        F.current_timestamp()
    )
)

display(df_kpi)

# COMMAND ----------

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    {catalog}.{schema}.gold_kpi
    """
)

(
    df_kpi.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.gold_kpi"
    )
)

print(
    "Created:",
    f"{catalog}.{schema}.gold_kpi"
)

# COMMAND ----------

gold_tables = [
    "gold_inventory_position",
    "gold_replenishment",
    "gold_inventory_summary",
    "gold_kpi"
]

for table_name in gold_tables:

    full_table_name = (
        f"{catalog}.{schema}.{table_name}"
    )

    row_count = (
        spark.table(full_table_name).count()
    )

    print(
        f"{table_name}: {row_count} rows"
    )

# COMMAND ----------

display(
    spark.sql(
        f"""
        SHOW TABLES IN {catalog}.{schema}
        """
    )
    .filter(
        F.col("tableName").startswith("gold_")
    )
)

# COMMAND ----------

display(
    spark.table(
        f"{catalog}.{schema}.gold_kpi"
    )
)

# COMMAND ----------


gold_tables = [
    "gold_inventory_position",
    "gold_replenishment",
    "gold_inventory_summary",
    "gold_kpi"
]

for table_name in gold_tables:
    full_name = f"{catalog}.{schema}.{table_name}"

    print(f"\n{table_name}")

    if spark.catalog.tableExists(full_name):
        df = spark.table(full_name)
        print("Rows:", df.count())
        print("Columns:", df.columns)
    else:
        print("TABLE MISSING")


# COMMAND ----------


print("Comparing Gold tables with refreshed DataFrames...")

checks = [
    (df_inventory_position, "gold_inventory_position"),
    (df_replenishment, "gold_replenishment"),
    (df_inventory_summary, "gold_inventory_summary"),
]

for refreshed_df, table_name in checks:
    saved_df = spark.table(f"{catalog}.{schema}.{table_name}")

    refreshed_count = refreshed_df.count()
    saved_count = saved_df.count()

    print(f"\n{table_name}")
    print("Refreshed DataFrame rows:", refreshed_count)
    print("Saved Gold table rows:", saved_count)

    if refreshed_count == saved_count:
        print("Row counts match; checking data differences...")

        columns = saved_df.columns
        saved = saved_df.select(*columns)
        refreshed = refreshed_df.select(*columns)

        print(
            "Differences (saved vs refreshed):",
            saved.exceptAll(refreshed).count()
        )
        print(
            "Differences (refreshed vs saved):",
            refreshed.exceptAll(saved).count()
        )
    else:
        print("Row counts differ; Gold needs refreshing.")


# COMMAND ----------


(
    df_inventory_position.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(
        f"{catalog}.{schema}.gold_inventory_position"
    )
)

print("gold_inventory_position refreshed successfully.")


# COMMAND ----------


saved_position = spark.table(
    f"{catalog}.{schema}.gold_inventory_position"
)

print(
    "Saved vs refreshed differences:",
    saved_position.exceptAll(
        df_inventory_position.select(*saved_position.columns)
    ).count()
)

print(
    "Refreshed vs saved differences:",
    df_inventory_position.exceptAll(
        saved_position.select(*df_inventory_position.columns)
    ).count()
)


# COMMAND ----------


saved_kpi = spark.table(
    f"{catalog}.{schema}.gold_kpi"
)

print("Saved KPI rows:", saved_kpi.count())
print("Refreshed KPI rows:", df_kpi.count())

print("Saved KPI:")
display(saved_kpi)

print("Refreshed KPI:")
display(df_kpi)


# COMMAND ----------

saved_kpi = spark.table(
    f"{catalog}.{schema}.gold_kpi"
)

# Compare all KPI columns except generated_at
compare_columns = [
    c for c in df_kpi.columns
    if c != "generated_at"
]

saved_values = saved_kpi.select(compare_columns)
refreshed_values = df_kpi.select(compare_columns)

print(
    "Saved KPI matches refreshed KPI:",
    saved_values.exceptAll(refreshed_values).count() == 0
    and refreshed_values.exceptAll(saved_values).count() == 0
)

# COMMAND ----------

