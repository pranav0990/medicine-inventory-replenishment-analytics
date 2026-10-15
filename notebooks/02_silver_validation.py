# Databricks notebook source
catalog = "medicine_inventory"
schema = "inventory"

volume_root = (
    "/Volumes/medicine_inventory/"
    "inventory/inventory_files"
)

landing_path = f"{volume_root}/landing"
incremental_path = f"{volume_root}/incremental"
checkpoint_path = f"{volume_root}/checkpoint"
quarantine_path = f"{volume_root}/quarantine"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("Catalog:", catalog)
print("Schema:", schema)
print("Volume:", volume_root)
print("Quarantine:", quarantine_path)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType
)

# COMMAND ----------

required_bronze_tables = [
    "bronze_suppliers",
    "bronze_warehouses",
    "bronze_products",
    "bronze_supplier_product",
    "bronze_opening_inventory",
    "bronze_stock_movements",
    "bronze_daily_demand",
    "bronze_supplier_lead_times"
]

missing_tables = []

for table_name in required_bronze_tables:

    full_name = f"{catalog}.{schema}.{table_name}"

    if spark.catalog.tableExists(full_name):
        row_count = spark.table(full_name).count()

        print(
            f"Found: {table_name}"
            f" | Rows: {row_count}"
        )
    else:
        missing_tables.append(table_name)

if missing_tables:
    raise RuntimeError(
        "Missing Bronze tables: "
        + ", ".join(missing_tables)
    )

print("\nAll required Bronze tables are available.")

# COMMAND ----------

def is_blank(column_name):

    return (
        F.col(column_name).isNull()
        |
        (
            F.trim(
                F.col(column_name)
            ) == ""
        )
    )


# COMMAND ----------


def write_validation_results(
    valid_df,
    quarantine_df,
    silver_table,
    quarantine_table
):

    silver_full_name = (
        f"{catalog}.{schema}.{silver_table}"
    )

    quarantine_full_name = (
        f"{catalog}.{schema}.{quarantine_table}"
    )

    # Protect existing incremental stock movement data.
    # These tables must be updated by the incremental
    # processing notebook, not replaced by baseline data.
    if silver_table == "silver_stock_movements":
        print(
            "Skipping overwrite of silver_stock_movements. "
            "Existing incremental records are preserved."
        )
        return

    if quarantine_table == "quarantine_stock_movements":
        print(
            "Skipping overwrite of quarantine_stock_movements. "
            "Existing quarantine records are preserved."
        )
        return

    # Rebuild other validation tables as before.
    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_full_name)
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(quarantine_full_name)
    )

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()

    print(
        f"Created {silver_table}: "
        f"{valid_count} rows"
    )

    print(
        f"Created {quarantine_table}: "
        f"{quarantine_count} rows"
    )


# COMMAND ----------

bronze_suppliers = spark.table(
    f"{catalog}.{schema}.bronze_suppliers"
)

supplier_window = Window.partitionBy(
    "supplier_id"
).orderBy(
    F.monotonically_increasing_id()
)

suppliers_checked = (
    bronze_suppliers
    .withColumn(
        "_duplicate_rank",
        F.row_number().over(supplier_window)
    )
    .withColumn(
        "reject_reason",
        F.when(
            is_blank("supplier_id"),
            F.lit("MISSING_SUPPLIER_ID")
        )
        .when(
            is_blank("supplier_name"),
            F.lit("MISSING_SUPPLIER_NAME")
        )
        .when(
            ~F.col("supplier_status").isin(
                "ACTIVE",
                "INACTIVE"
            ),
            F.lit("INVALID_SUPPLIER_STATUS")
        )
        .when(
            F.col("_duplicate_rank") > 1,
            F.lit("DUPLICATE_SUPPLIER_ID")
        )
    )
)

silver_suppliers = (
    suppliers_checked
    .filter(F.col("reject_reason").isNull())
    .drop("_duplicate_rank", "reject_reason")
)

quarantine_suppliers = (
    suppliers_checked
    .filter(F.col("reject_reason").isNotNull())
    .drop("_duplicate_rank")
)

write_validation_results(
    silver_suppliers,
    quarantine_suppliers,
    "silver_suppliers",
    "quarantine_suppliers"
)

display(quarantine_suppliers)

# COMMAND ----------

bronze_warehouses = spark.table(
    f"{catalog}.{schema}.bronze_warehouses"
)

warehouse_window = Window.partitionBy(
    "warehouse_id"
).orderBy(
    F.monotonically_increasing_id()
)

warehouses_checked = (
    bronze_warehouses
    .withColumn(
        "_duplicate_rank",
        F.row_number().over(warehouse_window)
    )
    .withColumn(
        "reject_reason",
        F.when(
            is_blank("warehouse_id"),
            F.lit("MISSING_WAREHOUSE_ID")
        )
        .when(
            is_blank("warehouse_name"),
            F.lit("MISSING_WAREHOUSE_NAME")
        )
        .when(
            F.col("_duplicate_rank") > 1,
            F.lit("DUPLICATE_WAREHOUSE_ID")
        )
    )
)

silver_warehouses = (
    warehouses_checked
    .filter(F.col("reject_reason").isNull())
    .drop("_duplicate_rank", "reject_reason")
)

quarantine_warehouses = (
    warehouses_checked
    .filter(F.col("reject_reason").isNotNull())
    .drop("_duplicate_rank")
)

write_validation_results(
    silver_warehouses,
    quarantine_warehouses,
    "silver_warehouses",
    "quarantine_warehouses"
)

display(quarantine_warehouses)

# COMMAND ----------

allowed_categories = [
    "Analgesic",
    "Antibiotic",
    "Antihistamine",
    "Antiviral",
    "Cardiovascular"
]

bronze_products = spark.table(
    f"{catalog}.{schema}.bronze_products"
)

valid_supplier_ids = (
    silver_suppliers
    .select("supplier_id")
    .withColumn(
        "_supplier_exists",
        F.lit(True)
    )
)

product_window = Window.partitionBy(
    "product_id"
).orderBy(
    F.monotonically_increasing_id()
)

products_checked = (
    bronze_products
    .join(
        valid_supplier_ids,
        on="supplier_id",
        how="left"
    )
    .withColumn(
        "_duplicate_rank",
        F.row_number().over(product_window)
    )
    .withColumn(
        "reject_reason",
        F.when(
            is_blank("product_id"),
            F.lit("MISSING_PRODUCT_ID")
        )
        .when(
            is_blank("product_name"),
            F.lit("MISSING_PRODUCT_NAME")
        )
        .when(
            is_blank("category"),
            F.lit("MISSING_CATEGORY")
        )
        .when(
            is_blank("supplier_id"),
            F.lit("MISSING_SUPPLIER_ID")
        )
        .when(
            ~F.col("category").isin(
                allowed_categories
            ),
            F.lit("INVALID_CATEGORY")
        )
        .when(
            F.col("_supplier_exists").isNull(),
            F.lit("UNKNOWN_SUPPLIER")
        )
        .when(
            F.col("_duplicate_rank") > 1,
            F.lit("DUPLICATE_PRODUCT_ID")
        )
    )
)

silver_products = (
    products_checked
    .filter(F.col("reject_reason").isNull())
    .drop(
        "_supplier_exists",
        "_duplicate_rank",
        "reject_reason"
    )
)

quarantine_products = (
    products_checked
    .filter(F.col("reject_reason").isNotNull())
    .drop(
        "_supplier_exists",
        "_duplicate_rank"
    )
)

write_validation_results(
    silver_products,
    quarantine_products,
    "silver_products",
    "quarantine_products"
)

display(quarantine_products)

# COMMAND ----------

bronze_supplier_products = spark.table(
    f"{catalog}.{schema}.bronze_supplier_product"
)

supplier_reference = (
    silver_suppliers
    .select("supplier_id")
    .withColumn(
        "_supplier_exists",
        F.lit(True)
    )
)

product_reference = (
    silver_products
    .select("product_id")
    .withColumn(
        "_product_exists",
        F.lit(True)
    )
)

relationship_window = (
    Window
    .partitionBy(
        "supplier_id",
        "product_id"
    )
    .orderBy(
        F.monotonically_increasing_id()
    )
)

supplier_products_checked = (
    bronze_supplier_products
    .withColumn(
        "_lead_time",
        F.col("lead_time_days").cast("integer")
    )
    .withColumn(
        "_unit_cost",
        F.col("unit_cost").cast("decimal(18,2)")
    )
    .withColumn(
        "_minimum_order_quantity",
        F.col("minimum_order_quantity").cast("integer")
    )
    .withColumn(
        "_is_preferred",
        F.col("is_preferred").cast("boolean")
    )
    .join(
        supplier_reference,
        on="supplier_id",
        how="left"
    )
    .join(
        product_reference,
        on="product_id",
        how="left"
    )
    .withColumn(
        "_duplicate_rank",
        F.row_number().over(relationship_window)
    )
    .withColumn(
        "reject_reason",
        F.when(
            is_blank("supplier_id"),
            F.lit("MISSING_SUPPLIER_ID")
        )
        .when(
            is_blank("product_id"),
            F.lit("MISSING_PRODUCT_ID")
        )
        .when(
            F.col("_supplier_exists").isNull(),
            F.lit("UNKNOWN_SUPPLIER")
        )
        .when(
            F.col("_product_exists").isNull(),
            F.lit("UNKNOWN_PRODUCT")
        )
        .when(
            F.col("_lead_time").isNull(),
            F.lit("INVALID_LEAD_TIME")
        )
        .when(
            F.col("_lead_time") < 0,
            F.lit("NEGATIVE_LEAD_TIME")
        )
        .when(
            F.col("_unit_cost").isNull()
            |
            (F.col("_unit_cost") <= 0),
            F.lit("INVALID_UNIT_COST")
        )
        .when(
            F.col("_minimum_order_quantity").isNull()
            |
            (
                F.col("_minimum_order_quantity") <= 0
            ),
            F.lit("INVALID_MINIMUM_ORDER_QUANTITY")
        )
        .when(
            F.col("_duplicate_rank") > 1,
            F.lit(
                "DUPLICATE_SUPPLIER_PRODUCT_RELATIONSHIP"
            )
        )
    )
)

silver_supplier_products = (
    supplier_products_checked
    .filter(F.col("reject_reason").isNull())
    .select(
        "supplier_id",
        "product_id",
        F.col("_lead_time").alias(
            "lead_time_days"
        ),
        F.col("_unit_cost").alias(
            "unit_cost"
        ),
        F.col("_minimum_order_quantity").alias(
            "minimum_order_quantity"
        ),
        F.col("_is_preferred").alias(
            "is_preferred"
        )
    )
)

quarantine_supplier_products = (
    supplier_products_checked
    .filter(F.col("reject_reason").isNotNull())
    .drop(
        "_supplier_exists",
        "_product_exists",
        "_duplicate_rank"
    )
)

write_validation_results(
    silver_supplier_products,
    quarantine_supplier_products,
    "silver_supplier_products",
    "quarantine_supplier_products"
)

display(quarantine_supplier_products)

# COMMAND ----------

# ============================================================
# SILVER VALIDATION - OPENING INVENTORY
# ============================================================

bronze_opening_inventory = spark.table(
    f"{catalog}.{schema}.bronze_opening_inventory"
)


# ------------------------------------------------------------
# Reference: valid warehouses from Silver
# ------------------------------------------------------------

warehouse_reference = (
    silver_warehouses
    .select("warehouse_id")
    .withColumn(
        "_warehouse_exists",
        F.lit(True)
    )
)


# ------------------------------------------------------------
# Reference: valid products from Silver
# ------------------------------------------------------------

product_reference = (
    silver_products
    .select("product_id")
    .withColumn(
        "_product_exists",
        F.lit(True)
    )
)


# ------------------------------------------------------------
# Duplicate key detection
#
# Duplicate key:
# warehouse_id + product_id + batch_id
# ------------------------------------------------------------

inventory_window = (
    Window
    .partitionBy(
        "warehouse_id",
        "product_id",
        "batch_id"
    )
    .orderBy(
        F.monotonically_increasing_id()
    )
)


# ------------------------------------------------------------
# Validate opening inventory
# ------------------------------------------------------------

opening_inventory_checked = (
    bronze_opening_inventory

    # Safely convert opening_quantity to integer.
    # Invalid numeric values become NULL instead of failing.
    .withColumn(
        "_opening_quantity",
        F.expr(
            "try_cast(opening_quantity AS INT)"
        )
    )

    # Safely convert expiry_date to DATE.
    # Example:
    # 2027-09-05  -> valid DATE
    # not-a-date  -> NULL
    .withColumn(
        "_expiry_date",
        F.expr(
            "try_cast(expiry_date AS DATE)"
        )
    )

    # Check warehouse reference
    .join(
        warehouse_reference,
        on="warehouse_id",
        how="left"
    )

    # Check product reference
    .join(
        product_reference,
        on="product_id",
        how="left"
    )

    # Detect duplicate warehouse/product/batch combinations
    .withColumn(
        "_duplicate_rank",
        F.row_number().over(
            inventory_window
        )
    )

    # Assign validation failure reason
    .withColumn(
        "reject_reason",

        # Missing warehouse
        F.when(
            is_blank("warehouse_id"),
            F.lit("MISSING_WAREHOUSE_ID")
        )

        # Warehouse doesn't exist in Silver warehouse reference
        .when(
            F.col("_warehouse_exists").isNull(),
            F.lit("UNKNOWN_WAREHOUSE")
        )

        # Missing product
        .when(
            is_blank("product_id"),
            F.lit("MISSING_PRODUCT_ID")
        )

        # Product doesn't exist in Silver product reference
        .when(
            F.col("_product_exists").isNull(),
            F.lit("UNKNOWN_PRODUCT")
        )

        # Missing batch
        .when(
            is_blank("batch_id"),
            F.lit("MISSING_BATCH_ID")
        )

        # Invalid expiry date
        .when(
            F.col("_expiry_date").isNull(),
            F.lit("INVALID_EXPIRY_DATE")
        )

        # opening_quantity couldn't be converted to integer
        .when(
            F.col("_opening_quantity").isNull(),
            F.lit("INVALID_OPENING_QUANTITY")
        )

        # Negative inventory is invalid
        .when(
            F.col("_opening_quantity") < 0,
            F.lit("NEGATIVE_OPENING_QUANTITY")
        )

        # Duplicate composite inventory key
        .when(
            F.col("_duplicate_rank") > 1,
            F.lit("DUPLICATE_INVENTORY_BATCH_KEY")
        )
    )
)


# ============================================================
# VALID RECORDS -> SILVER
# ============================================================

silver_opening_inventory = (
    opening_inventory_checked

    .filter(
        F.col("reject_reason").isNull()
    )

    .select(
        "warehouse_id",
        "product_id",
        "batch_id",

        F.col("_opening_quantity").alias(
            "opening_quantity"
        ),

        F.col("_expiry_date").alias(
            "expiry_date"
        )
    )
)


# ============================================================
# INVALID RECORDS -> QUARANTINE
# ============================================================

quarantine_opening_inventory = (
    opening_inventory_checked

    .filter(
        F.col("reject_reason").isNotNull()
    )

    # Keep original/raw values so we can see exactly
    # what was wrong with the source record.
    .select(
        "warehouse_id",
        "product_id",
        "batch_id",
        "opening_quantity",
        "expiry_date",
        "reject_reason"
    )
)


# ============================================================
# WRITE SILVER + QUARANTINE TABLES
# ============================================================

write_validation_results(
    silver_opening_inventory,
    quarantine_opening_inventory,
    "silver_opening_inventory",
    "quarantine_opening_inventory"
)


# ============================================================
# DISPLAY QUARANTINED RECORDS
# ============================================================

display(
    quarantine_opening_inventory
)

# COMMAND ----------

def validate_movements(input_df, source_name):

    # ============================================================
    # Columns that describe the actual movement payload
    # ============================================================

    movement_columns = [
        "warehouse_id",
        "product_id",
        "batch_id",
        "movement_type",
        "quantity",
        "event_time"
    ]


    # ============================================================
    # Create payload string for duplicate detection
    # ============================================================

    payload_expression = F.concat_ws(
        "||",
        *[
            F.coalesce(
                F.col(column_name).cast("string"),
                F.lit("<NULL>")
            )
            for column_name in movement_columns
        ]
    )


    # ============================================================
    # Window for detecting repeated movement IDs
    # ============================================================

    movement_id_window = (
        Window
        .partitionBy("movement_id")
    )


    # ============================================================
    # Window for detecting exact duplicates
    # ============================================================

    exact_duplicate_window = (
        Window
        .partitionBy(
            "movement_id",
            "_payload"
        )
        .orderBy(
            F.monotonically_increasing_id()
        )
    )


    # ============================================================
    # Valid warehouse reference
    # ============================================================

    warehouse_reference = (
        silver_warehouses
        .select("warehouse_id")
        .withColumn(
            "_warehouse_exists",
            F.lit(True)
        )
    )


    # ============================================================
    # Valid product reference
    # ============================================================

    product_reference = (
        silver_products
        .select("product_id")
        .withColumn(
            "_product_exists",
            F.lit(True)
        )
    )


    # ============================================================
    # Valid inventory batch reference
    # ============================================================

    inventory_reference = (
        silver_opening_inventory
        .select(
            "warehouse_id",
            "product_id",
            "batch_id",
            "opening_quantity"
        )
        .withColumn(
            "_inventory_key_exists",
            F.lit(True)
        )
    )


    # ============================================================
    # BASIC VALIDATION
    # ============================================================

    checked = (
        input_df

        # Keep track of where record came from
        .withColumn(
            "_source_name",
            F.lit(source_name)
        )

        # --------------------------------------------------------
        # SAFE quantity conversion
        #
        # "20"  -> 20
        # "-5"  -> -5
        # bad   -> NULL
        # --------------------------------------------------------

        .withColumn(
            "_quantity",
            F.expr(
                "try_cast(quantity AS INT)"
            )
        )

        # --------------------------------------------------------
        # SAFE timestamp conversion
        #
        # Valid timestamp
        #     -> TIMESTAMP
        #
        # "not-a-timestamp"
        #     -> NULL
        #
        # This prevents the notebook from crashing.
        # --------------------------------------------------------

        .withColumn(
            "_event_time",
            F.expr(
                "try_cast(event_time AS TIMESTAMP)"
            )
        )

        # Payload used to compare duplicate movement records
        .withColumn(
            "_payload",
            payload_expression
        )

        # Number of records having same movement_id
        .withColumn(
            "_movement_id_count",
            F.count("*").over(
                movement_id_window
            )
        )

        # Number of different payloads for same movement_id
        .withColumn(
            "_payload_variant_count",
            F.size(
                F.collect_set("_payload").over(
                    movement_id_window
                )
            )
        )

        # Rank identical records
        .withColumn(
            "_exact_duplicate_rank",
            F.row_number().over(
                exact_duplicate_window
            )
        )

        # --------------------------------------------------------
        # Warehouse validation
        # --------------------------------------------------------

        .join(
            warehouse_reference,
            on="warehouse_id",
            how="left"
        )

        # --------------------------------------------------------
        # Product validation
        # --------------------------------------------------------

        .join(
            product_reference,
            on="product_id",
            how="left"
        )

        # --------------------------------------------------------
        # Inventory/batch validation
        # --------------------------------------------------------

        .join(
            inventory_reference,
            on=[
                "warehouse_id",
                "product_id",
                "batch_id"
            ],
            how="left"
        )

        # ========================================================
        # ASSIGN REJECTION REASON
        # ========================================================

        .withColumn(
            "reject_reason",

            # Missing movement ID
            F.when(
                is_blank("movement_id"),
                F.lit("MISSING_MOVEMENT_ID")
            )

            # Missing warehouse
            .when(
                is_blank("warehouse_id"),
                F.lit("MISSING_WAREHOUSE_ID")
            )

            # Unknown warehouse
            .when(
                F.col("_warehouse_exists").isNull(),
                F.lit("UNKNOWN_WAREHOUSE")
            )

            # Missing product
            .when(
                is_blank("product_id"),
                F.lit("MISSING_PRODUCT_ID")
            )

            # Unknown product
            .when(
                F.col("_product_exists").isNull(),
                F.lit("UNKNOWN_PRODUCT")
            )

            # Missing batch
            .when(
                is_blank("batch_id"),
                F.lit("MISSING_BATCH_ID")
            )

            # Batch doesn't exist in opening inventory
            .when(
                F.col("_inventory_key_exists").isNull(),
                F.lit("UNKNOWN_INVENTORY_BATCH")
            )

            # Invalid movement type
            .when(
                ~F.col("movement_type").isin(
                    "ISSUE",
                    "RECEIPT"
                ),
                F.lit("INVALID_MOVEMENT_TYPE")
            )

            # Quantity could not be converted
            .when(
                F.col("_quantity").isNull(),
                F.lit("INVALID_QUANTITY")
            )

            # Zero quantity
            .when(
                F.col("_quantity") == 0,
                F.lit("ZERO_QUANTITY")
            )

            # Negative quantity
            .when(
                F.col("_quantity") < 0,
                F.lit("NEGATIVE_QUANTITY")
            )

            # Invalid timestamp
            #
            # not-a-timestamp -> NULL -> quarantine
            .when(
                F.col("_event_time").isNull(),
                F.lit("INVALID_EVENT_TIMESTAMP")
            )

            # Same ID but different contents
            .when(
                (
                    F.col("_movement_id_count") > 1
                )
                &
                (
                    F.col("_payload_variant_count") > 1
                ),
                F.lit(
                    "CONFLICTING_DUPLICATE_MOVEMENT_ID"
                )
            )

            # Same ID and same contents
            .when(
                (
                    F.col("_movement_id_count") > 1
                )
                &
                (
                    F.col("_payload_variant_count") == 1
                )
                &
                (
                    F.col("_exact_duplicate_rank") > 1
                ),
                F.lit(
                    "EXACT_DUPLICATE_MOVEMENT"
                )
            )
        )
    )


    # ============================================================
    # Keep records that passed basic validation
    # ============================================================

    basic_valid = (
        checked
        .filter(
            F.col("reject_reason").isNull()
        )
    )


    # ============================================================
    # STOCK CALCULATION WINDOW
    #
    # Calculate stock movement in timestamp order
    # for each warehouse/product/batch.
    # ============================================================

    movement_order_window = (
        Window
        .partitionBy(
            "warehouse_id",
            "product_id",
            "batch_id"
        )
        .orderBy(
            "_event_time",
            "movement_id"
        )
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )


    # ============================================================
    # CHECK WHETHER MOVEMENT WOULD MAKE STOCK NEGATIVE
    # ============================================================

    final_checked = (
        basic_valid

        # RECEIPT adds stock
        # ISSUE removes stock
        .withColumn(
            "_signed_quantity",

            F.when(
                F.col("movement_type") == "RECEIPT",
                F.col("_quantity")
            )

            .otherwise(
                -F.col("_quantity")
            )
        )

        # Running movement total
        .withColumn(
            "_cumulative_movement",
            F.sum("_signed_quantity").over(
                movement_order_window
            )
        )

        # Opening stock + movements
        .withColumn(
            "_projected_stock",
            F.col("opening_quantity")
            +
            F.col("_cumulative_movement")
        )

        # Reject if stock becomes negative
        .withColumn(
            "reject_reason",

            F.when(
                F.col("_projected_stock") < 0,
                F.lit(
                    "NEGATIVE_PROJECTED_STOCK"
                )
            )
        )
    )


    # ============================================================
    # QUARANTINE RECORDS THAT FAILED BASIC VALIDATION
    # ============================================================

    basic_quarantine = (
        checked
        .filter(
            F.col("reject_reason").isNotNull()
        )
    )


    # ============================================================
    # QUARANTINE RECORDS THAT CAUSED NEGATIVE STOCK
    # ============================================================

    negative_stock_quarantine = (
        final_checked
        .filter(
            F.col("reject_reason").isNotNull()
        )
    )


    # ============================================================
    # Standard quarantine columns
    #
    # Keep original quantity/event_time values so the bad source
    # values remain visible.
    # ============================================================

    quarantine_columns = [
        "movement_id",
        "warehouse_id",
        "product_id",
        "batch_id",
        "movement_type",
        "quantity",
        "event_time",
        "_source_name",
        "reject_reason"
    ]


    # ============================================================
    # COMBINE ALL QUARANTINED RECORDS
    # ============================================================

    quarantine_df = (
        basic_quarantine
        .select(
            quarantine_columns
        )

        .unionByName(

            negative_stock_quarantine
            .select(
                quarantine_columns
            )
        )
    )


    # ============================================================
    # FINAL VALID SILVER RECORDS
    # ============================================================

    valid_df = (
        final_checked

        .filter(
            F.col("reject_reason").isNull()
        )

        .select(
            "movement_id",
            "warehouse_id",
            "product_id",
            "batch_id",
            "movement_type",

            F.col("_quantity").alias(
                "quantity"
            ),

            F.col("_event_time").alias(
                "event_time"
            ),

            F.col("_source_name").alias(
                "source_name"
            )
        )
    )


    # ============================================================
    # RETURN VALID + QUARANTINE DATA
    # ============================================================

    return valid_df, quarantine_df

# COMMAND ----------

# MAGIC %md
# MAGIC Validate baseline stock movements

# COMMAND ----------

bronze_stock_movements = spark.table(
    f"{catalog}.{schema}.bronze_stock_movements"
)

(
    silver_stock_movements,
    quarantine_stock_movements
) = validate_movements(
    bronze_stock_movements,
    "stock_movements.csv"
)

write_validation_results(
    silver_stock_movements,
    quarantine_stock_movements,
    "silver_stock_movements",
    "quarantine_stock_movements"
)
    
display(quarantine_stock_movements)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validate daily demand
# MAGIC

# COMMAND ----------

# ============================================================
# SILVER VALIDATION - DAILY DEMAND
# ============================================================

bronze_daily_demand = spark.table(
    f"{catalog}.{schema}.bronze_daily_demand"
)


# ------------------------------------------------------------
# Valid warehouse reference
# ------------------------------------------------------------

warehouse_reference = (
    silver_warehouses
    .select("warehouse_id")
    .withColumn(
        "_warehouse_exists",
        F.lit(True)
    )
)


# ------------------------------------------------------------
# Valid product reference
# ------------------------------------------------------------

product_reference = (
    silver_products
    .select("product_id")
    .withColumn(
        "_product_exists",
        F.lit(True)
    )
)


# ------------------------------------------------------------
# Validate daily demand
# ------------------------------------------------------------

daily_demand_checked = (
    bronze_daily_demand

    # SAFE date conversion
    #
    # 2026-09-30 -> DATE
    # not-a-date -> NULL
    .withColumn(
        "_demand_date",
        F.expr(
            "try_cast(date AS DATE)"
        )
    )

    # SAFE quantity conversion
    #
    # 10 -> 10
    # invalid value -> NULL
    .withColumn(
        "_units_requested",
        F.expr(
            "try_cast(units_requested AS INT)"
        )
    )

    # Check warehouse
    .join(
        warehouse_reference,
        on="warehouse_id",
        how="left"
    )

    # Check product
    .join(
        product_reference,
        on="product_id",
        how="left"
    )

    # --------------------------------------------------------
    # Assign reject reason
    # --------------------------------------------------------

    .withColumn(
        "reject_reason",

        # Invalid date
        F.when(
            F.col("_demand_date").isNull(),
            F.lit("INVALID_DEMAND_DATE")
        )

        # Missing warehouse
        .when(
            is_blank("warehouse_id"),
            F.lit("MISSING_WAREHOUSE_ID")
        )

        # Unknown warehouse
        .when(
            F.col("_warehouse_exists").isNull(),
            F.lit("UNKNOWN_WAREHOUSE")
        )

        # Missing product
        .when(
            is_blank("product_id"),
            F.lit("MISSING_PRODUCT_ID")
        )

        # Unknown product
        .when(
            F.col("_product_exists").isNull(),
            F.lit("UNKNOWN_PRODUCT")
        )

        # Missing or malformed demand quantity
        .when(
            F.col("_units_requested").isNull(),
            F.lit("MISSING_OR_INVALID_DEMAND_QUANTITY")
        )

        # Negative demand
        .when(
            F.col("_units_requested") < 0,
            F.lit("NEGATIVE_DEMAND")
        )
    )
)


# ============================================================
# VALID -> SILVER
# ============================================================

silver_daily_demand = (
    daily_demand_checked

    .filter(
        F.col("reject_reason").isNull()
    )

    .select(
        F.col("_demand_date").alias("date"),
        "warehouse_id",
        "product_id",
        F.col("_units_requested").alias(
            "units_requested"
        )
    )
)


# ============================================================
# INVALID -> QUARANTINE
# Keep the original raw values for troubleshooting
# ============================================================

quarantine_daily_demand = (
    daily_demand_checked

    .filter(
        F.col("reject_reason").isNotNull()
    )

    .select(
        "date",
        "warehouse_id",
        "product_id",
        "units_requested",
        "reject_reason"
    )
)


# ============================================================
# WRITE RESULTS
# ============================================================

write_validation_results(
    silver_daily_demand,
    quarantine_daily_demand,
    "silver_daily_demand",
    "quarantine_daily_demand"
)


# ============================================================
# DISPLAY INVALID RECORDS
# ============================================================

display(
    quarantine_daily_demand
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Process supplier lead times

# COMMAND ----------

bronze_supplier_lead_times = spark.table(
    f"{catalog}.{schema}.bronze_supplier_lead_times"
)

supplier_reference = (
    silver_suppliers
    .select("supplier_id")
    .withColumn(
        "_supplier_exists",
        F.lit(True)
    )
)

product_reference = (
    silver_products
    .select("product_id")
    .withColumn(
        "_product_exists",
        F.lit(True)
    )
)

supplier_lead_times_checked = (
    bronze_supplier_lead_times
    .withColumn(
        "_lead_time_days",
        F.col("lead_time_days").cast("integer")
    )
    .join(
        supplier_reference,
        on="supplier_id",
        how="left"
    )
    .join(
        product_reference,
        on="product_id",
        how="left"
    )
    .withColumn(
        "reject_reason",
        F.when(
            F.col("_supplier_exists").isNull(),
            F.lit("UNKNOWN_SUPPLIER")
        )
        .when(
            F.col("_product_exists").isNull(),
            F.lit("UNKNOWN_PRODUCT")
        )
        .when(
            F.col("_lead_time_days").isNull(),
            F.lit("INVALID_LEAD_TIME")
        )
        .when(
            F.col("_lead_time_days") < 0,
            F.lit("NEGATIVE_LEAD_TIME")
        )
    )
)

silver_supplier_lead_times = (
    supplier_lead_times_checked
    .filter(F.col("reject_reason").isNull())
    .select(
        "supplier_id",
        "product_id",
        F.col("_lead_time_days").alias(
            "lead_time_days"
        )
    )
)

quarantine_supplier_lead_times = (
    supplier_lead_times_checked
    .filter(F.col("reject_reason").isNotNull())
    .drop(
        "_supplier_exists",
        "_product_exists"
    )
)

write_validation_results(
    silver_supplier_lead_times,
    quarantine_supplier_lead_times,
    "silver_supplier_lead_times",
    "quarantine_supplier_lead_times"
)

# COMMAND ----------

print(
    "Incremental processing is handled by "
    "the dedicated incremental processing job."
)

# COMMAND ----------

acceptance_fixture_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(
        f"{landing_path}/acceptance_fixture.csv"
    )
    .withColumn(
        "sequence",
        F.col("sequence").cast("integer")
    )
    .withColumn(
        "quantity",
        F.col("quantity").cast("integer")
    )
    .withColumn(
        "expected_stock",
        F.col("expected_stock").cast("integer")
    )
)

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    {catalog}.{schema}.silver_acceptance_fixture
    """
)

(
    acceptance_fixture_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.silver_acceptance_fixture"
    )
)

display(acceptance_fixture_df)

# COMMAND ----------

zero_demand_fixture_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(
        f"{landing_path}/zero_demand_fixture.csv"
    )
    .withColumn(
        "date",
        F.to_date("date")
    )
    .withColumn(
        "units_requested",
        F.col("units_requested").cast("integer")
    )
)

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    {catalog}.{schema}.silver_zero_demand_fixture
    """
)

(
    zero_demand_fixture_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{catalog}.{schema}.silver_zero_demand_fixture"
    )
)

display(zero_demand_fixture_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN medicine_inventory.inventory
# MAGIC LIKE 'silver_*';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN medicine_inventory.inventory
# MAGIC LIKE 'quarantine_*';

# COMMAND ----------

# ============================================================
# EXPORT QUARANTINE TABLES TO VOLUME
# ============================================================

quarantine_tables = [
    "quarantine_suppliers",
    "quarantine_warehouses",
    "quarantine_products",
    "quarantine_supplier_products",
    "quarantine_opening_inventory",
    "quarantine_stock_movements",
    "quarantine_daily_demand",
    "quarantine_supplier_lead_times"
]

for table_name in quarantine_tables:

    full_table_name = (
        f"{catalog}.{schema}.{table_name}"
    )

    output_path = (
        f"{quarantine_path}/{table_name}"
    )

    if spark.catalog.tableExists(full_table_name):

        quarantine_df = spark.table(
            full_table_name
        )

        row_count = quarantine_df.count()

        (
            quarantine_df.write
            .format("csv")
            .option("header", "true")
            .mode("overwrite")
            .save(output_path)
        )

        print(
            f"{table_name}: "
            f"{row_count} rows written to "
            f"{output_path}"
        )

    else:

        print(
            f"Skipped {table_name}: "
            "table does not exist."
        )

# COMMAND ----------

display(
    dbutils.fs.ls(
        f"{quarantine_path}/quarantine_products"
    )
)
df_check = (
    spark.read
    .option("header", "true")
    .csv(
        f"{quarantine_path}/quarantine_products"
    )
)

display(df_check)