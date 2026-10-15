# Databricks notebook source
# MAGIC %run ./New_Relic_Utilities

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, LongType, TimestampType
from delta.tables import DeltaTable

catalog = "medicine_inventory"
schema = "inventory"

volume_root = (
    "/Volumes/medicine_inventory/"
    "inventory/inventory_files"
)

incremental_path = f"{volume_root}/incremental"
checkpoint_root = f"{volume_root}/checkpoint"
checkpoint_path = f"{checkpoint_root}/processed_batches"
quarantine_root = f"{volume_root}/quarantine"

silver_movements_table = (
    f"{catalog}.{schema}.silver_stock_movements"
)

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("Catalog:", catalog)
print("Schema:", schema)
print("Incremental path:", incremental_path)
print("Checkpoint path:", checkpoint_path)
print("Quarantine path:", quarantine_root)

# COMMAND ----------

required_tables = [
    "silver_products",
    "silver_warehouses",
    "silver_opening_inventory",
    "silver_stock_movements"
]

missing_tables = []

for table_name in required_tables:

    full_table_name = (
        f"{catalog}.{schema}.{table_name}"
    )

    if spark.catalog.tableExists(full_table_name):

        row_count = (
            spark.table(full_table_name).count()
        )

        print(
            f"Found: {table_name}"
            f" | Rows: {row_count}"
        )

    else:
        missing_tables.append(table_name)

if missing_tables:

    raise RuntimeError(
        "Missing required Silver tables: "
        + ", ".join(missing_tables)
    )

print("All required Silver tables are available.")

# COMMAND ----------

silver_warehouses = (
    spark.table(
        f"{catalog}.{schema}.silver_warehouses"
    )
)

silver_products = (
    spark.table(
        f"{catalog}.{schema}.silver_products"
    )
)

silver_opening_inventory = (
    spark.table(
        f"{catalog}.{schema}.silver_opening_inventory"
    )
)

silver_stock_movements = (
    spark.table(
        silver_movements_table
    )
)

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

checkpoint_schema = StructType([
    StructField(
        "file_name",
        StringType(),
        False
    ),
    StructField(
        "status",
        StringType(),
        False
    ),
    StructField(
        "source_rows",
        LongType(),
        True
    ),
    StructField(
        "valid_rows",
        LongType(),
        True
    ),
    StructField(
        "inserted_rows",
        LongType(),
        True
    ),
    StructField(
        "quarantined_rows",
        LongType(),
        True
    ),
    StructField(
        "processed_at",
        TimestampType(),
        True
    )
])

if not DeltaTable.isDeltaTable(
    spark,
    checkpoint_path
):

    empty_checkpoint_df = (
        spark.createDataFrame(
            [],
            checkpoint_schema
        )
    )

    (
        empty_checkpoint_df.write
        .format("delta")
        .mode("overwrite")
        .save(checkpoint_path)
    )

    print("Checkpoint initialized.")

else:

    print("Checkpoint already exists.")

# COMMAND ----------

checkpoint_df = (
    spark.read
    .format("delta")
    .load(checkpoint_path)
)

display(
    checkpoint_df.orderBy("processed_at")
)

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

catalog = "medicine_inventory"
schema = "inventory"

volume_root = "/Volumes/medicine_inventory/inventory/inventory_files"
incremental_path = f"{volume_root}/incremental"
checkpoint_root = f"{volume_root}/checkpoint"
checkpoint_path = f"{checkpoint_root}/processed_batches"
quarantine_root = f"{volume_root}/quarantine"

silver_movements_table = (
    f"{catalog}.{schema}.silver_stock_movements"
)

silver_warehouses = spark.table(
    f"{catalog}.{schema}.silver_warehouses"
)

silver_products = spark.table(
    f"{catalog}.{schema}.silver_products"
)

silver_opening_inventory = spark.table(
    f"{catalog}.{schema}.silver_opening_inventory"
)

silver_stock_movements = spark.table(
    silver_movements_table
)

print("Silver reference tables loaded successfully.")
print("Warehouses:", silver_warehouses.count())
print("Products:", silver_products.count())
print("Opening inventory:", silver_opening_inventory.count())
print("Existing movements:", silver_stock_movements.count())


# COMMAND ----------


from pyspark.sql import functions as F

def is_blank(column_name):
    return (
        F.col(column_name).isNull()
        |
        (
            F.trim(F.col(column_name)) == ""
        )
    )

print("is_blank() defined successfully.")


# COMMAND ----------


def validate_movements(input_df, source_name):

    movement_payload_columns = [
        "warehouse_id",
        "product_id",
        "batch_id",
        "movement_type",
        "quantity",
        "event_time"
    ]

    # Preserve each row's actual filename when available.
    if "_source_name" not in input_df.columns:
        input_df = input_df.withColumn(
            "_source_name",
            F.lit(source_name)
        )

    # Create a payload to identify exact and conflicting duplicates.
    payload_expression = F.concat_ws(
        "||",
        *[
            F.coalesce(
                F.col(column_name).cast("string"),
                F.lit("<NULL>")
            )
            for column_name in movement_payload_columns
        ]
    )

    movement_id_window = Window.partitionBy("movement_id")

    exact_duplicate_window = (
        Window
        .partitionBy("movement_id", "_payload")
        .orderBy(F.monotonically_increasing_id())
    )

    warehouse_reference = (
        silver_warehouses
        .select("warehouse_id")
        .distinct()
        .withColumn("_warehouse_exists", F.lit(True))
    )

    product_reference = (
        silver_products
        .select("product_id")
        .distinct()
        .withColumn("_product_exists", F.lit(True))
    )

    inventory_reference = (
        silver_opening_inventory
        .select(
            "warehouse_id",
            "product_id",
            "batch_id",
            "opening_quantity"
        )
        .withColumn("_inventory_key_exists", F.lit(True))
    )

    checked = (
        input_df

        .withColumn(
            "_quantity",
            F.expr("try_cast(quantity AS INT)")
        )

        # Malformed timestamps become NULL instead of raising an error.
        .withColumn(
            "_event_time",
            F.expr("try_cast(event_time AS TIMESTAMP)")
        )

        .withColumn("_payload", payload_expression)

        .withColumn(
            "_movement_id_count",
            F.count("*").over(movement_id_window)
        )

        .withColumn(
            "_payload_variant_count",
            F.size(
                F.collect_set("_payload").over(
                    movement_id_window
                )
            )
        )

        .withColumn(
            "_exact_duplicate_rank",
            F.row_number().over(exact_duplicate_window)
        )

        .join(warehouse_reference, on="warehouse_id", how="left")
        .join(product_reference, on="product_id", how="left")
        .join(
            inventory_reference,
            on=["warehouse_id", "product_id", "batch_id"],
            how="left"
        )

        .withColumn(
            "reject_reason",

            F.when(
                is_blank("movement_id"),
                F.lit("MISSING_MOVEMENT_ID")
            )

            .when(
                is_blank("warehouse_id"),
                F.lit("MISSING_WAREHOUSE_ID")
            )

            .when(
                F.col("_warehouse_exists").isNull(),
                F.lit("UNKNOWN_WAREHOUSE")
            )

            .when(
                is_blank("product_id"),
                F.lit("MISSING_PRODUCT_ID")
            )

            .when(
                F.col("_product_exists").isNull(),
                F.lit("UNKNOWN_PRODUCT")
            )

            .when(
                is_blank("batch_id"),
                F.lit("MISSING_BATCH_ID")
            )

            .when(
                (F.col("_movement_id_count") > 1)
                & (F.col("_payload_variant_count") > 1),
                F.lit("CONFLICTING_DUPLICATE_MOVEMENT_ID")
            )

            .when(
                (F.col("_movement_id_count") > 1)
                & (F.col("_payload_variant_count") == 1)
                & (F.col("_exact_duplicate_rank") > 1),
                F.lit("EXACT_DUPLICATE_MOVEMENT")
            )

            .when(
                F.col("_inventory_key_exists").isNull(),
                F.lit("UNKNOWN_INVENTORY_BATCH")
            )

            .when(
                F.col("movement_type").isNull()
                | (~F.col("movement_type").isin("ISSUE", "RECEIPT")),
                F.lit("INVALID_MOVEMENT_TYPE")
            )

            .when(
                F.col("_quantity").isNull(),
                F.lit("INVALID_QUANTITY")
            )

            .when(
                F.col("_quantity") == 0,
                F.lit("ZERO_QUANTITY")
            )

            .when(
                F.col("_quantity") < 0,
                F.lit("NEGATIVE_QUANTITY")
            )

            .when(
                F.col("_event_time").isNull(),
                F.lit("INVALID_EVENT_TIMESTAMP")
            )
        )
    )

    basic_quarantine = checked.filter(
        F.col("reject_reason").isNotNull()
    )

    basic_valid = checked.filter(
        F.col("reject_reason").isNull()
    )

    # Calculate the inventory already represented in Silver.
    existing_movement_summary = (
        silver_stock_movements
        .groupBy("warehouse_id", "product_id", "batch_id")
        .agg(
            F.sum(
                F.when(
                    F.col("movement_type") == "RECEIPT",
                    F.col("quantity")
                )
                .when(
                    F.col("movement_type") == "ISSUE",
                    -F.col("quantity")
                )
                .otherwise(F.lit(0))
            ).alias("_existing_net_movement")
        )
    )

    current_inventory_reference = (
        silver_opening_inventory
        .groupBy("warehouse_id", "product_id", "batch_id")
        .agg(
            F.sum("opening_quantity").alias("_opening_inventory")
        )
        .join(
            existing_movement_summary,
            on=["warehouse_id", "product_id", "batch_id"],
            how="left"
        )
        .withColumn(
            "_existing_net_movement",
            F.coalesce(
                F.col("_existing_net_movement"),
                F.lit(0)
            )
        )
        .withColumn(
            "_starting_inventory",
            F.col("_opening_inventory")
            + F.col("_existing_net_movement")
        )
        .select(
            "warehouse_id",
            "product_id",
            "batch_id",
            "_starting_inventory"
        )
    )

    movement_order_window = (
        Window
        .partitionBy("warehouse_id", "product_id", "batch_id")
        .orderBy(
            F.col("_event_time").asc(),
            F.col("movement_id").asc()
        )
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    final_checked = (
        basic_valid
        .join(
            current_inventory_reference,
            on=["warehouse_id", "product_id", "batch_id"],
            how="left"
        )
        .withColumn(
            "_signed_quantity",
            F.when(
                F.col("movement_type") == "RECEIPT",
                F.col("_quantity")
            ).otherwise(-F.col("_quantity"))
        )
        .withColumn(
            "_batch_cumulative_movement",
            F.sum("_signed_quantity").over(movement_order_window)
        )
        .withColumn(
            "_projected_stock",
            F.col("_starting_inventory")
            + F.col("_batch_cumulative_movement")
        )
        .withColumn(
            "reject_reason",
            F.when(
                F.col("_projected_stock") < 0,
                F.lit("NEGATIVE_PROJECTED_STOCK")
            )
        )
    )

    stock_quarantine = final_checked.filter(
        F.col("reject_reason").isNotNull()
    )

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

    quarantine_df = (
        basic_quarantine.select(quarantine_columns)
        .unionByName(
            stock_quarantine.select(quarantine_columns)
        )
    )

    valid_df = (
        final_checked
        .filter(F.col("reject_reason").isNull())
        .select(
            "movement_id",
            "warehouse_id",
            "product_id",
            "batch_id",
            "movement_type",
            F.col("_quantity").alias("quantity"),
            F.col("_event_time").alias("event_time"),
            F.col("_source_name").alias("source_name")
        )
    )

    return valid_df, quarantine_df


# COMMAND ----------

incremental_items = (
    dbutils.fs.ls(incremental_path)
)

incremental_files = sorted([
    item.name
    for item in incremental_items
    if item.name.lower().endswith(".csv")
])

print("Incremental files found:")

for file_name in incremental_files:
    print(file_name)

if not incremental_files:
    raise RuntimeError(
        "No incremental CSV files were found."
    )

# COMMAND ----------

checkpoint_df = (
    spark.read
    .format("delta")
    .load(checkpoint_path)
)

processed_files = set(
    row["file_name"]
    for row in (
        checkpoint_df
        .filter(
            F.col("status") == "PROCESSED"
        )
        .select("file_name")
        .distinct()
        .collect()
    )
)

files_to_process = [
    file_name
    for file_name in incremental_files
    if file_name not in processed_files
]

print(
    "Already processed:",
    sorted(processed_files)
)

print(
    "Files to process:",
    files_to_process
)

# COMMAND ----------


def save_checkpoint(
    file_name,
    status,
    source_rows,
    valid_rows,
    inserted_rows,
    quarantined_rows
):
    checkpoint_record = (
        spark.createDataFrame(
            [
                (
                    file_name,
                    status,
                    int(source_rows),
                    int(valid_rows),
                    int(inserted_rows),
                    int(quarantined_rows)
                )
            ],
            [
                "file_name",
                "status",
                "source_rows",
                "valid_rows",
                "inserted_rows",
                "quarantined_rows"
            ]
        )
        .withColumn(
            "processed_at",
            F.current_timestamp()
        )
    )

    checkpoint_delta = DeltaTable.forPath(
        spark,
        checkpoint_path
    )

    (
        checkpoint_delta.alias("target")
        .merge(
            checkpoint_record.alias("source"),
            "target.file_name = source.file_name"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        f"Checkpoint saved: {file_name} | "
        f"Status: {status}"
    )


# COMMAND ----------


from pyspark.sql import functions as F

print("Silver stock movements:")
spark.table(
    "medicine_inventory.inventory.silver_stock_movements"
).printSchema()

print("Existing quarantine tables:")
display(
    spark.sql("""
        SHOW TABLES IN medicine_inventory.inventory
    """).filter(
        F.col("tableName").contains("quarantine")
    )
)


# COMMAND ----------


print("Quarantine stock movements schema:")

spark.table(
    "medicine_inventory.inventory.quarantine_stock_movements"
).printSchema()

print("Sample quarantine records:")

display(
    spark.table(
        "medicine_inventory.inventory.quarantine_stock_movements"
    ).limit(5)
)


# COMMAND ----------


volume_root = "/Volumes/medicine_inventory/inventory/inventory_files"
incremental_path = f"{volume_root}/incremental"
checkpoint_root = f"{volume_root}/checkpoint"
checkpoint_path = f"{checkpoint_root}/processed_batches"

from pyspark.sql import functions as F

print("Current checkpoint records:")

checkpoint_df = (
    spark.read
    .format("delta")
    .load(checkpoint_path)
)

display(
    checkpoint_df.orderBy(
        F.col("processed_at").desc_nulls_last()
    )
)

print("Files waiting to be processed:")

incremental_items = dbutils.fs.ls(incremental_path)

incremental_files = sorted([
    item.name
    for item in incremental_items
    if item.name.lower().endswith(".csv")
])

completed_files = set(
    row["file_name"]
    for row in (
        checkpoint_df
        .filter(F.col("status") == "COMPLETED")
        .select("file_name")
        .distinct()
        .collect()
    )
)

files_to_process = [
    file_name
    for file_name in incremental_files
    if file_name not in completed_files
]

print("All incremental CSV files:", incremental_files)
print("Already completed:", sorted(completed_files))
print("Waiting to process:", files_to_process)


# COMMAND ----------


from functools import reduce
from pyspark.sql import functions as F

volume_root = "/Volumes/medicine_inventory/inventory/inventory_files"
incremental_path = f"{volume_root}/incremental"

incremental_files = sorted([
    item.name
    for item in dbutils.fs.ls(incremental_path)
    if item.name.lower().endswith(".csv")
])

incoming_dfs = []

for file_name in incremental_files:
    file_path = f"{incremental_path}/{file_name}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(file_path)
        .select(
            "movement_id",
            "warehouse_id",
            "product_id",
            "batch_id",
            "movement_type",
            "quantity",
            "event_time"
        )
        .withColumn("_source_file", F.lit(file_name))
    )

    incoming_dfs.append(df)

if incoming_dfs:
    incoming_all = reduce(
        lambda left, right: left.unionByName(right),
        incoming_dfs
    )

    print("Incoming row count:", incoming_all.count())

    print("Duplicate movement IDs in incoming files:")
    display(
        incoming_all
        .groupBy("movement_id")
        .count()
        .filter(F.col("count") > 1)
        .orderBy(F.col("count").desc())
    )

    existing_ids = (
        spark.table(
            "medicine_inventory.inventory.silver_stock_movements"
        )
        .select("movement_id")
        .distinct()
    )

    print("Incoming IDs already present in Silver:")
    display(
        incoming_all
        .join(existing_ids, on="movement_id", how="inner")
        .select("movement_id", "_source_file")
        .orderBy("movement_id")
    )


# COMMAND ----------


display(
    incoming_all
    .filter(
        F.col("movement_id") == "INC_BAD_3_CONFLICT"
    )
    .select(
        "movement_id",
        "warehouse_id",
        "product_id",
        "batch_id",
        "movement_type",
        "quantity",
        "event_time",
        "_source_file"
    )
    .orderBy("_source_file")
)


# COMMAND ----------


display(
    incoming_all
    .filter(
        F.col("movement_id") == "INC_BAD_3_CONFLICT"
    )
    .groupBy("movement_id", "_source_file")
    .agg(
        F.count("*").alias("row_count"),
        F.collect_set("quantity").alias("quantities")
    )
)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.window import Window
test_file = "incremental_batch_3.csv"

test_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{incremental_path}/{test_file}")
)

test_valid_df, test_quarantine_df = validate_movements(
    test_df,
    test_file
)

print("Valid rows:", test_valid_df.count())
print("Quarantined rows:", test_quarantine_df.count())

print("Conflicting movement ID result:")
display(
    test_quarantine_df.filter(
        F.col("movement_id") == "INC_BAD_3_CONFLICT"
    )
)


# COMMAND ----------


display(
    spark.table(
        "medicine_inventory.inventory.silver_opening_inventory"
    )
    .filter(
        (F.col("warehouse_id") == "W001")
        & (F.col("product_id") == "P001")
        & (F.col("batch_id") == "B0001")
    )
)


# COMMAND ----------


display(
    spark.table(
        "medicine_inventory.inventory.silver_opening_inventory"
    )
    .filter(
        (F.col("warehouse_id") == "W001")
        & (F.col("product_id") == "P001")
    )
)


# COMMAND ----------


test_file = (
    f"{incremental_path}/incremental_batch_3.csv"
)

test_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(test_file)
)

display(
    test_df.select(
        "movement_id",
        "warehouse_id",
        "product_id",
        "batch_id",
        "movement_type",
        "quantity",
        "event_time"
    )
)


# COMMAND ----------


display(
    test_df
    .filter(
        F.col("movement_id") == "INC_BAD_3_CONFLICT"
    )
    .select(
        "movement_id",
        "warehouse_id",
        "product_id",
        "batch_id",
        "movement_type",
        "quantity",
        "event_time"
    )
)


# COMMAND ----------


test_valid_df, test_quarantine_df = validate_movements(
    test_df,
    "incremental_batch_3.csv"
)

display(
    test_quarantine_df.filter(
        F.col("movement_id") == "INC_BAD_3_CONFLICT"
    )
)


# COMMAND ----------


print("File: incremental_batch_3.csv")
print("Valid rows:", test_valid_df.count())
print("Quarantined rows:", test_quarantine_df.count())

print("Quarantine reasons:")
display(
    test_quarantine_df
    .groupBy("reject_reason")
    .count()
    .orderBy(F.col("count").desc())
)


# COMMAND ----------


for file_name in [
    "incremental_batch_1 (1).csv",
    "incremental_batch_2.csv"
]:
    file_path = f"{incremental_path}/{file_name}"

    test_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(file_path)
    )

    test_valid_df, test_quarantine_df = validate_movements(
        test_df,
        file_name
    )

    print(f"\nFile: {file_name}")
    print("Source rows:", test_df.count())
    print("Valid rows:", test_valid_df.count())
    print("Quarantined rows:", test_quarantine_df.count())

    display(
        test_quarantine_df
        .groupBy("reject_reason")
        .count()
        .orderBy(F.col("count").desc())
    )


# COMMAND ----------


print("Silver movement count:",
      spark.table(
          "medicine_inventory.inventory.silver_stock_movements"
      ).count())

print("Quarantine count:",
      spark.table(
          "medicine_inventory.inventory.quarantine_stock_movements"
      ).count())

print("Checkpoint:")
display(
    spark.read.format("delta")
    .load(checkpoint_path)
)


# COMMAND ----------


print("Existing quarantine records:")

display(
    spark.table(
        "medicine_inventory.inventory.quarantine_stock_movements"
    ).select(
        "movement_id",
        "_source_name",
        "reject_reason"
    ).orderBy(
        "_source_name",
        "movement_id"
    )
)


# COMMAND ----------


from pyspark.sql import functions as F

silver = spark.table(
    "medicine_inventory.inventory.silver_stock_movements"
)

print("Silver columns:")
print(silver.columns)

print("Total Silver rows:", silver.count())

print("Duplicate movement IDs in existing Silver:")
display(
    silver
    .groupBy("movement_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
)


# COMMAND ----------


from pyspark.sql import functions as F
from functools import reduce

incremental_path = (
    "/Volumes/medicine_inventory/"
    "inventory/inventory_files/incremental"
)

files = [
    f for f in dbutils.fs.ls(incremental_path)
    if f.name.lower().endswith(".csv")
]

print("CSV files found:")
for f in files:
    print(f.name, f.path)


# COMMAND ----------


import inspect

print(inspect.getsource(validate_movements))


# COMMAND ----------


catalog = "medicine_inventory"
schema = "inventory"

print("QUARANTINE TABLE SCHEMA")
spark.table(
    f"{catalog}.{schema}.quarantine_stock_movements"
).printSchema()

print("GOLD TABLES")
for table in spark.catalog.listTables(schema):
    if "gold" in table.name.lower():
        print(table.name)

print("CHECKPOINT SCHEMA")
spark.read.format("delta").load(
    checkpoint_path
).printSchema()


# COMMAND ----------


catalog = "medicine_inventory"
schema = "inventory"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print("Current catalog and schema:")
spark.sql("""
    SELECT current_catalog(), current_schema()
""").show(truncate=False)

print("Gold tables:")
display(
    spark.sql(f"""
        SHOW TABLES IN `{catalog}`.`{schema}`
    """).filter("lower(tableName) LIKE '%gold%'")
)

print("Checkpoint schema:")
spark.read.format("delta").load(
    "/Volumes/medicine_inventory/inventory/inventory_files/checkpoint/processed_batches"
).printSchema()


# COMMAND ----------

catalog = "medicine_inventory"
schema = "inventory"

print("ALL SILVER TABLES")
spark.sql(
    f"SHOW TABLES IN {catalog}.{schema}"
).filter(
    "lower(tableName) LIKE 'silver_%'"
).show(truncate=False)

print("ALL GOLD TABLES")
spark.sql(
    f"SHOW TABLES IN {catalog}.{schema}"
).filter(
    "lower(tableName) LIKE 'gold_%'"
).show(truncate=False)

# COMMAND ----------


catalog = "medicine_inventory"
schema = "inventory"

tables = spark.sql(
    f"SHOW TABLES IN `{catalog}`.`{schema}`"
).collect()

silver_tables = [
    row.tableName for row in tables
    if row.tableName.lower().startswith("silver_")
]

gold_tables = [
    row.tableName for row in tables
    if row.tableName.lower().startswith("gold_")
]

print(f"Silver tables found: {len(silver_tables)}")
for table_name in silver_tables:
    count = spark.table(
        f"`{catalog}`.`{schema}`.`{table_name}`"
    ).count()
    print(f"{table_name}: {count} rows")

print(f"\nGold tables found: {len(gold_tables)}")
for table_name in gold_tables:
    count = spark.table(
        f"`{catalog}`.`{schema}`.`{table_name}`"
    ).count()
    print(f"{table_name}: {count} rows")


# COMMAND ----------

# Check incremental files
display(dbutils.fs.ls(incremental_path))

# Check which files have already been processed
display(spark.read.format("delta").load(checkpoint_path))

# COMMAND ----------

checkpoint_df = spark.read.format("delta").load(checkpoint_path)

display(checkpoint_df)

# COMMAND ----------

display(
    spark.read.format("delta")
    .load(checkpoint_path)
    .select("file_name", "status", "source_rows", "valid_rows",
            "inserted_rows", "quarantined_rows")
)

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

files = [
    "incremental_batch_1 (1).csv",
    "incremental_batch_2.csv",
    "incremental_batch_3.csv"
]

batch_dfs = []

for file_name in files:
    file_path = f"{incremental_path}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(file_path)
        .withColumn("_source_name", F.lit(file_name))
    )

    print(f"{file_name}: {df.count()} rows")
    batch_dfs.append(df)

all_incoming = reduce(
    lambda df1, df2: df1.unionByName(df2),
    batch_dfs
)

print("Total incoming rows:", all_incoming.count())
display(all_incoming.limit(10))

# COMMAND ----------

valid_df, quarantine_df = validate_movements(
    all_incoming.drop("_source_name"),
    "incremental_combined.csv"
)

print("Valid rows:", valid_df.count())
print("Rejected rows:", quarantine_df.count())

display(quarantine_df)

# COMMAND ----------


valid_df, quarantine_df = validate_movements(
    all_incoming,
    "incremental_combined.csv"
)

print("Total incoming rows:", all_incoming.count())
print("Valid rows:", valid_df.count())
print("Rejected rows:", quarantine_df.count())

display(
    quarantine_df
    .groupBy("reject_reason")
    .count()
    .orderBy("reject_reason")
)


# COMMAND ----------


# Check the raw event_time values without converting them to timestamps
display(
    all_incoming
    .filter(
        F.col("event_time").isNotNull()
        & (F.col("event_time").cast("string") == "not-a-timestamp")
    )
    .select("_source_name", "movement_id", "event_time")
)


# COMMAND ----------


display(
    all_incoming
    .select(
        "movement_id",
        "event_time",
        F.expr(
            "try_cast(event_time AS TIMESTAMP)"
        ).alias("parsed_event_time")
    )
    .filter(
        F.col("parsed_event_time").isNull()
    )
)


# COMMAND ----------


print("Testing validation...")

valid_df, quarantine_df = validate_movements(
    all_incoming,
    "incremental_combined.csv"
)

print("Validation function executed successfully.")


# COMMAND ----------

print("Total incoming rows:", all_incoming.count())
print("Valid rows:", valid_df.count())
print("Rejected rows:", quarantine_df.count())

display(
    quarantine_df
    .groupBy("reject_reason")
    .count()
    .orderBy("reject_reason")
)

# COMMAND ----------


from functools import reduce
from pyspark.sql import functions as F

files = [
    "incremental_batch_1 (1).csv",
    "incremental_batch_2.csv",
    "incremental_batch_3.csv"
]

batch_dfs = []

for file_name in files:
    file_path = f"{incremental_path}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(file_path)
        .withColumn("_source_name", F.lit(file_name))
    )

    print(file_name, df.count())
    batch_dfs.append(df)

all_incoming = reduce(
    lambda a, b: a.unionByName(b),
    batch_dfs
)

print("Total incoming rows:", all_incoming.count())
print("event_time data type:", all_incoming.schema["event_time"].dataType)


# COMMAND ----------


valid_df, quarantine_df = validate_movements(
    all_incoming,
    "incremental_combined.csv"
)

print("Valid rows:", valid_df.count())
print("Rejected rows:", quarantine_df.count())


# COMMAND ----------

display(
    quarantine_df
    .groupBy("reject_reason")
    .count()
    .orderBy("reject_reason")
)

# COMMAND ----------

display(quarantine_df)

# COMMAND ----------

existing_ids = (
    spark.table(silver_movements_table)
    .select("movement_id")
    .distinct()
)

duplicate_existing_ids = (
    valid_df.alias("incoming")
    .join(
        existing_ids.alias("existing"),
        F.col("incoming.movement_id") == F.col("existing.movement_id"),
        "inner"
    )
    .select("incoming.*")
)

print(
    "Valid incoming rows whose IDs already exist in Silver:",
    duplicate_existing_ids.count()
)

display(duplicate_existing_ids)

# COMMAND ----------

display(
    quarantine_df
    .groupBy("reject_reason")
    .count()
    .orderBy("reject_reason")
)

# COMMAND ----------


print(
    "Silver rows:",
    spark.table(silver_movements_table).count()
)

print(
    "Existing quarantine rows:",
    spark.table(
        f"{catalog}.{schema}.quarantine_stock_movements"
    ).count()
)

print(
    "Checkpoint rows:",
    spark.read.format("delta").load(checkpoint_path).count()
)


# COMMAND ----------


display(
    quarantine_df.select(
        "movement_id",
        "warehouse_id",
        "product_id",
        "batch_id",
        "movement_type",
        "quantity",
        "event_time",
        "_source_name",
        "reject_reason"
    )
)


# COMMAND ----------


duplicate_incoming_ids = (
    valid_df
    .groupBy("movement_id")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate movement IDs in valid data:",
    duplicate_incoming_ids.count()
)

display(duplicate_incoming_ids)


# COMMAND ----------


print("Files to process:", len(files))
print("Valid records to insert:", valid_df.count())
print("Rejected records to quarantine:", quarantine_df.count())

print("Current Silver rows:",
      spark.table(silver_movements_table).count())

print("Current Quarantine rows:",
      spark.table(
          f"{catalog}.{schema}.quarantine_stock_movements"
      ).count())

print("Current Checkpoint rows:",
      spark.read.format("delta").load(checkpoint_path).count())


# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql import functions as F

silver_movements_table = (
    f"{catalog}.{schema}.silver_stock_movements"
)
quarantine_table = (
    f"{catalog}.{schema}.quarantine_stock_movements"
)

# Discover files dynamically
incremental_files = sorted([
    item.name
    for item in dbutils.fs.ls(incremental_path)
    if item.name.lower().endswith(".csv")
])

# Read existing completed checkpoints
checkpoint_df = (
    spark.read.format("delta").load(checkpoint_path)
)

completed_files = {
    row["file_name"]
    for row in (
        checkpoint_df
        .filter(F.col("status") == "COMPLETED")
        .select("file_name")
        .distinct()
        .collect()
    )
}

pending_files = [
    name for name in incremental_files
    if name not in completed_files
]

print("All files:", incremental_files)
print("Completed files:", sorted(completed_files))
print("Pending files:", pending_files)

for file_name in pending_files:

    print(f"\nProcessing: {file_name}")

    import time

    file_start_time = time.time()
    source_rows = 0
    valid_rows = 0
    rejected_rows = 0
    inserted_rows = 0

    input_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"{incremental_path}/{file_name}")
        .withColumn("_source_name", F.lit(file_name))
    )

    source_rows = input_df.count()

    print(f"Input rows in {file_name}: {source_rows}")

    # Refresh Silver movements before validating each file
    silver_stock_movements = spark.table(
        silver_movements_table
    )

    valid_batch, quarantine_batch = validate_movements(
        input_df, file_name
    )

    valid_rows = valid_batch.count()
    rejected_rows = quarantine_batch.count()

    rejected_percent = (
        round((rejected_rows / source_rows) * 100, 2)
        if source_rows > 0 else 0.0
    )

    # Keep only movement IDs not already in Silver
    existing_ids = (
        spark.table(silver_movements_table)
        .select("movement_id")
        .distinct()
    )

    new_valid = valid_batch.join(
        existing_ids,
        on="movement_id",
        how="left_anti"
    )

    inserted_rows = new_valid.count()

    # Merge valid records into Silver
    silver_columns = spark.table(
        silver_movements_table
    ).columns

    (
        DeltaTable.forName(spark, silver_movements_table)
        .alias("t")
        .merge(
            new_valid.select(*silver_columns).alias("s"),
            "t.movement_id = s.movement_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Merge rejected records into quarantine
    quarantine_columns = spark.table(
        quarantine_table
    ).columns

    quarantine_source = quarantine_batch.select(
        *quarantine_columns
    )

    quarantine_condition = " AND ".join(
        f"t.`{column}` <=> s.`{column}`"
        for column in quarantine_columns
    )

    (
        DeltaTable.forName(spark, quarantine_table)
        .alias("t")
        .merge(
            quarantine_source.alias("s"),
            quarantine_condition
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Record completion only after both merges succeed
    checkpoint_update = (
        spark.createDataFrame(
            [(
                file_name,
                "COMPLETED",
                source_rows,
                valid_rows,
                inserted_rows,
                rejected_rows
            )],
            [
                "file_name",
                "status",
                "source_rows",
                "valid_rows",
                "inserted_rows",
                "quarantined_rows"
            ]
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        DeltaTable.forPath(spark, checkpoint_path)
        .alias("t")
        .merge(
            checkpoint_update.alias("s"),
            "t.file_name = s.file_name"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        f"Completed {file_name}: "
        f"source={source_rows}, valid={valid_rows}, "
        f"inserted={inserted_rows}, rejected={rejected_rows}"
    )

print("\nIncremental processing finished.")

print(
    "Silver rows:",
    spark.table(silver_movements_table).count()
)

print(
    "Quarantine rows:",
    spark.table(quarantine_table).count()
)

display(
    spark.read.format("delta")
    .load(checkpoint_path)
    .orderBy("file_name")
)


# COMMAND ----------


quarantine_table = f"{catalog}.{schema}.quarantine_stock_movements"

print("Silver stock movements:",
      spark.table(silver_movements_table).count())

print("Quarantine records:",
      spark.table(quarantine_table).count())

print("Checkpoint records:")
display(
    spark.read.format("delta")
    .load(checkpoint_path)
    .orderBy("file_name")
)

print("Checkpoint status summary:")
display(
    spark.read.format("delta")
    .load(checkpoint_path)
    .groupBy("status")
    .count()
)


# COMMAND ----------

from pyspark.sql import functions as F

quarantine_df_saved = spark.table(
    "medicine_inventory.inventory.quarantine_stock_movements"
)

print("Total quarantine records:", quarantine_df_saved.count())

display(
    quarantine_df_saved
    .groupBy("_source_name", "reject_reason")
    .count()
    .orderBy("_source_name", "reject_reason")
)

# COMMAND ----------

quarantine_df_saved.groupBy(
    "_source_name", "reject_reason"
).count().orderBy(
    "_source_name", "reject_reason"
).show(100, truncate=False)


# COMMAND ----------

print("Current validation rejects:", quarantine_df.count())

display(
    quarantine_df
    .groupBy("_source_name", "reject_reason")
    .count()
    .orderBy("_source_name", "reject_reason")
)

# COMMAND ----------

display(
    quarantine_df
    .groupBy("_source_name", "reject_reason")
    .count()
    .orderBy("_source_name", "reject_reason")
)

# COMMAND ----------

print(
    "Total rejected rows:",
    quarantine_df.count()
)

print(
    "Sum of grouped rejection counts:",
    quarantine_df
    .groupBy("_source_name", "reject_reason")
    .count()
    .agg({"count": "sum"})
    .first()[0]
)

# COMMAND ----------

checkpoint_path = "/Volumes/medicine_inventory/inventory/inventory_files/checkpoint/processed_batches"

checkpoint_df = spark.read.format("delta").load(checkpoint_path)

display(checkpoint_df)

# COMMAND ----------

silver_df = spark.table(
    "medicine_inventory.inventory.silver_stock_movements"
)

print("Silver total rows:", silver_df.count())

print(
    "Duplicate movement IDs:",
    silver_df.groupBy("movement_id")
    .count()
    .filter("count > 1")
    .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Check checkpoint statuses
checkpoint_df.groupBy("status").count().show()

# 2. Check Silver row count
print(
    "Silver rows:",
    spark.table(
        "medicine_inventory.inventory.silver_stock_movements"
    ).count()
)

# 3. Check quarantine row count
print(
    "Quarantine rows:",
    spark.table(
        "medicine_inventory.inventory.quarantine_stock_movements"
    ).count()
)

# 4. Check Gold table row counts
for table_name in [
    "gold_inventory_position",
    "gold_replenishment",
    "gold_inventory_summary",
    "gold_kpi"
]:
    print(
        table_name,
        spark.table(
            f"medicine_inventory.inventory.{table_name}"
        ).count()
    )

# COMMAND ----------

send_event(
    "InventoryPipelineRun",
    {
        "team": "team1",
        "pipeline": "medicine_inventory",
        "stage": "incremental",
        "status": "SUCCESS",
        "testEvent": True
    }
)

# COMMAND ----------

