import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, expr, coalesce, abs ,trim ,lower
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    TimestampType
)

# ==============================================================================
# 1. DEFINE SCHEMAS FOR SOURCE TABLES
# These should match the Debezium payload for each table.
# ==============================================================================

ve_payload_schema = StructType([
    StructField("Posting Date", LongType(), True),
    StructField("Document Type", IntegerType(), True),
    StructField("Item No_", StringType(), True),
    StructField("Source No_", StringType(), True),
    StructField("Sales Amount (Actual)", DoubleType(), True),
    StructField("Cost Amount (Actual)", DoubleType(), True),
    StructField("Purchase Amount (Actual)", DoubleType(), True),
    StructField("Item Ledger Entry Quantity", DoubleType(), True),
    StructField("Salespers__Purch_ Code", StringType(), True),
    StructField("Location Code", StringType(), True),
    StructField("Document No_", StringType(), True)
])

item_master_payload_schema = StructType([
    StructField("No_", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Item Category Code", StringType(), True),
    StructField("Product Group Code", StringType(), True)
])

item_category_payload_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Description", StringType(), True)
])

item_group_payload_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Description", StringType(), True)
])

customer_master_payload_schema = StructType([
    StructField("No_", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country_Region Code", StringType(), True)
])

vendor_master_payload_schema = StructType([
    StructField("No_", StringType(), True),
    StructField("Name", StringType(), True)
])

salesperson_payload_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Name", StringType(), True)
])

location_payload_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Name", StringType(), True)
])

# Generic function to define the full Debezium message structure
def create_debezium_stream_schema(payload_content_schema):
    return StructType([
        StructField("payload", StructType([
            StructField("before", payload_content_schema, True),
            StructField("after", payload_content_schema, True),
            StructField("op", StringType(), True)
        ]), True)
    ])

# ==============================================================================
# 2. DEFINE THE BATCH PROCESSING FUNCTION
# This is where all the transformation and joining logic will live.
# It runs for each micro-batch of data from the Value Entry stream.
# ==============================================================================

def process_micro_batch(micro_batch_df, batch_id):
    """
    This function is executed for each micro-batch of the Value Entry stream.
    It loads the latest dimension data and performs batch joins.
    """
    print(f"--- [Batch ID: {batch_id}] Starting processing ---")

    if micro_batch_df.rdd.isEmpty():
        print(f"--- [Batch ID: {batch_id}] Batch is empty, skipping. ---")
        return


    spark = SparkSession.getActiveSession()
    if not spark:
        # This is a safeguard in case no active session is found, though it shouldn't happen here.
        print(f"--- [Batch ID: {batch_id}] Could not get active Spark session. Skipping. ---")
        return
    # ================================================================

    # --- Step A: Load LATEST dimension data from Kafka topics into static DataFrames ---
    print(f"--- [Batch ID: {batch_id}] Loading latest dimension data... ---")
    
    def load_latest_dimension_data(topic_name, schema, pk_col):
        """Reads an entire Kafka topic, gets the latest version of each record, and returns it as a static DataFrame."""
        if not topic_name: return None # Skip if topic env var is not set

        # Read the whole topic as a batch
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS_CONFIG")) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .select(from_json(col("value").cast(StringType()), create_debezium_stream_schema(schema)).alias("data")) \
            .select(coalesce(col("data.payload.after"), col("data.payload.before")).alias("data")) \
            .select("data.*")
        
        # Get only the latest record for each primary key
        latest_df = df.groupBy(pk_col).agg(expr("last(struct(*)) as latest")).select("latest.*")
        return latest_df

    # Load all dimension tables. `cache()` tells Spark to keep them in memory for faster access during joins.
    def clean_column(col_name):
        return when(
        col(col_name).isNull() | 
        (trim(col(col_name)) == "") | 
        (lower(trim(col(col_name))).isin("na", "n/a", "nav","non affecter","non affecté")),
        "UNKNOWN").otherwise(col(col_name))
        
        
    # items_df
    items_df = load_latest_dimension_data(os.environ.get("KAFKA_ITEM_MASTER_TOPIC"), item_master_payload_schema, "No_") \
        .select(
            col("No_").alias("dim_item_no"),
            clean_column("Description").alias("item_name"),
            clean_column("`Item Category Code`").alias("item_category_code_from_item"),
            clean_column("`Product Group Code`").alias("item_group_code_from_item")
        ).cache()

    # customers_df (already correct, but cleaned up for consistency)
    customers_df = load_latest_dimension_data(os.environ.get("KAFKA_CUSTOMER_MASTER_TOPIC"), customer_master_payload_schema, "No_") \
        .select(
            col("No_").alias("dim_cust_no"),
            clean_column("Name").alias("customer_name"),
            clean_column("City").alias("customer_city"),
            clean_column("`Country_Region Code`").alias("customer_country")
        ).cache()

    # vendors_df
    vendors_df = load_latest_dimension_data(os.environ.get("KAFKA_VENDOR_MASTER_TOPIC"), vendor_master_payload_schema, "No_") \
        .select(
            col("No_").alias("dim_vend_no"),
            clean_column("Name").alias("vendor_name")
        ).cache()

    # salespersons_df
    salespersons_df = load_latest_dimension_data(os.environ.get("KAFKA_SALESPERSON_TOPIC"), salesperson_payload_schema, "Code") \
        .select(
            col("Code").alias("dim_sp_code"),
            clean_column("Name").alias("salesperson_name")
        ).cache()

    # locations_df
    locations_df = load_latest_dimension_data(os.environ.get("KAFKA_LOCATION_TOPIC"), location_payload_schema, "Code") \
        .select(
            col("Code").alias("dim_loc_code"),
            clean_column("Name").alias("location_name")
        ).cache()

    # item_categories_df
    item_categories_df = load_latest_dimension_data(os.environ.get("KAFKA_ITEM_CATEGORY_TOPIC"), item_category_payload_schema, "Code") \
        .select(
            col("Code").alias("dim_cat_code"),
            clean_column("Description").alias("item_category_name")
        ).cache()

    # item_groups_df
    item_groups_df = load_latest_dimension_data(os.environ.get("KAFKA_ITEM_GROUP_TOPIC"), item_group_payload_schema, "Code") \
        .select(
            col("Code").alias("dim_grp_code"),
            clean_column("Description").alias("item_group_name")
        ).cache()


    # --- Step B: Process the incoming micro-batch of Value Entry data ---
    parsed_ve_df = micro_batch_df \
        .select(from_json(col("value"), create_debezium_stream_schema(ve_payload_schema)).alias("data")) \
        .select(coalesce(col("data.payload.after"), col("data.payload.before")).alias("data")) \
        .select("data.*")
    
    # Apply business logic
    # YOU MUST CONFIRM THESE INTEGER VALUES FOR YOUR ERP SYSTEM.
    SALES_INVOICE_TYPE, SALES_CREDIT_MEMO_TYPE = 2, 4
    PURCHASE_INVOICE_TYPE, PURCHASE_CREDIT_MEMO_TYPE = 6, 8
    
    base_transactions_df = parsed_ve_df.select(
         (col("`Posting Date`") / 1000).cast(TimestampType()).alias("transaction_date"),
        col("`Item No_`").alias("item_no"),
        col("`Location Code`").alias("location_code"),
        col("`Document No_`").alias("document_no"),
        when(col("`Document Type`") == SALES_INVOICE_TYPE, "SALE").when(col("`Document Type`") == SALES_CREDIT_MEMO_TYPE, "SALE_RETURN").when(col("`Document Type`") == PURCHASE_INVOICE_TYPE, "PURCHASE").when(col("`Document Type`") == PURCHASE_CREDIT_MEMO_TYPE, "PURCHASE_RETURN").otherwise("OTHER").alias("transaction_type"),
        when(col("`Document Type`") == SALES_INVOICE_TYPE, col("Sales Amount (Actual)")).when(col("`Document Type`") == SALES_CREDIT_MEMO_TYPE, -abs(col("Sales Amount (Actual)"))).otherwise(0).alias("sales_turnover"),
        when(col("`Document Type`") == PURCHASE_INVOICE_TYPE, coalesce(col("Purchase Amount (Actual)"), col("Cost Amount (Actual)"))).when(col("`Document Type`") == PURCHASE_CREDIT_MEMO_TYPE, -abs(coalesce(col("Purchase Amount (Actual)"), col("Cost Amount (Actual)")))).otherwise(0).alias("purchase_turnover"),
        col("`Item Ledger Entry Quantity`").alias("quantity"),
        when(col("`Document Type`").isin([SALES_INVOICE_TYPE, SALES_CREDIT_MEMO_TYPE]), col("Source No_")).otherwise("OTHER").alias("customer_no"),
        when(col("`Document Type`").isin([PURCHASE_INVOICE_TYPE, PURCHASE_CREDIT_MEMO_TYPE]), col("Source No_")).otherwise("OTHER").alias("vendor_no"),
        when(col("`Document Type`").isin([SALES_INVOICE_TYPE, SALES_CREDIT_MEMO_TYPE]), col("Salespers__Purch_ Code")).otherwise("OTHER").alias("salesperson_code")
    )

    # --- Step C: Join the transaction batch with the loaded dimension tables (simple batch joins) ---
    print(f"--- [Batch ID: {batch_id}] Performing joins... ---")
    final_enriched_batch = base_transactions_df \
    .join(items_df, base_transactions_df.item_no == items_df.dim_item_no, "left_outer") \
    .join(customers_df, base_transactions_df.customer_no == customers_df.dim_cust_no, "left_outer") \
    .join(vendors_df, base_transactions_df.vendor_no == vendors_df.dim_vend_no, "left_outer") \
    .join(salespersons_df, base_transactions_df.salesperson_code == salespersons_df.dim_sp_code, "left_outer") \
    .join(locations_df, base_transactions_df.location_code == locations_df.dim_loc_code, "left_outer") \
    .join(item_categories_df, col("item_category_code_from_item") == item_categories_df.dim_cat_code, "left_outer") \
    .join(item_groups_df, col("item_group_code_from_item") == item_groups_df.dim_grp_code, "left_outer")

    # --- Step D: Select final columns and write to Parquet ---
    # The NEW, CORRECTED block:
#     final_output_batch = final_enriched_batch.select(
#     col("transaction_date").alias("__time"),
#     "item_no", "item_name", "item_category_code_from_item", "item_category_name",
#     "item_group_code_from_item", "item_group_name",
#     "customer_no", "customer_name", "customer_city", "customer_country",
#     "vendor_no", "vendor_name",
#     "salesperson_code", "salesperson_name",
#     "location_code", "location_name",
#     "document_no", "transaction_type",
#     "sales_turnover", "purchase_turnover", "quantity"
# )
    columns_to_clean = [
    "item_name", "item_category_name", "item_group_name",
    "customer_name", "customer_city", "customer_country",
    "vendor_name", "salesperson_name", "location_name"
]

# Final select statement
    final_output_batch = final_enriched_batch.select(
        col("transaction_date").alias("__time"),
        *[
            clean_column(c).alias(c) if c in columns_to_clean else col(c)
            for c in [
                "item_no", "item_name", "item_category_code_from_item", "item_category_name",
                "item_group_code_from_item", "item_group_name",
                "customer_no", "customer_name", "customer_city", "customer_country",
                "vendor_no", "vendor_name",
                "salesperson_code", "salesperson_name",
                "location_code", "location_name",
                "document_no", "transaction_type",
                "sales_turnover", "purchase_turnover", "quantity"
            ]
        ]
    )

    # output_path = "/app/output/processed_data_for_druid"
    # print(f"--- [Batch ID: {batch_id}] Writing {final_output_batch.count()} rows to Parquet at {output_path}. ---")
    # final_output_batch.write.mode("append").parquet(output_path)
    
    output_topic = os.environ.get("KAFKA_PROCESSED_TOPIC")
    if not output_topic:
        raise ValueError("KAFKA_PROCESSED_TOPIC environment variable is not defined.")

    # Get Kafka bootstrap servers from an environment variable.
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_CONFIG")

    print(f"--- [Batch ID: {batch_id}] Writing {final_output_batch.count()} rows to Kafka topic: {output_topic} ---")

    # Serialize all columns into a single JSON string in a column named 'value'.
    # This is the required format for the Kafka sink.
    final_output_batch \
        .selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .save()

    # Unpersist cached dimension dataframes to free up memory for the next batch
    items_df.unpersist()
    customers_df.unpersist()
    vendors_df.unpersist()
    salespersons_df.unpersist()
    locations_df.unpersist()
    item_categories_df.unpersist()
    item_groups_df.unpersist()
    print(f"--- [Batch ID: {batch_id}] Finished processing. ---")


# ==============================================================================
# 3. MAIN FUNCTION TO SET UP AND START THE STREAMING QUERY
# ==============================================================================
# ==============================================================================
# 3. MAIN FUNCTION TO SET UP AND START THE STREAMING QUERY
# ==============================================================================
def main():
    print("SPARK_APP: Initializing Spark Session for foreachBatch processing...")
    spark = None
    try:
        # --- Spark and Kafka Configuration ---
        driver_host = os.environ.get("SPARK_DRIVER_HOSTNAME", "spark-app-host")
        kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "kafka_broker_host:29092")
        
        # Get topic names from environment variables
        ve_topic = os.environ.get("KAFKA_VE_TOPIC")
        # Ensure all other KAFKA_*_TOPIC variables are also defined in your docker-compose.yml
        # as they are used inside the process_micro_batch function.
        # Example:
        # KAFKA_ITEM_MASTER_TOPIC: "your_item_topic_name"
        # ... and so on for all 7 dimension tables ...

        if not ve_topic:
            print("ERROR: KAFKA_VE_TOPIC environment variable must be set. Exiting.")
            sys.exit(1)

        spark = SparkSession.builder.appName("RealTimeETL-foreachBatch") \
            .config("spark.driver.host", driver_host) \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")

        # --- Define and start the streaming query for Value Entries ---

        # 1. Define the readStream source from the Value Entry topic
        ve_raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", ve_topic) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 10000) \
            .option("failOnDataLoss", "false") \
            .load() # <-- load() creates the DataFrame

        # 2. Now that we have a DataFrame, we can select from it
        ve_string_value_stream = ve_raw_stream.select(col("value").cast(StringType()))

        # 3. Use foreachBatch to process each micro-batch of transactions
        query = ve_string_value_stream.writeStream \
            .foreachBatch(process_micro_batch) \
            .outputMode("update") \
            .option("checkpointLocation", "/app/output/checkpoints/foreach_batch_checkpoint") \
            .trigger(processingTime='1 minute') \
            .start()
        
        print("SPARK_APP: foreachBatch streaming query started. Awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        print(f"SPARK_APP: An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            print("SPARK_APP: Stopping SparkSession.")
            spark.stop()
        print("SPARK_APP: Spark application finished.")

if __name__ == "__main__":
    main()