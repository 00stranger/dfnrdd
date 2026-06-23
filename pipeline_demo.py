"""
E-Commerce Sales Analytics Pipeline
--------------------------------------------------------------
Simulates a pipeline that ingests raw order/transaction
data and produces business-ready aggregates, KPIs, and reporting tables.

Pipeline stages:
  1. Ingest & clean raw orders
  2. Enrich with time dimensions (Date/Timestamp functions)
  3. Build department-level sales aggregates (Aggregate functions)
  4. Rank products & reps within each department (Window functions)
  5. Detect delayed shipments & SLA breaches (Date arithmetic)
  6. Write summary tables (simulated as .show() calls)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# 0. Bootstrap
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("EcommerceSalesPipeline")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# Raw data 
# ---------------------------------------------------------------------------
raw_orders = [
    # (order_id, customer_id, rep_name,  department,   product,          amount, order_ts,                   ship_ts,                    delivered_ts)
    (1001, "C01", "Alice",   "Electronics", "Laptop Pro",     1200.00, "2024-03-01 08:15:00", "2024-03-02 10:00:00", "2024-03-05 14:00:00"),
    (1002, "C02", "Bob",     "Electronics", "Wireless Mouse",   45.99, "2024-03-01 09:30:00", "2024-03-01 18:00:00", "2024-03-03 11:00:00"),
    (1003, "C03", "Alice",   "Electronics", "Laptop Pro",     1200.00, "2024-03-02 10:00:00", "2024-03-03 09:00:00", "2024-03-07 16:00:00"),
    (1004, "C04", "Carol",   "Clothing",    "Winter Jacket",   189.99, "2024-03-02 11:00:00", "2024-03-04 08:00:00", "2024-03-06 10:00:00"),
    (1005, "C05", "Dave",    "Clothing",    "Sneakers",         95.00, "2024-03-03 07:45:00", "2024-03-04 12:00:00", "2024-03-05 09:00:00"),
    (1006, "C01", "Carol",   "Clothing",    "Winter Jacket",   189.99, "2024-03-03 14:00:00", "2024-03-05 08:00:00", "2024-03-10 11:00:00"),  # late delivery
    (1007, "C06", "Eve",     "Furniture",   "Office Chair",    349.00, "2024-03-04 09:00:00", "2024-03-06 10:00:00", "2024-03-12 13:00:00"),  # late
    (1008, "C07", "Frank",   "Furniture",   "Standing Desk",   599.00, "2024-03-04 10:30:00", "2024-03-05 09:00:00", "2024-03-08 15:00:00"),
    (1009, "C08", "Alice",   "Electronics", "USB-C Hub",        29.99, "2024-03-05 08:00:00", "2024-03-05 17:00:00", "2024-03-07 10:00:00"),
    (1010, "C09", "Bob",     "Electronics", "Wireless Mouse",   45.99, "2024-03-05 09:00:00", "2024-03-06 08:00:00", None),                   # not yet delivered
    (1011, "C10", "Dave",    "Clothing",    "Sneakers",         95.00, "2024-03-06 11:00:00", "2024-03-07 09:00:00", "2024-03-09 10:00:00"),
    (1012, "C02", "Eve",     "Furniture",   "Bookshelf",       210.00, "2024-03-06 14:00:00", "2024-03-08 10:00:00", "2024-03-11 09:00:00"),
    (1013, "C03", "Frank",   "Furniture",   "Office Chair",    349.00, "2024-03-07 08:00:00", "2024-03-09 10:00:00", "2024-03-14 12:00:00"),  # late
    (1014, "C11", "Carol",   "Clothing",    "Rain Coat",       149.00, "2024-03-07 10:00:00", "2024-03-08 08:00:00", "2024-03-10 14:00:00"),
    (1015, "C12", "Bob",     "Electronics", "Laptop Pro",     1200.00, "2024-03-08 09:00:00", "2024-03-09 11:00:00", "2024-03-12 16:00:00"),
]

schema = [
    "order_id", "customer_id", "rep_name", "department", "product",
    "amount", "order_ts", "ship_ts", "delivered_ts"
]

raw_df = spark.createDataFrame(raw_orders, schema)


# Ingest / Type-cast stage  (Date & Timestamp functions)
# ---------------------------------------------------------------------------
print("\n" + "="*70)
print("STAGE 2 – INGEST & TYPE CAST")
print("="*70)

orders_df = (
    raw_df
    .withColumn("order_ts",     F.to_timestamp("order_ts",     "yyyy-MM-dd HH:mm:ss"))
    .withColumn("ship_ts",      F.to_timestamp("ship_ts",      "yyyy-MM-dd HH:mm:ss"))
    .withColumn("delivered_ts", F.to_timestamp("delivered_ts", "yyyy-MM-dd HH:mm:ss"))
    # Derive useful date parts used throughout the pipeline
    .withColumn("order_date",   F.to_date("order_ts"))
    .withColumn("order_year",   F.year("order_ts"))
    .withColumn("order_month",  F.month("order_ts"))
    .withColumn("order_week",   F.weekofyear("order_ts"))
    .withColumn("order_dow",    F.dayofweek("order_ts"))       # 1=Sun … 7=Sat
    .withColumn("order_dom",    F.dayofmonth("order_ts"))
    .withColumn("order_hour",   F.hour("order_ts"))
    # Formatted date string for reports
    .withColumn("order_date_fmt", F.date_format("order_ts", "MMM-dd-yyyy"))
)

orders_df.select(
    "order_id", "rep_name", "department", "product", "amount",
    "order_date_fmt", "order_year", "order_month", "order_week",
    "order_dow", "order_hour"
).show(5, truncate=False)


# ---------------------------------------------------------------------------
# SLA & Shipment Analytics  (datediff, months_between, date_add, etc.)
print("\n" + "="*70)
print("STAGE 3 – SLA & SHIPMENT ANALYSIS")
print("="*70)

SLA_SHIP_DAYS      = 2   # ship within 2 days of order
SLA_DELIVERY_DAYS  = 7   # deliver within 7 days of order

shipment_df = (
    orders_df
    .withColumn("days_to_ship",     F.datediff("ship_ts", "order_ts"))
    .withColumn("days_to_deliver",  F.datediff("delivered_ts", "order_ts"))   # null if not delivered
    .withColumn("ship_sla_breach",  F.col("days_to_ship") > SLA_SHIP_DAYS)
    .withColumn("delivery_sla_breach",
                F.when(F.col("delivered_ts").isNull(), F.lit(None).cast("boolean"))
                 .otherwise(F.col("days_to_deliver") > SLA_DELIVERY_DAYS))
    .withColumn("expected_delivery", F.date_add("order_date", SLA_DELIVERY_DAYS))
    .withColumn("months_since_order",
                F.round(F.months_between(F.current_date(), "order_date"), 2))
    # next Sunday after order – useful for weekly reporting windows
    .withColumn("next_sunday",      F.next_day("order_date", "Sunday"))
)

# Show SLA breaches
print(">> Orders with SLA breaches:")
shipment_df.filter(
    F.col("ship_sla_breach") | (F.col("delivery_sla_breach") == True)
).select(
    "order_id", "rep_name", "product", "days_to_ship", "days_to_deliver",
    "ship_sla_breach", "delivery_sla_breach", "expected_delivery"
).show(truncate=False)


# ---------------------------------------------------------------------------
#Department-Level Aggregates  (all aggregate functions)
print("\n" + "="*70)
print("STAGE 4 – DEPARTMENT KPI AGGREGATES")
print("="*70)

dept_kpis = (
    orders_df.groupBy("department")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product").alias("unique_products"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_order_value"),
        F.mean("amount").alias("mean_order_value"),   # same as avg – demonstrates alias
        F.max("amount").alias("max_order_value"),
        F.min("amount").alias("min_order_value"),
        F.stddev("amount").alias("stddev_order_value"),
        F.variance("amount").alias("variance_order_value"),
        F.skewness("amount").alias("skewness_amount"),
        F.approx_count_distinct("customer_id").alias("approx_unique_cust"),
        F.first("product").alias("first_product_seen"),
        F.last("product").alias("last_product_seen"),
        F.collect_set("product").alias("product_catalogue"),   # unique products
        F.collect_list("product").alias("all_orders_products"), # with duplicates
        F.sumDistinct("amount").alias("revenue_distinct_amounts"),
    )
    .withColumn("avg_order_value",   F.round("avg_order_value", 2))
    .withColumn("mean_order_value",  F.round("mean_order_value", 2))
    .withColumn("total_revenue",     F.round("total_revenue", 2))
    .withColumn("stddev_order_value",F.round("stddev_order_value", 2))
    .withColumn("variance_order_value", F.round("variance_order_value", 2))
    .withColumn("skewness_amount",   F.round("skewness_amount", 4))
    .orderBy(F.col("total_revenue").desc())
)

print(">> Department KPIs:")
dept_kpis.select(
    "department", "total_orders", "unique_customers", "unique_products",
    "total_revenue", "avg_order_value", "max_order_value", "min_order_value",
    "stddev_order_value", "skewness_amount"
).show(truncate=False)

print(">> Product catalogues per department (collect_set):")
dept_kpis.select("department", "product_catalogue").show(truncate=False)


# ---------------------------------------------------------------------------
#Rep Performance with Window Functions
print("\n" + "="*70)
print("STAGE 5 – REP PERFORMANCE (WINDOW FUNCTIONS)")
print("="*70)

# Aggregate rep-level metrics first
rep_df = (
    orders_df.groupBy("department", "rep_name")
    .agg(
        F.sum("amount").alias("rep_revenue"),
        F.count("order_id").alias("rep_orders"),
    )
    .withColumn("rep_revenue", F.round("rep_revenue", 2))
)

# Window: partition by department, ordered by revenue descending
dept_win = Window.partitionBy("department").orderBy(F.col("rep_revenue").desc())

rep_ranked = (
    rep_df
    .withColumn("row_number",  F.row_number() .over(dept_win))  # always unique
    .withColumn("rank",        F.rank()        .over(dept_win))  # gaps on ties
    .withColumn("dense_rank",  F.dense_rank()  .over(dept_win))  # no gaps on ties
    .withColumn("prev_rep_rev", F.lag("rep_revenue",  1).over(dept_win))  # revenue of rep above
    .withColumn("next_rep_rev", F.lead("rep_revenue", 1).over(dept_win))  # revenue of rep below
    .withColumn("rev_gap_to_leader",
                F.col("rep_revenue") - F.first("rep_revenue").over(dept_win))  # negative = trails leader
)

print(">> Rep rankings within department:")
rep_ranked.select(
    "department", "rep_name", "rep_revenue", "rep_orders",
    "row_number", "rank", "dense_rank",
    "prev_rep_rev", "next_rep_rev", "rev_gap_to_leader"
).orderBy("department", "rank").show(truncate=False)


# ---------------------------------------------------------------------------
#Daily Revenue Trend with running totals  (more Window functions)
print("\n" + "="*70)
print("STAGE 6 – DAILY REVENUE TREND & RUNNING TOTALS")
print("="*70)

daily_df = (
    orders_df.groupBy("order_date")
    .agg(F.sum("amount").alias("daily_revenue"))
    .withColumn("daily_revenue", F.round("daily_revenue", 2))
    .orderBy("order_date")
)

# Unbounded preceding → current row for running total
time_win = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

daily_trend = (
    daily_df
    .withColumn("running_total",   F.sum("daily_revenue").over(time_win))
    .withColumn("prev_day_revenue",F.lag("daily_revenue", 1).over(Window.orderBy("order_date")))
    .withColumn("wow_change",
                F.round(F.col("daily_revenue") - F.col("prev_day_revenue"), 2))
    .withColumn("running_total",   F.round("running_total", 2))
)

print(">> Daily revenue trend with running total:")
daily_trend.show(truncate=False)


# ---------------------------------------------------------------------------
#Distinct-amount revenue check (sumDistinct vs sum)
print("\n" + "="*70)
print("STAGE 7 – DISTINCT vs ALL AMOUNT RECONCILIATION")
print("="*70)

total_rev      = orders_df.select(F.sum("amount")).collect()[0][0]
distinct_rev   = orders_df.select(F.sumDistinct("amount")).collect()[0][0]
approx_custs   = orders_df.select(F.approx_count_distinct("customer_id")).collect()[0][0]
total_orders   = orders_df.select(F.count("order_id")).collect()[0][0]

print(f"  Total Revenue (all orders):        ${total_rev:>10.2f}")
print(f"  Revenue (distinct amounts only):   ${distinct_rev:>10.2f}")
print(f"  Total Orders:                       {total_orders}")
print(f"  Approx Unique Customers:            {approx_custs}")


# ---------------------------------------------------------------------------
#Current-snapshot metadata  (current_date / current_timestamp)
print("\n" + "="*70)
print("STAGE 8 – PIPELINE AUDIT METADATA")
print("="*70)

audit_row = spark.range(1).select(
    F.current_date().alias("pipeline_run_date"),
    F.current_timestamp().alias("pipeline_run_ts"),
    F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("run_ts_fmt"),
    F.dayofweek(F.current_date()).alias("dow"),
    F.dayofmonth(F.current_date()).alias("dom"),
    F.dayofyear(F.current_date()).alias("doy"),
    F.hour(F.current_timestamp()).alias("run_hour"),
    F.minute(F.current_timestamp()).alias("run_minute"),
    F.second(F.current_timestamp()).alias("run_second"),
)

print(">> Pipeline run metadata:")
audit_row.show(truncate=False)

print("\nPipeline completed successfully.")
spark.stop()
