from pyspark.sql import SparkSession, functions as F, types as T

# Spark session (Glue gives you spark by default)
spark = SparkSession.builder.appName("job2-join-fact").getOrCreate()

# ---------------------------
# Hardcoded paths (edit these)
# ---------------------------
customers_path = "s3://glue-exercises-tg117/etl_jobs_exercises/job_3/datasets/raw/customers/"
orders_path    = "s3://glue-exercises-tg117/etl_jobs_exercises/job_3/datasets/raw/orders/"
dest_path      = "s3://glue-exercises-tg117/etl_jobs_exercises/job_3/datasets/curated/customer_order_fact/"

# ---------------------------
# 1) Read curated inputs
# ---------------------------
customers = spark.read.option("header", "true").csv(customers_path)
orders = spark.read.option("header", "true").csv(orders_path)

# Cast fields to correct types
customers = (customers
    .withColumn("customer_id", F.col("customer_id"))
    .withColumn("country", F.upper("country"))
    .withColumn("state", F.upper("state"))
)

orders = (orders
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    .withColumn("amount", F.col("amount").cast(T.DoubleType()))
    .withColumn("currency", F.upper("currency"))
    .withColumn("order_year", F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
)

# ---------------------------
# 2) Join → Fact Table
# ---------------------------
fact = (orders.alias("o")
    .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "inner")
    .select(
        "o.order_id",
        "o.order_date",
        "o.order_year",
        "o.order_month",
        "o.customer_id",
        "c.customer_name",
        "c.segment",
        "c.country",
        "c.state",
        "c.city",
        "o.amount",
        "o.currency"
    )
)

# ---------------------------
# 3) Write Fact Table as Parquet
# ---------------------------
(fact
    .repartition("country", "order_year", "order_month")  # better file grouping
    .write.mode("overwrite")
    .partitionBy("country", "order_year", "order_month")
    .parquet(dest_path)
)

print(f"Fact table written to {dest_path}")