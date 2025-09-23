from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("job3-aggregate-summaries").getOrCreate()

# ---------------------------
# Hardcoded paths (edit)
# ---------------------------
products_path = "s3://your-bucket/curated/job3/products/"
txns_path     = "s3://your-bucket/curated/job3/transactions/"
daily_out     = "s3://your-bucket/curated/daily_sales/"
monthly_out   = "s3://your-bucket/curated/monthly_sales/"

# Helpful write configs
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.shuffle.partitions", "64")

# ---------------------------
# 1) Read & clean
# ---------------------------
products = (spark.read.option("header", "true").csv(products_path))

txns = (spark.read.option("header", "true").csv(txns_path))
txns = (txns
        .withColumn("txn_date", F.to_date("txn_date", "yyyy-MM-dd"))
        .withColumn("qty", F.col("qty").cast(T.IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(T.DoubleType()))
        .withColumn("country", F.upper("country"))
        .withColumn("segment", F.initcap("segment"))  # Retail/SMB/Enterprise
        .withColumn("revenue", F.col("qty") * F.col("unit_price"))
        .withColumn("year", F.year("txn_date"))
        .withColumn("month", F.month("txn_date"))
        .withColumn("day", F.dayofmonth("txn_date"))
)

# ---------------------------
# 2) Join with products
# ---------------------------
joined = (txns.alias("t")
          .join(products.alias("p"), "product_id", "left")
          .select(
              "t.txn_id", "t.customer_id", "t.txn_date",
              "t.year", "t.month", "t.day",
              "t.product_id", "p.product_name", "p.category",
              "t.qty", "t.unit_price", "t.revenue",
              "t.country", "t.segment"
          ))

# ---------------------------
# 3) Aggregations
# ---------------------------

# Daily summary (by date/country/category/segment)
daily = (joined
         .groupBy("txn_date", "year", "month", "day", "country", "category", "segment")
         .agg(
             F.countDistinct("txn_id").alias("orders"),
             F.sum("qty").alias("units_sold"),
             F.round(F.sum("revenue"), 2).alias("revenue")
         )
        )

# Monthly summary (by month/country/category/segment)
monthly = (joined
           .groupBy("year", "month", "country", "category", "segment")
           .agg(
               F.countDistinct("txn_id").alias("orders"),
               F.sum("qty").alias("units_sold"),
               F.round(F.sum("revenue"), 2).alias("revenue")
           )
          )

# ---------------------------
# 4) Write Parquet (partitioned)
# ---------------------------
(daily
 .repartition("year", "month", "day", "country")
 .write.mode("overwrite")
 .partitionBy("year", "month", "day", "country")
 .parquet(daily_out)
)

(monthly
 .repartition("year", "month", "country")
 .write.mode("overwrite")
 .partitionBy("year", "month", "country")
 .parquet(monthly_out)
)

print(f"Wrote daily summary to:   {daily_out}")
print(f"Wrote monthly summary to: {monthly_out}")
