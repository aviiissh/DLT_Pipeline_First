import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, current_timestamp, to_date, 
    regexp_replace, concat_ws, coalesce, lit, when, upper, trim, least, current_date, col, year, month, quarter, when, lit, dense_rank, sum, count, countDistinct, avg, min, max, 
    datediff, to_date, current_date, monthname
)
from pyspark.sql.window import Window

# ───────────────────────────────────────────────
# BRONZE - CUSTOMERS (streaming CSV)
# ───────────────────────────────────────────────

@dlt.table(
    name="customer_bronze_v1",
    comment="Raw customer data ingested from CSV files (Autoloader)",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
def customer_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/pipeline_01/delta_demo/raw_data/customers/")
        .select(
            "*",
            current_timestamp().alias("processing_time"),
            col("_metadata.file_path").alias("source_file")
        )
        .withColumn(
            "registration_date_clean",
            F.coalesce(
                F.to_date(col("registration_date"), "yyyy-MM-dd"),
                F.to_date(col("registration_date"), "MM/dd/yyyy"),
                F.to_date(col("registration_date"), "dd-MM-yyyy"),
                F.to_date(col("registration_date"), "yyyyMMdd")
            )
        )
        .drop("registration_date")
        .withColumnRenamed("registration_date_clean", "registration_date")
        .withColumn(
            "phone",
            regexp_replace(col("phone"), "[^0-9]", "").cast("long")
        )
    )


# ───────────────────────────────────────────────
# SILVER - CUSTOMER DIMENSION
# ───────────────────────────────────────────────

@dlt.table(
    name="dim_customer_silver_v2",
    comment="Cleaned & enriched customer dimension",
    table_properties={"quality": "silver"}
)
def dim_customer_silver():
    customers = (
        dlt.read_stream("customer_bronze_v1")
        .drop("processing_time", "source_file")  # ← drop audit columns to prevent ambiguity
        .filter(col("customer_id").isNotNull())
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "geography_id",
            "registration_date",
            "phone"
        )
    )

    geo = dlt.read("geography_silver")

    return (
        customers
        .join(geo, on="geography_id", how="left")
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn("geography_id", coalesce(col("geography_id"), lit(0)))
        .withColumn("country", coalesce(col("country"), lit("Unknown")))
        .withColumn("state",   coalesce(col("state"),   lit("Unknown")))
        .withColumn("city",    coalesce(col("city"),    lit("Unknown")))
        .withColumn(
            "registration_date",
            coalesce(
                to_date(col("registration_date")),
                lit("2020-01-01").cast("date")
            )
        )
        .select(
            "customer_id", "full_name", "first_name", "last_name",
            "geography_id", "country", "state", "city",
            "registration_date", "phone"
        )  # explicit select → no hidden ambiguity
    )


# ───────────────────────────────────────────────
# BRONZE - ORDERS (streaming Parquet)
# ───────────────────────────────────────────────

@dlt.table(
    name="orders_bronze_v1",
    comment="Raw orders data ingested from Parquet files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/pipeline_01/delta_demo/raw_data/orders/")
        .select(
            "*",
            current_timestamp().alias("processing_time"),
            col("_metadata.file_path").alias("source_file")
        )
        .withColumn(
            "order_date_clean",
            F.coalesce(
                F.to_date(col("order_date"), "yyyy-MM-dd"),
                F.to_date(col("order_date"), "MM/dd/yyyy"),
                F.to_date(col("order_date"), "dd-MM-yyyy"),
                F.to_date(col("order_date"), "yyyyMMdd")
            )
        )
        .drop("order_date")
        .withColumnRenamed("order_date_clean", "order_date")
    )


# ───────────────────────────────────────────────
# SILVER - ORDERS FACT
# ───────────────────────────────────────────────

@dlt.table(
    name="orders_silver",
    comment="Order fact table enriched with dimensions",
    table_properties={
        "quality": "silver",
        "table_type": "fact"
    }
)
def orders_silver():
    orders = (
        dlt.read_stream("orders_bronze_v1")
        .drop("processing_time", "source_file")  # ← prevent carry-over
    )

    products = (
        dlt.read("product_silver_v2")
        .drop("processing_time", "source_file")  # if present
    )

    customers = dlt.read("dim_customer_silver_v2")

    enriched = (
        orders
        .join(products,  on="product_id",  how="left")
        .join(customers, on="customer_id", how="left")
    )

    return (
        enriched
        .withColumn("quantity",      coalesce(col("quantity"), lit(1)))
        .withColumn("unit_price",    coalesce(col("unit_price"), col("price")))
        .withColumn("discount",      least(coalesce(col("discount"), lit(0.0)), lit(1.0)))
        .withColumn("shipping_cost", coalesce(col("shipping_cost"), lit(0.0)))
        .withColumn("order_date",    coalesce(col("order_date"), current_date()))
        .withColumn("customer_id",   coalesce(col("customer_id"), lit(0)))
        .withColumn("product_id",    coalesce(col("product_id"), lit(0)))
        .select(
            "order_id", "order_date", "customer_id", "product_id",
            "quantity", "unit_price", "discount", "shipping_cost",
            "product_name", "category", "price", "cost",
            "full_name", "country", "state", "city"
        )  # no audit columns here
    )


# ───────────────────────────────────────────────
# BRONZE - PRODUCTS (streaming Parquet)
# ───────────────────────────────────────────────

@dlt.table(
    name="product_bronze_v1",
    comment="Raw product data ingested from Parquet files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
def product_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/pipeline_01/delta_demo/raw_data/products/")
        .select(
            "*",
            current_timestamp().alias("processing_time"),
            col("_metadata.file_path").alias("source_file")
        )
    )


# ───────────────────────────────────────────────
# SILVER - PRODUCTS
# ───────────────────────────────────────────────

@dlt.table(
    name="product_silver_v2",
    comment="Cleaned and enriched product dimension",
    table_properties={"quality": "silver"}
)
def product_silver():
    df = (
        dlt.read_stream("product_bronze_v1")
        .drop("processing_time", "source_file")  # ← drop early
        .filter(col("product_id").isNotNull())
    )

    return (
        df
        .withColumn(
            "price",
            when(col("price").isNull(), col("cost") * lit(1.5))
            .otherwise(col("price"))
        )
        .withColumn("category",     coalesce(col("category"), lit("UNKNOWN")))
        .withColumn("supplier_id",  coalesce(col("supplier_id"), lit(0)))
        .withColumn(
            "profit_margin_pct",
            F.when(col("price") > 0, ((col("price") - col("cost")) / col("price")) * lit(100))
             .otherwise(lit(0.0))  # avoid div-by-zero
        )
    )


# ───────────────────────────────────────────────
# BRONZE - GEOGRAPHY (batch – single file)
# ───────────────────────────────────────────────

@dlt.table(
    name="geography_bronze_v1",
    comment="Raw geography reference data (single CSV)",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
def geography_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/pipeline_01/delta_demo/raw_data/geographies.csv")
        .select(
            "*",
            current_timestamp().alias("processing_time"),
            lit("geographies.csv").alias("source_file")
        )
    )


# ───────────────────────────────────────────────
# SILVER - GEOGRAPHY
# ───────────────────────────────────────────────

@dlt.table(
    name="geography_silver",
    comment="Cleaned geography reference data",
    table_properties={
        "quality": "silver",
        "table_type": "reference"
    }
)
def geography_silver():
    return (
        dlt.read("geography_bronze_v1")
        .drop("processing_time", "source_file")  # drop audit columns
        .filter(col("geography_id").isNotNull())
        .withColumn(
            "postal_code",
            upper(
                regexp_replace(
                    trim(col("postal_code")),
                    " ", ""
                )
            )
        )
        .withColumn("updated_timestamp", current_timestamp())
    )


# ───────────────────────────────────────────────
# GOLD LAYER - AGGREGATIONS & ANALYTICS
# ───────────────────────────────────────────────

# Helper base query (common for many gold tables)
@dlt.table(
    name="gold_orders_enriched",
    comment="Temporary enriched view for gold layer calculations (not persisted as table)",
    spark_conf={"spark.databricks.delta.pipeline.avoidRepartition": "true"}
)
def gold_orders_enriched():
    """
    Base enriched dataset for gold layer.
    Calculates derived metrics once to avoid repetition.
    """
    return (
        dlt.read("orders_silver")
        .withColumn("total_amount", 
            col("quantity") * col("unit_price") * (lit(1.0) - col("discount"))
        )
        .withColumn("discount_amount", 
            col("quantity") * col("unit_price") * col("discount")
        )
        .withColumn("profit", 
            (col("unit_price") - col("cost")) * col("quantity") * (lit(1.0) - col("discount"))
        )
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
        .withColumn("quarter", quarter(col("order_date")))
        .withColumn("month_name", monthname(col("order_date")))
    )


# ───────────────────────────────────────────────
# Gold 1: Sales Summary by Date
# ───────────────────────────────────────────────

@dlt.table(
    name="sales_by_date",
    comment="Daily sales summary with key metrics",
    table_properties={"quality": "gold"}
)
def sales_by_date():
    df = dlt.read("gold_orders_enriched")

    return (
        df
        .groupBy("order_date", "year", "month", "quarter")
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity"),
            sum("total_amount").alias("total_revenue"),
            sum("discount_amount").alias("total_discount"),
            sum("shipping_cost").alias("total_shipping"),
            sum("profit").alias("total_profit"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products")
        )
        .withColumn("profit_margin_pct", 
            F.when(col("total_revenue") > 0, 
                   (col("total_profit") / col("total_revenue")) * lit(100)
            ).otherwise(lit(0.0))
        )
        .orderBy("order_date")
    )


# ───────────────────────────────────────────────
# Gold 2: Sales Summary by Product Category
# ───────────────────────────────────────────────

@dlt.table(
    name="sales_by_category",
    comment="Sales performance aggregated by product category",
    table_properties={"quality": "gold"}
)
def sales_by_category():
    df = dlt.read("gold_orders_enriched")

    category_agg = (
        df
        .groupBy("category")
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity_sold"),
            sum("total_amount").alias("total_revenue"),
            sum("profit").alias("total_profit"),
            avg("total_amount").alias("avg_order_value"),
            avg("unit_price").alias("avg_unit_price"),
            countDistinct("product_id").alias("unique_products"),
            countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("profit_margin_pct", 
            F.when(col("total_revenue") > 0, 
                   (col("total_profit") / col("total_revenue")) * lit(100)
            ).otherwise(lit(0.0))
        )
    )

    total_revenue_all = df.agg(sum("total_amount").alias("total")).collect()[0]["total"]

    return (
        category_agg
        .withColumn("revenue_share_pct", 
            (col("total_revenue") / lit(total_revenue_all)) * lit(100)
        )
        .orderBy(F.desc("total_revenue"))
    )


# ───────────────────────────────────────────────
# Gold 3: Sales Summary by Geography
# ───────────────────────────────────────────────

@dlt.table(
    name="sales_by_geography",
    comment="Sales performance by country/state/city",
    table_properties={"quality": "gold"}
)
def sales_by_geography():
    df = dlt.read("gold_orders_enriched")

    return (
        df
        .groupBy("country", "state", "city")
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity_sold"),
            sum("total_amount").alias("total_revenue"),
            sum("profit").alias("total_profit"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products")
        )
        .withColumn("profit_margin_pct", 
            F.when(col("total_revenue") > 0, 
                   (col("total_profit") / col("total_revenue")) * lit(100)
            ).otherwise(lit(0.0))
        )
        .orderBy(F.desc("total_revenue"))
    )


# ───────────────────────────────────────────────
# Gold 4: Customer Analytics (RFM-like + segmentation)
# ───────────────────────────────────────────────

@dlt.table(
    name="customer_analytics",
    comment="Customer-level lifetime metrics and segmentation",
    table_properties={"quality": "gold"}
)
def customer_analytics():
    df = dlt.read("gold_orders_enriched")

    return (
        df
        .groupBy("customer_id", "full_name", "country", "state", "city")
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_items_purchased"),
            sum("total_amount").alias("lifetime_value"),
            avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            countDistinct("product_id").alias("unique_products_purchased"),
            countDistinct("category").alias("unique_categories_purchased")
        )
        .withColumn("days_since_first_order", 
            datediff(current_date(), col("first_order_date"))
        )
        .withColumn("days_since_last_order", 
            datediff(current_date(), col("last_order_date"))
        )
        .withColumn("customer_segment",
            when(col("lifetime_value") >= 10000, lit("VIP"))
            .when(col("lifetime_value") >= 5000,  lit("Premium"))
            .when(col("lifetime_value") >= 1000,  lit("Regular"))
            .otherwise(lit("New"))
        )
        .orderBy(F.desc("lifetime_value"))
    )


# ───────────────────────────────────────────────
# Gold 5: Product Performance
# ───────────────────────────────────────────────

@dlt.table(
    name="product_performance",
    comment="Product-level sales and profitability metrics",
    table_properties={"quality": "gold"}
)
def product_performance():
    df = dlt.read("gold_orders_enriched")

    return (
        df
        .groupBy("product_id", "product_name", "category")
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity_sold"),
            sum("total_amount").alias("total_revenue"),
            sum("profit").alias("total_profit"),
            avg("unit_price").alias("avg_selling_price"),
            min("unit_price").alias("min_price"),
            max("unit_price").alias("max_price"),
            countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("profit_margin_pct", 
            F.when(col("total_revenue") > 0, 
                   (col("total_profit") / col("total_revenue")) * lit(100)
            ).otherwise(lit(0.0))
        )
        .withColumn("avg_quantity_per_order", 
            col("total_quantity_sold") / col("total_orders")
        )
        .withColumn("product_rank", 
            F.dense_rank().over(Window.orderBy(F.desc("total_revenue")))
        )
        .orderBy(F.desc("total_revenue"))
    )


# ───────────────────────────────────────────────
# Gold 6: Monthly Sales Trend
# ───────────────────────────────────────────────

@dlt.table(
    name="monthly_sales_trend",
    comment="Monthly aggregated sales trends",
    table_properties={"quality": "gold"}
)
def monthly_sales_trend():
    df = dlt.read("gold_orders_enriched")

    return (
        df
        .groupBy("year", "month", "quarter", "month_name")
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity"),
            sum("total_amount").alias("total_revenue"),
            sum("profit").alias("total_profit"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products")
        )
        .withColumn("profit_margin_pct", 
            F.when(col("total_revenue") > 0, 
                   (col("total_profit") / col("total_revenue")) * lit(100)
            ).otherwise(lit(0.0))
        )
        .orderBy("year", "month")
    )