from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum, year, month, dayofmonth, col, to_date, concat_ws
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from dotenv import load_dotenv
import os

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Sales-ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark

# Function to convert to snake case
def format_column(s: str):
    s = s.strip()
    return s.lower().replace(" ", "_").replace("-", "_")


def load_data(spark, path):
    sales_df = spark.read.option("header", "true").csv(path, inferSchema=True)

    sales_df = sales_df.toDF(*[format_column(col_name) for col_name in sales_df.columns])

    sales_df = sales_df.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))

    sales_df = sales_df.withColumn("order_sku_id", concat_ws("_", col("order_id"), col("sku")))

    sales_df = sales_df.dropDuplicates(["order_sku_id"])

    return sales_df


def create_date_dim(sales_df):
    date_dim = sales_df.select("date").distinct()
    date_dim = date_dim.withColumn("year", year(col("date"))) \
                       .withColumn("month", month(col("date"))) \
                       .withColumn("day", dayofmonth(col("date")))
    date_dim = date_dim.dropDuplicates(["date"])
    return date_dim

# Create Product Dimension Table
def create_product_dim(sales_df):
    product_dim = sales_df.select("sku", "style", "category", "size", "asin").distinct()
    product_dim = product_dim.dropDuplicates(["sku"])
    return product_dim

# Create Customer Dimension Table
def create_customer_dim(sales_df):
    customer_dim = sales_df.select("order_id", "ship_city", "ship_state", "ship_postal_code", "ship_country").distinct()
    return customer_dim

def create_shipping_dim(sales_df):
    # Define a window specification to prioritize non-cancelled and non-NaN entries for each Order ID
    window = Window.partitionBy("order_id").orderBy(
        F.when(F.col("courier_status") == "Cancelled", 1)
        .when(F.col("courier_status").isNull(), 2)
        .otherwise(0)
    )

    # Assign row numbers based on the window specification
    shipping_dim = sales_df.select(
        "order_id", 
        "fulfilment", 
        "sales_channel", 
        "ship_service_level", 
        "courier_status"
    ).withColumn("rank", F.row_number().over(window))

    # Filter to keep only the first row for each Order ID, prioritizing non-cancelled and non-NaN rows
    shipping_dim = shipping_dim.filter(F.col("rank") == 1)

    # Drop the rank column after filtering
    shipping_dim = shipping_dim.drop("rank").distinct()

    return shipping_dim

# Create Promotion Dimension Table
def create_promotion_dim(sales_df):
    promotion_dim = sales_df.select("promotion_ids").distinct()
    return promotion_dim

# Create Fact Table
def create_fact_sales(sales_df):

    decimal_type = DecimalType(10, 2)

    fact_sales = sales_df.select(
        "order_sku_id",
        "order_id", 
        "date", 
        "sku", 
        "qty", 
        F.col("amount").cast(decimal_type).alias("amount"),  # Casting "amount" to DecimalType
        "b2b", 
        "fulfilled_by"
    )
    return fact_sales

# Write DataFrame to PostgreSQL
def write_to_postgresql(df, table_name, db_url, db_user, db_password):
    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .option("ssl", "true") \
        .mode("append") \
        .save()

def main():
    load_dotenv()  # Load environment variables
    db_url = os.getenv("DB_URL")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    data_path = os.getenv("CSV_PATH")
    
    
    # Initialize Spark session
    spark = create_spark_session()
    
    # Load the dataset
    sales_df = load_data(spark, data_path)
    
    # Create dimension and fact tables
    date_dim = create_date_dim(sales_df)
    product_dim = create_product_dim(sales_df)
    customer_dim = create_customer_dim(sales_df)
    shipping_dim = create_shipping_dim(sales_df)
    promotion_dim = create_promotion_dim(sales_df)
    fact_sales = create_fact_sales(sales_df)
    
    # Write tables to PostgreSQL
    write_to_postgresql(date_dim, "dim_date", db_url, db_user, db_password)
    write_to_postgresql(product_dim, "dim_product", db_url, db_user, db_password)
    write_to_postgresql(customer_dim, "dim_customer", db_url, db_user, db_password)
    write_to_postgresql(shipping_dim, "dim_shipping", db_url, db_user, db_password)
    write_to_postgresql(promotion_dim, "dim_promotion", db_url, db_user, db_password)
    write_to_postgresql(fact_sales, "fact_sales", db_url, db_user, db_password)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
