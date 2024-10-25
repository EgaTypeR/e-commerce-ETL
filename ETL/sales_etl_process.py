from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, year, month, dayofmonth, col, to_date
from dotenv import load_dotenv
import os

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Sales-ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark

def load_data(spark, path):
    sales_df = spark.read.option("header", "true").csv(path, inferSchema=True)

    sales_df = sales_df.toDF(*[col_name.strip() for col_name in sales_df.columns])

    sales_df = sales_df.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))

    # Check for null dates
    null_count = sales_df.filter(col("Date").isNull()).count()
    print(f"Number of null dates after casting: {null_count}")

    return sales_df


def create_date_dim(sales_df):
    date_dim = sales_df.select("Date").distinct()
    date_dim = date_dim.withColumn("Year", year(col("Date"))) \
                       .withColumn("Month", month(col("Date"))) \
                       .withColumn("Day", dayofmonth(col("Date")))
    return date_dim

# Create Product Dimension Table
def create_product_dim(sales_df):
    product_dim = sales_df.select("SKU", "Style", "Category", "Size", "ASIN").distinct()
    return product_dim

# Create Customer Dimension Table
def create_customer_dim(sales_df):
    customer_dim = sales_df.select("Order ID", "ship-city", "ship-state", "ship-postal-code", "ship-country").distinct()
    return customer_dim

# Create Shipping Dimension Table
def create_shipping_dim(sales_df):
    shipping_dim = sales_df.select("Order ID", "Fulfilment", "Sales Channel", "ship-service-level", "Courier Status").distinct()
    return shipping_dim

# Create Promotion Dimension Table
def create_promotion_dim(sales_df):
    promotion_dim = sales_df.select("promotion-ids").distinct()
    return promotion_dim

# Create Fact Table
def create_fact_sales(sales_df):
    fact_sales = sales_df.select("Order ID", "Date", "SKU", "Qty", "Amount", "B2B", "fulfilled-by")
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
