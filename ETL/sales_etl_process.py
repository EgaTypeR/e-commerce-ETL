from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, year, month, dayofmonth, col
from dotenv import load_dotenv
import os

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Sales-ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .getOrCreate()
    return spark

def load_data(spark, path):
    sales_df = spark.read.option("header", "true").csv(path, inferSchema=True) \
        .withColumnRenamed(" Amount ", "Amount") \
        .withColumnRenamed("Sales Channel ", "Sales_Channel")
    sales_df = sales_df.withColumn("Date", col("Date").cast("date"))
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
    fact_sales = sales_df.select("Order ID", "Date", "SKU", "Qty", " Amount ", "B2B", "fulfilled-by") \
                         .withColumnRenamed(" Amount ", "Amount")
    return fact_sales

# Write DataFrame to PostgreSQL
def write_to_postgresql(df, table_name, db_url, db_properties):
    df.write.jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)

def main():
    load_dotenv()  # Load environment variables
    db_url = os.getenv("DB_URL")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    data_path = os.getenv("CSV_PATH")
    
    # Database connection details
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }
    
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
    write_to_postgresql(date_dim, "dim_date", db_url, db_properties)
    write_to_postgresql(product_dim, "dim_product", db_url, db_properties)
    write_to_postgresql(customer_dim, "dim_customer", db_url, db_properties)
    write_to_postgresql(shipping_dim, "dim_shipping", db_url, db_properties)
    write_to_postgresql(promotion_dim, "dim_promotion", db_url, db_properties)
    write_to_postgresql(fact_sales, "fact_sales", db_url, db_properties)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
