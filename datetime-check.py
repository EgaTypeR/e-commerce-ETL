from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, year, month, dayofmonth, col, to_date, date_format
from dotenv import load_dotenv
import os

load_dotenv()
data_path = "./data/amazon_sales_report.csv"

spark = SparkSession.builder \
        .appName("Check-DateTime") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .getOrCreate()

sales_df = spark.read.option("header", "true").csv(data_path, inferSchema=True)
sales_df = sales_df.toDF(*[col_name.strip() for col_name in sales_df.columns])
print(sales_df.head())
print("------\n")
sales_df = sales_df.withColumn("Date", to_date(date_format(col("Date"), "yyyy-MM-dd"), "yyyy-MM-dd"))
print(sales_df.head())
# Check for null dates
null_count = sales_df.filter(col("Date").isNull()).count()
print(f"Number of null dates after casting: {null_count}")