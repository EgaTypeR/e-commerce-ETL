from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName('Sales-ETL').getOrCreate()
# Load the dataset
data_path = "./sales.csv"  # Adjust this path to your dataset

# Load the Excel file
sales_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Calculate total sales, quantity, and profit by region
sales_summary_by_region = sales_df.groupBy("Region").agg(
    sum("Sales").alias("Total_Sales"),
    sum("Quantity").alias("Total_Quantity"),
    sum("Profit").alias("Total_Profit")
)

# Show the result
sales_summary_by_region.show()
