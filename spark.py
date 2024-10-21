from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Sales-ETL').getOrCreate()
# Load the dataset
data_path = "/sales.xlsx"  # Adjust this path to your dataset
sales_df = spark.read.excel(data_path, header=True, inferSchema=True)

# Show the dataset
sales_df.show()