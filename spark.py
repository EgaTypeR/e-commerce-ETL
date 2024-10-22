from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Sales-ETL').getOrCreate()
# Load the dataset
data_path = "/sales.xlsx"  # Adjust this path to your dataset

# Load the Excel file
excel_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").option("dataAddress", "'Sheet1'!A1").load(data_path)

# Show the dataset
excel_df.show()