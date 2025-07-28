from pyspark.sql import SparkSession
import pandas as pd
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("BigDataPipeline").getOrCreate()

# Input file
input_file = "student_data_with_duplicates.csv"

# Read data
df = spark.read.csv(input_file, header=True, inferSchema=True)

print("Original Data:")
df.show(5)

# Remove duplicates
df_clean = df.dropDuplicates()

# Create output file name
output_file = "cleaned_" + os.path.basename(input_file)

# Save cleaned data as single CSV file
df_clean.coalesce(1).write.csv(output_file, header=True, mode="overwrite")

spark.stop()
