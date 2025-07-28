from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnan, mean
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("BigDataPipelineEnhanced").getOrCreate()

# Input file
input_file = "student_data_with_duplicates.csv"

# Read data
df = spark.read.csv(input_file, header=True, inferSchema=True)

print("📌 Original Data Sample:")
df.show(5)

# 1️⃣ Remove Duplicates
df_clean = df.dropDuplicates()

# 2️⃣ Handle Missing Values
# Example: Fill missing numeric columns with mean, string columns with 'Unknown'
numeric_cols = [c for (c, t) in df_clean.dtypes if t in ('int', 'double')]
string_cols = [c for (c, t) in df_clean.dtypes if t == 'string']

for col_name in numeric_cols:
    mean_value = df_clean.select(mean(col(col_name))).collect()[0][0]
    df_clean = df_clean.na.fill({col_name: mean_value})

for col_name in string_cols:
    df_clean = df_clean.na.fill({col_name: 'Unknown'})

# 3️⃣ Remove Outliers (Example: Filtering unrealistic ages)
if 'age' in df_clean.columns:
    df_clean = df_clean.filter((col('age') > 0) & (col('age') < 120))

# 4️⃣ Add a Derived Column (Example: Category based on Marks)
if 'marks' in df_clean.columns:
    df_clean = df_clean.withColumn(
        "grade",
        when(col("marks") >= 90, "A")
        .when((col("marks") >= 75) & (col("marks") < 90), "B")
        .when((col("marks") >= 50) & (col("marks") < 75), "C")
        .otherwise("D")
    )

# 5️⃣ Basic Data Quality Check (Missing Value Count per Column)
print("📌 Missing Values Count per Column:")
df_clean.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df_clean.columns]).show()

# 6️⃣ Summary Statistics
print("📌 Summary Statistics:")
df_clean.describe().show()

# 7️⃣ Save cleaned data as a single CSV file
output_file = "cleaned_" + os.path.basename(input_file)
df_clean.coalesce(1).write.csv(output_file, header=True, mode="overwrite")

print(f"✅ Cleaned data saved to: {output_file}")

spark.stop()
