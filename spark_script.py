from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnan, mean, lit
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
numeric_cols = [c for (c, t) in df_clean.dtypes if t in ('int', 'double', 'float')]
string_cols = [c for (c, t) in df_clean.dtypes if t == 'string']

for col_name in numeric_cols:
    mean_value = df_clean.select(mean(col(col_name))).collect()[0][0]
    if mean_value is not None:
        df_clean = df_clean.na.fill({col_name: mean_value})

for col_name in string_cols:
    df_clean = df_clean.na.fill({col_name: 'Unknown'})

# 3️⃣ Remove Outliers (Example: Filtering unrealistic ages)
if 'age' in [c.lower() for c in df_clean.columns]:
    df_clean = df_clean.filter((col('age') > 0) & (col('age') < 120))

# 4️⃣ Add a Derived Column (Example: Category based on Marks)
if 'marks' in [c.lower() for c in df_clean.columns]:
    df_clean = df_clean.withColumn(
        "grade",
        when(col("marks") >= 90, "A")
        .when((col("marks") >= 75) & (col("marks") < 90), "B")
        .when((col("marks") >= 50) & (col("marks") < 75), "C")
        .otherwise("D")
    )

# 5️⃣ Basic Data Quality Check (Safe Missing Value Count)
# 5️⃣ Basic Data Quality Check (Safe Missing Value Count)
missing_exprs = []
for c, t in df_clean.dtypes:
    if t in ("double", "float", "int"):  # only numeric columns
        expr = count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
    else:
        # For string, date, boolean, timestamp → only check for nulls
        expr = count(when(col(c).isNull(), lit(1))).alias(c)
    missing_exprs.append(expr)

print("📌 Missing Values Count per Column:")
df_clean.select(missing_exprs).show()


# 6️⃣ Summary Statistics
print("📌 Summary Statistics:")
df_clean.describe().show()

# 7️⃣ Save cleaned data as a single CSV file
output_file = "cleaned_" + os.path.basename(input_file)
df_clean.coalesce(1).write.csv(output_file, header=True, mode="overwrite")

print(f"✅ Cleaned data saved to: {output_file}")

spark.stop()
