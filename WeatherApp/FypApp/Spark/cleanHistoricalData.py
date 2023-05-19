from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_unixtime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, approx_count_distinct
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql import Row
import sys

# Create a SparkSession
spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()

# Read the dataset
#df = spark.read.csv("D:\CSIT321P\Project\Changes-master\FypApp\DataStorage\data\combined_csv.csv", header=True, inferSchema=True)

if len(sys.argv) < 2:
    print("Please provide the input file path as a command-line argument.")
    sys.exit(1)

# Get the input file path from command-line argument

input_file_path = sys.argv[1]
print("File path given",input_file_path)
df = spark.read.csv(input_file_path, header=True, inferSchema=True)

print("Progress: 10%")
# Dropping unrelated columns
if 'Unnamed: 0' in df.columns:
    df = df.drop('Unnamed: 0')
if "time_epoch" in df.columns:
    df = df.drop('time_epoch')

df.show()
print("Progress: 20%")
if "condition" in df.columns:
    # perform some operation on the column
    clear_values = udf(lambda x: x.split("'text': '")[1].split("',")[0], StringType())
    df = df.withColumn('condition', clear_values(col('condition')))
else:
    print("Column 'condition' not found in the dataframe.")
# Use string manipulation to extract the 'text' value from 'condition' column

print("Progress: 30%")
###################################### DUPLICATES ############################################
# Check for duplicates
print(f"Checking duplicates in the dataset...")
duplicates = df.dropDuplicates()
print("Progress: 40%")
# Count the number of duplicates
num_duplicates = duplicates.count()

print(f"There are {num_duplicates} duplicates in the dataset.")
print("Progress: 50%")
# Drop the duplicates
if num_duplicates > 0:
    print(f"Cleaning duplicates...")
    df = df.dropDuplicates()
    print(f"Clean.")
else:
    print(f"Done.")
print("Progress: 70%")
df.show()
###################################### MISSING ############################################
#Checking missing columns
print(f"Checking missing values in the dataset...")
missing_values = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()

num_cols = []
cat_cols = []

if missing_values.any().any():
    cat_cols = [c[0] for c in df.dtypes if c[1] == 'string']
    num_cols = [c[0] for c in df.dtypes if c[1] != 'string']
    fill_values = {}
    for col in cat_cols:
        fill_values[col] = df.select(col).groupBy(col).count().orderBy(desc("count")).collect()[0][0]
    for col in num_cols:
        fill_values[col] = df.select(col).agg(avg(col)).collect()[0][0]
    df = df.fillna(fill_values)
else:
    print(f"No Missing Values.")
print("Progress: 80%")


##############################################################################################
# Cleaned datasets
#df = df.select(num_cols + cat_cols)
check = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
if check.any().any():
    print(f"Failed to clean datasets.")
else:
    print(f"Successfully cleaning data.")
print("Progress: 90%")
df.show()
df = df.sort(df.columns[0])
print(f"Importing to csv...")
df.write.mode("overwrite").csv("/usr/local/output", header=True)
print("Progress: 100%")
spark.stop()
print("Cleaned data")