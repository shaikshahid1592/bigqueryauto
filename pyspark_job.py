from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType

# Configuration
PROJECT_ID = "burnished-web-484613-t0"
BUCKET = "testhouseprice"
INPUT_FILE = f"gs://{BUCKET}/houseprice.csv"
OUTPUT_TABLE = f"{PROJECT_ID}:house_dataset.house_prices_stream"
TEMP_BUCKET = BUCKET

# Create Spark session
spark = SparkSession.builder \
    .appName("HousePriceProcessing") \
    .getOrCreate()


# Read CSV from GCS
df = spark.read.csv(
    INPUT_FILE,
    header=True,
    inferSchema=True
)

print("Original Data")
df.show(5)

print("Original Schema")
df.printSchema()


# Fix schema to match BigQuery table
# BigQuery expects FLOAT for price and bathrooms
df = df.withColumn("price", col("price").cast(DoubleType())) \
       .withColumn("bathrooms", col("bathrooms").cast(DoubleType())) \
       .withColumn("sqft_living", col("sqft_living").cast(IntegerType())) \
       .withColumn("bedrooms", col("bedrooms").cast(IntegerType())) \
       .withColumn("floors", col("floors").cast(IntegerType()))


# Transform data
df_transformed = df.withColumn(
    "price_per_sqft",
    col("price") / col("sqft_living")
)

print("Transformed Data")
df_transformed.show(5)

print("Transformed Schema")
df_transformed.printSchema()


# Write to BigQuery
df_transformed.write \
    .format("bigquery") \
    .option("table", OUTPUT_TABLE) \
    .option("temporaryGcsBucket", TEMP_BUCKET) \
    .mode("append") \
    .save()


print("Data written to BigQuery successfully")


# Stop Spark session
spark.stop()
