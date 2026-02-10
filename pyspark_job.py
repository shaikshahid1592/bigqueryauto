from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_ID = "burnished-web-484613-t0"
BUCKET = "testhouseprice"

spark = SparkSession.builder \
    .appName("HousePriceProcessing") \
    .getOrCreate()


# Read CSV from GCS
df = spark.read.csv(
    f"gs://{BUCKET}/houseprice.csv",
    header=True,
    inferSchema=True
)

print("Original Data")
df.show(5)


# Transform data
df_transformed = df.withColumn(
    "price_per_sqft",
    col("price") / col("sqft_living")
)

print("Transformed Data")
df_transformed.show(5)


df.write \
    .format("bigquery") \
    .option("table", "burnished-web-484613-t0:house_dataset.house_prices_stream") \
    .option("temporaryGcsBucket", "testhouseprice") \
    .mode("append") \
    .save()


print("Data written to BigQuery successfully")
