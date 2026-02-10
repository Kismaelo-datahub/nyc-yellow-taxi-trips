# %%
import sys
!{sys.executable} -m pip install pyspark==3.5.4

# %%
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("YellowTripData") \
    .getOrCreate()

# %%
# Load the Parquet file into a DataFrame
file_path = "data/yellow_tripdata_2024-11.parquet"
yellow_tripdata_df = spark.read.parquet(file_path)

# %%
# Show the first few rows of the DataFrame
yellow_tripdata_df.show()

# %%
from pyspark.sql.functions import col, isnan, when, count

# Print the schema of the DataFrame
yellow_tripdata_df.printSchema()


# %%
# Create a list to handle missing values appropriately for each column type
missing_values = yellow_tripdata_df.select(
    [
        count(when(col(c).isNull(), c)).alias(c)
        for c in yellow_tripdata_df.columns
    ]
)

# Show the missing values count per column
missing_values.show()

# %%
import sys
!{sys.executable} -m pip install pyarrow

# %%

import os
import io
import pyarrow.parquet as pq
from google.cloud import storage

PROJECT_ID = "smiling-rhythm-486213-b9"
BUCKET_NAME = f"{PROJECT_ID}-data-bucket"
GCS_FOLDER = "dataset/trips/"

storage_client = storage.Client(project=PROJECT_ID)

def inspect_parquet_schema(file_name):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{GCS_FOLDER}{file_name}")

    file_stream = io.BytesIO()
    blob.download_to_file(file_stream)
    file_stream.seek(0)

    table = pq.read_table(file_stream)
    print(table.schema)

inspect_parquet_schema("yellow_tripdata_2023-01.parquet")

# %%
inspect_parquet_schema("yellow_tripdata_2023-12.parquet")

# %%
inspect_parquet_schema("yellow_tripdata_2024-01.parquet")

# %%
inspect_parquet_schema("yellow_tripdata_2023-06.parquet")

# %%
inspect_parquet_schema("yellow_tripdata_2025-01.parquet")

