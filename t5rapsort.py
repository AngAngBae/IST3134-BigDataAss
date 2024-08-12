### Python Script to filter t5rap.csv into individual artist csv ###

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
import time

# Initialise a Spark session
spark = SparkSession.builder.appName("SortTop5Rap").getOrCreate()

# Start timing
start_time = time.time()

# Read the CSV file from HDFS
df = spark.read.option("header", "true") \
               .option("parserLib", "univocity") \
               .option("multiLine", "true") \
               .option("escape", '"') \
               .csv('/user/hadoop/BDAss/t5rap.csv', encoding='utf-8')

# List of Top 5 artists to filter
artists = ["Eminem", "JAY-Z", "Kendrick Lamar", "Nas", "2Pac"]

# Filter rows where the artist is in the list
filtered_df = df.filter(df.artist.isin(artists))

# Convert columns to lowercase
filtered_df = filtered_df.withColumn("title", lower(col("title")))
filtered_df = filtered_df.withColumn("artist", lower(col("artist")))
filtered_df = filtered_df.withColumn("lyrics", lower(col("lyrics")))

# For each artist, filter their songs and write to an individual CSV file
for artist in artists:
    artist_df = filtered_df.filter(col("artist") == artist.lower())
    
    # Define output path for each artist
    output_path = f'/user/hadoop/BDAss/rap/{artist.replace(" ", "_")}'
    
    # Write the DataFrame to HDFS
    artist_df.write.mode("overwrite").option("header", "true").csv(output_path)

# Stop timing
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time:.2f} seconds")

# Stop the Spark session
spark.stop()
