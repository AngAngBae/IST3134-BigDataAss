### Python Script to filter out Top 5 pop artists' songs ###

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace
import time

# Initialise a Spark session
spark = SparkSession.builder.appName("FilterTop5Pop").getOrCreate()

# Start timing
start_time = time.time()

# Read the CSV file from HDFS
df = spark.read.option("header", "true") \
               .option("parserLib", "univocity") \
               .option("multiLine", "true") \
               .option("escape", '"') \
               .csv('/user/hadoop/BDAss/song_lyrics.csv', encoding='utf-8')

# List of Top 5 artists to filter
artists = ["Beyonc", "Michael Jackson", "Taylor Swift", "Britney Spears", "Lady Gaga"]

# Normalize artist names in the DataFrame and in the list
df = df.withColumn("artist", trim(regexp_replace(col("artist"), "\u00A0", " ")))

# Filter rows where the artist is in the list
filtered_df = df.filter(col("artist").isin(artists))

# Select only the song name and song lyrics columns
selected_df = filtered_df.select("title", "artist", "lyrics")

# Write the filtered DataFrame back to HDFS
output_path = '/user/hadoop/BDAss/t5pop'
selected_df.write.option("header", "true").csv(output_path)

# Stop timing
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time:.2f} seconds")

# Stop the Spark session
spark.stop()

