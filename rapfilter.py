### Python Script to filter out Top 5 rap artists' songs ###

from pyspark.sql import SparkSession
import time

# Initialise a Spark session
spark = SparkSession.builder.appName("FilterTop5Rap").getOrCreate()

# Start timing
start_time = time.time()

# Read the CSV file from HDFS
df = spark.read.option("header", "true") \
               .option("parserLib", "univocity") \
               .option("multiLine", "true") \
               .option("escape", '"') \
               .csv('/user/hadoop/BDAss/song_lyrics.csv', encoding='utf-8')

# List of Top 5 artists to filter
artists = ["Eminem", "JAY-Z", "Kendrick Lamar", "Nas", "2Pac"]

# Filter rows where the artist is in the list
filtered_df = df.filter(df.artist.isin(artists))

# Select only the song name and song lyrics columns
selected_df = filtered_df.select("title", "artist", "lyrics")

# Write the filtered DataFrame back to HDFS
output_path = '/user/hadoop/BDAss/t5rap'
selected_df.write.option("header", "true").csv(output_path)

# Stop timing
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time:.2f} seconds")

# Stop the Spark session
spark.stop()
