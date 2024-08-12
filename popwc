### Python Script to run MapReduce on all the Top 5 pop artists' songs ###

from pyspark.sql import SparkSession
import subprocess
import os
import time

# Initialise a Spark session
spark = SparkSession.builder.appName("Top5PopWC").getOrCreate()

# Start timing
start_time = time.time()

## Running MapReduce
# Define base input and output paths
input_base_path = "BDAss/pop"
output_base_path = "BDAss/pop_wc"

# List of pop artists
artists = ["Beyonc", "Michael_Jackson", "Taylor_Swift", "Britney_Spears", "Lady_Gaga"]

# Define the Hadoop JAR command
hadoop_jar_command = "hadoop jar wc.jar stubs.WordCount"

# Function to run Hadoop command
def run_word_count(artist):
    input_path = f"{input_base_path}/{artist}.csv"
    output_path = f"{output_base_path}/{artist}wc"
    
    command = f"{hadoop_jar_command} {input_path} {output_path}"
    print(f"Running command: {command}")
    
    result = subprocess.run(command, shell=True, text=True, capture_output=True)
    if result.returncode == 0:
        print(f"Success for artist: {artist}")
    else:
        print(f"Error for artist: {artist}")
        print(f"Error message: {result.stderr}")

# Run word count for each artist
for artist in artists:
    run_word_count(artist)

print("Word count processing completed for all pop artists.")

## Merge the files
# Define the base directory where the artist folders are located
base_dir = "/user/hadoop/BDAss/pop_wc"

# Function to execute Hadoop commands
def run_command(command):
    try:
        subprocess.run(command, check=True, shell=True)
        print(f"Successfully executed: {command}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing: {command}\n{e}")

# Function to ensure the destination directory exists
def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Process each artist
for artist in artists:
    folder_path = f"{base_dir}/{artist}wc"
    output_file_path = f"/tmp/{artist}_wc"  # Use a local path for merging

    # Ensure the output directory exists
    ensure_directory_exists('/tmp')

    # Merge files from the artist's folder into a single file
    merge_command = f"hadoop fs -getmerge {folder_path} {output_file_path}"
    run_command(merge_command)
    
    # If merge is successful, upload the file back to HDFS and delete the local file
    upload_command = f"hadoop fs -copyFromLocal {output_file_path} {base_dir}/{artist}_wc"
    run_command(upload_command)
    
    # Remove the local temporary file
    os.remove(output_file_path)

    # Delete the artist's folder from HDFS
    delete_command = f"hadoop fs -rm -r {folder_path}"
    run_command(delete_command)

# Stop timing
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time:.2f} seconds")

# Stop the Spark session
spark.stop()
