### Python Script to collectively rename and merge csv files ###

from pyspark.sql import SparkSession
import subprocess
import os

# Initialise a Spark session
spark = SparkSession.builder.appName("Top5RapWC").getOrCreate()

# Define the base directory where the artist folders are located
base_dir = "/user/hadoop/BDAss/pop"

# List of artists
artists = ["Beyonc", "Michael_Jackson", "Taylor_Swift", "Britney_Spears", "Lady_Gaga"]

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
    folder_path = f"{base_dir}/{artist}"
    output_file_path = f"/tmp/{artist}.csv"  # Use a local path for merging

    # Ensure the output directory exists
    ensure_directory_exists('/tmp')

    # Merge files from the artist's folder into a single CSV file
    merge_command = f"hadoop fs -getmerge {folder_path} {output_file_path}"
    run_command(merge_command)
    
    # If merge is successful, upload the file back to HDFS and delete the local file
    upload_command = f"hadoop fs -copyFromLocal {output_file_path} {base_dir}/{artist}.csv"
    run_command(upload_command)
    
    # Remove the local temporary file
    os.remove(output_file_path)

    # Delete the artist's folder from HDFS
    delete_command = f"hadoop fs -rm -r {folder_path}"
    run_command(delete_command)

# Stop the Spark session
spark.stop()
