import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Function to read YAML configuration file
def read_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Read configuration
config = read_config("config.yaml")

# Create Spark session
spark = SparkSession.builder \
    .appName(config["spark"]["app"]["name"]) \
    .master(config["spark"]["master"]) \
    .config("spark.executor.memory", config["spark"]["executor"]["memory"]) \
    .config("spark.executor.cores", config["spark"]["executor"]["cores"]) \
    .getOrCreate()

# Read data from source
df = spark.read.format(config["data"]["input"]["format"]).load(config["data"]["input"]["path"])

# Transform data
df_transformed = df.withColumnRenamed(config["transformations"]["rename_columns"]["old_name"], 
                                      config["transformations"]["rename_columns"]["new_name"])
df_filtered = df_transformed.filter(config["transformations"]["filter_condition"])

# Show transformed data
df_filtered.show()

# Write transformed data to destination
df_filtered.write.format(config["data"]["output"]["format"]).save(config["data"]["output"]["path"])

print (This is a Python pipeline)