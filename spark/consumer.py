from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
from typing import TypeVar
import json

import findspark
import pandas as pd
import pickle

# Define a pandas UDF for salary prediction
@pandas_udf(DoubleType())
def predict_salary(*features: pd.Series) -> pd.Series:
    global model
    col_names = ["Experience", "Qualifications", "location", "Country", "latitude", "longitude", "Work Type", "Company Size", "Preference", "Job Title", "Role"]
    features = pd.concat(features, axis=1)
    features.columns = col_names
    # If there are any null values in 'Qualifications', return 0
    if features["Qualifications"].isnull().values.any():
        return pd.Series([0]*len(features))
    # Predict the salary using the model
    predictions = model.predict(features)
    return pd.Series(predictions)

# Function to process each batch of data
def process_batch(batch_df, batch_id):
    
    # Drop rows with any null values
    batch_df = batch_df.dropna()
        
    global inverse_mappings

    print(f"Processing batch {batch_id}")
    batch_df.show()
    
    # Cast columns to appropriate data types
    batch_df = batch_df \
        .withColumn("Experience", batch_df["Experience"].cast(FloatType())) \
        .withColumn("Qualifications", batch_df["Qualifications"].cast(IntegerType())) \
        .withColumn("location", batch_df["location"].cast(IntegerType())) \
        .withColumn("Country", batch_df["Country"].cast(IntegerType())) \
        .withColumn("latitude", batch_df["latitude"].cast(FloatType())) \
        .withColumn("longitude", batch_df["longitude"].cast(FloatType())) \
        .withColumn("Work Type", batch_df["Work Type"].cast(IntegerType())) \
        .withColumn("Company Size", batch_df["Company Size"].cast(IntegerType())) \
        .withColumn("Preference", batch_df["Preference"].cast(IntegerType())) \
        .withColumn("Job Title", batch_df["Job Title"].cast(IntegerType())) \
        .withColumn("Role", batch_df["Role"].cast(IntegerType()))
        
    # Columns to be used for the model
    model_cols = ["Experience", "Qualifications", "location", "Country", "latitude", "longitude", "Work Type", "Company Size", "Preference", "Job Title", "Role"]
    
    # Add salary predictions to the DataFrame
    batch_df = batch_df.withColumn("Salary", predict_salary(*[col(colmn) for colmn in model_cols]))
    
    col_names = ["Qualifications", "location", "Country", "Work Type", "Preference", "Job Title", "Role"]
    
    # If there are no null values in 'Qualifications', perform inverse mapping
    if batch_df.filter(col("Qualifications").isNull()).count() == 0:
        for colu in col_names:
            def inverse_map_category(encoded: pd.Series) -> pd.Series:
                return pd.Series([inverse_mappings[colu][str(enc)] for enc in encoded])
            inverse_udf = pandas_udf(inverse_map_category, StringType())
            batch_df = batch_df.withColumn(colu, inverse_udf(col(colu)))
    else:
        return
    
    batch_df = batch_df.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    
    # Write the batch DataFrame to the console
    batch_df.write \
        .format("console") \
        .option("truncate", False) \
        .save()
    
    batch_df.write.format("es").option("truncate", False) \
        .option("checkpointLocation", "/tmp/") \
        .option("es.port", "9200") \
        .option("es.nodes", "https://10.0.9.28:9200") \
        .option("es.nodes.wan.only", "false") \
        .option("es.resource", "xyz").mode("append").save()
# Load inverse mappings from a JSON file
inverse_mappings = json.load(open("inverse_mappings.json", "r"))

# Address of the Kafka server
address = "http://10.0.9.23:9092"

# Initialize findspark to locate the Spark installation
findspark.init()

# Load the pre-trained model from a pickle file
model = pickle.load(open("ridge_model.pkl", "rb"))

# Configure Spark settings for Elasticsearch
sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")

# Initialize Spark session
spark = SparkSession.builder.appName("nome").config(conf=sparkConf).getOrCreate()

# Set Spark log level to ERROR to minimize log output
spark.sparkContext.setLogLevel("ERROR")

# Read streaming data from Kafka
dt = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", address) \
    .option("subscribe", "job_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema for the incoming data
data_schema = StructType() \
    .add("Job Id", StringType()) \
    .add("Experience", StringType()) \
    .add("Qualifications", StringType()) \
    .add("location", StringType()) \
    .add("Country", StringType()) \
    .add("latitude", StringType()) \
    .add("longitude", StringType()) \
    .add("Work Type", StringType()) \
    .add("Company Size", StringType()) \
    .add("Preference", StringType()) \
    .add("Job Title", StringType()) \
    .add("Role", StringType()) \
    .add("timestamp", TimestampType())

# Parse the Kafka data and extract fields based on the schema
data_received = dt.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), data_schema).alias("data_received")) \
    .select("data_received.*")

# Write the processed data stream using the process_batch function
data_received.writeStream \
    .foreachBatch(process_batch) \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
