from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window

import findspark
import pandas as pd
import pickle

@pandas_udf(DoubleType())
def predict_salary(*features: pd.Series) -> pd.Series:
    global model
    col_names = ["Experience", "Qualifications", "location", "Country", "latitude", "longitude", "Work Type", "Company Size", "Preference", "Job Title", "Role", "timestamp"]
    features = pd.concat(features, axis=1)
    features.columns = col_names
    if features["Qualifications"].isnull().values.any():
        return pd.Series([0]*len(features))
    print(features.head())
    features = features.drop(columns=["timestamp"])

    predictions = model.predict(features)
    return pd.Series(predictions)

def process_batch(batch_df, batch_id):
    
    print(f"Processing batch {batch_id}")
    batch_df.show()
    
    batch_df = batch_df \
        .withColumn("Qualifications", batch_df["Qualifications"].cast(IntegerType())) \
        .withColumn("location", batch_df["location"].cast(IntegerType())) \
        .withColumn("Country", batch_df["Country"].cast(IntegerType())) \
        .withColumn("latitude", batch_df["latitude"].cast(FloatType())) \
        .withColumn("longitude", batch_df["longitude"].cast(FloatType())) \
        .withColumn("Work Type", batch_df["Work Type"].cast(IntegerType())) \
        .withColumn("Company Size", batch_df["Company Size"].cast(IntegerType())) \
        .withColumn("Preference", batch_df["Preference"].cast(IntegerType())) \
        .withColumn("Job Title", batch_df["Job Title"].cast(IntegerType())) \
        .withColumn("Role", batch_df["Role"].cast(IntegerType())) \
        .withColumn("Experience", batch_df["Experience"].cast(FloatType()))
    
    batch_df = batch_df.withColumn("Salary", predict_salary(*[col(colmn) for colmn in batch_df.columns]))
        
    batch_df.write \
        .format("console") \
        .option("truncate", False) \
        .save()
        
    batch_df.write.format("es").option("truncate", False) \
        .option("checkpointLocation","/tmp/").option("es.port","9200").option("es.nodes","https://10.0.9.28:9200") \
        .option("es.nodes.wan.only", "false") \
        .option("es.resource", "jobs").mode("append").save()



address = "http://10.0.9.23:9092"
findspark.init()

model = pickle.load(open("ridge_model.pkl", "rb"))

sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")

spark = SparkSession.builder.appName("nome").config(conf=sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

dt = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", address) \
    .option("subscribe", "job_topic").option("startingOffsets", "earliest") \
    .load()

from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

data_schema = StructType() \
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
    .add("timestamp",  TimestampType())


data_received = dt.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), data_schema).alias("data_received")) \
    .select("data_received.*")
    

    
    
    
data_received.writeStream \
    .foreachBatch(process_batch) \
    .option("truncate",False) \
    .start() \
    .awaitTermination()
    