from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import findspark

address = "http://10.0.9.23:9092"
findspark.init()

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
    .add("Job Id", StringType()) \
    .add("Experience", StringType()) \
    .add("Qualifications", StringType()) \
    .add("Salary Range", StringType()) \
    .add("location", StringType()) \
    .add("Country", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("Work Type", StringType()) \
    .add("Company Size", StringType()) \
    .add("Job Posting Date", StringType()) \
    .add("Preference", StringType()) \
    .add("Job Title", StringType()) \
    .add("Role", StringType()) \
    .add("Job Portal", StringType()) \
    .add("Job Description", StringType()) \
    .add("Benefits", StringType()) \
    .add("skills", StringType()) \
    .add("Responsibilities", StringType()) \
    .add("Company", StringType()) \
    .add("Company Profile", StringType())


data_received = dt.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), data_schema).alias("data_received")) \
    .select("data_received.*")
    
    
data_received.writeStream \
    .format("console") \
    .option("truncate",False) \
    .start() \
    .awaitTermination()
    