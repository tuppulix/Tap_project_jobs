from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import json
from map_categories import map_categories
import findspark


#findspark.init()

#spark = SparkSession.builder.appName("trainer").getOrCreate()

try:
    #training = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2.csv")
    training = pd.read_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2.csv")
except Exception as e:
    print("Errore durante il caricamento dei dati:", e)
    
list_of_columns = ["Qualifications","Country","Work Type","Preference","Job Portal", "Role", "Job Title", "location"]


to_drop = ["skills", "Job Portal", "Job Description", "Benefits", "Responsibilities"]
mappings = {}
for i in range (len(list_of_columns)):
    training, mapping = map_categories(training, list_of_columns[i])
    mappings[list_of_columns[i]] = mapping
    

training = training.drop(columns=to_drop)

training.to_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2_categorized.csv", index=False)
json.dump(mappings, open("mappings.json", "w"), indent=4)



