from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from map_categories import map_categories
import findspark


findspark.init()

spark = SparkSession.builder.appName("trainer").getOrCreate()

try:
    training = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/melo/Desktop/Tap_project_jobs/archive/archive/job_descriptions2.csv")
except Exception as e:
    print("Errore durante il caricamento dei dati:", e)
    
list_of_columns = ["Qualifications","Country","Work Type","Preference","Job Portal"]

mappings = []
for i in range (len(list_of_columns)):
    training, mapping = map_categories(training, list_of_columns[i])
    mappings.append({list_of_columns[i]: mapping})
    
print(mappings)

json.dump(mappings, open("mappings.json", "w"), indent=4)


training.show()

