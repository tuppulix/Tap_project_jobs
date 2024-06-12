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

# Initialize findspark to ensure PySpark can be used
# findspark.init()

# Create a Spark session for managing data operations
# spark = SparkSession.builder.appName("trainer").getOrCreate()

try:
    # Load the training dataset using pandas
    training = pd.read_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2.csv")
    # Load the entire dataset using pandas for mapping categories
    entire = pd.read_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions.csv")
except Exception as e:
    # Handle exceptions during data loading
    print("Errore durante il caricamento dei dati:", e)
    
# Define the list of columns that need to be mapped to categorical integers
list_of_columns = ["Qualifications","Country","Work Type","Preference","Job Portal", "Role", "Job Title", "location"]

# Define the columns to be dropped from the dataset
to_drop = ["skills", "Job Portal", "Job Description", "Benefits", "Responsibilities"]

# Initialize dictionaries to store mappings and inverse mappings
mappings = {}
inverse_mappings = {}

# Iterate over each column that needs to be mapped
for i in range(len(list_of_columns)):
    # Get unique categories from the entire dataset for the current column
    categories = entire[list_of_columns[i]].unique().tolist()
    # Create a mapping from category to integer
    mapped = {category: i for i, category in enumerate(categories)}
    # Create an inverse mapping from integer to category
    inverse_mapped = {v: k for k, v in mapped.items()}
    
    # Map the categories in the training dataset using the created mapping
    training = map_categories(training, list_of_columns[i], mapped)
    # Store the mappings and inverse mappings
    mappings[list_of_columns[i]] = mapped
    inverse_mappings[list_of_columns[i]] = inverse_mapped
    
# Drop the specified columns from the training dataset
training = training.drop(columns=to_drop)

# Save the modified training dataset to a new CSV file
training.to_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2_categorized.csv", index=False)

# Save the mappings and inverse mappings to JSON files
json.dump(mappings, open("mappings.json", "w"), indent=4)
json.dump(inverse_mappings, open("inverse_mappings.json", "w"), indent=4)
