from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import findspark


def map_categories(df, column, mapped):
    """
    Maps categories in the specified column of a DataFrame to integer values based on the provided mapping.
    
    Args:
        df (DataFrame): The input DataFrame.
        column (str): The column name containing categorical values to be mapped.
        mapped (dict): A dictionary mapping original category values to integers.
    
    Returns:
        DataFrame: The DataFrame with the specified column mapped to integers.
    """
    
    # Define a function to apply the mapping
    def apply_mapping(x):
        return mapped[x]

    # Apply the mapping to the column using the defined function
    df[column] = df[column].apply(apply_mapping)

    return df
