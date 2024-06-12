from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import findspark


def map_categories(df,column, mapped):
    """ dfselected = dataset.select(column).collect()
    list_of_categories = [x[column] for x in dfselected]
    list_of_categories = list(set(list_of_categories))
    mapped = {category: i for i, category in enumerate(list_of_categories)}
    apply_cat_udf = udf(lambda x: mapped[x], IntegerType())
    dataset = dataset.withColumn(column, apply_cat_udf(dataset[column]))
    mapped_fin = {v: k for k, v in mapped.items()}
    return dataset, mapped_fin """

        # Get unique categories

    # Define a function to apply the mapping (using lambda for brevity)
    def apply_mapping(x):
        return mapped[x]

    # Apply the mapping to the column
    df[column] = df[column].apply(apply_mapping)

    # Create a mapping from encoded value to original category (optional)

    return df




