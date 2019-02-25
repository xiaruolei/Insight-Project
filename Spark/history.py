import sys
import glob
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sc = SparkContext(appName="history")
        
    # Load and parse the data
    # ratings_data = sc.textFile("ml-latest-small/ratings.csv")
    ratings_data = sc.textFile("s3n://data-movie/ml-latest-small/ratings.csv")
    header = ratings_data.first()

    ratings = ratings_data.filter(lambda x: x != header) \
        .map(lambda l: l.split(',')) \
        .map(lambda l: (int(l[0]), int(l[1]), float(l[2])))

    # Result RDD to DF
    spark = SparkSession \
        .builder \
        .appName("History DataFrame") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Import data types
    from pyspark.sql.types import *

    # The schema is encoded in a string.
    schemaString = "userId movieId rating"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaHistory = spark.createDataFrame(ratings, schema)

    # Creates a temporary view using the DataFrame
    schemaHistory.createOrReplaceTempView("history")

    # SQL can be run over DataFrames that have been registered as a table.
    historyDF = spark.sql("SELECT * FROM history")

    historyDF.coalesce(1).write.format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save("history", header = 'true')
