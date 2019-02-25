# coding=utf-8
from unidecode import unidecode
import sys
import glob
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sc = SparkContext(appName="mapping")
        
    # Load and parse the data
    # movies_data = sc.textFile("ml-latest-small/movies.csv")
    movies_data = sc.textFile("s3n://data-movie/ml-latest-small/movies.csv")
    header = movies_data.first()

    movies = movies_data.filter(lambda x: x != header) \
        .map(lambda l: l.replace("\"", "")) \
        .map(lambda l: l.split(',')) \
        .map(lambda l: (int(l[0]), l[1]))

    # Result RDD to DF
    spark = SparkSession \
        .builder \
        .appName("Mapping DataFrame") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Import data types
    from pyspark.sql.types import *

    # The schema is encoded in a string.
    schemaString = "movieId title"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaMapping = spark.createDataFrame(movies, schema)

    # Creates a temporary view using the DataFrame
    schemaMapping.createOrReplaceTempView("mapping")

    # SQL can be run over DataFrames that have been registered as a table.
    mappingDF = spark.sql("SELECT * FROM mapping")

    mappingDF.coalesce(1).write.format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save("mapping", header = 'true')
