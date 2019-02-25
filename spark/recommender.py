import sys
import glob
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sc = SparkContext(appName="recommender")
    # Load model
    model = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")

    # recommend products for users
    products_for_users = model.recommendProductsForUsers(5)
    # result = products_for_users.map(lambda x: (x[0], x[1][0][1], x[1][1][1], x[1][2][1], x[1][3][1], x[1][4][1]))
    result = products_for_users.map(lambda x: (x[0], (x[1][0][1], x[1][1][1], x[1][2][1], x[1][3][1], x[1][4][1]))) \
        .flatMapValues(lambda movies: [movie for movie in movies])
    print(result.take(15))

    # Result RDD to DF
    spark = SparkSession \
        .builder \
        .appName("recommender DataFrame") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Import data types
    from pyspark.sql.types import *

    # The schema is encoded in a string.
    schemaString = "userId movieId"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaResult = spark.createDataFrame(result, schema)

    # Creates a temporary view using the DataFrame
    schemaResult.createOrReplaceTempView("result")

    # SQL can be run over DataFrames that have been registered as a table.
    resultDF = spark.sql("SELECT * FROM result")

    resultDF.coalesce(1).write.format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save("result", header = 'true')
