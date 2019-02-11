from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
spark = SparkSession.builder.appName('ReadData').getOrCreate()
df = spark.read.format("jdbc").options(
    url='jdbc:mysql://localhost/test',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'R',
    user = 'rosie',
    password = 'root').load()
df.show()
