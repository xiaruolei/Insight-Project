import glob
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("WriteMappingToDB") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

file_name = glob.glob('mapping/part*.csv')
df = spark.read.csv(file_name, header='true')
df.show()
df.write.format("jdbc").options(
    url='jdbc:mysql://ec2-54-200-1-210.us-west-2.compute.amazonaws.com:3306/insight',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'mapping',
    user = 'rosie',
    password = 'root').mode('overwrite').save()
