import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# spark conf

## get_spark logic
conf = SparkConf()
conf.setMaster("local[*]")
conf.setAppName('pyspark')

sc = SparkContext(conf=conf)
sql_context = SQLContext(sc)

df = sql_context.read.jdbc(
    url='jdbc:postgresql://34.66.210.112:5432/fitbit',
    properties={
        "driver": "org.postgresql.Driver",
        "user": os.environ['POSTGRES_USERNAME'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "currentSchema": "heart"
    },
    table='daily'
)
df.show()

