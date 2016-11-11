from pyspark.sql import SparkSession, SQLContext, Row
from pyspark import SparkContext, SparkConf
import time

spark = SparkSession.builder.appName("TestSQL").master("local").config("spark.eventLog.enabled", "true").getOrCreate()

sc = spark.sparkContext
files = sc.textFile("/home/chunchun/Documents/ICEWS/1995/*.tab")

t0 = time.time()
lines = files.map(lambda l: l.split("\t"))
interactions = lines.map(lambda p: Row(source=p[4], to=p[10], cameo=p[6]))
t1 = time.time()
print "finished in ",t1-t0,' seconds.'

sql = spark.createDataFrame(interactions)


sql.printSchema()

