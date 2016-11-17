from pyspark.sql import SparkSession, SQLContext, Row
from pyspark import SparkContext, SparkConf
import time

spark = SparkSession.builder.appName("TestSQL").master("local").config("spark.eventLog.enabled", "true").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)
files = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='false').load('/home/chunchun/Documents/ICEWS/1995/*.tab')
#files = sc.textFile("/home/chunchun/Documents/ICEWS/1995/*.tab")

t0 = time.time()
lines = files.map(lambda l: l.split("\t"))
interactions = lines.map(lambda p: Row(source=p[4], to=p[10], cameo=p[6]))
t1 = time.time()
print "finished in ",t1-t0,' seconds.'

sql = spark.createDataFrame(interactions)

def read_line(line):
    return line.split("\t")

def keep_columns(line):
    if line[1] != "Event Date":
        return (datetime.strptime(line[1], "%Y-%m-%d").strftime("%Y-%m"), line[4], line[10], line[6])
    #return (line[1], line[4], line[10], line[6])
    #return (datetime.strptime(line[1], "%Y-%m-%d").strftime("%Y-%m"))

def print_f(line):
    print line

def by_month(month):

    t0 = time.time()

    extracted_files = files.map(read_line).map(keep_columns).coalesce(8)
    #{(YYYY-MM, Source, Target, CAMEO):count}
    mapping = extracted_files.countByValue()


    t1 = time.time()

    print mapping
    print "finished in ",t1-t0,' seconds.'

by_month(1)




