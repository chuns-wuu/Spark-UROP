from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import time
from datetime import datetime


def extract(row):
    date = datetime.strptime(row["Event Date"], "%Y-%m-%d").strftime("%Y-%m")
    return ((date, row["Source Country"], row["Target Country"], row["CAMEO Code"]), 1)

def append_res(line):
    return line[0]+(line[1],)

conf = SparkConf().setAppName("TestSQL").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

t0 = time.time()
files = spark.read.options(header="true", delimiter="\t").csv("/home/chunchun/Documents/ICEWS/1995/*.tab")

#files = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='false').load('/home/chunchun/Documents/ICEWS/1995/*.tab')
#files = sc.textFile("/home/chunchun/Documents/ICEWS/1995/*.tab")


#print files.dtypes
#files.select(col("Event Date").alias("EventDate"))
files_rdd = files.rdd
res = files_rdd.map(extract).coalesce(3).countByKey().items()
ans = sc.parallelize(res).map(append_res)
df = spark.createDataFrame(ans, ["Date", "Source", "Target", "CAMEO", "Count"])
df.write.csv("/home/chunchun/Documents/ICEWS/preagg-sql")
t1 = time.time()
#print files.columns
#print ans
print "finished in ",t1-t0,' seconds.'








