from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import time, sys
from datetime import datetime


def init_session():
    conf = SparkConf().setAppName("TestSQL").setMaster("local").set("spark.eventLog.enabled", "true")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return sc, spark

def extract(row):
    date = datetime.strptime(row["Event Date"], "%Y-%m-%d").strftime("%Y-%m")
    return ((date, row["Source Country"], row["Target Country"], row["CAMEO Code"]), 1)

def append_res(line):
    return line[0]+(line[1],)

def rdd_op(files):
    files_rdd = files.rdd
    res = files_rdd.map(extract).coalesce(8).countByKey().items()
    return res


#files = spark.read.options(header="true", delimiter="\t").csv("/home/chunchun/Documents/ICEWS/1995-2013/*.tab")


if __name__ == '__main__': #Arguments: path to files, delimiter
    sc, spark = init_session()
    path = sys.argv[1]
    delimiter = sys.argv[2]

    t0 = time.time()
    files = spark.read.options(header="true", delimiter=delimiter).csv(path)
    res = rdd_op(files)

    ans = sc.parallelize(res).map(append_res)

    df = spark.createDataFrame(ans, ["Date", "Source", "Target", "CAMEO", "Count"])
    df.write.csv("/home/chunchun/Documents/ICEWS/preagg-sql")
    t1 = time.time()

    print "finished in ",t1-t0,' seconds.'








