from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time
from datetime import datetime

conf = SparkConf().setAppName("Pre-agregate raw files").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)

global files
global mapping

files = sc.textFile("/home/chunchun/Documents/ICEWS/1995/*.tab")


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


