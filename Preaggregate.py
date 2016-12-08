from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time
from datetime import datetime
import sys
reload(sys)

def read_line(line):
    return line.split("\t")

def write_line(row):
    return ",".join(map(encode_ascii, row[0]+(str(row[1]),)))

def encode_ascii(row):
    return row.encode('ascii', 'ignore')

def keep_columns(line):
    if line[1] != "Event Date":
        source = line[4] if len(line[4]) > 0 else "Unknown"
        target = line[10] if len(line[10]) > 0 else "Unknown"
        return (datetime.strptime(line[1], "%Y-%m-%d").strftime("%Y-%m"), source, target, line[6])
    return ('File count',)

def print_f(line):
    print line

def by_month():

    t0 = time.time()

    extracted_files = files.map(read_line).map(keep_columns).coalesce(8)
    #{(YYYY-MM, Source, Target, CAMEO):count}
    mapping = extracted_files.countByValue().items()
    output = sc.parallelize(mapping).map(write_line).saveAsTextFile("/home/chunchun/Documents/ICEWS/preagg")

    t1 = time.time()

    print "finished in ",t1-t0,' seconds.'

sys.setdefaultencoding('utf-8')

conf = SparkConf().setAppName("Pre-agregate raw files").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)

global files
global mapping

files = sc.textFile("/home/chunchun/Documents/ICEWS/1995-2013/*.tab")

by_month()


