from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time
from datetime import datetime
import sys
reload(sys)
"""
It reads in raw tab files from ICEWS folder, and preaggregates by month. This program writes a file
titled "part-00000" in csv format, so simply rename it to "part-00000.csv".

*You should change the path in line 48 and 65*
"""

def read_line(line):
    return line.split("\t")

def write_line(row):
    return ",".join(map(encode_ascii, row[0]+(str(row[1]),)))

def encode_ascii(row):
    return row.encode('ascii', 'ignore')

def keep_columns(line):
    """
    Only keep columns 1(date), 4(source country), 6(cameo code), 10(target country).
    If current line is a header, return string "File count" as a placeholder.
    """
    if line[1] != "Event Date":
        source = line[4] if len(line[4]) > 0 else "Unknown"
        source = " ".join(source.split(",")) if "," in source else source
        target = line[10] if len(line[10]) > 0 else "Unknown"
        target = " ".join(target.split(",")) if "," in target else target
        return (datetime.strptime(line[1], "%Y-%m-%d").strftime("%Y-%m"), source, target, line[6])
    return ('File count',)

def print_f(line):
    print line

def by_month():

    t0 = time.time()

    extracted_files = files.map(read_line).map(keep_columns).coalesce(8)#coalesce to reduce shuffle
    #mapping should have this format: {(YYYY-MM, Source, Target, CAMEO): count}, its type is a list
    mapping = extracted_files.countByValue().items()

    output = sc.parallelize(mapping).map(write_line).saveAsTextFile("/home/chunchun/Documents/ICEWS/preagg2")
    t1 = time.time()

    print "finished in ",t1-t0,' seconds.'


sys.setdefaultencoding('utf-8')#To resolve some encoding issue with spark


conf = SparkConf().setAppName("Pre-agregate raw files").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)

global files
global mapping

#This is the files to preaggregate, you can preaggregate the whole ICEWS data, or you can preaggregate a group of
#five years to test. Use "/*.tab" to include all the tab format files.
files = sc.textFile("/home/chunchun/Documents/ICEWS/1995-1998/*.tab")

#Preaggregate by month
by_month()


