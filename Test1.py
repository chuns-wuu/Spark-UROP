from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time

conf = SparkConf().setAppName("Test1").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)

global files
global mapping

mapping = {}
files = sc.textFile("/home/chunchun/Documents/ICEWS/1995/*.tab")


def read_line(line):
    return line.split("\t")

def except_all(line):
    return [line[2] != 'all' and line[3] != 'all']

def keep_columns(line):
    return ((line[1], line[4], line[10], line[6]), 1)

def print_f(line):
    print line


def sum_events(fromdate=None, todate=None):
    """
    Use spark to run through tab files and return sum_events.
    Input:
    fromadte: YYYYMMDD or YYYYMM OR YYYY
    todate: YYYYMMDD or YYYYMM OR YYYY

    Output:
    mapping: dictionary
            {(source country, to country, CAMEO code): sum of events}}
    """
    t0 = time.time()

    extracted_files = files.map(read_line).map(keep_columns).coalesce(8)
    mapping = extracted_files.reduceByKey(lambda x, y: x+y).collectAsMap()

    t1 = time.time()

    print mapping
    print "finished in ",t1-t0,' seconds.'
    #Event ID   Event Date  Source Name Source Sectors  Source Country  Event Text  CAMEO Code  Intensity   Target Name Target Sectors  Target Country  Story ID    Sentence Number Publisher   City    District    Province    Country Latitude    Longitude

sum_events()

