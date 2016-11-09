from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time

conf = SparkConf().setAppName("Test1").setMaster("local")
sc = SparkContext(conf=conf)

global files
global mapping

mapping = {}
files = sc.textFile("/home/chunchun/Documents/ICEWS/1995-1996/*.tab")

def read_line(line):
    return line.split("\t")

def except_all(line):
    return [line[2] != 'all' and line[3] != 'all']

def keep_columns(line):
    return [line[4], line[10], line[6]]

def print_f(line):
    print line

def add_map(line):
    return (tuple(line[:]), 1)

def sum_events():
    """
    Use spark to run through tab files and return sum_events.

    Output:
    mapping: dictionary
            {(source country, to country, CAMEO code): sum of events}}
    """
    t0 = time.time()
    extracted_files = files.map(read_line).map(keep_columns)
    mapping = extracted_files.map(add_map).reduceByKey(lambda x, y: x+y).collectAsMap()
    t1 = time.time()

    print mapping
    print "finished in ",t1-t0,' seconds.'
    #Event ID   Event Date  Source Name Source Sectors  Source Country  Event Text  CAMEO Code  Intensity   Target Name Target Sectors  Target Country  Story ID    Sentence Number Publisher   City    District    Province    Country Latitude    Longitude

sum_events()

