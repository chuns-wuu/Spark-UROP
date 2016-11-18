from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time
from datetime import datetime

conf = SparkConf().setAppName("Direct Query").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)

global files
global mapping

mapping = {}
files = sc.textFile("/home/chunchun/Documents/ICEWS/1995/*.tab")


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

    start_date = datetime.strptime(fromdate, "%Y-%m")
    end_date = datetime.strptime(todate, "%Y-%m")

    def read_line(line):
        row = line.split("\t")
        if time_range(row[1]):
            return ((row[1], row[4], row[10], row[6]), 1)
        return ("None", 0)

    def time_range(date):
        try:
            date = datetime.strptime(date, "%Y-%m-%d")
            return start_date <= date <= end_date
        except Exception:
            return False

    extracted_files = files.map(read_line).coalesce(8)
    mapping = extracted_files.reduceByKey(lambda x, y: x+y).collectAsMap()

    t1 = time.time()

    print mapping
    print "finished in ",t1-t0,' seconds.'
    #Event ID   Event Date  Source Name Source Sectors  Source Country  Event Text  CAMEO Code  Intensity   Target Name Target Sectors  Target Country  Story ID    Sentence Number Publisher   City    District    Province    Country Latitude    Longitude

sum_events('1995-01', '1995-02')
