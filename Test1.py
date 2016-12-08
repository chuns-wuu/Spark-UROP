from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time
from datetime import datetime

class Key:
    def __init__(self, line):
        self.row = line.split(",")
        self.date = datetime.strptime(self.row[0], "%Y-%m") if self.row[0] != 'File count' else datetime.min
        self.pair = (self.row[1], self.row[2]) if self.row[0] != 'File count' else ("", "")
        try:
            self.score = golstein_lookup[self.row[3]] if self.row[0] != 'File count' else 99999
        except KeyError:
            raise Exception("CAMEO code "+self.row[3]+" is not in the lookup table!")
        self.count = self.row[4] if self.row[0] != 'File count' else 0

    def add_qclass(self, q):
        self.q = q
    def add_goldstein(self, g):
        self.g = g

    def get_qclass(self):
        return {(self.date.strftime("%Y-%m"), self.pair): self.q}

def gscore_map(file_name):
    d = {}
    with open(file_name) as f:
        for line in f:
            if not line.startswith('#'):
                (key, val) = line.split()
                d[str(key)] = val
    return d

def print_f(line):
    print line

def read_csv(line):
    row = line.split(",")
    return (tuple(row[:-1]), row[-1])

def create_keys(line):
    key = Key(line)
    key.add_qclass("Qclass")
    return key

def get_dic(key):
    return key.get_qclass()

def sum_events(fromdate=None, todate=None):
    """
    Use spark to run through tab files and return sum_events.
    Input:
    fromadte: YYYYMM OR YYYY
    todate: YYYYMM OR YYYY

    Output:
    mapping: dictionary
            {(source country, to country, CAMEO code): sum of events}}
    """
    t0 = time.time()
    start_date = datetime.strptime(fromdate, "%Y-%m")
    end_date = datetime.strptime(todate, "%Y-%m")

    def read_line(line):
        row = line.split(",")
        if time_range(row[0]):
            return (tuple(row[:-1]), row[-1])
        return ("None", 0)

    def time_range(date):
        try:
            date = datetime.strptime(date, "%Y-%m")
            return start_date <= date <= end_date
        except Exception:
            return False

    extracted_files = files.map(read_line).coalesce(11)#finished in  7.88416600227  seconds. Use 1995-1998.csv
    mapping = extracted_files.collectAsMap()

    t1 = time.time()

    #print mapping
    print "finished in ",t1-t0,' seconds.'

def load_pregg():
    t0 = time.time()
    mapping = files.map(read_csv).coalesce(5).collectAsMap()#finished in  3.72139501572  seconds. Use 1995-1998
    t1 = time.time()
    print "finished in ",t1-t0,' seconds.'

def load_and_filter(fromdate=None, todate=None):
    start_date = datetime.strptime(fromdate, "%Y-%m")
    end_date = datetime.strptime(todate, "%Y-%m")

    t0 = time.time()
    mapping = files.map(read_csv).filter(lambda row: row[0][0] != "File count" and start_date <= datetime.strptime(row[0][0], '%Y-%m') <= end_date).coalesce(5).collectAsMap()#finished in  7.60540795326  seconds. Use 1995-1998
    t1 = time.time()
    print "finished in ",t1-t0,' seconds.'

def use_key(fromdate=None, todate=None):
    start_date = datetime.strptime(fromdate, "%Y-%m")
    end_date = datetime.strptime(todate, "%Y-%m")

    t0 = time.time()
    mapping = files.map(create_keys).filter(lambda key: start_date <= key.date <= end_date).map(get_dic).collect()
    t1 = time.time()
    print mapping
    print "finished in ",t1-t0,' seconds.'
#sum_events("1995-01", "1996-01")
#load_pregg()
#load_and_filter("1995-01", "1996-01")

conf = SparkConf().setAppName("Use pre-aggregated data").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)

global files
global mapping
global golstein_lookup

mapping = {}
files = sc.textFile("/home/chunchun/Documents/ICEWS/preagg/*.csv")
GOLDSTEIN = 'CAMEO.goldsteinscale.txt'
golstein_lookup = gscore_map(GOLDSTEIN)

use_key("1995-01", "1996-01")
