from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time
from datetime import datetime

class Key:
    def __init__(self, line):
        self.row = line.split(",")
        self.date = datetime.strptime(self.row[0], "%Y-%m") if self.row[0] != 'File count' else datetime.min
        self.pair = (self.row[1], self.row[2]) if self.row[0] != 'File count' else ("", "")
        self.CAMEO = self.row[3] if self.row[0] != 'File count' else -1
        self.count = self.row[4] if self.row[0] != 'File count' else 0

        try:
            self.score = golstein_lookup[self.row[3]] if self.row[0] != 'File count' else 99999
            self.qclass = qclass_lookup[self.row[3][:2]] if self.row[0] != 'File count' else 'PLACEHOLDER'
            self.description = cameo_lookup[self.row[3]] if self.row[0] != 'File count' else 'PLACEHOLDER'
        except KeyError:
            raise Exception("CAMEO code "+self.row[3]+" is not in the lookup table(s)!")

    def __repr__(self):
        return {(self.date.strftime("%Y-%m"), self.pair[0], self.pair[1]): (self.score, self.qclass, self.count)}

    def __str__(self):
        return str({(self.date.strftime("%Y-%m"), self.pair[0], self.pair[1]): (self.score, self.qclass, self.count)})

def lookup_generator(file_name, delimiter=" "):
    d = {}
    with open(file_name) as f:
        for line in f:
            if not line.startswith('#'):
                (key, val) = line.strip().split(delimiter)
                d[str(key)] = val
    return d

def print_f(line):
    print line

def create_keys(line):
    key = Key(line)
    return key

def return_key(key):
    return key

def use_key(fromdate=None, todate=None):
    start_date = datetime.strptime(fromdate, "%Y-%m")
    end_date = datetime.strptime(todate, "%Y-%m")

    t0 = time.time()
    mapping = files.map(create_keys).filter(lambda key: start_date <= key.date <= end_date).foreach(print_f)#.map(return_key).collect()
    t1 = time.time()
    print mapping
    print "finished in ",t1-t0,' seconds.'

conf = SparkConf().setAppName("Use pre-aggregated data").setMaster("local").set("spark.eventLog.enabled", "true")
sc = SparkContext(conf=conf)

global files
global mapping
global golstein_lookup

mapping = {}
files = sc.textFile("/home/chunchun/Documents/ICEWS/preagg2/*.csv")
GOLDSTEIN = 'GOLDSTEINSCALE.txt'
QCLASS = 'QCLASS.txt'
CAMEO = 'CAMEOEVENTCODE.txt'

golstein_lookup = lookup_generator(GOLDSTEIN)
qclass_lookup = lookup_generator(QCLASS, ",")
cameo_lookup = lookup_generator(CAMEO, ",")

use_key("1995-01", "1996-01")
