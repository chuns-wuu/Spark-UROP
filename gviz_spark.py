from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time
from datetime import datetime
from key import Key
"""
It reads in preaggregated csv files, and filter the entries based on a time range, ex. the example filters
1995-01 to 1996-01(inclusive). In addition, it looks up goldstein score from GOLDSTEINSCALE.txt, and categorizes
based on the QCLASS.txt and CAMEOEVENTCODE.txt. Its output is a list of dictionaries.

*Change line 53 to match the output path in Preaggregate.py(line 47), use "/*.csv" to include part-00000.csv*

"""
def lookup_generator(file_name, delimiter=" "):
    """
    return a dictionary with key = cameo code, and val = goldstein score or qclass(depends on the file_name)
    """
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
    key = Key(line, qclass_lookup, cameo_lookup, golstein_lookup)
    return key

def return_key(key):
    return key

def use_key(fromdate=None, todate=None):
    start_date = datetime.strptime(fromdate, "%Y-%m")
    end_date = datetime.strptime(todate, "%Y-%m")

    t0 = time.time()
    #Uncomment next line if you want to print the output one by one to debug:
    #mapping = files.map(create_keys).filter(lambda key: start_date <= key.date <= end_date).foreach(print_f)
    mapping = files.map(create_keys).filter(lambda key: start_date <= key.date <= end_date).collect()
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

#Save these lookup files in the same directory as this python program
GOLDSTEIN = 'GOLDSTEINSCALE.txt'
QCLASS = 'QCLASS.txt'
CAMEO = 'CAMEOEVENTCODE.txt'


golstein_lookup = lookup_generator(GOLDSTEIN)
qclass_lookup = lookup_generator(QCLASS, ",")
cameo_lookup = lookup_generator(CAMEO, ",")

#Filter the data
use_key("1995-01", "1996-01")
