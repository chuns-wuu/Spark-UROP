from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time

conf = SparkConf().setAppName("Test1").setMaster("local")
sc = SparkContext(conf=conf)

global files
global mapping

mapping = {}
files = sc.textFile("/home/chunchun/Documents/month\ files/by_month/2010/*.csv")

def read_line(line):
    return line.split(",")

def except_all(line):
    return [line[2] != 'all' and line[3] != 'all']

def keep_columns(line):
    return [line[0], line[2], line[3], line[10]]

def print_f(line):
    print line

def add_map(line):
    return (tuple(line[:3]), int(line[-1]))

def time_interaction():
    """
    Use spark to run through csv files and return sum_articles, avg_gs, count_actions of all pairs of countries recorded.

    Input: empty mapping

    mapping: dictionary
    {(year, from code, to code): [sum_articles, avg_gs, count_action]}}
    """
    t0 = time.time()
    extracted_files = files.map(read_line).filter(except_all).map(keep_columns)
    mapping = extracted_files.map(add_map).combineByKey(lambda event_count: event_count,
        lambda old, val: old+val, lambda old, new: old+new).collectAsMap()
    t1 = time.time()

    print mapping
    print "finished in ",t1-t0,' seconds.'
    #Year[0],Month[1],source country[2], target country[3], EventRootCode[4], EventBaseCode[5], EventCode[6], QuadClass[7], Goldstein[8], Narticle[9], Nevent[10]


time_interaction()

