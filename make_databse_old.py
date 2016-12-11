from __future__ import print_function
import sqlalchemy
import cubes
import csv
import string
from sqlalchemy.orm import sessionmaker
from joblib import Parallel, delayed
from cubes import Workspace, browser
import time
import urllib2
from boto.s3.connection import S3Connection, Bucket, Key
from sqlalchemy.schema import MetaData
import os

#Extracting data from GDELT
def get_row(row):
        #Year,Month,Date,SourceCon,TargetCon,
        #QuadClass,EventRootCode,EventBaseCode,EventCode,
        #Goldstein,numberArticle,numberEvent
    insert_row = []
    insert_row.append(int(row[0]))#year
    insert_row.append(int(row[1])) #month
    insert_row.append(row[2]) #from_code
    insert_row.append(row[3]) #to_code
    insert_row.append(row[2]+"_"+row[3]) #from_to_code
    insert_row.append(row[7]) #qclass_code
    insert_row.append(row[4]) #root_code
    insert_row.append(row[5]) #base_code
    insert_row.append(row[6]) #event_code
    insert_row.append(float(row[8])) #gs
    insert_row.append(int(row[9])) #articles
    insert_row.append(int(row[10])) #actions
    insert_row.append(float(row[8])*float(row[9])) #gs_articles
    return insert_row


def make_data_table(connectable, table_name, fields,create_id=False, schema=None):
    ##print("TABLE ",table_name,"\n", fields)
    metadata = sqlalchemy.MetaData(bind=connectable)

    table = sqlalchemy.Table(table_name, metadata, autoload=False, schema=schema)


    if table.exists(): #if the table exists,
        table.drop(checkfirst=False)

    type_map = {"integer": sqlalchemy.Integer,
                "float": sqlalchemy.Numeric,
                "string": sqlalchemy.String(256),
                "text": sqlalchemy.Text,
                "date": sqlalchemy.Text,
                "boolean": sqlalchemy.Integer}
    #creating columns for table.
    if create_id:
        col = sqlalchemy.schema.Column('id', sqlalchemy.Integer, primary_key=True)
        table.append_column(col)

    for (field_name, field_type) in fields:
        col = sqlalchemy.schema.Column(field_name, type_map[field_type.lower()])
        table.append_column(col)

    table.create() #Create table

    sampling = 1 #sampling rate -- set to 1 to process all rows
    firstNumFiles = False; #True if limit to first numFiles files, False if limit to last numFiles files
    #if sampling > 1:
        #print("\n!!!!!!!!!!!!!!!!!!!!!! SAMPLING EVERY",sampling,"ROWS !!!!!!!!!!!!!!!!!!\n")
    numFiles = 0 #file count limit -- set to 0 to process all files
    # if numFiles > 0:
    #   #print("\n!!!!!!!!!!!!!!!!!!!!!! CUBING ONLY",
    #       ("FIRST" if firstNumFiles else "LAST"),
    #       ("FILE" if numFiles == 1 else (str(numFiles) + " FILES")),
    #       "!!!!!!!!!!!!!!!!!!!!!!\n")
    index = 0
    fileCount = 0
    records = []
    insert_batch = 100000 #number of records to insert per batch -- vital for speed of pre-cubing

    #====================THIS BLOCK USES THE LOCAL FILE REPOSITORY=====================
    filedir = "/home/chunchun/Documents/ICEWS/preagg"
    filenames = [ filename for filename in sorted(os.listdir(filedir)) if filename.endswith( ".csv" ) ]

    for key in filenames:
        index += 1
            filepath = filedir + key
            csvfile = open(filepath,"rb")
            try:
                data_list = list(csv.reader(csvfile, delimiter=","))
                for row in data_list:
                    rowCount += 1;
                    insert_row = get_row(row)
                    try:
                        record = dict(zip(field_names, insert_row))
                        if len(records) > insert_batch:
                            table.insert().execute(records)
                            records = []
                        else:
                            records.append(record)
                        #print("FILE",fileCount,": ROW",rowCount*sampling)
                    except Exception,e:
                        print("INSERT",e)
                if len(records) > 0: table.insert().execute(records)
            except Exception,e:
                print("FILE",e)
            finally:
                csvfile.close()
                # #print(rowCount,"ROWS PROCESSED",
                #   "IN",int(time.time() - file_time),"SECONDS",
                #   "AT",int(time.time() - start_time),"SECOND MARK")
    #print(fileCount, "FILES INSERTED INTO", table_name)

    '''
    #====================THIS BLOCK USES THE S3 FILE REPOSITORY=====================
    conn = S3Connection("AKIAJPVDGY35YIWKQOYA", "tX5lBwchrnbAA1Udxggga7eyr9d/lxh/7Zxa921j")
    bucket = conn.get_bucket('gdeltdatamedialab') #you can just open each one and then read it all in.
    #comment-toggle the following two lines to switch between limit from end or beginning of file list
    endList = numFiles if firstNumFiles else len(bucket.get_all_keys())
    startList = 0  if firstNumFiles else endList-numFiles
    for key in bucket.list():
        index += 1
        if numFiles != 0 and index > endList:
            break
        if numFiles == 0 or index > startList:
            fileCount += 1
            url = key.generate_url(0, query_auth=False, force_http=True)
            #print("FILE",fileCount,":",url)
            response = urllib2.urlopen(url)
            file_time = time.time()
            data_list = list(csv.reader(response, delimiter=","))
            rowCount = 0
            for row in data_list[::sampling]:
                #country codes not same or invalid
                rowCount += 1;
                insert_row = get_row(row)
                try:
                    record = dict(zip(field_names, insert_row))
                    if len(records) > insert_batch:
                        table.insert().execute(records)
                        records = []
                    else:
                        records.append(record)
                    #print("FILE",fileCount,": ROW",rowCount*sampling)
                except Exception,e:
                    #print(e)
            if len(records) > 0: table.insert().execute(records)
            #print(rowCount,"PROCESSED",
                "IN",int(time.time() - file_time),"SECONDS",
                "AT",int(time.time() - start_time),"SECOND MARK")
    #print(fileCount, "FILES INSERTED INTO", table_name)
    '''

    #print("COMMENCING INDEXING ON", table_name)
    indexing_time = time.time()
    idx_date = sqlalchemy.Index("idx_date",table.c.year,table.c.month)
    idx_action = sqlalchemy.Index("idx_action",
        table.c.qclass_code,table.c.root_code,table.c.base_code,table.c.event_code)
    idx_from = sqlalchemy.Index("idx_from",table.c.from_code)
    idx_to = sqlalchemy.Index("idx_to",table.c.to_code)
    idx_from_to = sqlalchemy.Index("idx_from_to",table.c.from_to_code)
    idx_date.create(connectable)
    idx_action.create(connectable)
    idx_from.create(connectable)
    idx_to.create(connectable)
    idx_from_to.create(connectable)


engine = sqlalchemy.create_engine("sqlite:///data.sqlite")
start_time = time.time()
make_data_table(engine, table_name="gdelt_ml_actions", fields=[("year", "integer"), ("month", "integer"), ("from_country", "string"), ("to_country","string"), ("event_code","string"),  ("count", "integer")])


