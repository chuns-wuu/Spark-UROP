from numpy import genfromtxt
from time import time
from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

def Load_Data(filedir, file_names):
    data = []
    for file_name in file_names:
        data += genfromtxt(filedir+"/"+file_name, delimiter=',', skip_header=1, autostrip=True, dtype="|S10",
            converters={4: lambda c: int(c)}).tolist()
    #print data
    return data

Base = declarative_base()

class Country_Interactions(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Country_Interactions'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date)
    source = Column(String(32))
    target = Column(String(32))
    cameo = Column(String(32))
    count = Column(Integer)

if __name__ == "__main__":
    t = time()

    #Create the database
    engine = create_engine('sqlite:///preagg_csv.db')
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    try:
        filedir = "/home/chunchun/Documents/ICEWS/preagg2"
        file_names = [ filename for filename in os.listdir(filedir) if filename.endswith( ".csv" ) ]
        data = Load_Data(filedir, file_names)
        for i in data:
            record = Country_Interactions(**{
                'date' : datetime.strptime(i[0], "%Y-%m").date(),
                'source' : i[1],
                'target' : i[2],
                'cameo' : i[3],
                'count' : i[4],
            })
            s.add(record) #Add all the records
            #print 'added 1'
        s.commit() #Attempt to commit all the records
    except Exception as e:
        print e
        s.rollback() #Rollback the changes on error
    finally:
        s.close() #Close the connection
    print "Time elapsed: " + str(time() - t) + " s."
