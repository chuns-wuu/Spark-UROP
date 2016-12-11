from numpy import genfromtxt
from time import time
from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=',', skip_header=1, autostrip=True, dtype=None)
    return data.tolist()

Base = declarative_base()

class Price_History(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Price_History'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date)
    opn = Column(Float)
    hi = Column(Float)
    lo = Column(Float)
    close = Column(Float)
    vol = Column(Float)

if __name__ == "__main__":
    t = time()

    #Create the database
    engine = create_engine('sqlite:///csv_test.db')
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    try:
        file_name = "t.csv" #sample CSV file used:  http://www.google.com/finance/historical?q=NYSE%3AT&ei=W4ikVam8LYWjmAGjhoHACw&output=csv
        data = Load_Data(file_name)
        for i in data:
            record = Price_History(**{
                'date' : datetime.strptime(i[0], '%d-%b-%y').date(),
                'opn' : i[1],
                'hi' : i[2],
                'lo' : i[3],
                'close' : i[4],
                'vol' : i[5]
            })
            print i
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
    except Exception as e:
        print e
        s.rollback() #Rollback the changes on error
    finally:
        s.close() #Close the connection
    print "Time elapsed: " + str(time() - t) + " s." #0.091s
