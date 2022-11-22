import pandas as pd
from datetime import datetime, timedelta
import time
from dateutil import parser
import psycopg2
import great_expectations as ge
hostname = 'materialisation-dev-1.cia5jb7mksc7.ap-south-1.rds.amazonaws.com'
database = 'flink_test'
username = 'postgres'
pwd = 'gdq04SmIFpG4Hro5Od45'
port_id = '5432'


conn = psycopg2.connect (
            host =hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)
start_time = parser.parse(str(datetime.now())) - timedelta(days=7)
string = str(start_time)
element = datetime.strptime(string,"%Y-%m-%d %H:%M:%S.%f")
timestamp = datetime.timestamp(element)
y = (int(timestamp*1000))
print(y)

end_time = parser.parse(str(datetime.now())) - timedelta(days=180)
string1 = str(end_time)

element1 = datetime.strptime(string1,"%Y-%m-%d %H:%M:%S.%f")
  
timestamp_1 = datetime.timestamp(element1)
x = (int(timestamp_1*1000))
print(x)

print("tum")
cursor = conn.cursor()
postgreSQL_select_Query = f'SELECT * FROM public."dkchannelExpectedViews" WHERE "metaData.createdAt" BETWEEN {x} AND {y} ORDER BY "metaData.createdAt" ASC limit 15;'

cursor.execute(postgreSQL_select_Query)
testedView = cursor.fetchall()
for i in testedView:
    print(i)

conn.commit()
cursor = conn.cursor()
conn.close()