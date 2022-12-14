import pandas as pd
import psycopg2

hostname = 'Hostname'
database = 'NAME '
username = 'postgres'
pwd = 'PASSWORD'
port_id = '54'


conn = psycopg2.connect (
            host =hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

print("tum")

cursor = conn.cursor()
postgreSQL_select_Query = 'SELECT * FROM public."channelExpectedViews" where "expectedViewsShorts" != -1.0'

cursor.execute(postgreSQL_select_Query)
testedViews = cursor.fetchall()
sql_insert_query = 'INSERT INTO public."testedExpectedViews"("expectedViewsVideo","expectedViewsShorts","channelId","metaData.generatorId","metaData.jobId","metaData.createdAt","metaData.messageId","metaData.isAuth","channel_id") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
result = cursor.executemany(sql_insert_query, testedViews)
conn.commit()
print(cursor.rowcount, "Record inserted successfully into tested Expected Views")



print("complete")

conn.close()
