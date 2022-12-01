'''
dump movie name data into a new table
get rid of all the dummy info indicated by () and [] 
e.g. [VHS] (blueray, hd) 
'''
import psycopg2
import pandas as pd


conn = psycopg2.connect('dbname=movies user=postgres password=39339')
sql = 'select * from movieid'
movieid_list = pd.read_sql_query(sql, con=conn)


def execute(sql):
    conn = psycopg2.connect('dbname=movies user=postgres password=39339')
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


for index, i in movieid_list.iterrows():
    sql = ("insert into productname (movieid, moviename) values ('" + i['movieid']+"','"
           + (i['moviename'].split("(")[0]).split("[")[0].strip()+"')")
    execute(sql)
    if index % 1000 == 0:
        print(index)
