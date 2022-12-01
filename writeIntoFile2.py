import psycopg2
import pandas as pd
import numpy as np


conn = psycopg2.connect('dbname=movies user=postgres password=39339')


sql = 'select productid from moviereview where productid in (select productid from moviereview group by productid having count(*) >175)'
movieid_list = pd.read_sql_query(sql, con=conn)
idlist = list(movieid_list['productid'])
with open('Data/moviename', 'w', encoding='utf-8') as f:
    for i in idlist:
        sql = "select moviename from productname where movieid =" + "'" + i + "'"
        movieName = pd.read_sql_query(sql, con=conn)
        try:
            movieName = list(movieName['moviename'])[0].strip()
        except:
            movieName = i
        f.write(movieName+'\n')
