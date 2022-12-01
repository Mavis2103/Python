import psycopg2
import pandas as pd


conn = psycopg2.connect('dbname=movies user=postgres password=39339')
sql = 'select distinct moviename from productname'
movie_list = pd.read_sql_query(sql, con=conn)
movie_list = list(movie_list['moviename'])


with open('Data/movielist', 'w', encoding='utf-8') as f:
    for i in movie_list:
        f.write(i+'\n')

print((movie_list))
