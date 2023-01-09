'''
dump movie name data into a new table
get rid of all the dummy info indicated by () and [] 
e.g. [VHS] (blueray, hd) 
'''
import psycopg2
import pandas as pd

# Connect to the database
conn = psycopg2.connect('dbname=movies user=postgres password=39339')
cur = conn.cursor()

# Query the movieid table
sql = 'select * from movieid'
movieid_list = pd.read_sql_query(sql, con=conn)

# Create a list of movies to insert
movie_list = [(i['movieid'], (i['moviename'].split("(")[0]).split("[")[0].strip())
              for index, i in movieid_list.iterrows()]
# Insert the movies in a batch
cur.executemany(
    "INSERT INTO productname (movieid, moviename) VALUES (%s, %s)", movie_list)
conn.commit()

# Close the connection
cur.close()
conn.close()
