
import threading
import psycopg2
from gensim.models import Doc2Vec
import pandas as pd
import polars as pl
from scipy.spatial import distance
from numpy import linalg as LA
import numpy as np
from psycopg2.pool import SimpleConnectionPool

NUM_THREADS = 8

# Set the number of rows to insert in each batch
BATCH_SIZE = 1000

# Set the connection pool parameters
pool = SimpleConnectionPool(
    NUM_THREADS, NUM_THREADS,
    database='movies', user='postgres', password='39339'
)

model = Doc2Vec.load('Data/doc2vec_moviename.model')


def check(char):
    if ((ord(char) >= 97 and ord(char) <= 122) or (ord(char) >= 65 and ord(char) <= 90)) or (ord(char) >= 48 and ord(char) <= 57):
        return True
    else:
        return False


def find_genre(moviename):
    genre_list = ['action', 'romance', 'epic', 'political', 'fiction',
                  'comedy', 'crime', 'documentary', 'drama', 'venture', 'horror', 'fantasy']

    genre_vectors = np.array([model.wv[genre] for genre in genre_list])
    moviename_vector = model.dv[moviename]

    # Calculate similarity between moviename_vector and each genre_vector in genre_vectors
    similarity = np.dot(genre_vectors, moviename_vector) / \
        (LA.norm(genre_vectors, axis=1) * LA.norm(moviename_vector))
    # Sort the genres based on their similarity scores in descending order
    sorted_indices = np.argsort(similarity)[::-1]
    # Return the top 3 most similar genres
    return [genre_list[i] for i in sorted_indices[:3]]


def out_similar(moviename):
    result = []
    # Check if the passed string is an empty string or just a newline character
    if moviename.strip() == "":
        return []
    moviename = moviename.strip('').strip('\n')
    similar_list = model.dv.most_similar(moviename, topn=70)
    for i in similar_list:
        duplicate = 0
        name = i[0]
        if len(i[0]) == 10:
            continue
        for index in range(0, len(result)//2):
            temp_name = ''.join(filter(check, name)).lower()
            temp_j = ''.join(filter(check, result[2*index])).lower()
            if temp_j in temp_name:
                duplicate = 1
                break
            if temp_name in temp_j:
                result[2*index] = name
                duplicate = 1
                break
        if duplicate == 1 or len(name) > 30:
            continue

        result.append(name)
        # Retrieve a connection from the pool
        conn = pool.getconn()
        # Create a cursor using the connection
        cur = conn.cursor()
        name = name.replace("'", "''")
        sql = "SELECT movieid FROM productname WHERE moviename='{}'".format(
            name)
        # Execute the SELECT query
        cur.execute(sql)
        # Fetch the results of the query
        movieidtemp = cur.fetchone()
        # Commit any changes made
        conn.commit()
        # movieidtemp['movieid'] -> B00004ZEHM -> ["B00004ZEHM"]
        result.append(list(movieidtemp)[0])
        # Close the cursor and connection
        cur.close()
        pool.putconn(conn)
    # Retrieve a connection from the pool
    conn = pool.getconn()
    # Create a cursor using the connection
    cur = conn.cursor()
    sql = "select movieid,reviewtext,reviewsummary from productname join moviereview on productname.movieid = moviereview.productid where productname.moviename='{}' and length(moviereview.reviewtext) >500 and length(moviereview.reviewtext) < 700 limit 1".format(
        moviename.replace("'", "''"))
    cur.execute(sql)
    temp = list(cur.fetchone())
    movieidtemp = temp[0]
    review = temp[1]
    summary = temp[2]
    genre = find_genre(moviename)
    cur.close()
    pool.putconn(conn)
    with open('Data/movielist.txt', 'r', encoding='utf-8') as f:
        x = f.read().split('\n')
    # print(result)
    output = [moviename]+[movieidtemp]+result[:12]+[genre, review, summary, x]
    return output


result = []
count = 0
with open("Data/movielist.txt", 'r', encoding='utf-8') as i:
    for name in i.readlines():
        count = count + 1
        movie = name.strip("\n")
        result.append(out_similar(movie))
        if count % 100 == 0:
            print(count)
    df = pd.DataFrame(result)
    df.to_csv('Data/result.csv')
print("9-done")
# Close the connection pool
pool.closeall()
