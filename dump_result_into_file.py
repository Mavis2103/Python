
import psycopg2
from gensim.models import Doc2Vec
import pandas as pd
from scipy.spatial import distance
from numpy import linalg as LA

conn = psycopg2.connect('dbname=movies user=postgres password=39339')


model = Doc2Vec.load('Data/doc2vec_moviename.model')


def check(char):
    if ((ord(char) >= 97 and ord(char) <= 122) or (ord(char) >= 65 and ord(char) <= 90)) or (ord(char) >= 48 and ord(char) <= 57):
        return True
    else:
        return False


def similarity(movieid, genre):
    return distance.euclidean(model.wv[genre], model.dv[movieid])/LA.norm(model.wv[genre])/LA.norm(model.dv[movieid])


def find_genre(movieid):
    genre = []
    genre_list = ['action', 'romance', 'epic', 'political', 'fiction',
                  'comedy', 'crime', 'documentary', 'drama', 'venture', 'horror', 'fantasy']
    for i in genre_list:
        genre.append({'genre': i, 'prob': similarity(movieid, i)})
    # Sorting the list of dictionaries by the value of the key 'prob'
    genre = sorted(genre, key=lambda x: x['prob'])
    return [genre[0]['genre'], genre[1]['genre'], genre[2]['genre']]


def out_similar(movieid):
    # print(movieid)
    result = []

    movieid = movieid.strip('').strip('\n')
    similar_list = model.dv.most_similar(movieid, topn=70)
    # print(similar_list)
    for i in similar_list:
        duplicate = 0
        name = i[0]
        # print(name)
        if len(i[0]) == 10:
            #    print('fail')
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
        sql = "select movieid from productname where moviename= '" + name + "'"
        movieidtemp = pd.read_sql_query(sql, con=conn)
        # movieidtemp['movieid'] -> B00004ZEHM -> ["B00004ZEHM"]
        result.append(list(movieidtemp['movieid'])[0])
        # print(result)
    sql = "select movieid from productname where moviename= '" + movieid + "'"
    movieidtemp = list(pd.read_sql_query(sql, con=conn)['movieid'])[0]
    genre = find_genre(movieid)
    sql = "select reviewtext,reviewsummary from moviereview where length(reviewtext) >500 and length(reviewtext) < 700 and productid= '" + \
        movieidtemp + "'"
    temp = pd.read_sql_query(sql, con=conn)
    review = list(temp['reviewtext'])[0]
    summary = list(temp['reviewsummary'])[0]
    with open('Data/movielist', 'r', encoding='utf-8') as f:
        x = f.read().split('\n')
    # print(result)
    output = [movieid]+[movieidtemp]+result[:12]+[genre, review, summary, x]
    return output


result = []
count = 0
with open("Data/movielist", 'r', encoding='utf-8') as i:
    for name in i.readlines():
        count = count + 1
        movie = name.strip("\n")
        result.append(out_similar(movie))
        if count % 100 == 0:
            print(count)
    df = pd.DataFrame(result)
    df.to_csv('Data/result.csv')
