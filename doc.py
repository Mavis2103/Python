from nltk import RegexpTokenizer
from nltk.corpus import stopwords
import multiprocessing
from gensim.models.doc2vec import TaggedDocument
from gensim.models import Doc2Vec
import pandas as pd
from scipy.spatial import distance
from numpy import linalg as LA
import numpy as np
import psycopg2
import sys
conn = psycopg2.connect('dbname=movies user=postgres password=39339')

tokenizer = RegexpTokenizer('\w+|\$[\d\.]+|\S+')
stopword_set = set(stopwords.words('english'))


def nlp_clean(data):
    new_str = data.lower()
    dlist = tokenizer.tokenize(new_str)
    dlist = list(set(dlist).difference(stopword_set))
    return dlist


doc = "The Empire Strikes Back is the best film in the original Star Wars trilogy. It has all the great qualities that the original Star Wars has: great effects (at the time of its release). al ppealing characters, and lots of spellbinding action. It also has eliminated some of the problems that plagued the first: the storyline is tighter, and goes much deeper into character development The performances are I terrific, especially by Harrison Ford as Han Solo, and Billy Dee Williams as Lando Calrissian. George"

model_dsc = Doc2Vec.load('Data/doc2vec.model')


def check(char):
    if ((ord(char) >= 97 and ord(char) <= 122) or (ord(char) >= 65 and ord(char) <= 90)) or (ord(char) >= 48 and ord(char) <= 57):
        return True
    else:
        return False


def similarity(movieid, genre):
    # return distance.euclidean(model.wv[genre], model.dv[movieid])/LA.norm(model.wv[genre])/LA.norm(model.dv[movieid])
    return distance.euclidean(model_dsc.wv[genre], model_dsc.dv[movieid])/LA.norm(model_dsc.wv[genre])/LA.norm(model_dsc.dv[movieid])


def find_genre(movieid):
    genre = []
    genre_list = ['action', 'romance', 'epic', 'political', 'fiction',
                  'comedy', 'crime', 'documentary', 'drama', 'venture', 'horror', 'fantasy']
    for i in genre_list:
        genre.append({'genre': i, 'prob': similarity(movieid, i)})
    # Sorting the list of dictionaries by the value of the key 'prob'
    genre = sorted(genre, key=lambda x: x['prob'])
    return [genre[0]['genre'], genre[1]['genre'], genre[2]['genre']]


def out_similar(doc):
    # print(movieid)
    result = []

    similar_list = model_dsc.dv.most_similar(
        positive=[model_dsc.infer_vector(nlp_clean(doc))])
    # print(similar_list)
    for i in similar_list:
        mopvieid = i[0]
        sql = "select * from movieid where movieid= '" + mopvieid + "'"
        movieidtemp = pd.read_sql_query(sql, con=conn)
        # movieidtemp['movieid'] -> B00004ZEHM -> ["B00004ZEHM"]
        print("--------------------------------", mopvieid)
        if len(list(movieidtemp['moviename'])) > 0:
            result.append({
                "name": list(movieidtemp['moviename'])[0],
                "id": list(movieidtemp['movieid'])[0],
                "image": list(movieidtemp['movieimg'])[0],
            })
    return result
