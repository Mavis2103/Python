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


def out_similar(doc):
    result = []

    similar_list = model_dsc.dv.most_similar(
        positive=[model_dsc.infer_vector(nlp_clean(doc))])
    # print(similar_list)
    for i in similar_list:
        mopvieid = i[0]
        sql = "select * from movieid where movieid= '" + mopvieid + "'"
        movieidtemp = pd.read_sql_query(sql, con=conn)
        print("--------------------------------", mopvieid)
        if len(list(movieidtemp['moviename'])) > 0:
            result.append({
                "name": list(movieidtemp['moviename'])[0],
                "id": list(movieidtemp['movieid'])[0],
                "image": list(movieidtemp['movieimg'])[0],
            })
    return result
