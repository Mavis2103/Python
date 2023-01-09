from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from gensim.models import Doc2Vec
import pandas as pd
import psycopg2

# Define the tokenizer and stopwords set at the top of the code
tokenizer = RegexpTokenizer('\w+|\$[\d\.]+|\S+')
stopword_set = set(stopwords.words('english'))


def nlp_clean(data):
    new_str = data.lower()
    dlist = tokenizer.tokenize(new_str)
    dlist = list(set(dlist).difference(stopword_set))
    return dlist


doc = "The Empire Strikes Back is the best film in the original Star Wars trilogy. It has all the great qualities that the original Star Wars has: great effects (at the time of its release). al ppealing characters, and lots of spellbinding action. It also has eliminated some of the problems that plagued the first: the storyline is tighter, and goes much deeper into character development The performances are I terrific, especially by Harrison Ford as Han Solo, and Billy Dee Williams as Lando Calrissian"

model_dsc = Doc2Vec.load('Data/doc2vec.model')


def out_similar(doc):
    result = []

    # Infer the vector for the input document once and store it in a variable
    doc_vector = model_dsc.infer_vector(nlp_clean(doc))

    similar_list = model_dsc.dv.most_similar(positive=[doc_vector])

    # Use a list comprehension to create the result list
    for i in similar_list:
        mopvieid = i[0]
        # Use string formatting and pass the movieid as a parameter
        # to prevent SQL injection attacks
        sql = "SELECT * FROM movieid WHERE movieid = %s"
        with psycopg2.connect('dbname=movies user=postgres password=39339') as conn:
            movieidtemp = pd.read_sql_query(sql, con=conn, params=(mopvieid,))
            if len(movieidtemp) > 0:
                result.append({
                    "name": movieidtemp['moviename'].tolist()[0],
                    "id": movieidtemp['movieid'].tolist()[0],
                    "image": movieidtemp['movieimg'].tolist()[0],
                })
    return result
