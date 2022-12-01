from nltk import RegexpTokenizer
from nltk.corpus import stopwords
import multiprocessing
from gensim.models.doc2vec import TaggedDocument

tokenizer = RegexpTokenizer('\w+|\$[\d\.]+|\S+')
stopword_set = set(stopwords.words('english'))

#This function does all cleaning of data using two objects above
def nlp_clean(data):
    new_str = data.lower()
    dlist = tokenizer.tokenize(new_str)
    dlist = list(set(dlist).difference(stopword_set))
    return dlist

class LabeledLineSentence(object):
    def __init__(self, filename1,filename2):
        self.filename1 = filename1
        self.filename2 = filename2
    def __iter__(self):
        for uid, (line1,line2) in enumerate(zip(open(self.filename1,'r',encoding='utf-8'),open(self.filename2,'r',encoding='utf-8'))):
            # yield gensim.models.doc2vec.LabeledSentence(nlp_clean(line1),line2.split())
            yield TaggedDocument(nlp_clean(line1),line2.split())

sentences = LabeledLineSentence('./Data/reviewlist','./Data/idlist')
# sentences = TaggedDocument('./Data/reviewlist','./Data/idlist')
import gensim
from gensim.models import Doc2Vec
import os
import logging

logging.basicConfig(level=logging.INFO)
# print(os.sched_getaffinity)
# os.sched_setaffinity(0, range(4))

assert gensim.models.doc2vec.FAST_VERSION > -1

model = Doc2Vec(sentences, vector_size=300, min_count=10, alpha=0.025, min_alpha=0.001, workers=multiprocessing.cpu_count())

model.save("./Data/doc2vec.model")