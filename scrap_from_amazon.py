import psycopg2
import pandas as pd
import numpy as np
# from amazon.api import AmazonAPI
import requests
import time
import os
from bs4 import BeautifulSoup
import requests
import random
# 1115242

agents = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56',
  'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.3',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
]

def crawl_movie(id):
    # amazon = AmazonAPI("AKIAILGCGSOAJBIYFIQA",
    #     "xz53Hn0stSpZ2LpqRejomhnZflsqNt/Yhq58VOAK", "ericpan2018-20")
    # movie = amazon.lookup(ItemId = id)
    # with open(os.path.join('Data/poster/'+id+'.jpg'),'wb') as f:
    #     f.write(requests.get(movie.large_image_url).content)
    # print(movie.title)
    # return movie.title
    # The webpage URL
    URL = f'https://www.amazon.com/dp/{id}/'
    # Headers for request
    HEADERS = ({'User-Agent':
                random.choice(agents),
                'Accept-Language': 'en-US, en;q=0.5'})
    # HEADERS = ({'User-Agent':
    # 				'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56'})
    # HTTP Request
    webpage = requests.get(URL, headers=HEADERS)
    # Soup Object containing all data
    soup = BeautifulSoup(webpage.content, "lxml")
    # Film infos
    landing_poster = soup.find('img', id='landingImage')
    if (landing_poster):
        title = soup.find('span', id='productTitle').text.strip()
        poster = landing_poster.get('src')
    else:
        title = soup.find('h1', class_='_2IIDsE').text
        poster = soup.find('img', class_="Zk8aEm").get('src')
    movie_info = {'title': title, 'poster': poster}
    print(movie_info["title"]+"','"+movie_info["poster"])
    return movie_info["title"]+"','"+movie_info["poster"]


conn = psycopg2.connect('dbname=movies user=postgres password=39339')
# Get all product have more than 175 post review
sql = 'select productid from moviereview group by productid having count(*) >175'
movieid_list = pd.read_sql_query(sql, con=conn)
conn.close()
movieid_list = list(movieid_list['productid'])
# Length of movieId_list
len_movieid_list = len(movieid_list)

for id in range(0,len_movieid_list):
    if(id&100):
      print(id)
    try:
        conn = psycopg2.connect('dbname=movies user=postgres password=39339')
        cur = conn.cursor()
        sql = "insert into movieid2 (movieid, moviename, movieimg) values ('"
        sql = sql+movieid_list[id]+"','"+crawl_movie(movieid_list[id])+"')"
        cur.execute(sql)
        time.sleep(1)
        conn.commit()
        cur.close()
        conn.close()
    except:
        continue
