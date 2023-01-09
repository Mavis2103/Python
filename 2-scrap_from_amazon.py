import polars as pl
from amazon_paapi import AmazonApi
import pickle

conn = "postgres://postgres:39339@127.0.0.1:5432/movies"
sql = 'select productid from moviereview group by productid having count(*) > 100'
movieid_list = pl.read_sql(sql, conn)
movieid_list = list(movieid_list['productid'])
len_movieid_list = len(movieid_list)
amazon = AmazonApi("KEY",
                   "SECRET_KEY", "PARTNET", "US", throttling=0)
movie = amazon.get_items(movieid_list)

with open("Data/movie.pickle", "wb") as f:
    pickle.dump(movie, f)
    print("done")
