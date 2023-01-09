import psycopg2
import pickle

# Connect to the database
conn = psycopg2.connect('dbname=movies user=postgres password=39339')
cur = conn.cursor()

# Load the "movie" object from the file using UTF-8 encoding
with open("Data/movie.pickle", "rb") as f:
    movies = pickle.load(f)

# Create a list of movies to insert
movie_list = [(movie.asin, movie.item_info.title.display_value,
               movie.images.primary.large.url) for movie in movies]

# Insert the movies in a batch
cur.executemany(
    """INSERT INTO movieid (movieid, moviename, movieimg) VALUES (%s, %s, %s)""", movie_list)
conn.commit()

# Close the connection
cur.close()
conn.close()
