import io
import time
import re
import signal
from threading import Thread
from queue import Queue
from psycopg2.pool import SimpleConnectionPool
import sys

# Set the number of threads to use for inserting rows
NUM_THREADS = 8

# Set the number of rows to insert in each batch
BATCH_SIZE = 1000

# Set the connection pool parameters
pool = SimpleConnectionPool(
    NUM_THREADS, NUM_THREADS,
    database='movies', user='postgres', password='39339'
)

# Regular expression to extract the relevant information from each line
pattern = re.compile(
    r'(product/productId: )(.*)|(review/helpfulness: )(.*)|(review/score: )(.*)|(review/summary: )(.*)|(review/text: )(.*)')

# Helper function to insert rows in a batch


def insert_rows(rows):
    conn = pool.getconn()
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO moviereview (productid, reviewfelphulness, reviewscore, reviewsummary, reviewtext)
        VALUES (%s, %s, %s, %s, %s)
    """, rows)
    conn.commit()
    cur.close()
    pool.putconn(conn)

# Thread function to insert rows in batches


def insert_thread():
    rows = []
    while True:
        line = q.get()
        if line is None:
            if rows:
                insert_rows(rows)
            break
        rows.append(line)
        if len(rows) >= BATCH_SIZE:
            insert_rows(rows)
            rows = []
    q.task_done()

# Function to handle the KeyboardInterrupt exception


def signal_handler(sig, frame):
    for i in range(NUM_THREADS):
        q.put(None)
    q.join()

    for t in threads:
        t.join()

    pool.closeall()
    sys.exit(0)


# Set the signal handler for the KeyboardInterrupt exception
signal.signal(signal.SIGINT, signal_handler)

# Initialize the queue and threads
q = Queue()
threads = []
for i in range(NUM_THREADS):
    t = Thread(target=insert_thread)
    t.start()
    threads.append(t)

# Read the file and process each line
with open('Data/movies.txt', encoding="latin-1") as in_file:
    temp = []
    for line in in_file:
        # Extract the relevant information from the line
        match = pattern.match(line)
        if match:
            result = [g for g in match.groups() if g]
            if len(result) > 1:
                temp.append(result[1])
                if (len(temp) == 5):
                    productid, reviewfelphulness, reviewscore, reviewsummary, reviewtext = temp
                    reviewsummary = ("''").join(reviewsummary.split("'"))
                    reviewtext = ("''").join(reviewtext.split("'"))
                    # Add the row to the queue for insertion
                    q.put((productid, reviewfelphulness,
                          reviewscore, reviewsummary, reviewtext))
                    temp = []
            else:
                temp.append("")
    else:
        # Signal the end of the file to the threads
        print("stop")
        for i in range(NUM_THREADS):
            q.put(None)

# Wait for all threads to finish
q.join()

for t in threads:
    t.join()

pool.closeall()
