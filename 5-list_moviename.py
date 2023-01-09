import psycopg2

# Connect to the database
conn = psycopg2.connect('dbname=movies user=postgres password=39339')
cur = conn.cursor()

# Execute the query
cur.execute("SELECT DISTINCT moviename FROM productname")

# Open the file in write mode
with open('Data/movielist.txt', 'w', encoding='utf-8') as f:
    # Fetch the results of the query in batches
    while True:
        rows = cur.fetchall()
        if not rows:
            break
        for row in rows:
            # Write the movie name to the file
            f.write(row[0]+'\n')

# Close the cursor and connection
cur.close()
conn.close()
