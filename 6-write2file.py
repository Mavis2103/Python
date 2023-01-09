import psycopg2

# Connect to the database
conn = psycopg2.connect('dbname=movies user=postgres password=39339')
cur = conn.cursor()

# Execute the query to retrieve all the necessary data in one go
cur.execute("""
    SELECT productid, moviename, reviewtext 
    FROM moviereview mr 
    INNER JOIN productname pn ON mr.productid = pn.movieid
    WHERE mr.productid IN (SELECT productid FROM moviereview GROUP BY productid HAVING COUNT(*) > 100)
""")

# Open the files in write mode
with open('Data/idlist.txt', 'w', encoding='utf-8') as f_idlist:
    with open('Data/moviename.txt', 'w', encoding='utf-8') as f_moviename:
        with open('Data/reviewlist.txt', 'w', encoding='utf-8') as f_reviewlist:
            # Fetch the results of the query in batches
            while True:
                rows = cur.fetchall()
                if not rows:
                    break
                for row in rows:
                    # Write the data to the respective files
                    f_idlist.write(row[0]+'\n')
                    f_moviename.write(row[1]+'\n')
                    f_reviewlist.write(row[2]+'\n')

# Close the cursor and connection
cur.close()
conn.close()
