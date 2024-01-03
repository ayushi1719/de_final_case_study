import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from google.cloud import bigtable

def load_to_bigtable():
    print("Connecting to PostgreSQL...")
    username = "group5"
    password = "Tata"
    host = "35.184.55.219"
    port = "5432"
    database_name = "group5"

    connection_str = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"

    engine = create_engine(connection_str)
    
  # Create a cursor
    conn = engine.connect()

    # Create a Bigtable client and table
    print("Connecting to Bigtable...")
    client = bigtable.Client(project='manipalpr-1677473929525', admin=True)
    instance = client.instance('group5')
    table = instance.table('group5')

    # Execute PostgreSQL query to fetch movie data
    print("Executing PostgreSQL query...")
    result = conn.execute("""
        SELECT Title, Year, Summary, "Short Summary", "IMDB ID", Runtime, "YouTube Trailer", Rating, "Movie Poster", Director, Writers, Casting
        FROM movies
        LIMIT 100
    """)

    # Fetch data from PostgreSQL
    rows = result.fetchall()
    print(f"Fetched {len(rows)} rows from PostgreSQL.")

    # Ingest data into Bigtable
    print("Ingesting data into Bigtable...")
    with table.batch() as batch:
        for row in rows:
            key = row[4]  # Assuming "IMDB ID" is the fifth column and it serves as the key
            columns_data = {
                b'movie_attributes:Title': str(row[0]).encode(),
                b'movie_attributes:Year': str(row[1]).encode(),
                b'movie_attributes:Summary': str(row[2]).encode(),
                b'movie_attributes:Short_Summary': str(row[3]).encode(),
                b'movie_attributes:IMDB_ID': str(row[4]).encode(),
                b'movie_attributes:Runtime': str(row[5]).encode(),
                b'movie_attributes:YouTube_Trailer': str(row[6]).encode(),
                b'movie_attributes:Rating': str(row[7]).encode(),
                b'movie_attributes:Movie_Poster': str(row[8]).encode(),
                b'movie_attributes:Director': str(row[9]).encode(),
                b'movie_attributes:Writers': str(row[10]).encode(),
                b'movie_attributes:Cast': str(row[11]).encode()
                # Adjust column family and qualifier names as per your Bigtable schema
            }
            batch.put(key.encode(), columns_data)

    print("Data ingested into Bigtable.")

    # Close the connection
    conn.close()

# Call the function
load_to_bigtable()