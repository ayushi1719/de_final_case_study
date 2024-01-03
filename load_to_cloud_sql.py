from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
def load_data_to_cloud_sql():
    # Initialize Spark session
    spark = SparkSession.builder.appName("MovieDataProcessing").getOrCreate()
 
    # Define schema based on your database schema
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("summary", StringType(), True),
        StructField("short_summary", StringType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("youtube_trailer", StringType(), True),
        StructField("rating", FloatType(), True),
        StructField("movie_poster", StringType(), True),
        StructField("director", StringType(), True),
        StructField("writers", StringType(), True),
        StructField("cast", StringType(), True)
    ])
 
    # Load data from Cloud Storage
    gcs_path = "gs://group5assignment/MoviesDataset/MoviesDataset.csv"
    df = spark.read.format("csv").schema(schema).option("header", "true").load(gcs_path)
 
    # Configure database connection
    database_url = "jdbc:postgresql://35.184.55.219:5432/group5"
    properties = {
        "user": "group5",
        "password": "Tata",
        "driver": "org.postgresql.Driver"
    }
 
    # Write data to Cloud SQL
    df.write.jdbc(url=database_url, table="movies", mode="overwrite", properties=properties)
 
    # Stop Spark session
    spark.stop()