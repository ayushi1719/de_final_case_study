from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, countDistinct

def perform_data_transformations():
    spark = SparkSession.builder.appName("DataTransformations").getOrCreate()

    # Load data from Cloud SQL
    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://35.184.55.219:5432/group5") \
        .option("dbtable", "movies").option("user", "group5").option("password", "Tata").load()

    # Data transformations
    # i. Which movies were released in the year 2020?
    df_2020 = df.filter(col("year") == 2020)

    # ii. What is the average IMDb rating of the movies in the dataset?
    avg_imdb_rating = df.agg(avg(col("rating"))).collect()[0][0]

    # iii. Which movies have the longest and shortest runtimes?
    longest_runtime = df.filter(col("runtime") == df.agg(max(col("runtime"))).collect()[0][0])
    shortest_runtime = df.filter(col("runtime") == df.agg(min(col("runtime"))).collect()[0][0])

    # iv. How many movies were directed by each director?
    movies_per_director = df.groupBy("director").agg(countDistinct("title").alias("movies_count"))

    # v. Who are the unique writers in the dataset?
    unique_writers = df.select("writers").distinct()

    # vi. Which movies have an IMDb rating greater than 8.0?
    high_rated_movies = df.filter(col("rating") > 8.0)

    # vii. Which movies do not have a YouTube trailer code?
    movies_without_trailer = df.filter(col("youtube_trailer").isNull())

    # viii. How many movies does each cast member appear in?
    movies_per_cast_member = df.groupBy("cast").agg(countDistinct("title").alias("movies_count"))

if __name__ == "__main__":
    perform_data_transformations()