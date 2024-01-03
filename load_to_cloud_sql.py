from pyspark.sql import SparkSession

def load_to_cloud_sql():
    spark = SparkSession.builder.appName("LoadToCloudSQL").getOrCreate()

    df = spark.read.csv("gs://your-cloud-storage-bucket/MoviesDataset/MoviesDataset.csv", header=True)

    # Convert Spark DataFrame to Pandas DataFrame
    df_pandas = df.toPandas()

    # Save Pandas DataFrame to Cloud SQL
    df_pandas.to_sql('movies', 'postgresql://your-username:your-password@your-cloud-sql-ip:5432/your-database-name', if_exists='replace', index=False)

if __name__ == "__main__":
    load_to_cloud_sql()