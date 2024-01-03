from google.cloud import storage

def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file)

if __name__ == "__main__":
    bucket_name = "group5assignment"
    source_file = "MoviesDataset.csv"
    destination_blob_name = "MoviesDataset/MoviesDataset.csv"

    upload_to_gcs(bucket_name, source_file, destination_blob_name)