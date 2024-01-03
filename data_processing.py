from google.cloud import dataproc_v1 as dataproc

# Replace with your Google Cloud project ID, region, cluster name, bucket name, and processing script path
project_id = 'your-project-id'
region = 'your-region'
cluster_name = 'your-dataproc-cluster-name'
bucket_name = 'your-bucket-name'
processing_script_path = 'gs://your-bucket-name/path/to/processing_script.py'

client = dataproc.JobControllerClient()

job = {
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {
        "main_python_file_uri": processing_script_path
    },
}

operation = client.submit_job_as_operation(
    request={"project_id": project_id, "region": region, "job": job}
)
response = operation.result()
print(f"Job submitted: {response.reference.job_id}")