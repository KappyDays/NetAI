# file_uploader.py MinIO Python SDK example
from minio import Minio
from minio.error import S3Error
import json
import os

from dotenv import load_dotenv
load_dotenv(dotenv_path='.my_env')

def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(f"{os.getenv('LOCAL_IP_ADDRESS')}:9002",
        access_key="pRWLQmzIoCE5nUKyac1O",
        secret_key="8FpYdGdHL14opVBipvvGzjScTMNaQSHOjH9WaUZp",
        secure=False
    )

    # The file to upload, change this path if needed
    source_file = "./sample_logs.json"

    # The destination bucket and filename on the MinIO server
    bucket_name = "deltalake-bucket"
    destination_file = "logs_test.json"

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # # Upload the file, renaming it in the process
    # client.fput_object(
    #     bucket_name, destination_file, source_file,
    # )
    # print(
    #     source_file, "successfully uploaded as object",
    #     destination_file, "to bucket", bucket_name,
    # )

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)