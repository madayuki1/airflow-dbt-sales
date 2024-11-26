from minio import Minio
from minio.error import S3Error
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


def upload_to_minio(file_path, bucket_name, object_name, minio_url='minio:9000', access_key='minioadmin', secret_key='minioadmin', timestamp=False):
    """
    Uploads a file to MinIO using the MinIO SDK.

    :param file_path: The path to the file you want to upload.
    :param bucket_name: The name of the bucket where the file will be uploaded.
    :param object_name: The name of the object in MinIO (i.e., file name).
    :param minio_url: The URL of the MinIO server (default is 'localhost:9000').
    :param access_key: The access key for MinIO (default is 'minioadmin').
    :param secret_key: The secret key for MinIO (default is 'minioadmin').
    """
    if timestamp == True:
         # Get current datetime
        current_time = datetime.now()
        
        # Format as yymmdd_hhmmss
        formatted_time = current_time.strftime("%y%m%d_%H%M%S")
        
        # Generate the final filename
        object_name = f"{object_name}_{formatted_time}"

    # Initialize MinIO client
    client = Minio(
        minio_url,
        access_key=access_key,
        secret_key=secret_key,
        secure=False  # Set this to True if using https
    )

    try:
        # Create the bucket if it doesn't exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # Upload the file
        client.fput_object(bucket_name, object_name, file_path)
        print(f"File '{file_path}' uploaded as '{object_name}' to bucket '{bucket_name}'.")

    except S3Error as e:
        print(f"Error occurred: {e}")


def upload_dataframe_to_minio_parquet(df, bucket_name, object_name, minio_url='minio:9000', access_key='minioadmin', secret_key='minioadmin', timestamp=False):
    """
    Uploads a Pandas DataFrame to MinIO as a CSV.

    :param df: The Pandas DataFrame to upload.
    :param bucket_name: The name of the bucket where the file will be uploaded.
    :param object_name: The name of the object in MinIO (i.e., file name).
    :param minio_url: The URL of the MinIO server (default is 'localhost:9000').
    :param access_key: The access key for MinIO (default is 'minioadmin').
    :param secret_key: The secret key for MinIO (default is 'minioadmin').
    """

    if timestamp == True:
         # Get current datetime
        current_time = datetime.now()
        
        # Format as yymmdd_hhmmss
        formatted_time = current_time.strftime("%y%m%d_%H%M%S")
        
        # Generate the final filename
        object_name = f"{object_name}_{formatted_time}"


    # Ensure the object name has the correct file extension
    if not object_name.endswith('.parquet'):
        object_name += '.parquet'

    # Convert DataFrame to CSV format (in-memory)
    parquet_buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_buffer, compression='snappy')
    parquet_buffer.seek(0)

    # Initialize MinIO client
    client = Minio(
        minio_url,
        access_key=access_key,
        secret_key=secret_key,
        secure=False  # Set this to True if using https
    )

    try:
        # Create the bucket if it doesn't exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # Upload the CSV data as an object to MinIO
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/parquet"
        )
        print(f"DataFrame uploaded as '{object_name}' to bucket '{bucket_name}'.")

    except S3Error as e:
        print(f"Error occurred: {e}")
