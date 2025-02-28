import s3fs
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_KEY

def connect_to_s3():
    try: 
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=AWS_ACCESS_KEY_ID,   
            secret=AWS_SECRET_KEY   
        )
        return s3  
    except Exception as e:
        print(e)  

    

def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket: str):
    try:
        if not s3.exists(bucket):  # Check if bucket exists
            s3.mkdir(bucket)       # Create the bucket
            print("Bucket Created")
        else:
            print("Bucket already exists")
    except Exception as e:
        print(f"Error: {e}")  

import s3fs

def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, s3_file_name: str):
    try:
        s3.put(file_path, bucket + '/raw/' + s3_file_name)  
        print('File is uploaded to S3')  
    except FileNotFoundError:
        print('No file found in the specified file path')  
    except Exception as e:
        print(f"Error: {e}")  

