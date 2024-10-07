from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='.my_env')

def test_minio_connection():
    try:
        # MinIO 클라이언트 초기화
        client = Minio(
            f"{os.getenv('LOCAL_IP_ADDRESS')}:9002",
            access_key="pRWLQmzIoCE5nUKyac1O",
            secret_key="8FpYdGdHL14opVBipvvGzjScTMNaQSHOjH9WaUZp",
            secure=False  # http 사용시 False
        )
        
        # 버킷 리스트 가져오기
        buckets = client.list_buckets()
        print("버킷 목록:")
        for bucket in buckets:
            print(f" - {bucket.name}")
            
        # 특정 버킷의 객체 리스트 가져오기
        objects = client.list_objects('python-test-bucket')
        print("\n'python-test-bucket' 버킷의 객체:")
        for obj in objects:
            print(f" - {obj.object_name}")
            
    except S3Error as e:
        print(f"에러 발생: {e}")

if __name__ == "__main__":
    test_minio_connection()