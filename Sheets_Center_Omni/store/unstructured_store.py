import paramiko
from minio import Minio
from minio.error import S3Error
import json
import os

with open("my_config.json", 'r') as f:
    configs = json.load(f)
    
# SSH 연결 정보 설정
hostname = configs['hostname']  # 서버의 호스트네임 또는 IP 주소
port = configs['port']                     # SSH 포트 (기본값은 22)
username = configs['username']     # SSH 사용자 이름
password = configs['password']   # SSH 비밀번호

# SSH 클라이언트 설정
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 자동으로 호스트 키를 추가

# SSH 연결
client.connect(hostname, port=port, username=username, password=password)

# kubectl 명령 실행
stdin, stdout, stderr = client.exec_command(configs['kubectl_command'])

# 명령어 출력 받아오기
kubectl_output = stdout.read().decode('utf-8')

# SSH 연결 종료
client.close()

## Minio (Object Storage) 연결
# MinIO 클라이언트 생성
client = Minio(configs['minio_endpoint'],
    access_key=configs['minio_access_key'],
    secret_key=configs['minio_secret_key'],
    secure=False
)

# 문자열을 a.txt 파일로 저장
content = kubectl_output
local_file_path = "a.txt"
# 업로드할 버킷 이름과 파일 이름 설정
bucket_name = "kkr-python-test-bucket"
destination_file = "a.txt"

# 'a.txt' 파일로 저장
with open(local_file_path, "w") as file:
    file.write(content)

# 1. 파일 업로드
try:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created")

    client.fput_object(
        bucket_name, destination_file, local_file_path
    )
    print(f"'{local_file_path}' successfully uploaded to bucket '{bucket_name}'")

except S3Error as e:
    print(f"Error occurred while uploading: {e}")    
print("업로드 완료")


# 2. 로컬 파일 삭제
try:
    if os.path.exists(local_file_path):
        os.remove(local_file_path)
        print(f"Local file '{local_file_path}' has been deleted.")
    else:
        print(f"File '{local_file_path}' does not exist.")
    
except Exception as e:
    print(f"Error occurred while deleting local file: {e}")