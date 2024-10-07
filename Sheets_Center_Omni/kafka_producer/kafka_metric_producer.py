import paramiko
import json
import time
from confluent_kafka import Producer, KafkaError

with open("my_config.json", 'r') as f:
    configs = json.load(f)

# 명령어 실행할 서버 접속 정보
hostname = configs['hostname']  # 서버의 호스트네임 또는 IP 주소
port = configs['port']                     # SSH 포트 (기본값은 22)
username = configs['username']     # SSH 사용자 이름
password = configs['password']   # SSH 비밀번호
monitoring_endpoint = configs['endpoint']

# kafka config
kafka_conf = {'bootstrap.servers': configs['kafka_server:port']}
producer = Producer(kafka_conf)
        
        
# 실행할 명령어
metric_names = ['DCGM_FI_DEV_FB_USED', # GPU 메모리 사용량
                'DCGM_FI_DEV_FB_FREE', # GPU 메모리 남은 양
                'DCGM_FI_DEV_GPU_TEMP', # GPU 온도
                'DCGM_FI_DEV_GPU_UTIL', # GPU 사용률
                'DCGM_FI_DEV_POWER_USAGE' # GPU 전력 사용량
                ]

# SSH 클라이언트 객체 생성
client = paramiko.SSHClient()

# 서버의 호스트 키를 자동으로 추가하도록 설정
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    # SSH 서버에 접속
    client.connect(hostname, port=port, username=username, password=password)

    while True:
        start_time = time.time()  # Record the start time
        
        # Loop through each metric and execute the command
        for metric in metric_names:
            # Update the command with the current metric
            command = 'curl -g "{0}/api/v1/query?query={1}{{container=\\"session\\"}}" | jq'.format(monitoring_endpoint, metric)
            
            # 명령어 실행
            stdin, stdout, stderr = client.exec_command(command)
            
            # 명령어 출력 읽기
            output = stdout.read().decode('utf-8')
            errors = stderr.read().decode('utf-8')

            # json 형식으로 출력 결과 확인
            try:
                json_data = json.loads(output)
                # print("Command output as JSON:", json.dumps(json_data, indent=4))
            except json.JSONDecodeError as e:
                pass
                # print("Error decoding JSON:", e)
                    
            # 명령어 출력 결과 확인
            # print(json_data['data']['result'][0]['value'][1])
            try:
                producer.produce('k_test_metric_ctest', key=metric, value=json_data['data']['result'][0]['value'][1])
                producer.flush()
                print("Producer connected and message sent successfully.")
            except KafkaError as e:
                print(f"Failed to connect Producer: {e}")
            # break
            # print(f"Command output for {metric}: {output}")
            if errors:
                pass
                # print(f"Errors for {metric}: {errors}")

        # Measure how long the loop took
        elapsed_time = time.time() - start_time

        # Calculate how much time to sleep to make the total duration 60 seconds
        sleep_time = max(0, 60 - elapsed_time)
        time.sleep(sleep_time)        
            
finally:
    # SSH 연결 종료
    client.close()