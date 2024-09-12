# Python 베이스 이미지 사용
FROM python:3.12-slim

# 작업 디렉토리 설정
WORKDIR /app

# 로컬의 Python 스크립트와 요구사항 파일 복사
COPY requirements.txt requirements.txt
COPY kafka_metric_producer.py .

# 필요 라이브러리 설치
RUN pip install --no-cache-dir -r requirements.txt

# Python 스크립트 실행
CMD ["python", "kafka_metric_producer.py"]